use crate::config::QueueMode;
use crate::web_base::AppState;
use tracing::{info, warn};
use uuid::Uuid;

/// Attempt to dispatch eligible queue items to a specific worker.
///
/// Scans the queue in FIFO order looking for items that:
/// 1. The worker satisfies capability-wise.
/// 2. The worker has not already been delivered to for this item.
/// 3. The item is not yet delivery-saturated.
/// 4. The worker has a free concurrency slot for the item's queue.
///
/// Dispatches as many items as the worker has available slots.
pub async fn try_dispatch(state: &AppState, worker_id: &str) {
  loop {
    let dispatched = try_dispatch_one(state, worker_id).await;
    if !dispatched {
      break;
    }
  }
}

/// Try to dispatch one item to a specific worker.  Returns true if an item
/// was dispatched, false if no eligible item was found.
async fn try_dispatch_one(state: &AppState, worker_id: &str) -> bool {
  let mut queue = state.queue.lock().await;
  let mut registry = state.registry.lock().await;

  let worker = match registry.get(worker_id) {
    Some(w) => w,
    None => return false,
  };

  // Find the first eligible item.
  let eligible_item_id = queue.iter().find_map(|(item_id, entry)| {
    // Worker must satisfy item requirements.
    if !garage_queue_lib::matching::satisfies(
      &entry.item.requirements,
      &worker.capabilities,
    ) {
      return None;
    }

    // Worker must not already have this item.
    if entry.delivered.contains(worker_id) {
      return None;
    }

    // Item must not be delivery-saturated.
    if is_delivery_saturated(entry, &registry) {
      return None;
    }

    // Worker must have a free slot for this queue.
    if !worker.is_ready_for(&entry.item.queue) {
      return None;
    }

    Some(*item_id)
  });

  let item_id = match eligible_item_id {
    Some(id) => id,
    None => return false,
  };

  // We found an eligible item — dispatch it.
  let entry = queue.get_mut(&item_id).unwrap();
  let item = entry.item.clone();
  entry.delivered.insert(worker_id.to_string());

  let tx = worker.tx.clone();
  let worker = registry.get_mut(worker_id).unwrap();
  worker.increment_in_flight(&item.queue);

  drop(registry);
  drop(queue);

  if tx.send(item.clone()).await.is_err() {
    warn!(
      worker_id = %worker_id,
      item_id = %item_id,
      "Worker channel closed during dispatch, rolling back"
    );
    // Roll back: remove from delivered, decrement in-flight.
    let mut queue = state.queue.lock().await;
    if let Some(entry) = queue.get_mut(&item_id) {
      entry.delivered.remove(worker_id);
    }
    let mut registry = state.registry.lock().await;
    if let Some(worker) = registry.get_mut(worker_id) {
      worker.decrement_in_flight(&item.queue);
    }
    return false;
  }

  info!(
    item_id = %item_id,
    worker_id = %worker_id,
    queue = %item.queue,
    "Dispatched item to worker"
  );
  true
}

/// Attempt to dispatch items to all ready workers.  Called when a new item
/// is enqueued.
pub async fn try_dispatch_all(state: &AppState) {
  let worker_ids = {
    let registry = state.registry.lock().await;
    registry.worker_ids()
  };

  for wid in worker_ids {
    try_dispatch(state, &wid).await;
  }
}

/// Handle a worker's result submission.  Records the response, checks
/// completion, and re-dispatches if the worker has free slots.
pub async fn handle_result(
  state: &AppState,
  worker_id: &str,
  item_id: Uuid,
  response: serde_json::Value,
) -> bool {
  let queue_name;
  let should_complete;

  {
    let mut queue = state.queue.lock().await;
    let entry = match queue.get_mut(&item_id) {
      Some(e) => e,
      None => return false,
    };

    queue_name = entry.item.queue.clone();
    entry
      .completed
      .insert(worker_id.to_string(), response.clone());

    should_complete = is_completion_met(entry);
  }

  // Decrement in-flight for this worker.
  {
    let mut registry = state.registry.lock().await;
    if let Some(worker) = registry.get_mut(worker_id) {
      worker.decrement_in_flight(&queue_name);
    }
  }

  if should_complete {
    complete_item(state, item_id).await;
  }

  info!(
    item_id = %item_id,
    worker_id = %worker_id,
    "Result recorded"
  );

  // Worker may now be ready for more work.
  try_dispatch(state, worker_id).await;

  true
}

/// Complete an item: run combiner (broadcast) or forward response (exclusive),
/// then remove from queue.
async fn complete_item(state: &AppState, item_id: Uuid) {
  let entry = {
    let mut queue = state.queue.lock().await;
    queue.shift_remove(&item_id)
  };

  let Some(entry) = entry else { return };

  let result = match entry.mode {
    QueueMode::Exclusive => {
      // Forward the single response.
      entry.completed.into_values().next().unwrap_or_default()
    }
    QueueMode::Broadcast => {
      // Run the combiner.
      let responses: Vec<(String, serde_json::Value)> =
        entry.completed.into_iter().collect();
      let live = state.live.read().await;
      match live.compiled_queues.get(&entry.item.queue) {
        Some(compiled) => match compiled.combine(&entry.item.queue, &responses)
        {
          Ok(combined) => combined,
          Err(e) => {
            warn!(
              item_id = %item_id,
              error = %e,
              "Combiner failed for broadcast item"
            );
            serde_json::json!({ "error": e.to_string() })
          }
        },
        None => {
          warn!(
            queue = %entry.item.queue,
            item_id = %item_id,
            "Compiled queue missing for broadcast combiner"
          );
          return;
        }
      }
    }
  };

  if let Some(tx) = entry.producer_tx {
    if tx.send(result).is_err() {
      warn!(
        item_id = %item_id,
        "Producer dropped before result delivered"
      );
    }
  }
}

/// Handle a worker disconnecting.  Removes the worker from `delivered` on
/// all queue entries.  Un-saturated broadcast items become eligible again.
/// Broadcast items where all delivered workers have completed are finished.
pub async fn handle_disconnect(state: &AppState, worker_id: &str) {
  let items_to_complete: Vec<Uuid>;
  {
    let mut queue = state.queue.lock().await;
    let mut to_complete = Vec::new();
    for (item_id, entry) in queue.iter_mut() {
      entry.delivered.remove(worker_id);
      // Check if this broadcast item is now complete: all delivered workers
      // have responded.
      if entry.mode == QueueMode::Broadcast && is_completion_met(entry) {
        to_complete.push(*item_id);
      }
    }
    items_to_complete = to_complete;
  }

  for item_id in items_to_complete {
    complete_item(state, item_id).await;
  }

  // After removing the worker from delivered sets, some items may have become
  // un-saturated.  Try dispatching to remaining workers.
  try_dispatch_all(state).await;
}

/// Check if an item has been delivered to enough workers.
fn is_delivery_saturated(
  entry: &crate::web_base::QueueEntry,
  registry: &crate::registry::WorkerRegistry,
) -> bool {
  match entry.mode {
    QueueMode::Exclusive => !entry.delivered.is_empty(),
    QueueMode::Broadcast => {
      // Saturated when all currently-connected matching workers have been
      // delivered to.
      let matching = registry.matching_worker_ids(&entry.item.requirements);
      matching.iter().all(|wid| entry.delivered.contains(wid))
    }
  }
}

/// Check if the completion condition is met for an item.
fn is_completion_met(entry: &crate::web_base::QueueEntry) -> bool {
  match entry.mode {
    QueueMode::Exclusive => !entry.completed.is_empty(),
    QueueMode::Broadcast => {
      // Complete when all delivered workers have responded.
      !entry.delivered.is_empty()
        && entry
          .delivered
          .iter()
          .all(|wid| entry.completed.contains_key(wid))
    }
  }
}
