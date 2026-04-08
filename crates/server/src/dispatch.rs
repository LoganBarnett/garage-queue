use crate::state::{AppState, BroadcastPending};
use garage_queue_lib::protocol::QueueItem;
use std::collections::HashSet;
use tokio::sync::oneshot;
use tracing::{info, warn};

/// Result of attempting to dispatch an item.
pub enum DispatchOutcome {
  /// The item was sent directly to a worker (or workers).
  Dispatched,
  /// No matching worker was available; the item remains in the pending queue.
  Queued,
}

/// Attempt to dispatch an exclusive item to the first idle matching worker.
///
/// If a worker is available, the item is sent via its channel and the worker
/// is marked busy.  Otherwise the item is placed in the pending queue and
/// the producer's oneshot is registered for later completion.
pub async fn dispatch_exclusive(
  state: &AppState,
  item: QueueItem,
  producer_tx: oneshot::Sender<serde_json::Value>,
) -> DispatchOutcome {
  let mut registry = state.registry.lock().await;
  let workers = registry.idle_workers_matching(&item.requirements);

  if let Some(worker) = workers.into_iter().next() {
    let worker_id = worker.worker_id.clone();
    let tx = worker.tx.clone();
    registry.mark_busy(&worker_id);
    drop(registry);

    // Register the oneshot before sending, so the result handler finds it.
    state
      .pending_responses
      .lock()
      .await
      .insert(item.id, producer_tx);

    if tx.send(item.clone()).await.is_err() {
      warn!(
        worker_id = %worker_id,
        item_id = %item.id,
        "Worker channel closed during dispatch, re-queuing"
      );
      // Worker disconnected between lookup and send — re-queue.
      let mut pending = state.pending.lock().await;
      pending.push_back(item);
      return DispatchOutcome::Queued;
    }

    info!(
      item_id = %item.id,
      worker_id = %worker_id,
      "Dispatched item to worker (exclusive)"
    );
    DispatchOutcome::Dispatched
  } else {
    drop(registry);
    // No worker available — queue the item for later.
    state
      .pending_responses
      .lock()
      .await
      .insert(item.id, producer_tx);
    state.pending.lock().await.push_back(item);
    DispatchOutcome::Queued
  }
}

/// Attempt to dispatch a broadcast item to all matching workers.
///
/// If matching workers exist, the item is cloned to each worker's channel
/// and a BroadcastPending entry tracks the expected respondent set.  If no
/// workers are available, the item is placed in the pending queue.
pub async fn dispatch_broadcast(
  state: &AppState,
  item: QueueItem,
  producer_tx: oneshot::Sender<serde_json::Value>,
) -> DispatchOutcome {
  let registry = state.registry.lock().await;
  let workers = registry.all_workers_matching(&item.requirements);

  if workers.is_empty() {
    drop(registry);
    state.pending.lock().await.push_back(item.clone());
    state
      .pending_broadcast_tx
      .lock()
      .await
      .insert(item.id, producer_tx);
    return DispatchOutcome::Queued;
  }

  let remaining: HashSet<String> =
    workers.iter().map(|w| w.worker_id.clone()).collect();
  let channels: Vec<_> = workers
    .iter()
    .map(|w| (w.worker_id.clone(), w.tx.clone()))
    .collect();
  drop(registry);

  let pending_entry = BroadcastPending {
    remaining,
    responses: Vec::new(),
    queue: item.queue.clone(),
    tx: producer_tx,
  };

  state
    .broadcast_pending
    .lock()
    .await
    .insert(item.id, pending_entry);

  for (worker_id, tx) in channels {
    if tx.send(item.clone()).await.is_err() {
      warn!(
        worker_id = %worker_id,
        item_id = %item.id,
        "Worker channel closed during broadcast dispatch"
      );
      // Remove this worker from the expected respondent set.
      let mut bp = state.broadcast_pending.lock().await;
      if let Some(entry) = bp.get_mut(&item.id) {
        entry.remaining.remove(&worker_id);
      }
    }
  }

  // Check if all workers failed immediately.
  let should_complete = {
    let bp = state.broadcast_pending.lock().await;
    bp.get(&item.id)
      .map(|e| e.remaining.is_empty())
      .unwrap_or(false)
  };

  if should_complete {
    complete_broadcast(state, item.id).await;
  }

  info!(
    item_id = %item.id,
    "Dispatched item to workers (broadcast)"
  );
  DispatchOutcome::Dispatched
}

/// Called when a worker becomes idle or first connects.  Attempts to
/// dispatch the first matching pending item to the worker.
pub async fn try_dispatch_pending(state: &AppState, worker_id: &str) {
  let mut pending = state.pending.lock().await;
  let registry = state.registry.lock().await;

  let worker = match registry
    .idle_workers_matching(&[])
    .into_iter()
    .find(|w| w.worker_id == worker_id)
  {
    Some(w) => w,
    None => return,
  };

  let position = pending.iter().position(|item| {
    garage_queue_lib::matching::satisfies(
      &item.requirements,
      &worker.capabilities,
    )
  });

  let Some(idx) = position else { return };
  let item = pending.remove(idx).expect("position just confirmed");
  let tx = worker.tx.clone();
  let wid = worker.worker_id.clone();
  drop(registry);
  drop(pending);

  // Determine if this pending item is exclusive or broadcast by checking
  // which oneshot map contains it.
  let is_broadcast = state
    .pending_broadcast_tx
    .lock()
    .await
    .contains_key(&item.id);

  if is_broadcast {
    // For a pending broadcast item, we need to re-dispatch it fully now
    // that a worker is available.
    let producer_tx = state.pending_broadcast_tx.lock().await.remove(&item.id);
    if let Some(ptx) = producer_tx {
      // Re-dispatch as broadcast (now workers exist).
      dispatch_broadcast(state, item, ptx).await;
    }
  } else {
    // Exclusive: send directly to this worker.
    let mut reg = state.registry.lock().await;
    reg.mark_busy(&wid);
    drop(reg);

    if tx.send(item.clone()).await.is_err() {
      warn!(
        worker_id = %wid,
        item_id = %item.id,
        "Worker channel closed during pending dispatch"
      );
      state.pending.lock().await.push_back(item);
    } else {
      info!(
        item_id = %item.id,
        worker_id = %wid,
        "Dispatched pending item to worker (exclusive)"
      );
    }
  }
}

/// Complete a broadcast when all responses have been collected (or all
/// remaining workers have disconnected).  Runs the combiner and sends
/// the result to the waiting producer.
pub async fn complete_broadcast(state: &AppState, item_id: uuid::Uuid) {
  let entry = state.broadcast_pending.lock().await.remove(&item_id);
  let Some(entry) = entry else { return };

  let live = state.live.read().await;
  let result = match live.compiled_queues.get(&entry.queue) {
    Some(compiled) => compiled.combine(&entry.queue, &entry.responses),
    None => {
      warn!(
        queue = %entry.queue,
        item_id = %item_id,
        "Compiled queue missing for broadcast combiner"
      );
      return;
    }
  };

  match result {
    Ok(combined) => {
      if entry.tx.send(combined).is_err() {
        warn!(
          item_id = %item_id,
          "Producer dropped before broadcast result delivered"
        );
      }
    }
    Err(e) => {
      warn!(
        item_id = %item_id,
        error = %e,
        "Combiner failed for broadcast item"
      );
      // Send the error as a JSON response to unblock the producer.
      let _ = entry.tx.send(serde_json::json!({ "error": e.to_string() }));
    }
  }
}

/// Handle a worker disconnecting from an in-flight broadcast.  Removes the
/// worker from remaining respondent sets.  If it was the last one for any
/// broadcast, completes that broadcast.
pub async fn handle_broadcast_disconnect(state: &AppState, worker_id: &str) {
  let items_to_complete: Vec<uuid::Uuid> = {
    let mut bp = state.broadcast_pending.lock().await;
    let mut to_complete = Vec::new();
    for (item_id, entry) in bp.iter_mut() {
      entry.remaining.remove(worker_id);
      if entry.remaining.is_empty() {
        to_complete.push(*item_id);
      }
    }
    to_complete
  };

  for item_id in items_to_complete {
    complete_broadcast(state, item_id).await;
  }
}
