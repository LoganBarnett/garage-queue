use crate::dispatch::{complete_broadcast, try_dispatch_pending};
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use garage_queue_lib::matching::satisfies;
use garage_queue_lib::protocol::{WorkPoll, WorkResult};
use tracing::{info, warn};

/// POST /api/work/poll
///
/// Workers call this with their capabilities.  The server returns the oldest
/// pending item whose requirements the worker satisfies, or 204 if none match.
///
/// Deprecated: use POST /api/work/connect for SSE-based dispatch instead.
pub async fn poll(
  State(state): State<AppState>,
  Json(body): Json<WorkPoll>,
) -> impl IntoResponse {
  warn!("POST /api/work/poll is deprecated; migrate to SSE via POST /api/work/connect");

  let mut pending = state.pending.lock().await;

  let position = pending
    .iter()
    .position(|item| satisfies(&item.requirements, &body.capabilities));

  match position {
    None => StatusCode::NO_CONTENT.into_response(),
    Some(idx) => {
      let item = pending.remove(idx).expect("position just confirmed");
      info!(
          item_id = %item.id,
          queue = %item.queue,
          worker_tags = ?body.capabilities.tags,
          "Dispatching item to worker",
      );
      Json(item).into_response()
    }
  }
}

/// POST /api/work/result
///
/// Workers call this after completing an item.  The waiting producer's
/// connection is resolved with the result.
pub async fn result(
  State(state): State<AppState>,
  Json(body): Json<WorkResult>,
) -> impl IntoResponse {
  // Check exclusive path first.
  let exclusive_tx = state.pending_responses.lock().await.remove(&body.item_id);

  if let Some(tx) = exclusive_tx {
    if tx.send(body.response).is_err() {
      warn!(
        item_id = %body.item_id,
        "Producer connection was dropped before result arrived"
      );
    }
    info!(item_id = %body.item_id, "Result delivered to producer (exclusive)");

    // Mark the worker idle and try to dispatch next pending item.
    {
      let mut registry = state.registry.lock().await;
      registry.mark_idle(&body.worker_id);
    }
    try_dispatch_pending(&state, &body.worker_id).await;

    return StatusCode::OK.into_response();
  }

  // Check broadcast path.
  {
    let mut bp = state.broadcast_pending.lock().await;
    if let Some(entry) = bp.get_mut(&body.item_id) {
      entry.remaining.remove(&body.worker_id);
      entry
        .responses
        .push((body.worker_id.clone(), body.response));
      let remaining = entry.remaining.len();
      let done = remaining == 0;
      drop(bp);

      info!(
        item_id = %body.item_id,
        worker_id = %body.worker_id,
        remaining = remaining,
        "Broadcast response recorded"
      );

      // Mark this worker idle and check for pending work.
      {
        let mut registry = state.registry.lock().await;
        registry.mark_idle(&body.worker_id);
      }
      try_dispatch_pending(&state, &body.worker_id).await;

      if done {
        complete_broadcast(&state, body.item_id).await;
      }

      return StatusCode::OK.into_response();
    }
  }

  warn!(
    item_id = %body.item_id,
    "Result arrived for unknown or already-resolved item"
  );
  StatusCode::NOT_FOUND.into_response()
}
