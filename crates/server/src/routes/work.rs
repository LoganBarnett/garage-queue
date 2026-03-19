use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use garage_queue_lib::matching::satisfies;
use garage_queue_lib::protocol::{WorkPoll, WorkResult};
use tracing::{info, warn};

/// POST /api/work/poll
///
/// Workers call this with their capabilities.  The server returns the oldest
/// pending item whose requirements the worker satisfies, or 204 if none match.
pub async fn poll(
    State(state): State<AppState>,
    Json(body): Json<WorkPoll>,
) -> impl IntoResponse {
    let mut pending = state.pending.lock().await;

    let position = pending
        .iter()
        .position(|item| satisfies(&item.requirements, &body.capabilities));

    match position {
        None => StatusCode::NO_CONTENT.into_response(),
        Some(idx) => {
            let item = pending.remove(idx).expect("position just confirmed");
            info!(item_id = %item.id, queue = %item.queue, "Dispatching item to worker");
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
    let tx = state.pending_responses.lock().await.remove(&body.item_id);

    match tx {
        None => {
            warn!(item_id = %body.item_id, "Result arrived for unknown or already-resolved item");
            StatusCode::NOT_FOUND.into_response()
        }
        Some(tx) => {
            if tx.send(body.response).is_err() {
                warn!(item_id = %body.item_id, "Producer connection was dropped before result arrived");
            }
            info!(item_id = %body.item_id, "Result delivered to producer");
            StatusCode::OK.into_response()
        }
    }
}
