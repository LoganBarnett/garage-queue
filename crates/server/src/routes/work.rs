use crate::dispatch::handle_result;
use crate::web_base::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use garage_queue_lib::protocol::WorkResult;
use tracing::warn;

/// POST /api/work/result
///
/// Workers call this after completing an item.  The waiting producer's
/// connection is resolved with the result.
pub async fn result(
  State(state): State<AppState>,
  Json(body): Json<WorkResult>,
) -> impl IntoResponse {
  let found =
    handle_result(&state, &body.worker_id, body.item_id, body.response).await;

  if found {
    StatusCode::OK.into_response()
  } else {
    warn!(
      item_id = %body.item_id,
      "Result arrived for unknown or already-resolved item"
    );
    StatusCode::NOT_FOUND.into_response()
  }
}
