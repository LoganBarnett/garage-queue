use crate::state::AppState;
use axum::{
    extract::{OriginalUri, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use garage_queue_lib::protocol::QueueItem;
use tokio::sync::oneshot;
use tracing::{error, info};
use uuid::Uuid;

/// POST /*path
///
/// Generic intake endpoint.  The request path is matched against each queue's
/// configured route; the payload is enqueued on the matching queue and this
/// handler holds the connection open until a worker returns a result.
pub async fn handle_intake(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let path = uri.path().to_string();

    let (queue_name, requirements) = {
        let live = state.live.read().await;

        let queue_name = live
            .config
            .queues
            .iter()
            .find(|(_, q)| q.route.as_deref() == Some(&path))
            .map(|(name, _)| name.clone());

        let queue_name = match queue_name {
            Some(n) => n,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": format!("no queue mapped to '{path}'") })),
                )
                    .into_response();
            }
        };

        let compiled = match live.compiled_queues.get(&queue_name) {
            Some(q) => q,
            None => {
                // Config is validated at load time so this should not happen,
                // but handle it defensively rather than panicking.
                error!(queue = %queue_name, "compiled queue missing for configured route");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };

        let requirements = match compiled.extract(&payload) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Capability extraction failed");
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("capability extraction failed: {e}")
                    })),
                )
                    .into_response();
            }
        };

        (queue_name, requirements)
    };

    let item = QueueItem {
        id: Uuid::new_v4(),
        queue: queue_name.clone(),
        payload,
        requirements: requirements.clone(),
    };

    info!(
        item_id = %item.id,
        queue = %queue_name,
        requirements = ?requirements,
        "Enqueuing item",
    );

    if let Err(e) = persist_to_nats(&state, &item).await {
        error!(item_id = %item.id, error = %e, "Failed to persist item to NATS");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "failed to enqueue item" })),
        )
            .into_response();
    }

    let (tx, rx) = oneshot::channel::<serde_json::Value>();

    {
        let mut pending = state.pending.lock().await;
        let mut responses = state.pending_responses.lock().await;
        pending.push_back(item.clone());
        responses.insert(item.id, tx);
    }

    match rx.await {
        Ok(response) => Json(response).into_response(),
        Err(_) => {
            error!(item_id = %item.id, "Response channel closed before result arrived");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn persist_to_nats(state: &AppState, item: &QueueItem) -> Result<(), String> {
    let subject = format!("items.{}", item.queue);
    let payload = serde_json::to_vec(item).expect("QueueItem is always serialisable");
    state
        .jetstream
        .publish(subject, payload.into())
        .await
        .map_err(|e| e.to_string())?
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
