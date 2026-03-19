use crate::state::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use garage_queue_lib::protocol::QueueItem;
use tokio::sync::oneshot;
use tracing::{error, info};
use uuid::Uuid;

/// POST /api/generate
///
/// Ollama-compatible endpoint.  The request is enqueued and this handler
/// holds the HTTP connection open until a worker returns a result.
pub async fn handle_generate(
    State(state): State<AppState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let queue_name = state.config.generate_queue.clone();

    let compiled = match state.compiled_queues.get(&queue_name) {
        Some(q) => q,
        None => {
            error!(queue = %queue_name, "generate_queue references unconfigured queue");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "error": format!("queue '{}' is not configured", queue_name)
            })))
            .into_response();
        }
    };

    let requirements = match compiled.extract(&payload) {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "Capability extraction failed");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                "error": format!("capability extraction failed: {}", e)
            })))
            .into_response();
        }
    };

    let item = QueueItem {
        id: Uuid::new_v4(),
        queue: queue_name.clone(),
        payload,
        requirements,
    };

    info!(item_id = %item.id, queue = %queue_name, "Enqueuing item");

    // Persist to NATS before registering the response channel so that a
    // crash after persistence but before registration results in the item
    // being reprocessed rather than silently lost.
    if let Err(e) = persist_to_nats(&state, &item).await {
        error!(item_id = %item.id, error = %e, "Failed to persist item to NATS");
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": "failed to enqueue item"
        })))
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
