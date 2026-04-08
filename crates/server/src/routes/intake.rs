use crate::config::QueueMode;
use crate::dispatch::{dispatch_broadcast, dispatch_exclusive};
use crate::state::AppState;
use axum::{
  extract::{OriginalUri, State},
  http::StatusCode,
  response::IntoResponse,
  Json,
};
use garage_queue_lib::protocol::QueueItem;
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Error)]
enum NatsPersistError {
  #[error("Failed to publish item to NATS subject '{subject}': {source}")]
  Publish {
    subject: String,
    source: async_nats::jetstream::context::PublishError,
  },

  #[error("NATS acknowledgement failed for subject '{subject}': {source}")]
  Ack {
    subject: String,
    source: async_nats::jetstream::context::PublishError,
  },
}

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

  let (queue_name, requirements, mode) = {
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

    let mode = compiled.mode;

    (queue_name, requirements, mode)
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
      mode = ?mode,
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

  match mode {
    QueueMode::Exclusive => {
      dispatch_exclusive(&state, item, tx).await;
    }
    QueueMode::Broadcast => {
      dispatch_broadcast(&state, item, tx).await;
    }
  }

  match rx.await {
    Ok(response) => Json(response).into_response(),
    Err(_) => {
      error!("Response channel closed before result arrived");
      StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
  }
}

async fn persist_to_nats(
  state: &AppState,
  item: &QueueItem,
) -> Result<(), NatsPersistError> {
  let subject = format!("items.{}", item.queue);
  // QueueItem derives Serialize; serialisation failure here is a logic bug.
  let payload = serde_json::to_vec(item).expect("QueueItem derives Serialize");
  state
    .jetstream
    .publish(subject.clone(), payload.into())
    .await
    .map_err(|source| NatsPersistError::Publish {
      subject: subject.clone(),
      source,
    })?
    .await
    .map_err(|source| NatsPersistError::Ack { subject, source })?;
  Ok(())
}
