use crate::dispatch::{handle_disconnect, try_dispatch};
use crate::web_base::AppState;
use axum::{
  extract::State,
  http::StatusCode,
  response::{
    sse::{Event, KeepAlive, Sse},
    IntoResponse,
  },
  Json,
};
use garage_queue_lib::protocol::{ConcurrencyConfig, QueueItem, WorkerConnect};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

/// POST /api/work/connect
///
/// Workers establish an SSE connection by posting their identity and
/// capabilities.  The server registers them in the worker registry and
/// pushes work items as SSE events on the returned stream.  The worker's
/// concurrency config controls how many items may be in-flight at once.
pub async fn connect(
  State(state): State<AppState>,
  Json(body): Json<WorkerConnect>,
) -> impl IntoResponse {
  let (tx, rx) = mpsc::channel::<QueueItem>(32);

  let concurrency = body.concurrency.unwrap_or_else(ConcurrencyConfig::default);

  {
    let mut registry = state.registry.lock().await;
    if let Err(e) = registry.register(
      body.worker_id.clone(),
      body.capabilities.clone(),
      concurrency,
      tx,
    ) {
      warn!(worker_id = %e.worker_id, "Duplicate worker registration");
      return (
        StatusCode::CONFLICT,
        Json(serde_json::json!({
          "error": format!("worker '{}' is already connected", e.worker_id)
        })),
      )
        .into_response();
    }
  }

  info!(
    worker_id = %body.worker_id,
    tags = ?body.capabilities.tags,
    "Worker connected via SSE"
  );

  // Check queue for items this worker can handle.
  let worker_id = body.worker_id.clone();
  let dispatch_state = state.clone();
  tokio::spawn(async move {
    try_dispatch(&dispatch_state, &worker_id).await;
  });

  let inner = ReceiverStream::new(rx);
  let stream = CleanupStream {
    inner,
    state: Some(state),
    worker_id: Some(body.worker_id),
  };

  Sse::new(stream)
    .keep_alive(
      KeepAlive::new()
        .interval(std::time::Duration::from_secs(1))
        .text("ping"),
    )
    .into_response()
}

/// Wraps a ReceiverStream to deregister the worker when the stream is dropped
/// (client disconnect or channel closure).
struct CleanupStream {
  inner: ReceiverStream<QueueItem>,
  state: Option<AppState>,
  worker_id: Option<String>,
}

impl futures_util::Stream for CleanupStream {
  type Item = Result<Event, axum::BoxError>;

  fn poll_next(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Self::Item>> {
    let this = self.get_mut();
    let inner = Pin::new(&mut this.inner);
    match futures_util::Stream::poll_next(inner, cx) {
      Poll::Ready(Some(item)) => {
        let data =
          serde_json::to_string(&item).expect("QueueItem derives Serialize");
        Poll::Ready(Some(Ok(Event::default().event("work").data(data))))
      }
      Poll::Ready(None) => Poll::Ready(None),
      Poll::Pending => Poll::Pending,
    }
  }
}

impl Drop for CleanupStream {
  fn drop(&mut self) {
    if let (Some(state), Some(worker_id)) =
      (self.state.take(), self.worker_id.take())
    {
      tokio::spawn(async move {
        {
          let mut registry = state.registry.lock().await;
          registry.deregister(&worker_id);
        }

        info!(
          worker_id = %worker_id,
          "Worker disconnected, deregistered"
        );

        handle_disconnect(&state, &worker_id).await;
      });
    }
  }
}
