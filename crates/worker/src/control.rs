use axum::{
  extract::{FromRef, State},
  http::StatusCode,
  response::IntoResponse,
  routing::{get, post},
  Router,
};
use prometheus::Registry;
use rust_template_foundation::server::{
  health::{healthz_handler, HealthRegistry},
  metrics::metrics_handler,
};
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tracing::info;

/// The worker's operating state, controlled via the local HTTP control server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
  /// Actively polling for and processing work items.
  Running,
  /// Not accepting new items; will complete any item currently in progress.
  Paused,
  /// Finish the current item, then exit.
  StoppingGraceful,
  /// Exit as soon as possible, abandoning any in-progress item.
  StoppingImmediate,
}

#[derive(Debug, Error)]
pub enum ControlError {
  #[error("Failed to bind control server to '{address}': {source}")]
  Bind {
    address: SocketAddr,
    source: std::io::Error,
  },

  #[error("Control server encountered a runtime error: {0}")]
  Serve(#[source] std::io::Error),
}

pub type StatusSender = watch::Sender<WorkerStatus>;
pub type StatusReceiver = watch::Receiver<WorkerStatus>;

pub fn status_channel() -> (StatusSender, StatusReceiver) {
  watch::channel(WorkerStatus::Running)
}

/// Composite state for the control server, holding the status channel
/// alongside health and metrics registries.
#[derive(Clone)]
pub struct ControlState {
  pub status_tx: Arc<StatusSender>,
  pub health_registry: HealthRegistry,
  pub metrics_registry: Arc<Registry>,
}

impl FromRef<ControlState> for Arc<StatusSender> {
  fn from_ref(state: &ControlState) -> Self {
    state.status_tx.clone()
  }
}

impl FromRef<ControlState> for HealthRegistry {
  fn from_ref(state: &ControlState) -> Self {
    state.health_registry.clone()
  }
}

impl FromRef<ControlState> for Arc<Registry> {
  fn from_ref(state: &ControlState) -> Self {
    state.metrics_registry.clone()
  }
}

/// Builds the control server router.  Separated from `serve` so that tests can
/// mount the router on an ephemeral listener without going through `serve`.
pub fn router(state: ControlState) -> Router {
  Router::new()
    .route("/healthz", get(healthz_handler))
    .route("/metrics", get(metrics_handler))
    .route("/control/pause", post(pause))
    .route("/control/resume", post(resume))
    .route("/control/stop", post(stop_graceful))
    .route("/control/stop/immediate", post(stop_immediate))
    .with_state(state)
}

/// Starts the local control HTTP server.  This server is intentionally bound
/// to localhost only and is intended for use by process supervisors (e.g.
/// sytter) running on the same host.
pub async fn serve(
  bind: SocketAddr,
  state: ControlState,
) -> Result<(), ControlError> {
  let app = router(state);

  let listener =
    tokio::net::TcpListener::bind(bind)
      .await
      .map_err(|source| ControlError::Bind {
        address: bind,
        source,
      })?;

  info!(address = %bind, "Control server listening");

  axum::serve(listener, app)
    .await
    .map_err(ControlError::Serve)
}

async fn pause(State(tx): State<Arc<StatusSender>>) -> impl IntoResponse {
  tx.send_modify(|s| *s = WorkerStatus::Paused);
  info!("Worker paused");
  StatusCode::OK
}

async fn resume(State(tx): State<Arc<StatusSender>>) -> impl IntoResponse {
  tx.send_modify(|s| *s = WorkerStatus::Running);
  info!("Worker resumed");
  StatusCode::OK
}

async fn stop_graceful(
  State(tx): State<Arc<StatusSender>>,
) -> impl IntoResponse {
  tx.send_modify(|s| *s = WorkerStatus::StoppingGraceful);
  info!("Worker stopping gracefully after current item");
  StatusCode::OK
}

async fn stop_immediate(
  State(tx): State<Arc<StatusSender>>,
) -> impl IntoResponse {
  tx.send_modify(|s| *s = WorkerStatus::StoppingImmediate);
  info!("Worker stopping immediately");
  StatusCode::OK
}
