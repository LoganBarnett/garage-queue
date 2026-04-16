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
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tokio_listener::ListenerAddress;
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
  ControlBind {
    address: ListenerAddress,
    source: std::io::Error,
  },

  #[error("Failed to bind observability server to '{address}': {source}")]
  ObserveBind {
    address: ListenerAddress,
    source: std::io::Error,
  },

  #[error("Control server encountered a runtime error: {0}")]
  ControlServe(#[source] std::io::Error),

  #[error("Observability server encountered a runtime error: {0}")]
  ObserveServe(#[source] std::io::Error),
}

pub type StatusSender = watch::Sender<WorkerStatus>;
pub type StatusReceiver = watch::Receiver<WorkerStatus>;

pub fn status_channel() -> (StatusSender, StatusReceiver) {
  watch::channel(WorkerStatus::Running)
}

/// State for the observability server (/healthz, /metrics).
#[derive(Clone)]
pub struct ObserveState {
  pub health_registry: HealthRegistry,
  pub metrics_registry: Arc<Registry>,
}

impl FromRef<ObserveState> for HealthRegistry {
  fn from_ref(state: &ObserveState) -> Self {
    state.health_registry.clone()
  }
}

impl FromRef<ObserveState> for Arc<Registry> {
  fn from_ref(state: &ObserveState) -> Self {
    state.metrics_registry.clone()
  }
}

/// Builds the observability router (health and metrics endpoints).
pub fn observe_router(state: ObserveState) -> Router {
  Router::new()
    .route("/healthz", get(healthz_handler))
    .route("/metrics", get(metrics_handler))
    .with_state(state)
}

/// Builds the control router (pause/resume/stop endpoints).
pub fn control_router(tx: Arc<StatusSender>) -> Router {
  Router::new()
    .route("/control/pause", post(pause))
    .route("/control/resume", post(resume))
    .route("/control/stop", post(stop_graceful))
    .route("/control/stop/immediate", post(stop_immediate))
    .with_state(tx)
}

/// Starts the observability HTTP server.
pub async fn serve_observe(
  bind: ListenerAddress,
  state: ObserveState,
) -> Result<(), ControlError> {
  let app = observe_router(state);

  let listener = tokio_listener::Listener::bind(
    &bind,
    &tokio_listener::SystemOptions::default(),
    &tokio_listener::UserOptions::default(),
  )
  .await
  .map_err(|source| ControlError::ObserveBind {
    address: bind.clone(),
    source,
  })?;

  info!(address = %bind, "Observability server listening");

  axum::serve(listener, app.into_make_service())
    .await
    .map_err(ControlError::ObserveServe)
}

/// Starts the local control HTTP server.  This server is intentionally
/// separate from the observability server so that process control
/// endpoints are not exposed alongside freely-scrapable metrics.
pub async fn serve_control(
  bind: ListenerAddress,
  tx: Arc<StatusSender>,
) -> Result<(), ControlError> {
  let app = control_router(tx);

  let listener = tokio_listener::Listener::bind(
    &bind,
    &tokio_listener::SystemOptions::default(),
    &tokio_listener::UserOptions::default(),
  )
  .await
  .map_err(|source| ControlError::ControlBind {
    address: bind.clone(),
    source,
  })?;

  info!(address = %bind, "Control server listening");

  axum::serve(listener, app.into_make_service())
    .await
    .map_err(ControlError::ControlServe)
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
