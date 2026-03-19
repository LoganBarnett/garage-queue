use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Router};
use std::net::SocketAddr;
use std::sync::Arc;
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

pub type StatusSender = watch::Sender<WorkerStatus>;
pub type StatusReceiver = watch::Receiver<WorkerStatus>;

pub fn status_channel() -> (StatusSender, StatusReceiver) {
    watch::channel(WorkerStatus::Running)
}

/// Starts the local control HTTP server.  This server is intentionally bound
/// to localhost only and is intended for use by process supervisors (e.g.
/// sytter) running on the same host.
pub async fn serve(bind: SocketAddr, tx: Arc<StatusSender>) {
    let app = Router::new()
        .route("/control/pause", post(pause))
        .route("/control/resume", post(resume))
        .route("/control/stop", post(stop_graceful))
        .route("/control/stop/immediate", post(stop_immediate))
        .with_state(tx);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .expect("failed to bind control server");

    info!(address = %bind, "Control server listening");

    axum::serve(listener, app)
        .await
        .expect("control server error");
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

async fn stop_graceful(State(tx): State<Arc<StatusSender>>) -> impl IntoResponse {
    tx.send_modify(|s| *s = WorkerStatus::StoppingGraceful);
    info!("Worker stopping gracefully after current item");
    StatusCode::OK
}

async fn stop_immediate(State(tx): State<Arc<StatusSender>>) -> impl IntoResponse {
    tx.send_modify(|s| *s = WorkerStatus::StoppingImmediate);
    info!("Worker stopping immediately");
    StatusCode::OK
}
