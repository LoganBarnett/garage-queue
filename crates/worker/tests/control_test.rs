use garage_queue_worker::control::{
  self, status_channel, ObserveState, WorkerStatus,
};
use prometheus::Registry;
use rust_template_foundation::server::health::HealthRegistry;
use std::sync::Arc;

async fn start_control_server() -> (String, control::StatusReceiver) {
  let (tx, rx) = status_channel();
  let tx = Arc::new(tx);

  let app = control::control_router(Arc::clone(&tx));

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind failed");
  let addr = listener.local_addr().unwrap();

  tokio::spawn(async move {
    axum::serve(listener, app).await.ok();
  });

  (format!("http://{addr}"), rx)
}

async fn start_observe_server() -> String {
  let state = ObserveState {
    health_registry: HealthRegistry::default(),
    metrics_registry: Arc::new(Registry::new()),
  };

  let app = control::observe_router(state);

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind failed");
  let addr = listener.local_addr().unwrap();

  tokio::spawn(async move {
    axum::serve(listener, app).await.ok();
  });

  format!("http://{addr}")
}

#[tokio::test]
async fn pause_sets_status_to_paused() {
  let (base, rx) = start_control_server().await;

  let resp = reqwest::Client::new()
    .post(format!("{base}/control/pause"))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
  assert_eq!(*rx.borrow(), WorkerStatus::Paused);
}

#[tokio::test]
async fn resume_sets_status_to_running() {
  let (base, rx) = start_control_server().await;

  // Pause first, then resume.
  reqwest::Client::new()
    .post(format!("{base}/control/pause"))
    .send()
    .await
    .unwrap();

  let resp = reqwest::Client::new()
    .post(format!("{base}/control/resume"))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
  assert_eq!(*rx.borrow(), WorkerStatus::Running);
}

#[tokio::test]
async fn stop_sets_status_to_stopping_graceful() {
  let (base, rx) = start_control_server().await;

  let resp = reqwest::Client::new()
    .post(format!("{base}/control/stop"))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
  assert_eq!(*rx.borrow(), WorkerStatus::StoppingGraceful);
}

#[tokio::test]
async fn stop_immediate_sets_status_to_stopping_immediate() {
  let (base, rx) = start_control_server().await;

  let resp = reqwest::Client::new()
    .post(format!("{base}/control/stop/immediate"))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
  assert_eq!(*rx.borrow(), WorkerStatus::StoppingImmediate);
}

#[tokio::test]
async fn worker_healthz_returns_200() {
  let base = start_observe_server().await;

  let resp = reqwest::get(format!("{base}/healthz")).await.unwrap();
  assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn worker_metrics_returns_200() {
  let base = start_observe_server().await;

  let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();
  assert_eq!(resp.status(), 200);
}
