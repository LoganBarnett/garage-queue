use garage_queue_worker::control::{self, status_channel, WorkerStatus};
use std::sync::Arc;

async fn start_control_server() -> (String, control::StatusReceiver) {
  let (tx, rx) = status_channel();
  let tx = Arc::new(tx);

  let app = control::router(Arc::clone(&tx));

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind failed");
  let addr = listener.local_addr().unwrap();

  tokio::spawn(async move {
    axum::serve(listener, app).await.ok();
  });

  (format!("http://{addr}"), rx)
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
