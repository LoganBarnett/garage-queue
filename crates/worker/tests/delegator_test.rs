use axum::{routing::post, Json, Router};
use garage_queue_worker::delegator::HttpDelegator;
use serde_json::json;

/// Starts a mock HTTP server that echoes the received JSON body back.
async fn start_echo_server() -> String {
  let app = Router::new().route(
    "/api/generate",
    post(|Json(body): Json<serde_json::Value>| async move { Json(body) }),
  );

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind failed");
  let addr = listener.local_addr().unwrap();

  tokio::spawn(async move {
    axum::serve(listener, app).await.ok();
  });

  format!("http://{addr}/api/generate")
}

#[tokio::test]
async fn successful_delegation_forwards_payload() {
  let url = start_echo_server().await;
  let delegator = HttpDelegator::new(&url);

  let payload = json!({ "model": "llama3.2:8b", "prompt": "hello" });
  let result = delegator.delegate(&payload).await.unwrap();

  // The echo server returns what it received; the delegator forces stream: false.
  assert_eq!(result["model"], "llama3.2:8b");
  assert_eq!(result["prompt"], "hello");
}

#[tokio::test]
async fn stream_false_is_injected() {
  let url = start_echo_server().await;
  let delegator = HttpDelegator::new(&url);

  // Send a payload with stream: true — the delegator should override it.
  let payload = json!({ "model": "test", "stream": true });
  let result = delegator.delegate(&payload).await.unwrap();

  assert_eq!(result["stream"], false);
}

#[tokio::test]
async fn connection_failure_returns_http_error() {
  // Point at a port that nothing is listening on.
  let delegator = HttpDelegator::new("http://127.0.0.1:1");
  let payload = json!({ "model": "test" });

  let err = delegator.delegate(&payload).await.unwrap_err();
  assert!(matches!(
    err,
    garage_queue_worker::delegator::DelegateError::Http(_)
  ));
}
