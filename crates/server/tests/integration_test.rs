// Integration tests for the queue server.
//
// Run with:
//   cargo test -p garage-queue-server

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use garage_queue_lib::{
  capability::WorkerCapabilities,
  protocol::{QueueItem, WorkResult, WorkerConnect},
};
use garage_queue_lib::{LogFormat, LogLevel};
use garage_queue_server::{
  build_router,
  config::{
    Config, ExtractorConfig, ExtractorKind, JqSource, Method, QueueConfig,
    QueueMode,
  },
  intake::CompiledQueue,
  AppState,
};
use serde_json::json;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_listener::ListenerAddress;

// ── In-process server fixture ─────────────────────────────────────────────────

struct TestServer {
  pub addr: SocketAddr,
  handle: JoinHandle<()>,
}

impl TestServer {
  async fn start_with(config: Config) -> Self {
    let compiled_queues = config
      .queues
      .iter()
      .map(|(name, q)| {
        (name.clone(), CompiledQueue::build(q).expect("extractor build failed"))
      })
      .collect::<HashMap<_, _>>();

    let config = Arc::new(config);
    let state = AppState::new(Arc::clone(&config), compiled_queues);
    let app = build_router(state, &config);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
      .await
      .expect("bind failed");
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
      axum::serve(listener, app).await.ok();
    });

    Self { addr, handle }
  }

  /// Start a server with a single no-requirements queue named "test" at
  /// route "/test".  Any worker capabilities will match items on this queue.
  async fn start() -> Self {
    Self::start_with(Config {
      log_level: LogLevel::Warn,
      log_format: LogFormat::Text,
      listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
      queues: HashMap::from([(
        "test".to_string(),
        QueueConfig {
          route: Some("/test".to_string()),
          method: Some(Method::Post),
          mode: QueueMode::Exclusive,
          combiner: None,
          extractors: vec![],
          delegate_path: None,
          delegate_method: None,
          intake_timeout_secs: None,
        },
      )]),
    })
    .await
  }
}

impl Drop for TestServer {
  fn drop(&mut self) {
    self.handle.abort();
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Read the next SSE event with event type "work" from a stream, with timeout.
async fn next_sse_work_event<S>(stream: &mut S) -> eventsource_stream::Event
where
  S: futures_util::Stream<
      Item = Result<
        eventsource_stream::Event,
        eventsource_stream::EventStreamError<reqwest::Error>,
      >,
    > + Unpin,
{
  tokio::time::timeout(Duration::from_secs(5), async {
    loop {
      match stream.next().await {
        Some(Ok(ev)) if ev.event == "work" => return ev,
        Some(Ok(_)) => continue,
        other => panic!("unexpected SSE event: {other:?}"),
      }
    }
  })
  .await
  .expect("timed out waiting for SSE work event")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_returns_200() {
  let server = TestServer::start().await;

  let resp = reqwest::get(format!("http://{}/healthz", server.addr))
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn result_for_unknown_item_returns_404() {
  let server = TestServer::start().await;

  let resp = reqwest::Client::new()
    .post(format!("http://{}/api/work/result", server.addr))
    .json(&WorkResult {
      item_id: uuid::Uuid::new_v4(),
      worker_id: "test-worker".into(),
      response: json!({}),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn unmapped_route_returns_404() {
  let server = TestServer::start().await;

  let resp = reqwest::Client::new()
    .post(format!("http://{}/no/such/route", server.addr))
    .json(&json!({}))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 404);
}

// ── SSE tests ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn sse_connect_and_receive_work() {
  let server = TestServer::start().await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect a worker via SSE.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "sse-worker-1".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  assert!(sse_resp.status().is_success());

  let mut sse_stream = sse_resp.bytes_stream().eventsource();

  // Enqueue an item — the SSE worker should receive it.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/test"))
        .json(&json!({ "prompt": "hello via SSE" }))
        .send()
        .await
        .unwrap()
    })
  };

  // Read the work event from the SSE stream.
  let event = next_sse_work_event(&mut sse_stream).await;
  let item: QueueItem =
    serde_json::from_str(&event.data).expect("deserialise QueueItem");
  assert_eq!(item.payload["prompt"], "hello via SSE");

  // Post result.
  let result_resp = client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "sse-worker-1".into(),
      response: json!({ "answer": "hi" }),
    })
    .send()
    .await
    .unwrap();
  assert_eq!(result_resp.status(), 200);

  // The intake request should now resolve.
  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
  let body: serde_json::Value = intake_resp.json().await.unwrap();
  assert_eq!(body, json!({ "answer": "hi" }));
}

#[tokio::test]
async fn sse_duplicate_worker_id_rejected() {
  let server = TestServer::start().await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  let body = WorkerConnect {
    worker_id: "dupe-worker".into(),
    capabilities: WorkerCapabilities::default(),
    concurrency: None,
  };

  // First connection should succeed (keep it alive by holding the response).
  let _first = client
    .post(format!("{base}/api/work/connect"))
    .json(&body)
    .send()
    .await
    .unwrap();

  // Give the server time to register the worker.
  tokio::time::sleep(Duration::from_millis(50)).await;

  // Second connection with the same ID should get 409.
  let second = client
    .post(format!("{base}/api/work/connect"))
    .json(&body)
    .send()
    .await
    .unwrap();

  assert_eq!(second.status(), 409);
}

#[tokio::test]
async fn sse_disconnect_allows_re_registration() {
  let server = TestServer::start().await;
  let base = format!("http://{}", server.addr);

  let body = WorkerConnect {
    worker_id: "reconnect-worker".into(),
    capabilities: WorkerCapabilities::default(),
    concurrency: None,
  };

  // Connect, start reading the stream, then drop it to force a disconnect.
  {
    // Use a separate client with no connection pool so the TCP socket
    // closes immediately on drop.
    let ephemeral = reqwest::Client::builder()
      .pool_max_idle_per_host(0)
      .build()
      .unwrap();
    let resp = ephemeral
      .post(format!("{base}/api/work/connect"))
      .json(&body)
      .send()
      .await
      .unwrap();
    // Start reading the stream so the server begins writing keep-alives.
    let mut stream = resp.bytes_stream().eventsource();
    // Read one keep-alive or event to confirm the connection is live.
    let _ =
      tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
    // Stream and client dropped here — TCP socket closes.
  }

  // Give the server time to detect the closed connection and deregister.
  // Keep-alive fires every 1 second; the failed write triggers cleanup.
  tokio::time::sleep(Duration::from_secs(3)).await;

  // Re-registration should succeed.
  let client = reqwest::Client::new();
  let resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&body)
    .send()
    .await
    .unwrap();

  assert!(resp.status().is_success());
}

#[tokio::test]
async fn exclusive_pending_dispatched_on_connect() {
  let server = TestServer::start().await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue an item when no workers are connected.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/test"))
        .json(&json!({ "prompt": "pending dispatch" }))
        .send()
        .await
        .unwrap()
    })
  };

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Now connect a worker — it should immediately receive the pending item.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "late-worker".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();

  let mut sse_stream = sse_resp.bytes_stream().eventsource();

  let event = next_sse_work_event(&mut sse_stream).await;
  let item: QueueItem = serde_json::from_str(&event.data).unwrap();
  assert_eq!(item.payload["prompt"], "pending dispatch");

  // Complete the item.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "late-worker".into(),
      response: json!({ "done": true }),
    })
    .send()
    .await
    .unwrap();

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
}

// ── Broadcast tests ───────────────────────────────────────────────────────────

fn broadcast_config() -> Config {
  Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "bcast".to_string(),
      QueueConfig {
        route: Some("/bcast".to_string()),
        method: Some(Method::Post),
        mode: QueueMode::Broadcast,
        combiner: Some(JqSource::Inline("[.[] | .response] | add".to_string())),
        extractors: vec![],
        delegate_path: None,
        delegate_method: None,
        intake_timeout_secs: None,
      },
    )]),
  }
}

#[tokio::test]
async fn broadcast_all_workers_receive_item() {
  let server = TestServer::start_with(broadcast_config()).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect two workers.
  let sse1 = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "bw1".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut stream1 = sse1.bytes_stream().eventsource();

  let sse2 = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "bw2".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut stream2 = sse2.bytes_stream().eventsource();

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Enqueue a broadcast item.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/bcast"))
        .json(&json!({ "query": "tags" }))
        .send()
        .await
        .unwrap()
    })
  };

  // Both workers should receive the item.
  let ev1 = next_sse_work_event(&mut stream1).await;
  let ev2 = next_sse_work_event(&mut stream2).await;

  let item1: QueueItem = serde_json::from_str(&ev1.data).unwrap();
  let item2: QueueItem = serde_json::from_str(&ev2.data).unwrap();
  assert_eq!(item1.id, item2.id, "both workers should get the same item");

  // Both workers respond.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item1.id,
      worker_id: "bw1".into(),
      response: json!({ "models": ["a"] }),
    })
    .send()
    .await
    .unwrap();

  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item1.id,
      worker_id: "bw2".into(),
      response: json!({ "models": ["b"] }),
    })
    .send()
    .await
    .unwrap();

  // The intake request should resolve with the combined result.
  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
  let body: serde_json::Value = intake_resp.json().await.unwrap();

  // The combiner is `[.[] | .response] | add`, which concatenates the
  // response objects.  With two objects {"models":["a"]} and
  // {"models":["b"]}, `add` on objects merges them; last key wins.
  // The exact output depends on response order, but the body should be
  // a valid JSON object.
  assert!(body.is_object(), "combined result should be an object");
}

#[tokio::test]
async fn broadcast_combiner_merges_responses() {
  // Use a combiner that collects all response values into an array.
  let config = Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "merge".to_string(),
      QueueConfig {
        route: Some("/merge".to_string()),
        method: Some(Method::Post),
        mode: QueueMode::Broadcast,
        combiner: Some(JqSource::Inline(
          "[.[] | .response.value] | sort".to_string(),
        )),
        extractors: vec![],
        delegate_path: None,
        delegate_method: None,
        intake_timeout_secs: None,
      },
    )]),
  };

  let server = TestServer::start_with(config).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect two workers.
  let sse1 = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "mw1".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut stream1 = sse1.bytes_stream().eventsource();

  let sse2 = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "mw2".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut stream2 = sse2.bytes_stream().eventsource();

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Enqueue.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/merge"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
    })
  };

  // Drain work events.
  let ev1 = next_sse_work_event(&mut stream1).await;
  let item1: QueueItem = serde_json::from_str(&ev1.data).unwrap();
  let _ev2 = next_sse_work_event(&mut stream2).await;

  // Workers respond with distinct values.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item1.id,
      worker_id: "mw1".into(),
      response: json!({ "value": 10 }),
    })
    .send()
    .await
    .unwrap();

  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item1.id,
      worker_id: "mw2".into(),
      response: json!({ "value": 20 }),
    })
    .send()
    .await
    .unwrap();

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
  let body: serde_json::Value = intake_resp.json().await.unwrap();
  assert_eq!(body, json!([10, 20]));
}

#[tokio::test]
async fn get_intake_uses_empty_payload() {
  let server = TestServer::start_with(Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "get-queue".to_string(),
      QueueConfig {
        route: Some("/get-test".to_string()),
        method: Some(Method::Get),
        mode: QueueMode::Exclusive,
        combiner: None,
        extractors: vec![],
        delegate_path: None,
        delegate_method: None,
        intake_timeout_secs: None,
      },
    )]),
  })
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect a worker via SSE to receive the item.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "get-worker".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut sse_stream = sse_resp.bytes_stream().eventsource();

  // GET /get-test — no body.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client.get(format!("{base}/get-test")).send().await.unwrap()
    })
  };

  // Worker receives the item via SSE.
  let event = next_sse_work_event(&mut sse_stream).await;
  let item: QueueItem = serde_json::from_str(&event.data).unwrap();
  assert_eq!(
    item.payload,
    json!({}),
    "GET intake should produce empty payload"
  );

  // Return result so the intake request can resolve.
  let worker_response = json!({ "result": "ok" });
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "get-worker".into(),
      response: worker_response.clone(),
    })
    .send()
    .await
    .unwrap();

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
  let body: serde_json::Value = intake_resp.json().await.unwrap();
  assert_eq!(body, worker_response);
}

#[tokio::test]
async fn delegation_hints_present_in_dispatched_item() {
  let server = TestServer::start_with(Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "delegated".to_string(),
      QueueConfig {
        route: Some("/delegated".to_string()),
        method: Some(Method::Get),
        mode: QueueMode::Exclusive,
        combiner: None,
        extractors: vec![],
        delegate_path: Some("/api/tags".to_string()),
        delegate_method: Some(Method::Get),
        intake_timeout_secs: None,
      },
    )]),
  })
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect a worker via SSE.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "hint-worker".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut sse_stream = sse_resp.bytes_stream().eventsource();

  // GET /delegated — no body.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .get(format!("{base}/delegated"))
        .send()
        .await
        .unwrap()
    })
  };

  // Worker receives the item via SSE with delegation hints.
  let event = next_sse_work_event(&mut sse_stream).await;
  let item: QueueItem = serde_json::from_str(&event.data).unwrap();
  assert_eq!(
    item.delegate_path.as_deref(),
    Some("/api/tags"),
    "delegate_path should be set"
  );
  assert_eq!(
    item.delegate_method.as_deref(),
    Some("get"),
    "delegate_method should be set"
  );

  // Return result so the intake request can resolve.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "hint-worker".into(),
      response: json!({ "models": [] }),
    })
    .send()
    .await
    .unwrap();

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
}

#[tokio::test]
async fn intake_timeout_returns_504() {
  let server = TestServer::start_with(Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "timeout-queue".to_string(),
      QueueConfig {
        route: Some("/timeout-test".to_string()),
        method: Some(Method::Post),
        mode: QueueMode::Exclusive,
        combiner: None,
        extractors: vec![],
        delegate_path: None,
        delegate_method: None,
        intake_timeout_secs: Some(1),
      },
    )]),
  })
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue an item but do NOT respond — should timeout.
  let resp = client
    .post(format!("{base}/timeout-test"))
    .json(&json!({ "data": "will timeout" }))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 504, "expected 504 Gateway Timeout");
  let body: serde_json::Value = resp.json().await.unwrap();
  assert!(
    body["error"].as_str().unwrap().contains("timed out"),
    "error body should mention timeout"
  );
}

#[tokio::test]
async fn sse_tag_requirement_matching() {
  // Server with a queue that requires a model tag.
  let server = TestServer::start_with(Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "tagged".to_string(),
      QueueConfig {
        route: Some("/tagged".to_string()),
        method: Some(Method::Post),
        mode: QueueMode::Exclusive,
        combiner: None,
        extractors: vec![ExtractorConfig {
          capability: "model".to_string(),
          kind: ExtractorKind::Tag,
          source: JqSource::Inline(".model".to_string()),
        }],
        delegate_path: None,
        delegate_method: None,
        intake_timeout_secs: None,
      },
    )]),
  })
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect a worker with the wrong tag — should NOT receive item.
  let sse_wrong = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "wrong-worker".into(),
      capabilities: WorkerCapabilities {
        tags: vec!["gemma2:9b".to_string()],
        ..Default::default()
      },
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut _stream_wrong = sse_wrong.bytes_stream().eventsource();

  // Connect a worker with the correct tag.
  let sse_right = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "right-worker".into(),
      capabilities: WorkerCapabilities {
        tags: vec!["llama3.2:8b".to_string()],
        ..Default::default()
      },
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut stream_right = sse_right.bytes_stream().eventsource();

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Enqueue an item requiring "llama3.2:8b".
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/tagged"))
        .json(&json!({ "model": "llama3.2:8b" }))
        .send()
        .await
        .unwrap()
    })
  };

  // The right worker should receive it.
  let event = next_sse_work_event(&mut stream_right).await;
  let item: QueueItem = serde_json::from_str(&event.data).unwrap();
  assert_eq!(item.payload["model"], "llama3.2:8b");

  // Complete the item.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "right-worker".into(),
      response: json!({ "done": true }),
    })
    .send()
    .await
    .unwrap();

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
}

#[tokio::test]
async fn sse_scalar_capability_matching() {
  let server = TestServer::start_with(Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    queues: HashMap::from([(
      "gpu".to_string(),
      QueueConfig {
        route: Some("/gpu".to_string()),
        method: Some(Method::Post),
        mode: QueueMode::Exclusive,
        combiner: None,
        extractors: vec![ExtractorConfig {
          capability: "vram_mb".to_string(),
          kind: ExtractorKind::Scalar,
          source: JqSource::Inline(".vram_mb".to_string()),
        }],
        delegate_path: None,
        delegate_method: None,
        intake_timeout_secs: None,
      },
    )]),
  })
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect a worker with sufficient VRAM.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "big-gpu".into(),
      capabilities: WorkerCapabilities {
        scalars: HashMap::from([("vram_mb".to_string(), 16384.0)]),
        ..Default::default()
      },
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut sse_stream = sse_resp.bytes_stream().eventsource();

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Enqueue an item requiring 8192 MB of VRAM.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/gpu"))
        .json(&json!({ "vram_mb": 8192 }))
        .send()
        .await
        .unwrap()
    })
  };

  // Worker should receive the item.
  let event = next_sse_work_event(&mut sse_stream).await;
  let item: QueueItem = serde_json::from_str(&event.data).unwrap();

  // Complete the item.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "big-gpu".into(),
      response: json!({ "done": true }),
    })
    .send()
    .await
    .unwrap();

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
}

#[tokio::test]
async fn exclusive_items_dispatched_fifo() {
  let server = TestServer::start().await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue two items before any worker connects.
  let mut intake_handles = vec![];
  for i in 0..2 {
    let client = client.clone();
    let base = base.clone();
    intake_handles.push(tokio::spawn(async move {
      client
        .post(format!("{base}/test"))
        .json(&json!({ "seq": i }))
        .send()
        .await
        .unwrap()
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Connect a worker — should get seq=0 first.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "fifo-worker".into(),
      capabilities: WorkerCapabilities::default(),
      concurrency: None,
    })
    .send()
    .await
    .unwrap();
  let mut sse_stream = sse_resp.bytes_stream().eventsource();

  let ev1 = next_sse_work_event(&mut sse_stream).await;
  let item1: QueueItem = serde_json::from_str(&ev1.data).unwrap();
  assert_eq!(item1.payload["seq"], 0, "first item should be seq 0");

  // Complete first item.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item1.id,
      worker_id: "fifo-worker".into(),
      response: json!({ "seq": 0 }),
    })
    .send()
    .await
    .unwrap();

  // Should now get seq=1.
  let ev2 = next_sse_work_event(&mut sse_stream).await;
  let item2: QueueItem = serde_json::from_str(&ev2.data).unwrap();
  assert_eq!(item2.payload["seq"], 1, "second item should be seq 1");

  // Complete second item.
  client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item2.id,
      worker_id: "fifo-worker".into(),
      response: json!({ "seq": 1 }),
    })
    .send()
    .await
    .unwrap();

  for h in intake_handles {
    let resp = h.await.unwrap();
    assert_eq!(resp.status(), 200);
  }
}

// ── Server compliance endpoint tests ─────────────────────────────────────────

#[tokio::test]
async fn metrics_returns_200() {
  let server = TestServer::start().await;

  let resp = reqwest::get(format!("http://{}/metrics", server.addr))
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn openapi_spec_returns_valid_json() {
  let server = TestServer::start().await;

  let resp =
    reqwest::get(format!("http://{}/api-docs/openapi.json", server.addr))
      .await
      .unwrap();

  assert_eq!(resp.status(), 200);
  let body: serde_json::Value = resp.json().await.unwrap();
  assert!(body.is_object(), "OpenAPI spec should be a JSON object");
}

#[tokio::test]
async fn scalar_ui_returns_200() {
  let server = TestServer::start().await;

  let resp = reqwest::get(format!("http://{}/scalar", server.addr))
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
}
