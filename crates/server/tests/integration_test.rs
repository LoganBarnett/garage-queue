// Integration tests for the queue server.
//
// A NATS server is started automatically on port 14222 for the duration of the
// test binary.  Set NATS_URL to point at an already-running instance instead
// (useful in CI where NATS is a sidecar service).
//
// Run with:
//   cargo test -p garage-queue-server

use async_nats::jetstream;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use garage_queue_lib::{
  capability::WorkerCapabilities,
  protocol::{QueueItem, WorkPoll, WorkResult, WorkerConnect},
};
use garage_queue_lib::{LogFormat, LogLevel};
use garage_queue_server::{
  build_router,
  config::{
    Config, ExtractorConfig, ExtractorKind, JqSource, Method, QueueConfig,
    QueueMode,
  },
  intake::CompiledQueue,
  state::AppState,
};
use serde_json::json;
use std::{
  collections::HashMap,
  net::SocketAddr,
  process::{Child, Command, Stdio},
  sync::{Arc, Mutex, Once},
  time::Duration,
};
use tokio::task::JoinHandle;
use tokio_listener::ListenerAddress;
use uuid::Uuid;

// ── NATS fixture ──────────────────────────────────────────────────────────────

const TEST_NATS_PORT: u16 = 14222;

// All tests share one NATS process started by the first test to run.
static NATS_INIT: Once = Once::new();
static NATS_CHILD: Mutex<Option<Child>> = Mutex::new(None);

struct TestNats {
  pub url: String,
}

impl TestNats {
  fn acquire() -> Self {
    if let Ok(url) = std::env::var("NATS_URL") {
      return Self { url };
    }

    NATS_INIT.call_once(|| {
      let child = Command::new("nats-server")
        .args(["-p", &TEST_NATS_PORT.to_string(), "-js"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("nats-server not found in PATH; add it to the dev shell");

      // Busy-wait until the TCP port accepts connections.
      for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(50));
        if std::net::TcpStream::connect(("127.0.0.1", TEST_NATS_PORT)).is_ok() {
          break;
        }
      }

      // Intentionally not stopped on drop — the process is shared for
      // the entire test binary run and cleaned up by the OS on exit.
      *NATS_CHILD.lock().unwrap() = Some(child);
    });

    Self {
      url: format!("nats://127.0.0.1:{TEST_NATS_PORT}"),
    }
  }
}

// ── In-process server fixture ─────────────────────────────────────────────────

// All test servers publish to subjects under "items.>" and use a single shared
// stream.  Because the pending queue is entirely in-memory inside AppState,
// each TestServer instance is isolated from others regardless of the shared
// NATS stream.
const TEST_STREAM: &str = "GARAGE_QUEUE_TEST";

struct TestServer {
  pub addr: SocketAddr,
  handle: JoinHandle<()>,
}

impl TestServer {
  async fn start_with(nats_url: &str, config: Config) -> Self {
    let nats = async_nats::connect(nats_url)
      .await
      .expect("NATS connect failed");
    let js = jetstream::new(nats);

    js.get_or_create_stream(jetstream::stream::Config {
      name: TEST_STREAM.to_string(),
      subjects: vec!["items.>".to_string()],
      ..Default::default()
    })
    .await
    .expect("stream setup failed");

    let compiled_queues = config
      .queues
      .iter()
      .map(|(name, q)| {
        (name.clone(), CompiledQueue::build(q).expect("extractor build failed"))
      })
      .collect::<HashMap<_, _>>();

    let config = Arc::new(config);
    let state = AppState::new(Arc::clone(&config), js, compiled_queues);
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
  async fn start(nats_url: &str) -> Self {
    Self::start_with(
      nats_url,
      Config {
        log_level: LogLevel::Warn,
        log_format: LogFormat::Text,
        listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
        nats_url: nats_url.to_string(),
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
          },
        )]),
      },
    )
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
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;

  let resp = reqwest::get(format!("http://{}/healthz", server.addr))
    .await
    .unwrap();

  assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn poll_empty_queue_returns_204() {
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;

  let resp = reqwest::Client::new()
    .post(format!("http://{}/api/work/poll", server.addr))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 204);
}

#[tokio::test]
async fn result_for_unknown_item_returns_404() {
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;

  let resp = reqwest::Client::new()
    .post(format!("http://{}/api/work/result", server.addr))
    .json(&WorkResult {
      item_id: Uuid::new_v4(),
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
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;

  let resp = reqwest::Client::new()
    .post(format!("http://{}/no/such/route", server.addr))
    .json(&json!({}))
    .send()
    .await
    .unwrap();

  assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn intake_poll_result_round_trip() {
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // POST /test blocks until a worker returns a result, so run it in a
  // separate task.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/test"))
        .json(&json!({ "model": "llama3.2:8b", "prompt": "hello" }))
        .send()
        .await
        .unwrap()
    })
  };

  // Give the request time to reach the server and be enqueued.
  tokio::time::sleep(Duration::from_millis(100)).await;

  // Poll — no requirements on the test queue, so any capabilities match.
  let poll_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(poll_resp.status(), 200, "expected item from poll");
  let item: QueueItem = poll_resp.json().await.unwrap();

  // Return a result; the waiting intake request should now resolve.
  let worker_response = json!({ "response": "The sky is blue." });
  let result_resp = client
    .post(format!("{base}/api/work/result"))
    .json(&WorkResult {
      item_id: item.id,
      worker_id: "test-worker".into(),
      response: worker_response.clone(),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(result_resp.status(), 200);

  let intake_resp = intake_handle.await.unwrap();
  assert_eq!(intake_resp.status(), 200);
  let body: serde_json::Value = intake_resp.json().await.unwrap();
  assert_eq!(body, worker_response);
}

#[tokio::test]
async fn poll_skips_items_with_unmet_tag_requirement() {
  let nats = TestNats::acquire();

  // Build a server whose "tagged" queue requires a model tag extracted from
  // the payload.
  let server = TestServer::start_with(
    &nats.url,
    Config {
      log_level: LogLevel::Warn,
      log_format: LogFormat::Text,
      listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
      nats_url: nats.url.clone(),
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
        },
      )]),
    },
  )
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue an item that requires the "llama3.2:8b" tag.
  let _enqueue = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/tagged"))
        .json(&json!({ "model": "llama3.2:8b" }))
        .send()
        .await
        .ok()
    })
  };

  tokio::time::sleep(Duration::from_millis(100)).await;

  // A worker advertising the wrong model should see nothing.
  let wrong_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities {
        tags: vec!["gemma2:9b".to_string()],
        ..Default::default()
      },
    })
    .send()
    .await
    .unwrap();

  assert_eq!(wrong_resp.status(), 204, "wrong tag should get 204");

  // A worker advertising the correct model should receive the item.
  let right_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities {
        tags: vec!["llama3.2:8b".to_string()],
        ..Default::default()
      },
    })
    .send()
    .await
    .unwrap();

  assert_eq!(right_resp.status(), 200, "correct tag should get item");
}

#[tokio::test]
async fn poll_matches_scalar_capability() {
  let nats = TestNats::acquire();

  let server = TestServer::start_with(
    &nats.url,
    Config {
      log_level: LogLevel::Warn,
      log_format: LogFormat::Text,
      listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
      nats_url: nats.url.clone(),
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
        },
      )]),
    },
  )
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue an item requiring 8192 MB of VRAM.
  let _enqueue = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/gpu"))
        .json(&json!({ "vram_mb": 8192 }))
        .send()
        .await
        .ok()
    })
  };

  tokio::time::sleep(Duration::from_millis(100)).await;

  // A worker with insufficient VRAM should see nothing.
  let small_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities {
        scalars: HashMap::from([("vram_mb".to_string(), 4096.0)]),
        ..Default::default()
      },
    })
    .send()
    .await
    .unwrap();

  assert_eq!(small_resp.status(), 204, "insufficient scalar should get 204");

  // A worker with enough VRAM should receive the item.
  let big_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities {
        scalars: HashMap::from([("vram_mb".to_string(), 16384.0)]),
        ..Default::default()
      },
    })
    .send()
    .await
    .unwrap();

  assert_eq!(big_resp.status(), 200, "sufficient scalar should get item");
}

#[tokio::test]
async fn multiple_items_are_dispatched_fifo() {
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue two items in order.
  for i in 0..2 {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/test"))
        .json(&json!({ "seq": i }))
        .send()
        .await
        .ok()
    });
    // Small delay to guarantee ordering.
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  tokio::time::sleep(Duration::from_millis(100)).await;

  // First poll should get seq=0.
  let first: QueueItem = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

  assert_eq!(first.payload["seq"], 0, "first item should be seq 0");

  // Second poll should get seq=1.
  let second: QueueItem = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

  assert_eq!(second.payload["seq"], 1, "second item should be seq 1");

  // Third poll: queue is empty.
  let empty = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(empty.status(), 204, "queue should be empty after two polls");
}

#[tokio::test]
async fn concurrent_polls_get_different_items() {
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Enqueue two items.
  for i in 0..2 {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client
        .post(format!("{base}/test"))
        .json(&json!({ "seq": i }))
        .send()
        .await
        .ok()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Two concurrent polls should each get a different item.
  let (r1, r2) = tokio::join!(
    client
      .post(format!("{base}/api/work/poll"))
      .json(&WorkPoll {
        capabilities: WorkerCapabilities::default()
      })
      .send(),
    client
      .post(format!("{base}/api/work/poll"))
      .json(&WorkPoll {
        capabilities: WorkerCapabilities::default()
      })
      .send(),
  );

  let item1: QueueItem = r1.unwrap().json().await.unwrap();
  let item2: QueueItem = r2.unwrap().json().await.unwrap();

  assert_ne!(item1.id, item2.id, "concurrent polls should get different items");
}

// ── SSE tests ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn sse_connect_and_receive_work() {
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect a worker via SSE.
  let sse_resp = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "sse-worker-1".into(),
      capabilities: WorkerCapabilities::default(),
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
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  let body = WorkerConnect {
    worker_id: "dupe-worker".into(),
    capabilities: WorkerCapabilities::default(),
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
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
  let base = format!("http://{}", server.addr);

  let body = WorkerConnect {
    worker_id: "reconnect-worker".into(),
    capabilities: WorkerCapabilities::default(),
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
  let nats = TestNats::acquire();
  let server = TestServer::start(&nats.url).await;
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

fn broadcast_config(nats_url: &str) -> Config {
  Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    nats_url: nats_url.to_string(),
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
      },
    )]),
  }
}

#[tokio::test]
async fn broadcast_all_workers_receive_item() {
  let nats = TestNats::acquire();
  let server =
    TestServer::start_with(&nats.url, broadcast_config(&nats.url)).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect two workers.
  let sse1 = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "bw1".into(),
      capabilities: WorkerCapabilities::default(),
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
  let nats = TestNats::acquire();

  // Use a combiner that collects all response values into an array.
  let config = Config {
    log_level: LogLevel::Warn,
    log_format: LogFormat::Text,
    listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
    nats_url: nats.url.clone(),
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
      },
    )]),
  };

  let server = TestServer::start_with(&nats.url, config).await;
  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // Connect two workers.
  let sse1 = client
    .post(format!("{base}/api/work/connect"))
    .json(&WorkerConnect {
      worker_id: "mw1".into(),
      capabilities: WorkerCapabilities::default(),
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
  let nats = TestNats::acquire();

  let server = TestServer::start_with(
    &nats.url,
    Config {
      log_level: LogLevel::Warn,
      log_format: LogFormat::Text,
      listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
      nats_url: nats.url.clone(),
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
        },
      )]),
    },
  )
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

  // GET /get-test — no body.
  let intake_handle = {
    let client = client.clone();
    let base = base.clone();
    tokio::spawn(async move {
      client.get(format!("{base}/get-test")).send().await.unwrap()
    })
  };

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Poll — should get item with empty JSON object payload.
  let poll_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(poll_resp.status(), 200, "expected item from poll");
  let item: QueueItem = poll_resp.json().await.unwrap();
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
async fn delegation_hints_present_in_polled_item() {
  let nats = TestNats::acquire();

  let server = TestServer::start_with(
    &nats.url,
    Config {
      log_level: LogLevel::Warn,
      log_format: LogFormat::Text,
      listen_address: "127.0.0.1:0".parse::<ListenerAddress>().unwrap(),
      nats_url: nats.url.clone(),
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
        },
      )]),
    },
  )
  .await;

  let base = format!("http://{}", server.addr);
  let client = reqwest::Client::new();

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

  tokio::time::sleep(Duration::from_millis(100)).await;

  // Poll — should get item with delegation hints.
  let poll_resp = client
    .post(format!("{base}/api/work/poll"))
    .json(&WorkPoll {
      capabilities: WorkerCapabilities::default(),
    })
    .send()
    .await
    .unwrap();

  assert_eq!(poll_resp.status(), 200);
  let item: QueueItem = poll_resp.json().await.unwrap();
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
