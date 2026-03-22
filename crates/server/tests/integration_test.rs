// Integration tests for the queue server.
//
// A NATS server is started automatically on port 14222 for the duration of the
// test binary.  Set NATS_URL to point at an already-running instance instead
// (useful in CI where NATS is a sidecar service).
//
// Run with:
//   cargo test -p garage-queue-server

use async_nats::jetstream;
use garage_queue_lib::{
    capability::WorkerCapabilities,
    protocol::{QueueItem, WorkPoll, WorkResult},
};
use garage_queue_server::{
    build_router,
    config::{Config, ExtractorConfig, ExtractorKind, JqSource, QueueConfig},
    intake::CompiledQueue,
    state::AppState,
};
use garage_queue_lib::{LogFormat, LogLevel};
use serde_json::json;
use std::{
    collections::HashMap,
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, Once},
    time::Duration,
};
use tokio_listener::ListenerAddress;
use tokio::task::JoinHandle;
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
                if std::net::TcpStream::connect(("127.0.0.1", TEST_NATS_PORT))
                    .is_ok()
                {
                    break;
                }
            }

            // Intentionally not stopped on drop — the process is shared for
            // the entire test binary run and cleaned up by the OS on exit.
            *NATS_CHILD.lock().unwrap() = Some(child);
        });

        Self { url: format!("nats://127.0.0.1:{TEST_NATS_PORT}") }
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
        let nats =
            async_nats::connect(nats_url).await.expect("NATS connect failed");
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

        let state = AppState::new(Arc::new(config), js, compiled_queues);
        let app = build_router(state);

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
                    QueueConfig { route: Some("/test".to_string()), extractors: vec![] },
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
        .json(&WorkPoll { capabilities: WorkerCapabilities::default() })
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
        .json(&WorkResult { item_id: Uuid::new_v4(), response: json!({}) })
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
        .json(&WorkPoll { capabilities: WorkerCapabilities::default() })
        .send()
        .await
        .unwrap();

    assert_eq!(poll_resp.status(), 200, "expected item from poll");
    let item: QueueItem = poll_resp.json().await.unwrap();

    // Return a result; the waiting intake request should now resolve.
    let worker_response = json!({ "response": "The sky is blue." });
    let result_resp = client
        .post(format!("{base}/api/work/result"))
        .json(&WorkResult { item_id: item.id, response: worker_response.clone() })
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
                    extractors: vec![ExtractorConfig {
                        capability: "model".to_string(),
                        kind: ExtractorKind::Tag,
                        source: JqSource::Inline(".model".to_string()),
                    }],
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
