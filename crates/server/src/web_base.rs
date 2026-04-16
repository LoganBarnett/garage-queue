use crate::config::{Config, Method, QueueMode};
use crate::health::WorkerConnectivityCheck;
use crate::intake::CompiledQueue;
use crate::metrics::ServerMetrics;
use crate::registry::WorkerRegistry;
use crate::routes;
use aide::axum::ApiRouter;
use aide::openapi::{Info, OpenApi};
use axum::extract::FromRef;
use axum::routing::{get, post};
use axum::Router;
use garage_queue_lib::protocol::QueueItem;
use indexmap::IndexMap;
use prometheus::Registry;
use rust_template_foundation::server::{
  health::{healthz_handler, HealthRegistry},
  metrics::metrics_handler,
  openapi::openapi_routes,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{oneshot, Mutex, RwLock};
use tower_http::trace::TraceLayer;
use uuid::Uuid;

// ── State types ──────────────────────────────────────────────────────────────

/// The reloadable portion of server state: config and compiled extractors.
///
/// Wrapped in an RwLock so that a future SIGHUP handler can atomically swap
/// both fields together without touching the runtime queues.
pub struct LiveConfig {
  pub config: Arc<Config>,
  pub compiled_queues: HashMap<String, CompiledQueue>,
}

/// A single item in the unified queue, tracking delivery and completion state.
pub struct QueueEntry {
  pub item: QueueItem,
  pub mode: QueueMode,
  /// When this item was enqueued, for duration metrics.
  pub enqueued_at: Instant,
  /// Worker IDs to which this item has been sent.
  pub delivered: HashSet<String>,
  /// Worker IDs that have responded, with their response values.
  pub completed: HashMap<String, serde_json::Value>,
  /// Channel to deliver the final result to the waiting producer.
  pub producer_tx: Option<oneshot::Sender<serde_json::Value>>,
}

/// Shared server state passed to every Axum handler.
#[derive(Clone)]
pub struct AppState {
  pub live: Arc<RwLock<LiveConfig>>,

  /// Unified queue: all items awaiting completion, ordered by arrival time.
  /// Insertion-ordered for FIFO scanning, O(1) lookup by item ID.
  pub queue: Arc<Mutex<IndexMap<Uuid, QueueEntry>>>,

  /// Registry of workers connected via SSE.
  pub registry: Arc<Mutex<WorkerRegistry>>,

  /// Composable health-check registry consumed by the /healthz handler.
  pub health_registry: HealthRegistry,

  /// Prometheus metrics registry consumed by the /metrics handler.
  pub metrics_registry: Arc<Registry>,

  /// Application-level Prometheus metrics.
  pub metrics: ServerMetrics,
}

impl AppState {
  pub fn new(
    config: Arc<Config>,
    compiled_queues: HashMap<String, CompiledQueue>,
  ) -> Self {
    let metrics_registry = Arc::new(Registry::new());
    let metrics = ServerMetrics::register(&metrics_registry);
    Self {
      live: Arc::new(RwLock::new(LiveConfig {
        config,
        compiled_queues,
      })),
      queue: Arc::new(Mutex::new(IndexMap::new())),
      registry: Arc::new(Mutex::new(WorkerRegistry::new())),
      health_registry: HealthRegistry::default(),
      metrics_registry,
      metrics,
    }
  }

  /// Register health checks that depend on metrics state.  Separate from
  /// `new` because `HealthRegistry::register` is async.
  pub async fn register_health_checks(&self) {
    self
      .health_registry
      .register(
        "workers",
        WorkerConnectivityCheck::new(self.metrics.workers_connected.clone()),
      )
      .await;
  }
}

impl FromRef<AppState> for HealthRegistry {
  fn from_ref(state: &AppState) -> Self {
    state.health_registry.clone()
  }
}

impl FromRef<AppState> for Arc<Registry> {
  fn from_ref(state: &AppState) -> Self {
    state.metrics_registry.clone()
  }
}

// ── Router assembly ──────────────────────────────────────────────────────────

/// Assemble the application router with all routes and middleware attached.
/// The caller is responsible for binding a listener and calling axum::serve.
pub fn build_router(state: AppState, config: &Config) -> Router {
  aide::generate::on_error(|error| {
    tracing::warn!(%error, "OpenAPI generation error");
  });

  let mut api = OpenApi {
    info: Info {
      title: "garage-queue".to_string(),
      ..Default::default()
    },
    ..Default::default()
  };

  // Infrastructure routes (health, metrics) registered via ApiRouter so
  // they appear in the generated OpenAPI spec.
  let infra: Router<AppState> = ApiRouter::new()
    .route("/healthz", get(healthz_handler))
    .route("/metrics", get(metrics_handler))
    .finish_api(&mut api);

  let openapi = openapi_routes(Arc::new(api), "garage-queue");

  // Application routes.
  let mut app = Router::new()
    .route("/api/work/connect", post(routes::sse::connect))
    .route("/api/work/result", post(routes::work::result));

  for queue_cfg in config.queues.values() {
    if let (Some(ref path), Some(method)) = (&queue_cfg.route, queue_cfg.method)
    {
      let method_router = match method {
        Method::Get => get(routes::intake::handle_intake),
        Method::Post => post(routes::intake::handle_intake),
        Method::Put => axum::routing::put(routes::intake::handle_intake),
        Method::Patch => axum::routing::patch(routes::intake::handle_intake),
        Method::Delete => axum::routing::delete(routes::intake::handle_intake),
      };
      app = app.route(path, method_router);
    }
  }

  app
    .merge(infra)
    .layer(TraceLayer::new_for_http())
    .with_state(state)
    .merge(openapi)
}
