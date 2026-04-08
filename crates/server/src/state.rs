use crate::config::Config;
use crate::intake::CompiledQueue;
use crate::registry::WorkerRegistry;
use garage_queue_lib::protocol::QueueItem;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use uuid::Uuid;

/// The reloadable portion of server state: config and compiled extractors.
///
/// Wrapped in an RwLock so that a future SIGHUP handler can atomically swap
/// both fields together without touching the runtime queues or NATS context.
pub struct LiveConfig {
  pub config: Arc<Config>,
  pub compiled_queues: HashMap<String, CompiledQueue>,
}

/// Tracks an in-flight broadcast dispatch awaiting responses from all workers.
pub struct BroadcastPending {
  /// Worker IDs we are still waiting on.
  pub remaining: HashSet<String>,

  /// Responses collected so far: (worker_id, response).
  pub responses: Vec<(String, serde_json::Value)>,

  /// Queue name, used to look up the combiner at completion time.
  pub queue: String,

  /// Channel to deliver the combined result to the waiting producer.
  pub tx: oneshot::Sender<serde_json::Value>,
}

/// Shared server state passed to every Axum handler.
#[derive(Clone)]
pub struct AppState {
  pub live: Arc<RwLock<LiveConfig>>,
  pub jetstream: async_nats::jetstream::Context,

  /// Items waiting to be assigned to a worker, ordered by arrival time.
  pub pending: Arc<Mutex<VecDeque<QueueItem>>>,

  /// Channels to notify a waiting producer when its item's result arrives.
  /// Keyed by item ID.  Used for exclusive-mode items.
  pub pending_responses:
    Arc<Mutex<HashMap<Uuid, oneshot::Sender<serde_json::Value>>>>,

  /// Registry of workers connected via SSE.
  pub registry: Arc<Mutex<WorkerRegistry>>,

  /// In-flight broadcast dispatches keyed by item ID.
  pub broadcast_pending: Arc<Mutex<HashMap<Uuid, BroadcastPending>>>,

  /// Oneshot senders for broadcast items that are pending in the queue
  /// because no workers were available at dispatch time.
  pub pending_broadcast_tx:
    Arc<Mutex<HashMap<Uuid, oneshot::Sender<serde_json::Value>>>>,
}

impl AppState {
  pub fn new(
    config: Arc<Config>,
    jetstream: async_nats::jetstream::Context,
    compiled_queues: HashMap<String, CompiledQueue>,
  ) -> Self {
    Self {
      live: Arc::new(RwLock::new(LiveConfig {
        config,
        compiled_queues,
      })),
      jetstream,
      pending: Arc::new(Mutex::new(VecDeque::new())),
      pending_responses: Arc::new(Mutex::new(HashMap::new())),
      registry: Arc::new(Mutex::new(WorkerRegistry::new())),
      broadcast_pending: Arc::new(Mutex::new(HashMap::new())),
      pending_broadcast_tx: Arc::new(Mutex::new(HashMap::new())),
    }
  }
}
