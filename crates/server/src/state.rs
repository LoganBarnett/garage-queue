use crate::config::Config;
use crate::intake::CompiledQueue;
use garage_queue_lib::protocol::QueueItem;
use std::collections::{HashMap, VecDeque};
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

/// Shared server state passed to every Axum handler.
#[derive(Clone)]
pub struct AppState {
  pub live: Arc<RwLock<LiveConfig>>,
  pub jetstream: async_nats::jetstream::Context,

  /// Items waiting to be assigned to a worker, ordered by arrival time.
  pub pending: Arc<Mutex<VecDeque<QueueItem>>>,

  /// Channels to notify a waiting producer when its item's result arrives.
  /// Keyed by item ID.
  pub pending_responses:
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
    }
  }
}
