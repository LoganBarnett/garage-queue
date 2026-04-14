use crate::config::{Config, QueueMode};
use crate::intake::CompiledQueue;
use crate::registry::WorkerRegistry;
use garage_queue_lib::protocol::QueueItem;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use uuid::Uuid;

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
}

impl AppState {
  pub fn new(
    config: Arc<Config>,
    compiled_queues: HashMap<String, CompiledQueue>,
  ) -> Self {
    Self {
      live: Arc::new(RwLock::new(LiveConfig {
        config,
        compiled_queues,
      })),
      queue: Arc::new(Mutex::new(IndexMap::new())),
      registry: Arc::new(Mutex::new(WorkerRegistry::new())),
    }
  }
}
