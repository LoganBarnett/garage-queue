use crate::config::Config;
use crate::intake::CompiledQueue;
use garage_queue_lib::protocol::QueueItem;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

/// Shared server state passed to every Axum handler.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub jetstream: async_nats::jetstream::Context,

    /// Compiled jq extractors for each configured queue, built at startup.
    pub compiled_queues: Arc<HashMap<String, CompiledQueue>>,

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
            config,
            jetstream,
            compiled_queues: Arc::new(compiled_queues),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
