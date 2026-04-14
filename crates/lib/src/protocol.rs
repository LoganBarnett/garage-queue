use crate::capability::{CapabilityRequirement, WorkerCapabilities};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// An item stored in the queue awaiting processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueItem {
  /// Unique identifier assigned at intake.
  pub id: Uuid,

  /// Name of the queue this item belongs to (matches a key in server config).
  pub queue: String,

  /// Raw payload to be forwarded verbatim to the worker's delegator.
  pub payload: serde_json::Value,

  /// Capability requirements extracted from the payload at intake time.
  pub requirements: Vec<CapabilityRequirement>,

  /// Optional URL path the worker should use when delegating this item,
  /// overriding the worker's default configured URL path.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub delegate_path: Option<String>,

  /// Optional HTTP method the worker should use when delegating this item
  /// (e.g. "get", "post").  Overrides the worker's default POST.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub delegate_method: Option<String>,
}

/// Per-queue concurrency limits advertised by a worker at connect time.
///
/// The `default` field sets the concurrency for any queue not listed in
/// `overrides`.  Workers without this field default to sequential processing
/// (concurrency 1 everywhere).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
  pub default: u32,
  #[serde(default)]
  pub overrides: HashMap<String, u32>,
}

impl ConcurrencyConfig {
  /// Return the concurrency limit for a given queue name.
  pub fn limit_for(&self, queue: &str) -> u32 {
    self.overrides.get(queue).copied().unwrap_or(self.default)
  }
}

impl Default for ConcurrencyConfig {
  fn default() -> Self {
    Self {
      default: 1,
      overrides: HashMap::new(),
    }
  }
}

/// Body sent by a worker when establishing an SSE connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConnect {
  pub worker_id: String,
  pub capabilities: WorkerCapabilities,
  /// Per-queue concurrency limits.  When absent the worker defaults to
  /// sequential processing (concurrency 1 on all queues).
  #[serde(default)]
  pub concurrency: Option<ConcurrencyConfig>,
}

/// Body of a worker's result submission.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkResult {
  /// ID of the item being completed.
  pub item_id: Uuid,

  /// Identifies which worker produced this result.
  pub worker_id: String,

  /// The response from the delegator, returned verbatim to the producer.
  pub response: serde_json::Value,
}
