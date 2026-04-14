use crate::capability::{CapabilityRequirement, WorkerCapabilities};
use serde::{Deserialize, Serialize};
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

/// Body sent by a worker when establishing an SSE connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConnect {
  pub worker_id: String,
  pub capabilities: WorkerCapabilities,
}

/// Body of a worker's request for a new work item.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkPoll {
  pub capabilities: WorkerCapabilities,
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

/// Server response to a WorkPoll.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkPollResponse {
  /// A matching item is available.
  Item(QueueItem),
  /// No matching item is currently available; worker should retry later.
  Empty,
}
