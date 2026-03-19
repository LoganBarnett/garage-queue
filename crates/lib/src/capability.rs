use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single capability requirement that must be satisfied before a queue item
/// can be assigned to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum CapabilityRequirement {
    /// The worker must possess this tag exactly.
    Tag { value: String },

    /// The worker's scalar for this name must be >= the required value.
    ///
    /// Scalar matching always uses >= semantics: the worker is advertising
    /// capacity, and the item is advertising consumption. A worker with
    /// vram_mb = 16384 satisfies an item requiring vram_mb = 8192.
    Scalar { name: String, value: f64 },
}

/// Capabilities advertised by a worker when requesting items.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    /// Tags this worker possesses (e.g. model names, hardware flags).
    #[serde(default)]
    pub tags: Vec<String>,

    /// Named scalar capacities (e.g. vram_mb, daily_tokens_remaining).
    #[serde(default)]
    pub scalars: HashMap<String, f64>,
}

impl WorkerCapabilities {
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    pub fn scalar(&self, name: &str) -> Option<f64> {
        self.scalars.get(name).copied()
    }
}
