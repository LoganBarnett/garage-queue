use garage_queue_lib::capability::WorkerCapabilities;
use garage_queue_lib::protocol::{ConcurrencyConfig, QueueItem};
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
#[error("Worker '{worker_id}' is already registered")]
pub struct AlreadyRegistered {
  pub worker_id: String,
}

/// A worker that holds an active SSE connection.
pub struct ConnectedWorker {
  pub worker_id: String,
  pub capabilities: WorkerCapabilities,
  pub concurrency: ConcurrencyConfig,
  /// Number of items currently in-flight (delivered but not completed) per queue.
  pub in_flight: HashMap<String, usize>,
  pub tx: mpsc::Sender<QueueItem>,
}

impl ConnectedWorker {
  /// Returns true if the worker has a free concurrency slot for the given queue.
  pub fn is_ready_for(&self, queue_name: &str) -> bool {
    let current = self.in_flight.get(queue_name).copied().unwrap_or(0);
    let limit = self.concurrency.limit_for(queue_name) as usize;
    current < limit
  }

  pub fn increment_in_flight(&mut self, queue_name: &str) {
    *self.in_flight.entry(queue_name.to_string()).or_insert(0) += 1;
  }

  pub fn decrement_in_flight(&mut self, queue_name: &str) {
    if let Some(count) = self.in_flight.get_mut(queue_name) {
      *count = count.saturating_sub(1);
    }
  }
}

/// Tracks all workers with active SSE connections.
pub struct WorkerRegistry {
  workers: HashMap<String, ConnectedWorker>,
}

impl WorkerRegistry {
  pub fn new() -> Self {
    Self {
      workers: HashMap::new(),
    }
  }

  pub fn register(
    &mut self,
    worker_id: String,
    capabilities: WorkerCapabilities,
    concurrency: ConcurrencyConfig,
    tx: mpsc::Sender<QueueItem>,
  ) -> Result<(), AlreadyRegistered> {
    if self.workers.contains_key(&worker_id) {
      return Err(AlreadyRegistered {
        worker_id: worker_id.clone(),
      });
    }
    self.workers.insert(
      worker_id.clone(),
      ConnectedWorker {
        worker_id,
        capabilities,
        concurrency,
        in_flight: HashMap::new(),
        tx,
      },
    );
    Ok(())
  }

  pub fn deregister(&mut self, worker_id: &str) {
    self.workers.remove(worker_id);
  }

  pub fn get(&self, worker_id: &str) -> Option<&ConnectedWorker> {
    self.workers.get(worker_id)
  }

  pub fn get_mut(&mut self, worker_id: &str) -> Option<&mut ConnectedWorker> {
    self.workers.get_mut(worker_id)
  }

  /// Returns all connected worker IDs.
  pub fn worker_ids(&self) -> Vec<String> {
    self.workers.keys().cloned().collect()
  }

  /// Returns IDs of all connected workers whose capabilities satisfy the
  /// given requirements.
  pub fn matching_worker_ids(
    &self,
    requirements: &[garage_queue_lib::capability::CapabilityRequirement],
  ) -> Vec<String> {
    self
      .workers
      .values()
      .filter(|w| {
        garage_queue_lib::matching::satisfies(requirements, &w.capabilities)
      })
      .map(|w| w.worker_id.clone())
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn caps(tags: &[&str]) -> WorkerCapabilities {
    WorkerCapabilities {
      tags: tags.iter().map(|s| s.to_string()).collect(),
      ..Default::default()
    }
  }

  fn channel() -> mpsc::Sender<QueueItem> {
    let (tx, _rx) = mpsc::channel(1);
    tx
  }

  #[test]
  fn register_and_deregister() {
    let mut reg = WorkerRegistry::new();
    reg
      .register("w1".into(), caps(&[]), ConcurrencyConfig::default(), channel())
      .unwrap();
    assert!(reg.get("w1").is_some());
    reg.deregister("w1");
    assert!(reg.get("w1").is_none());
  }

  #[test]
  fn duplicate_registration_rejected() {
    let mut reg = WorkerRegistry::new();
    reg
      .register("w1".into(), caps(&[]), ConcurrencyConfig::default(), channel())
      .unwrap();
    let err = reg
      .register("w1".into(), caps(&[]), ConcurrencyConfig::default(), channel())
      .unwrap_err();
    assert_eq!(err.worker_id, "w1");
  }

  #[test]
  fn is_ready_for_respects_concurrency() {
    let mut reg = WorkerRegistry::new();
    reg
      .register(
        "w1".into(),
        caps(&[]),
        ConcurrencyConfig {
          default: 2,
          overrides: HashMap::new(),
        },
        channel(),
      )
      .unwrap();

    let worker = reg.get("w1").unwrap();
    assert!(worker.is_ready_for("test"));

    let worker = reg.get_mut("w1").unwrap();
    worker.increment_in_flight("test");
    worker.increment_in_flight("test");
    assert!(!worker.is_ready_for("test"));

    worker.decrement_in_flight("test");
    assert!(worker.is_ready_for("test"));
  }

  #[test]
  fn concurrency_overrides_per_queue() {
    let mut reg = WorkerRegistry::new();
    let mut overrides = HashMap::new();
    overrides.insert("fast-queue".to_string(), 4);
    reg
      .register(
        "w1".into(),
        caps(&[]),
        ConcurrencyConfig {
          default: 1,
          overrides,
        },
        channel(),
      )
      .unwrap();

    let worker = reg.get("w1").unwrap();
    // default queue: limit 1
    assert!(worker.is_ready_for("default-queue"));
    // fast-queue: limit 4
    assert!(worker.is_ready_for("fast-queue"));
  }
}
