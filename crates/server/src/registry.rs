use garage_queue_lib::capability::WorkerCapabilities;
use garage_queue_lib::matching::satisfies;
use garage_queue_lib::protocol::QueueItem;
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
  pub busy: bool,
  pub tx: mpsc::Sender<QueueItem>,
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
        busy: false,
        tx,
      },
    );
    Ok(())
  }

  pub fn deregister(&mut self, worker_id: &str) {
    self.workers.remove(worker_id);
  }

  pub fn mark_busy(&mut self, worker_id: &str) {
    if let Some(w) = self.workers.get_mut(worker_id) {
      w.busy = true;
    }
  }

  pub fn mark_idle(&mut self, worker_id: &str) {
    if let Some(w) = self.workers.get_mut(worker_id) {
      w.busy = false;
    }
  }

  /// Returns idle workers whose capabilities satisfy the given requirements.
  pub fn idle_workers_matching(
    &self,
    requirements: &[garage_queue_lib::capability::CapabilityRequirement],
  ) -> Vec<&ConnectedWorker> {
    self
      .workers
      .values()
      .filter(|w| !w.busy && satisfies(requirements, &w.capabilities))
      .collect()
  }

  /// Returns all workers (regardless of busy state) whose capabilities
  /// satisfy the given requirements.
  pub fn all_workers_matching(
    &self,
    requirements: &[garage_queue_lib::capability::CapabilityRequirement],
  ) -> Vec<&ConnectedWorker> {
    self
      .workers
      .values()
      .filter(|w| satisfies(requirements, &w.capabilities))
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
    reg.register("w1".into(), caps(&[]), channel()).unwrap();
    assert_eq!(reg.workers.len(), 1);
    reg.deregister("w1");
    assert_eq!(reg.workers.len(), 0);
  }

  #[test]
  fn duplicate_registration_rejected() {
    let mut reg = WorkerRegistry::new();
    reg.register("w1".into(), caps(&[]), channel()).unwrap();
    let err = reg.register("w1".into(), caps(&[]), channel()).unwrap_err();
    assert_eq!(err.worker_id, "w1");
  }

  #[test]
  fn idle_workers_matching_respects_busy() {
    let mut reg = WorkerRegistry::new();
    reg.register("w1".into(), caps(&["a"]), channel()).unwrap();
    reg.register("w2".into(), caps(&["a"]), channel()).unwrap();

    reg.mark_busy("w1");

    let idle = reg.idle_workers_matching(&[]);
    assert_eq!(idle.len(), 1);
    assert_eq!(idle[0].worker_id, "w2");
  }

  #[test]
  fn all_workers_matching_ignores_busy() {
    let mut reg = WorkerRegistry::new();
    reg.register("w1".into(), caps(&["a"]), channel()).unwrap();
    reg.register("w2".into(), caps(&["a"]), channel()).unwrap();

    reg.mark_busy("w1");

    let all = reg.all_workers_matching(&[]);
    assert_eq!(all.len(), 2);
  }
}
