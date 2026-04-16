use prometheus::IntGauge;
use rust_template_foundation::server::health::{ComponentHealth, HealthCheck};

/// Reports degraded health when no workers are connected.
pub struct WorkerConnectivityCheck {
  workers_connected: IntGauge,
}

impl WorkerConnectivityCheck {
  pub fn new(workers_connected: IntGauge) -> Self {
    Self { workers_connected }
  }
}

impl HealthCheck for WorkerConnectivityCheck {
  fn check(&self) -> ComponentHealth {
    if self.workers_connected.get() > 0 {
      ComponentHealth::Healthy
    } else {
      ComponentHealth::Degraded("no workers connected".to_string())
    }
  }
}
