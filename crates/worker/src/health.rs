use rust_template_foundation::server::health::{ComponentHealth, HealthCheck};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Reports degraded health when the worker is not connected to the server.
pub struct ServerConnectivityCheck {
  connected: Arc<AtomicBool>,
}

impl ServerConnectivityCheck {
  pub fn new(connected: Arc<AtomicBool>) -> Self {
    Self { connected }
  }
}

impl HealthCheck for ServerConnectivityCheck {
  fn check(&self) -> ComponentHealth {
    if self.connected.load(Ordering::Relaxed) {
      ComponentHealth::Healthy
    } else {
      ComponentHealth::Degraded("not connected to server".to_string())
    }
  }
}
