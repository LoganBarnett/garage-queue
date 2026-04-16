use prometheus::{
  HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
  Registry,
};

const DURATION_BUCKETS: &[f64] = &[
  0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Prometheus metrics for the queue server.
#[derive(Clone)]
pub struct ServerMetrics {
  pub queue_depth: IntGaugeVec,
  pub items_enqueued_total: IntCounterVec,
  pub items_completed_total: IntCounterVec,
  pub item_duration_seconds: HistogramVec,
  pub workers_connected: IntGauge,
  pub items_in_flight: IntGauge,
}

impl ServerMetrics {
  /// Create and register all server metrics with the given registry.
  pub fn register(registry: &Registry) -> Self {
    let queue_depth = IntGaugeVec::new(
      Opts::new("gq_server_queue_depth", "Current items in the queue"),
      &["queue"],
    )
    .expect("metric definition is valid");

    let items_enqueued_total = IntCounterVec::new(
      Opts::new("gq_server_items_enqueued_total", "Total items enqueued"),
      &["queue"],
    )
    .expect("metric definition is valid");

    let items_completed_total = IntCounterVec::new(
      Opts::new("gq_server_items_completed_total", "Total items completed"),
      &["queue"],
    )
    .expect("metric definition is valid");

    let item_duration_seconds = HistogramVec::new(
      HistogramOpts::new(
        "gq_server_item_duration_seconds",
        "Enqueue-to-completion latency",
      )
      .buckets(DURATION_BUCKETS.to_vec()),
      &["queue"],
    )
    .expect("metric definition is valid");

    let workers_connected =
      IntGauge::new("gq_server_workers_connected", "Connected SSE workers")
        .expect("metric definition is valid");

    let items_in_flight = IntGauge::new(
      "gq_server_items_in_flight",
      "Items dispatched but not yet completed",
    )
    .expect("metric definition is valid");

    registry
      .register(Box::new(queue_depth.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(items_enqueued_total.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(items_completed_total.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(item_duration_seconds.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(workers_connected.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(items_in_flight.clone()))
      .expect("metric registration should not conflict");

    Self {
      queue_depth,
      items_enqueued_total,
      items_completed_total,
      item_duration_seconds,
      workers_connected,
      items_in_flight,
    }
  }
}
