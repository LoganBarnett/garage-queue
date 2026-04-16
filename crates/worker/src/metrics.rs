use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};

const DURATION_BUCKETS: &[f64] = &[
  0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Prometheus metrics for the queue worker.
#[derive(Clone)]
pub struct WorkerMetrics {
  pub items_processed_total: IntCounter,
  pub items_processing: IntGauge,
  pub capacity_total: IntGauge,
  pub item_duration_seconds: Histogram,
}

impl WorkerMetrics {
  /// Create and register all worker metrics with the given registry.
  pub fn register(registry: &Registry) -> Self {
    let items_processed_total = IntCounter::new(
      "gq_worker_items_processed_total",
      "Total items processed to completion",
    )
    .expect("metric definition is valid");

    let items_processing =
      IntGauge::new("gq_worker_items_processing", "Currently in-flight items")
        .expect("metric definition is valid");

    let capacity_total =
      IntGauge::new("gq_worker_capacity_total", "Configured concurrency")
        .expect("metric definition is valid");

    let item_duration_seconds = Histogram::with_opts(
      HistogramOpts::new(
        "gq_worker_item_duration_seconds",
        "Per-item processing time",
      )
      .buckets(DURATION_BUCKETS.to_vec()),
    )
    .expect("metric definition is valid");

    registry
      .register(Box::new(items_processed_total.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(items_processing.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(capacity_total.clone()))
      .expect("metric registration should not conflict");
    registry
      .register(Box::new(item_duration_seconds.clone()))
      .expect("metric registration should not conflict");

    Self {
      items_processed_total,
      items_processing,
      capacity_total,
      item_duration_seconds,
    }
  }
}
