mod config;

use clap::Parser;
use config::{CliRaw, Config, ConfigError, DelegatorConfig};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use garage_queue_lib::protocol::{WorkResult, WorkerConnect};
use garage_queue_worker::control::{
  self, status_channel, ControlError, ControlState, WorkerStatus,
};
use garage_queue_worker::delegator::HttpDelegator;
use garage_queue_worker::health::ServerConnectivityCheck;
use garage_queue_worker::metrics::WorkerMetrics;
use prometheus::Registry;
use rust_template_foundation::logging::init_server_logging;
use rust_template_foundation::server::health::HealthRegistry;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

#[derive(Debug, Error)]
enum ApplicationError {
  #[error("Failed to load configuration: {0}")]
  ConfigurationLoad(#[from] ConfigError),

  #[error("{0}")]
  ControlServer(#[from] ControlError),
}

#[tokio::main]
async fn main() -> Result<(), ApplicationError> {
  let cli = CliRaw::parse();
  let config = Config::from_cli_and_file(cli)
    .map_err(ApplicationError::ConfigurationLoad)?;

  init_server_logging(config.log_level, config.log_format);
  info!(worker_id = %config.worker_id, "Starting garage-queue-worker");

  let (status_tx, status_rx) = status_channel();
  let status_tx = Arc::new(status_tx);

  let delegator = match &config.delegator {
    DelegatorConfig::Http { url } => HttpDelegator::new(url),
  };

  // Metrics and health infrastructure.
  let connected = Arc::new(AtomicBool::new(false));
  let metrics_registry = Arc::new(Registry::new());
  let metrics = WorkerMetrics::register(&metrics_registry);
  metrics
    .capacity_total
    .set(config.concurrency.default as i64);

  let health_registry = HealthRegistry::default();
  health_registry
    .register("server", ServerConnectivityCheck::new(Arc::clone(&connected)))
    .await;

  let control_state = ControlState {
    status_tx: Arc::clone(&status_tx),
    health_registry,
    metrics_registry,
  };

  let control_bind = config.control_bind;
  tokio::spawn(async move {
    if let Err(e) = control::serve(control_bind, control_state).await {
      error!(error = %e, "Control server failed");
      std::process::exit(1);
    }
  });

  run_sse_loop(config, delegator, status_rx, connected, metrics).await;

  info!("Shutdown complete");
  Ok(())
}

async fn run_sse_loop(
  config: Config,
  delegator: HttpDelegator,
  mut status: control::StatusReceiver,
  connected: Arc<AtomicBool>,
  metrics: WorkerMetrics,
) {
  let client = reqwest::Client::new();
  let connect_url = format!("{}/api/work/connect", config.server_url);
  let result_url = format!("{}/api/work/result", config.server_url);
  let reconnect_interval = Duration::from_millis(config.reconnect_interval_ms);

  let connect_body = WorkerConnect {
    worker_id: config.worker_id.clone(),
    capabilities: config.capabilities.clone(),
    concurrency: Some(config.concurrency.clone()),
  };

  let delegator = Arc::new(delegator);

  loop {
    // Check status before connecting.
    let current_status = *status.borrow();
    match current_status {
      WorkerStatus::StoppingImmediate => {
        info!("Immediate stop requested");
        return;
      }
      WorkerStatus::StoppingGraceful => {
        info!("Graceful stop: not connected, exiting");
        return;
      }
      WorkerStatus::Paused => {
        if status.changed().await.is_err() {
          return;
        }
        continue;
      }
      WorkerStatus::Running => {}
    }

    info!(url = %connect_url, "Connecting to server via SSE");

    let response = client.post(&connect_url).json(&connect_body).send().await;

    let response = match response {
      Ok(r) if r.status().is_success() => r,
      Ok(r) => {
        warn!(
          status = %r.status(),
          "SSE connect rejected, retrying"
        );
        connected.store(false, Ordering::Relaxed);
        tokio::time::sleep(reconnect_interval).await;
        continue;
      }
      Err(e) => {
        warn!(error = %e, "SSE connect failed, retrying");
        connected.store(false, Ordering::Relaxed);
        tokio::time::sleep(reconnect_interval).await;
        continue;
      }
    };

    connected.store(true, Ordering::Relaxed);
    info!("SSE connection established");

    let mut stream = response.bytes_stream().eventsource();
    let mut join_set = JoinSet::new();

    loop {
      // Check for immediate stop.
      if *status.borrow() == WorkerStatus::StoppingImmediate {
        info!("Immediate stop: aborting in-flight items");
        join_set.abort_all();
        return;
      }

      // Check for graceful stop: stop reading new events, but wait for
      // in-flight items to finish.
      if *status.borrow() == WorkerStatus::StoppingGraceful {
        info!(
          in_flight = join_set.len(),
          "Graceful stop: finishing in-flight items"
        );
        while let Some(result) = join_set.join_next().await {
          if let Err(e) = result {
            warn!(error = %e, "In-flight task failed during graceful stop");
          }
        }
        return;
      }

      tokio::select! {
        ev = stream.next() => {
          let event = match ev {
            Some(Ok(ev)) => ev,
            Some(Err(e)) => {
              warn!(error = %e, "SSE stream error, reconnecting");
              break;
            }
            None => {
              warn!("SSE stream ended, reconnecting");
              break;
            }
          };

          if event.event != "work" {
            continue;
          }

          let item: garage_queue_lib::protocol::QueueItem =
            match serde_json::from_str(&event.data) {
              Ok(item) => item,
              Err(e) => {
                error!(error = %e, "Failed to deserialise work item from SSE");
                continue;
              }
            };

          info!(item_id = %item.id, queue = %item.queue, "Received item");

          metrics.items_processing.inc();

          // Spawn concurrent processing task.
          let delegator = Arc::clone(&delegator);
          let client = client.clone();
          let result_url = result_url.clone();
          let worker_id = config.worker_id.clone();
          let metrics = metrics.clone();

          join_set.spawn(async move {
            info!(item_id = %item.id, "Processing item");

            let start = Instant::now();

            match delegator
              .delegate(
                &item.payload,
                item.delegate_path.as_deref(),
                item.delegate_method.as_deref(),
              )
              .await
            {
              Ok(response) => {
                let work_result = WorkResult {
                  item_id: item.id,
                  worker_id,
                  response,
                };
                if let Err(e) =
                  client.post(&result_url).json(&work_result).send().await
                {
                  error!(
                    item_id = %item.id,
                    error = %e,
                    "Failed to submit result"
                  );
                }
                metrics.items_processed_total.inc();
                metrics.item_duration_seconds.observe(start.elapsed().as_secs_f64());
                info!(item_id = %item.id, "Item complete");
              }
              Err(e) => {
                error!(
                  item_id = %item.id,
                  error = %e,
                  "Delegation failed"
                );
              }
            }
            metrics.items_processing.dec();
          });
        }
        _ = status.changed() => {
          let s = *status.borrow();
          if s == WorkerStatus::StoppingImmediate {
            info!("Immediate stop: aborting in-flight items");
            join_set.abort_all();
            return;
          }
          if s == WorkerStatus::StoppingGraceful {
            info!(
              in_flight = join_set.len(),
              "Graceful stop: finishing in-flight items"
            );
            while let Some(result) = join_set.join_next().await {
              if let Err(e) = result {
                warn!(error = %e, "In-flight task failed during graceful stop");
              }
            }
            return;
          }
          // Paused or resumed — continue the event loop.
          continue;
        }
        Some(result) = join_set.join_next() => {
          // Reap completed tasks.
          if let Err(e) = result {
            warn!(error = %e, "Task panicked");
          }
        }
      }
    }

    connected.store(false, Ordering::Relaxed);

    // Reconnect after stream ends.  Wait for in-flight items first.
    if !join_set.is_empty() {
      info!(
        in_flight = join_set.len(),
        "Waiting for in-flight items before reconnect"
      );
      while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
          warn!(error = %e, "In-flight task failed during reconnect");
        }
      }
    }

    if *status.borrow() == WorkerStatus::StoppingGraceful {
      info!("Graceful stop: disconnected, exiting");
      return;
    }

    tokio::time::sleep(reconnect_interval).await;
  }
}
