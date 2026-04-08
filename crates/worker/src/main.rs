mod config;
mod logging;

use clap::Parser;
use config::{CliRaw, Config, ConfigError, DelegatorConfig};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use garage_queue_lib::protocol::{WorkResult, WorkerConnect};
use garage_queue_worker::control::{
  self, status_channel, ControlError, WorkerStatus,
};
use garage_queue_worker::delegator::HttpDelegator;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
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

  logging::init_logging(config.log_level, config.log_format);
  info!(worker_id = %config.worker_id, "Starting garage-queue-worker");

  let (status_tx, status_rx) = status_channel();
  let status_tx = Arc::new(status_tx);

  let delegator = match &config.delegator {
    DelegatorConfig::Http { url } => HttpDelegator::new(url),
  };

  let control_bind = config.control_bind;
  let control_tx = Arc::clone(&status_tx);
  tokio::spawn(async move {
    if let Err(e) = control::serve(control_bind, control_tx).await {
      error!(error = %e, "Control server failed");
      std::process::exit(1);
    }
  });

  run_sse_loop(config, delegator, status_rx).await;

  info!("Shutdown complete");
  Ok(())
}

async fn run_sse_loop(
  config: Config,
  delegator: HttpDelegator,
  mut status: control::StatusReceiver,
) {
  let client = reqwest::Client::new();
  let connect_url = format!("{}/api/work/connect", config.server_url);
  let result_url = format!("{}/api/work/result", config.server_url);
  let reconnect_interval = Duration::from_millis(config.reconnect_interval_ms);

  let connect_body = WorkerConnect {
    worker_id: config.worker_id.clone(),
    capabilities: config.capabilities.clone(),
  };

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
        tokio::time::sleep(reconnect_interval).await;
        continue;
      }
      Err(e) => {
        warn!(error = %e, "SSE connect failed, retrying");
        tokio::time::sleep(reconnect_interval).await;
        continue;
      }
    };

    info!("SSE connection established");

    let mut stream = response.bytes_stream().eventsource();

    loop {
      // Check for stop before waiting on the next event.
      if *status.borrow() == WorkerStatus::StoppingImmediate {
        info!("Immediate stop: disconnecting");
        return;
      }

      let event = tokio::select! {
        ev = stream.next() => ev,
        _ = status.changed() => {
          let s = *status.borrow();
          if s == WorkerStatus::StoppingImmediate {
            info!("Immediate stop: disconnecting");
            return;
          }
          if s == WorkerStatus::StoppingGraceful {
            info!("Graceful stop: no item in progress, exiting");
            return;
          }
          // Paused or resumed — just continue the event loop.
          continue;
        }
      };

      let event = match event {
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

      info!(item_id = %item.id, queue = %item.queue, "Processing item");

      // Check for immediate stop before delegating.
      if *status.borrow() == WorkerStatus::StoppingImmediate {
        info!(
          item_id = %item.id,
          "Immediate stop: abandoning item (will be requeued)"
        );
        return;
      }

      match delegator.delegate(&item.payload).await {
        Ok(response) => {
          let work_result = WorkResult {
            item_id: item.id,
            worker_id: config.worker_id.clone(),
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

      // After completing an item, honour a graceful stop request.
      if *status.borrow() == WorkerStatus::StoppingGraceful {
        info!("Graceful stop: item complete, exiting");
        return;
      }
    }

    // Reconnect after stream ends.
    if *status.borrow() == WorkerStatus::StoppingGraceful {
      info!("Graceful stop: disconnected, exiting");
      return;
    }

    tokio::time::sleep(reconnect_interval).await;
  }
}
