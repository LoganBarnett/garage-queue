mod config;
mod control;
mod delegator;
mod logging;

use clap::Parser;
use config::{CliRaw, Config, ConfigError, DelegatorConfig};
use control::{WorkerStatus, status_channel};
use delegator::HttpDelegator;
use garage_queue_lib::protocol::{WorkPoll, WorkResult};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info, warn};

#[derive(Debug, Error)]
enum ApplicationError {
    #[error("Failed to load configuration: {0}")]
    ConfigurationLoad(#[from] ConfigError),
}

#[tokio::main]
async fn main() -> Result<(), ApplicationError> {
    let cli = CliRaw::parse();
    let config =
        Config::from_cli_and_file(cli).map_err(ApplicationError::ConfigurationLoad)?;

    logging::init_logging(config.log_level, config.log_format);
    info!("Starting garage-queue-worker");

    let (status_tx, status_rx) = status_channel();
    let status_tx = Arc::new(status_tx);

    let delegator = match &config.delegator {
        DelegatorConfig::Http { url } => HttpDelegator::new(url),
    };

    let control_bind = config.control_bind;
    let control_tx = Arc::clone(&status_tx);
    tokio::spawn(async move {
        control::serve(control_bind, control_tx).await;
    });

    run_poll_loop(config, delegator, status_rx).await;

    info!("Shutdown complete");
    Ok(())
}

async fn run_poll_loop(
    config: Config,
    delegator: HttpDelegator,
    mut status: control::StatusReceiver,
) {
    let client = reqwest::Client::new();
    let poll_url = format!("{}/api/work/poll", config.server_url);
    let result_url = format!("{}/api/work/result", config.server_url);
    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    loop {
        // Check current status before doing anything.
        let current_status = *status.borrow();
        match current_status {
            WorkerStatus::StoppingImmediate => {
                info!("Immediate stop requested");
                return;
            }
            WorkerStatus::StoppingGraceful | WorkerStatus::Paused => {
                if current_status == WorkerStatus::StoppingGraceful {
                    // No item in flight at this point; safe to exit.
                    info!("Graceful stop: no item in progress, exiting");
                    return;
                }
                // Paused: wait for status to change before polling.
                if status.changed().await.is_err() {
                    return;
                }
                continue;
            }
            WorkerStatus::Running => {}
        }

        let poll_body = WorkPoll { capabilities: config.capabilities.clone() };

        let response = client
            .post(&poll_url)
            .json(&poll_body)
            .send()
            .await;

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Poll request failed, retrying");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if response.status() == reqwest::StatusCode::NO_CONTENT {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let item = match response.json::<garage_queue_lib::protocol::QueueItem>().await {
            Ok(item) => item,
            Err(e) => {
                error!(error = %e, "Failed to deserialise work item");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        info!(item_id = %item.id, queue = %item.queue, "Processing item");

        // Check for immediate stop before we start processing.
        if *status.borrow() == WorkerStatus::StoppingImmediate {
            info!(item_id = %item.id, "Immediate stop: abandoning item (will be requeued)");
            return;
        }

        match delegator.delegate(&item.payload).await {
            Ok(response) => {
                let result = WorkResult { item_id: item.id, response };
                if let Err(e) = client.post(&result_url).json(&result).send().await {
                    error!(item_id = %item.id, error = %e, "Failed to submit result");
                }
                info!(item_id = %item.id, "Item complete");
            }
            Err(e) => {
                error!(item_id = %item.id, error = %e, "Delegation failed");
                // The item's producer will time out waiting.  Future: dead
                // letter queue or explicit error result.
            }
        }

        // After completing an item, honour a graceful stop request.
        if *status.borrow() == WorkerStatus::StoppingGraceful {
            info!("Graceful stop: item complete, exiting");
            return;
        }
    }
}
