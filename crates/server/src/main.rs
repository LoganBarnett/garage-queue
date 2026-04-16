use garage_queue_server::{
  build_router,
  config::{CliRaw, Config, ConfigError},
  intake::{CompiledQueue, IntakeError},
  AppState,
};

use clap::Parser;
use rust_template_foundation::{
  logging::init_server_logging,
  server::{
    shutdown::shutdown_signal,
    systemd::{notify_ready, spawn_watchdog},
  },
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
enum ApplicationError {
  #[error("Failed to load configuration: {0}")]
  ConfigurationLoad(#[from] ConfigError),

  #[error("Failed to build queue extractor for queue '{queue}': {source}")]
  ExtractorBuild { queue: String, source: IntakeError },

  #[error("Failed to bind listener to '{address}': {source}")]
  ListenerBind {
    address: String,
    source: std::io::Error,
  },

  #[error("Server runtime error: {0}")]
  ServerRuntime(#[source] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), ApplicationError> {
  let cli = CliRaw::parse();
  let config = Config::from_cli_and_file(cli)
    .map_err(ApplicationError::ConfigurationLoad)?;

  init_server_logging(config.log_level, config.log_format);
  info!("Starting garage-queue-server");

  let compiled_queues = build_compiled_queues(&config)?;
  info!(queues = %compiled_queues.len(), "Queue extractors compiled");

  let config = Arc::new(config);
  let state = AppState::new(Arc::clone(&config), compiled_queues);

  let app = build_router(state, &config);

  let listener = tokio_listener::Listener::bind(
    &config.listen_address,
    &tokio_listener::SystemOptions::default(),
    &tokio_listener::UserOptions::default(),
  )
  .await
  .map_err(|source| ApplicationError::ListenerBind {
    address: config.listen_address.to_string(),
    source,
  })?;

  info!(address = %config.listen_address, "Listening");

  notify_ready();
  spawn_watchdog();

  axum::serve(listener, app.into_make_service())
    .with_graceful_shutdown(shutdown_signal())
    .await
    .map_err(ApplicationError::ServerRuntime)?;

  info!("Shutdown complete");
  Ok(())
}

fn build_compiled_queues(
  config: &Config,
) -> Result<HashMap<String, CompiledQueue>, ApplicationError> {
  config
    .queues
    .iter()
    .map(|(name, queue_cfg)| {
      CompiledQueue::build(queue_cfg)
        .map(|compiled| (name.clone(), compiled))
        .map_err(|source| ApplicationError::ExtractorBuild {
          queue: name.clone(),
          source,
        })
    })
    .collect()
}
