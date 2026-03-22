mod systemd;

use garage_queue_server::{
    build_router,
    config::{CliRaw, Config, ConfigError},
    intake::{CompiledQueue, IntakeError},
    logging::init_logging,
    state::AppState,
};

use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::signal;
use tracing::info;

#[derive(Debug, Error)]
enum ApplicationError {
    #[error("Failed to load configuration: {0}")]
    ConfigurationLoad(#[from] ConfigError),

    #[error("Failed to build queue extractor for queue '{queue}': {source}")]
    ExtractorBuild { queue: String, source: IntakeError },

    #[error("Failed to connect to NATS at '{url}': {message}")]
    NatsConnect { url: String, message: String },

    #[error("Failed to create or verify NATS stream: {0}")]
    NatsStream(String),

    #[error("Failed to bind listener to '{address}': {source}")]
    ListenerBind { address: String, source: std::io::Error },

    #[error("Server runtime error: {0}")]
    ServerRuntime(#[source] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), ApplicationError> {
    let cli = CliRaw::parse();
    let config =
        Config::from_cli_and_file(cli).map_err(ApplicationError::ConfigurationLoad)?;

    init_logging(config.log_level, config.log_format);
    info!("Starting garage-queue-server");

    let compiled_queues = build_compiled_queues(&config)?;
    info!(queues = %compiled_queues.len(), "Queue extractors compiled");

    let nats = async_nats::connect(&config.nats_url)
        .await
        .map_err(|e| ApplicationError::NatsConnect {
            url: config.nats_url.clone(),
            message: e.to_string(),
        })?;

    let jetstream = async_nats::jetstream::new(nats);

    jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "GARAGE_QUEUE_ITEMS".to_string(),
            subjects: vec!["items.>".to_string()],
            ..Default::default()
        })
        .await
        .map_err(|e| ApplicationError::NatsStream(e.to_string()))?;

    info!("Connected to NATS and stream ready");

    let config = Arc::new(config);
    let state = AppState::new(Arc::clone(&config), jetstream, compiled_queues);

    let app = build_router(state);

    let listener = tokio_listener::Listener::bind(
        &config.listen_address,
        &tokio_listener::SystemOptions::default(),
        &tokio_listener::UserOptions::default(),
    )
    .await
    .map_err(|source| {
        ApplicationError::ListenerBind {
            address: config.listen_address.to_string(),
            source,
        }
    })?;

    info!(address = %config.listen_address, "Listening");

    systemd::notify_ready();
    systemd::spawn_watchdog();

    tokio_listener::axum07::serve(listener, app.into_make_service())
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

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down"),
        _ = terminate => info!("Received SIGTERM, shutting down"),
    }
}
