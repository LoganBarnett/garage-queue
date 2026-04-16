mod commands;
mod config;

use clap::Parser;
use config::{CliRaw, Command, Config, ConfigError};
use rust_template_foundation::logging::init_cli_logging;
use thiserror::Error;

#[derive(Debug, Error)]
enum ApplicationError {
  #[error("Failed to load configuration: {0}")]
  ConfigurationLoad(#[from] ConfigError),

  #[error("{0}")]
  Command(#[from] commands::CommandError),
}

#[tokio::main]
async fn main() -> Result<(), ApplicationError> {
  let cli = CliRaw::parse();
  let config = Config::from_cli_and_file(cli)?;
  init_cli_logging(config.log_level, config.log_format);

  match config.command {
    Command::Health => commands::health(&config.server_url).await?,
    Command::Generate { payload } => {
      commands::generate(&config.server_url, payload).await?
    }
  }

  Ok(())
}
