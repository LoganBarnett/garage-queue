use clap::Parser;
use garage_queue_lib::{LogFormat, LogLevel};
use rust_template_foundation::config::{
  find_config_file, load_toml, resolve_log_settings, CommonConfigFile,
  ConfigFileError,
};
use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("Failed to load configuration file: {0}")]
  ConfigFile(#[from] ConfigFileError),

  #[error("Configuration validation failed: {0}")]
  Validation(String),
}

#[derive(Debug, Parser)]
#[command(author, version, about = "garage-queue management CLI")]
pub struct CliRaw {
  /// Log level (trace, debug, info, warn, error)
  #[arg(long, env = "LOG_LEVEL", global = true)]
  pub log_level: Option<String>,

  /// Log format (text, json)
  #[arg(long, env = "LOG_FORMAT", global = true)]
  pub log_format: Option<String>,

  /// Path to configuration file
  #[arg(short, long, env = "GARAGE_QUEUE_CONFIG", global = true)]
  pub config: Option<PathBuf>,

  /// Queue server URL (overrides config file)
  #[arg(long, env = "GARAGE_QUEUE_SERVER_URL", global = true)]
  pub server_url: Option<String>,

  #[command(subcommand)]
  pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
  /// Check whether the server is healthy
  Health,

  /// Submit a generate request and print the response
  Generate {
    /// JSON payload (reads from stdin if omitted)
    #[arg(short, long)]
    payload: Option<String>,
  },
}

#[derive(Debug, Deserialize, Default)]
pub struct ConfigFileRaw {
  #[serde(flatten)]
  pub common: CommonConfigFile,
  pub server_url: Option<String>,
}

#[derive(Debug)]
pub struct Config {
  pub log_level: LogLevel,
  pub log_format: LogFormat,
  pub server_url: String,
  pub command: Command,
}

impl Config {
  pub fn from_cli_and_file(cli: CliRaw) -> Result<Self, ConfigError> {
    let config_path = find_config_file("garage-queue", cli.config.as_deref());

    let file = match config_path {
      Some(ref path) => load_toml::<ConfigFileRaw>(path)?,
      None => ConfigFileRaw::default(),
    };

    let (log_level, log_format) =
      resolve_log_settings(cli.log_level, cli.log_format, &file.common)
        .map_err(ConfigError::Validation)?;

    let server_url = cli
      .server_url
      .or(file.server_url)
      .unwrap_or_else(|| "http://127.0.0.1:9090".to_string());

    Ok(Config {
      log_level,
      log_format,
      server_url,
      command: cli.command,
    })
  }
}
