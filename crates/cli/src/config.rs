use clap::Parser;
use garage_queue_lib::{LogFormat, LogLevel};
use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("Failed to read configuration file at {path:?}: {source}")]
  FileRead {
    path: PathBuf,
    #[source]
    source: std::io::Error,
  },

  #[error("Failed to parse configuration file at {path:?}: {source}")]
  Parse {
    path: PathBuf,
    #[source]
    source: toml::de::Error,
  },

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
  pub log_level: Option<String>,
  pub log_format: Option<String>,
  pub server_url: Option<String>,
}

impl ConfigFileRaw {
  pub fn from_file(path: &PathBuf) -> Result<Self, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| {
      ConfigError::FileRead {
        path: path.clone(),
        source,
      }
    })?;
    toml::from_str(&contents).map_err(|source| ConfigError::Parse {
      path: path.clone(),
      source,
    })
  }
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
    let config_file = match &cli.config {
      Some(path) => ConfigFileRaw::from_file(path)?,
      None => {
        let default_path = PathBuf::from("config.toml");
        if default_path.exists() {
          ConfigFileRaw::from_file(&default_path)?
        } else {
          ConfigFileRaw::default()
        }
      }
    };

    let log_level = cli
      .log_level
      .or(config_file.log_level)
      .unwrap_or_else(|| "info".to_string())
      .parse::<LogLevel>()
      .map_err(|e| ConfigError::Validation(e.to_string()))?;

    let log_format = cli
      .log_format
      .or(config_file.log_format)
      .unwrap_or_else(|| "text".to_string())
      .parse::<LogFormat>()
      .map_err(|e| ConfigError::Validation(e.to_string()))?;

    let server_url = cli
      .server_url
      .or(config_file.server_url)
      .unwrap_or_else(|| "http://127.0.0.1:3000".to_string());

    Ok(Config {
      log_level,
      log_format,
      server_url,
      command: cli.command,
    })
  }
}
