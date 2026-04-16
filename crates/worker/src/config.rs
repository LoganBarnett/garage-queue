use clap::Parser;
use garage_queue_lib::capability::WorkerCapabilities;
use garage_queue_lib::{LogFormat, LogLevel};
use rust_template_foundation::config::{
  find_config_file, load_toml, resolve_log_settings, CommonCli,
  CommonConfigFile, ConfigFileError,
};
use serde::Deserialize;
use thiserror::Error;
use tokio_listener::ListenerAddress;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("Failed to load configuration file: {0}")]
  ConfigFile(#[from] ConfigFileError),

  #[error("Failed to parse listen address '{address}': {reason}")]
  ListenerAddressParse {
    address: String,
    reason: &'static str,
  },

  #[error("Configuration validation failed: {0}")]
  Validation(String),
}

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(author, version, about = "garage-queue worker")]
pub struct CliRaw {
  #[command(flatten)]
  pub common: CommonCli,
}

// ── Raw (deserialised) types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct ConfigFileRaw {
  #[serde(flatten)]
  pub common: CommonConfigFile,
  pub worker: Option<WorkerSectionRaw>,
  pub control: Option<ListenSectionRaw>,
  pub observe: Option<ListenSectionRaw>,
  pub capabilities: Option<WorkerCapabilities>,
  pub concurrency: Option<ConcurrencyConfigRaw>,
  pub delegator: Option<DelegatorConfigRaw>,
}

#[derive(Debug, Deserialize)]
pub struct ConcurrencyConfigRaw {
  pub default: Option<u32>,
  #[serde(flatten)]
  pub overrides: std::collections::HashMap<String, u32>,
}

#[derive(Debug, Deserialize)]
pub struct WorkerSectionRaw {
  pub server_url: Option<String>,

  /// Unique identifier for this worker.  Required for SSE dispatch.
  pub id: Option<String>,

  /// How long to wait before reconnecting after a disconnection, in
  /// milliseconds.
  pub reconnect_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ListenSectionRaw {
  pub listen_address: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DelegatorConfigRaw {
  Http { url: String },
}

// ── Validated config ─────────────────────────────────────────────────────────

pub struct Config {
  pub log_level: LogLevel,
  pub log_format: LogFormat,
  pub server_url: String,
  pub worker_id: String,
  pub reconnect_interval_ms: u64,
  pub control_bind: ListenerAddress,
  pub observe_bind: ListenerAddress,
  pub capabilities: WorkerCapabilities,
  pub concurrency: garage_queue_lib::protocol::ConcurrencyConfig,
  pub delegator: DelegatorConfig,
}

pub enum DelegatorConfig {
  Http { url: String },
}

impl Config {
  pub fn from_cli_and_file(cli: CliRaw) -> Result<Self, ConfigError> {
    let config_path =
      find_config_file("garage-queue", cli.common.config.as_deref());

    let file = match config_path {
      Some(ref path) => load_toml::<ConfigFileRaw>(path)?,
      None => ConfigFileRaw::default(),
    };

    let (log_level, log_format) = resolve_log_settings(
      cli.common.log_level,
      cli.common.log_format,
      &file.common,
    )
    .map_err(ConfigError::Validation)?;

    let worker = file.worker.unwrap_or(WorkerSectionRaw {
      server_url: None,
      id: None,
      reconnect_interval_ms: None,
    });

    let server_url = worker
      .server_url
      .unwrap_or_else(|| "http://127.0.0.1:9090".to_string());

    let worker_id = worker.id.ok_or_else(|| {
      ConfigError::Validation("worker.id is required".to_string())
    })?;

    let reconnect_interval_ms = worker.reconnect_interval_ms.unwrap_or(1000);

    let control_address = file
      .control
      .and_then(|s| s.listen_address)
      .unwrap_or_else(|| "127.0.0.1:9091".to_string());
    let control_bind: ListenerAddress =
      control_address.parse().map_err(|reason| {
        ConfigError::ListenerAddressParse {
          address: control_address,
          reason,
        }
      })?;

    let observe_address = file
      .observe
      .and_then(|s| s.listen_address)
      .unwrap_or_else(|| "127.0.0.1:9092".to_string());
    let observe_bind: ListenerAddress =
      observe_address.parse().map_err(|reason| {
        ConfigError::ListenerAddressParse {
          address: observe_address,
          reason,
        }
      })?;

    let capabilities = file.capabilities.unwrap_or_default();

    let concurrency = match file.concurrency {
      Some(raw) => {
        let default_val = raw.default.unwrap_or(1);
        garage_queue_lib::protocol::ConcurrencyConfig {
          default: default_val,
          overrides: raw.overrides,
        }
      }
      None => garage_queue_lib::protocol::ConcurrencyConfig::default(),
    };

    let delegator = match file.delegator {
      Some(DelegatorConfigRaw::Http { url }) => DelegatorConfig::Http { url },
      None => {
        return Err(ConfigError::Validation(
          "delegator configuration is required".to_string(),
        ));
      }
    };

    Ok(Config {
      log_level,
      log_format,
      server_url,
      worker_id,
      reconnect_interval_ms,
      control_bind,
      observe_bind,
      capabilities,
      concurrency,
      delegator,
    })
  }
}
