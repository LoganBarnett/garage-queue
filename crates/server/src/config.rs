use clap::Parser;
use garage_queue_lib::{LogFormat, LogLevel};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;
use tokio_listener::ListenerAddress;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("Failed to read configuration file at '{path}': {source}")]
  FileRead {
    path: PathBuf,
    source: std::io::Error,
  },

  #[error("Failed to parse configuration file at '{path}': {source}")]
  Parse {
    path: PathBuf,
    source: toml::de::Error,
  },

  #[error("Invalid listen address '{address}': {reason}")]
  InvalidListenAddress {
    address: String,
    reason: &'static str,
  },

  #[error("Extractor '{extractor}' in queue '{queue}': {message}")]
  ExtractorInvalid {
    queue: String,
    extractor: String,
    message: String,
  },

  #[error("Configuration validation failed: {0}")]
  Validation(String),
}

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(author, version, about = "garage-queue server")]
pub struct CliRaw {
  #[arg(long, env = "LOG_LEVEL")]
  pub log_level: Option<String>,

  #[arg(long, env = "LOG_FORMAT")]
  pub log_format: Option<String>,

  #[arg(short, long, env = "CONFIG_FILE")]
  pub config: Option<PathBuf>,
}

// ── Raw (deserialised) types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct ConfigFileRaw {
  pub log_level: Option<String>,
  pub log_format: Option<String>,
  pub server: Option<ServerSectionRaw>,

  #[serde(default)]
  pub queues: HashMap<String, QueueConfigRaw>,
}

#[derive(Debug, Deserialize)]
pub struct ServerSectionRaw {
  pub listen: Option<String>,
  pub nats_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueueConfigRaw {
  /// HTTP path at which this queue accepts intake requests.  When set, the
  /// server registers a POST handler at this path that enqueues payloads and
  /// waits for a worker result.
  pub route: Option<String>,

  #[serde(default)]
  pub extractors: HashMap<String, ExtractorConfigRaw>,
}

#[derive(Debug, Deserialize)]
pub struct ExtractorConfigRaw {
  pub kind: ExtractorKindRaw,
  pub capability: String,
  pub jq_exp: Option<String>,
  pub jq_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtractorKindRaw {
  Tag,
  Scalar,
}

impl ConfigFileRaw {
  pub fn from_file(path: &PathBuf) -> Result<Self, ConfigError> {
    std::fs::read_to_string(path)
      .map_err(|source| ConfigError::FileRead {
        path: path.clone(),
        source,
      })
      .and_then(|contents| {
        toml::from_str(&contents).map_err(|source| ConfigError::Parse {
          path: path.clone(),
          source,
        })
      })
  }
}

// ── Validated config ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Config {
  pub log_level: LogLevel,
  pub log_format: LogFormat,
  pub listen_address: ListenerAddress,
  pub nats_url: String,
  pub queues: HashMap<String, QueueConfig>,
}

#[derive(Debug)]
pub struct QueueConfig {
  /// HTTP path at which this queue accepts intake requests, if any.
  pub route: Option<String>,
  pub extractors: Vec<ExtractorConfig>,
}

#[derive(Debug)]
pub struct ExtractorConfig {
  pub capability: String,
  pub kind: ExtractorKind,
  pub source: JqSource,
}

#[derive(Debug)]
pub enum ExtractorKind {
  Tag,
  Scalar,
}

#[derive(Debug)]
pub enum JqSource {
  Inline(String),
  File(PathBuf),
}

impl Config {
  pub fn from_cli_and_file(cli: CliRaw) -> Result<Self, ConfigError> {
    let file = if let Some(ref path) = cli.config {
      ConfigFileRaw::from_file(path)?
    } else {
      let default = PathBuf::from("config.toml");
      if default.exists() {
        ConfigFileRaw::from_file(&default)?
      } else {
        ConfigFileRaw::default()
      }
    };

    let log_level = cli
      .log_level
      .or_else(|| file.log_level.clone())
      .unwrap_or_else(|| "info".to_string())
      .parse::<LogLevel>()
      .map_err(|e| ConfigError::Validation(e.to_string()))?;

    let log_format = cli
      .log_format
      .or_else(|| file.log_format.clone())
      .unwrap_or_else(|| "text".to_string())
      .parse::<LogFormat>()
      .map_err(|e| ConfigError::Validation(e.to_string()))?;

    let server = file.server.unwrap_or_else(|| ServerSectionRaw {
      listen: None,
      nats_url: None,
    });

    let listen_str = server
      .listen
      .unwrap_or_else(|| "127.0.0.1:9090".to_string());
    let listen_address =
      listen_str.parse::<ListenerAddress>().map_err(|reason| {
        ConfigError::InvalidListenAddress {
          address: listen_str.clone(),
          reason,
        }
      })?;

    let nats_url = server
      .nats_url
      .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());

    let queues = file
      .queues
      .into_iter()
      .map(|(queue_name, raw)| {
        validate_queue_config(&queue_name, raw).map(|cfg| (queue_name, cfg))
      })
      .collect::<Result<HashMap<_, _>, _>>()?;

    Ok(Config {
      log_level,
      log_format,
      listen_address,
      nats_url,
      queues,
    })
  }
}

fn validate_queue_config(
  queue_name: &str,
  raw: QueueConfigRaw,
) -> Result<QueueConfig, ConfigError> {
  let extractors = raw
    .extractors
    .into_iter()
    .map(|(extractor_name, raw)| {
      validate_extractor(queue_name, &extractor_name, raw)
    })
    .collect::<Result<Vec<_>, _>>()?;

  Ok(QueueConfig {
    route: raw.route,
    extractors,
  })
}

fn validate_extractor(
  queue_name: &str,
  extractor_name: &str,
  raw: ExtractorConfigRaw,
) -> Result<ExtractorConfig, ConfigError> {
  let source = match (raw.jq_exp, raw.jq_file) {
    (Some(exp), None) => JqSource::Inline(exp),
    (None, Some(file)) => JqSource::File(file),
    (Some(_), Some(_)) => {
      return Err(ConfigError::ExtractorInvalid {
        queue: queue_name.to_string(),
        extractor: extractor_name.to_string(),
        message: "specify either jq_exp or jq_file, not both".to_string(),
      });
    }
    (None, None) => {
      return Err(ConfigError::ExtractorInvalid {
        queue: queue_name.to_string(),
        extractor: extractor_name.to_string(),
        message: "one of jq_exp or jq_file is required".to_string(),
      });
    }
  };

  let kind = match raw.kind {
    ExtractorKindRaw::Tag => ExtractorKind::Tag,
    ExtractorKindRaw::Scalar => ExtractorKind::Scalar,
  };

  Ok(ExtractorConfig {
    capability: raw.capability,
    kind,
    source,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  fn cli_with_config(path: &str) -> CliRaw {
    CliRaw {
      log_level: None,
      log_format: None,
      config: Some(PathBuf::from(path)),
    }
  }

  #[test]
  fn missing_config_file_returns_file_read_error() {
    let cli = cli_with_config("/nonexistent/config.toml");
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::FileRead { .. }));
  }

  #[test]
  fn invalid_toml_returns_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "this is {{not valid toml").unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::Parse { .. }));
  }

  #[test]
  fn valid_minimal_config_uses_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("minimal.toml");
    std::fs::write(&path, "").unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let config = Config::from_cli_and_file(cli).unwrap();

    assert!(matches!(config.log_level, LogLevel::Info));
    assert!(matches!(config.log_format, LogFormat::Text));
    assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
    assert!(config.queues.is_empty());
  }

  #[test]
  fn extractor_with_both_jq_exp_and_jq_file_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("both.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
route = "/test"

[queues.test.extractors.bad]
kind = "tag"
capability = "model"
jq_exp = ".model"
jq_file = "model.jq"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::ExtractorInvalid { .. }));
  }

  #[test]
  fn extractor_with_neither_jq_exp_nor_jq_file_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("neither.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
route = "/test"

[queues.test.extractors.bad]
kind = "tag"
capability = "model"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::ExtractorInvalid { .. }));
  }

  #[test]
  fn cli_log_level_overrides_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "log_level = \"warn\"").unwrap();

    let cli = CliRaw {
      log_level: Some("debug".to_string()),
      log_format: None,
      config: Some(path),
    };
    let config = Config::from_cli_and_file(cli).unwrap();
    assert!(matches!(config.log_level, LogLevel::Debug));
  }
}
