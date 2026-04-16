use clap::Parser;
use garage_queue_lib::{LogFormat, LogLevel};
use rust_template_foundation::config::{
  find_config_file, load_toml, resolve_log_settings, CommonCli,
  CommonConfigFile, ConfigFileError,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;
use tokio_listener::ListenerAddress;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("Failed to load configuration file: {0}")]
  ConfigFile(#[from] ConfigFileError),

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

  #[error(
    "Broadcast queue '{queue}' requires exactly one of combiner_jq_exp \
     or combiner_jq_file"
  )]
  BroadcastMissingCombiner { queue: String },

  #[error("Exclusive queue '{queue}' must not have a combiner")]
  ExclusiveCombinerForbidden { queue: String },

  #[error("Queue '{queue}' has a route but no method")]
  RouteMissingMethod { queue: String },

  #[error("Queue '{queue}' has a method but no route")]
  MethodWithoutRoute { queue: String },

  #[error("Configuration validation failed: {0}")]
  Validation(String),
}

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(author, version, about = "garage-queue server")]
pub struct CliRaw {
  #[command(flatten)]
  pub common: CommonCli,

  #[arg(long, env = "LISTEN")]
  pub listen: Option<String>,
}

// ── Raw (deserialised) types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct ConfigFileRaw {
  #[serde(flatten)]
  pub common: CommonConfigFile,
  pub server: Option<ServerSectionRaw>,

  #[serde(default)]
  pub queues: HashMap<String, QueueConfigRaw>,
}

#[derive(Debug, Deserialize)]
pub struct ServerSectionRaw {
  pub listen: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum QueueModeRaw {
  #[default]
  Exclusive,
  Broadcast,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MethodRaw {
  Get,
  Post,
  Put,
  Patch,
  Delete,
}

#[derive(Debug, Deserialize)]
pub struct QueueConfigRaw {
  /// HTTP path at which this queue accepts intake requests.  When set, the
  /// server registers a handler at this path that enqueues payloads and
  /// waits for a worker result.
  pub route: Option<String>,

  /// HTTP method for the intake route (get, post, put, patch, delete).
  /// Required when route is set; forbidden when route is omitted.
  pub method: Option<MethodRaw>,

  /// Dispatch mode: exclusive (one worker) or broadcast (all matching workers).
  #[serde(default)]
  pub mode: QueueModeRaw,

  /// Inline jq expression to combine broadcast responses into a single result.
  pub combiner_jq_exp: Option<String>,

  /// Path to a jq file to combine broadcast responses into a single result.
  pub combiner_jq_file: Option<PathBuf>,

  /// Optional URL path hint for workers delegating items from this queue.
  pub delegate_path: Option<String>,

  /// Optional HTTP method hint for workers delegating items from this queue.
  pub delegate_method: Option<MethodRaw>,

  /// Optional timeout in seconds for intake requests waiting on worker
  /// responses.  When set, the server returns 504 if no result arrives
  /// within this duration.
  pub intake_timeout_secs: Option<u64>,

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

// ── Validated config ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Config {
  pub log_level: LogLevel,
  pub log_format: LogFormat,
  pub listen_address: ListenerAddress,
  pub queues: HashMap<String, QueueConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMode {
  Exclusive,
  Broadcast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
  Get,
  Post,
  Put,
  Patch,
  Delete,
}

#[derive(Debug)]
pub struct QueueConfig {
  /// HTTP path at which this queue accepts intake requests, if any.
  pub route: Option<String>,
  /// HTTP method for the intake route, if any.
  pub method: Option<Method>,
  pub mode: QueueMode,
  pub combiner: Option<JqSource>,
  pub extractors: Vec<ExtractorConfig>,
  /// Optional URL path hint for workers delegating items from this queue.
  pub delegate_path: Option<String>,
  /// Optional HTTP method hint (lowercase string) for workers.
  pub delegate_method: Option<Method>,
  /// Optional timeout in seconds for intake requests awaiting worker results.
  pub intake_timeout_secs: Option<u64>,
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

    let server = file.server.unwrap_or(ServerSectionRaw { listen: None });

    let listen_str = cli
      .listen
      .or(server.listen)
      .unwrap_or_else(|| "127.0.0.1:9090".to_string());
    let listen_address =
      listen_str.parse::<ListenerAddress>().map_err(|reason| {
        ConfigError::InvalidListenAddress {
          address: listen_str.clone(),
          reason,
        }
      })?;

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
      queues,
    })
  }
}

fn validate_queue_config(
  queue_name: &str,
  raw: QueueConfigRaw,
) -> Result<QueueConfig, ConfigError> {
  let mode = match raw.mode {
    QueueModeRaw::Exclusive => QueueMode::Exclusive,
    QueueModeRaw::Broadcast => QueueMode::Broadcast,
  };

  // Route and method must both be present or both absent.
  let method = match (&raw.route, raw.method) {
    (Some(_), Some(m)) => Some(match m {
      MethodRaw::Get => Method::Get,
      MethodRaw::Post => Method::Post,
      MethodRaw::Put => Method::Put,
      MethodRaw::Patch => Method::Patch,
      MethodRaw::Delete => Method::Delete,
    }),
    (Some(_), None) => {
      return Err(ConfigError::RouteMissingMethod {
        queue: queue_name.to_string(),
      });
    }
    (None, Some(_)) => {
      return Err(ConfigError::MethodWithoutRoute {
        queue: queue_name.to_string(),
      });
    }
    (None, None) => None,
  };

  let combiner = match (raw.combiner_jq_exp, raw.combiner_jq_file) {
    (Some(exp), None) => Some(JqSource::Inline(exp)),
    (None, Some(file)) => Some(JqSource::File(file)),
    (Some(_), Some(_)) => {
      return Err(ConfigError::BroadcastMissingCombiner {
        queue: queue_name.to_string(),
      });
    }
    (None, None) => None,
  };

  match mode {
    QueueMode::Broadcast if combiner.is_none() => {
      return Err(ConfigError::BroadcastMissingCombiner {
        queue: queue_name.to_string(),
      });
    }
    QueueMode::Exclusive if combiner.is_some() => {
      return Err(ConfigError::ExclusiveCombinerForbidden {
        queue: queue_name.to_string(),
      });
    }
    _ => {}
  }

  let delegate_method = raw.delegate_method.map(|m| match m {
    MethodRaw::Get => Method::Get,
    MethodRaw::Post => Method::Post,
    MethodRaw::Put => Method::Put,
    MethodRaw::Patch => Method::Patch,
    MethodRaw::Delete => Method::Delete,
  });

  let extractors = raw
    .extractors
    .into_iter()
    .map(|(extractor_name, raw)| {
      validate_extractor(queue_name, &extractor_name, raw)
    })
    .collect::<Result<Vec<_>, _>>()?;

  Ok(QueueConfig {
    route: raw.route,
    method,
    mode,
    combiner,
    extractors,
    delegate_path: raw.delegate_path,
    delegate_method,
    intake_timeout_secs: raw.intake_timeout_secs,
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
      common: CommonCli {
        log_level: None,
        log_format: None,
        config: Some(PathBuf::from(path)),
      },
      listen: None,
    }
  }

  #[test]
  fn missing_config_file_returns_config_file_error() {
    let cli = cli_with_config("/nonexistent/config.toml");
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::ConfigFile(_)));
  }

  #[test]
  fn invalid_toml_returns_config_file_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "this is {{not valid toml").unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::ConfigFile(_)));
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
method = "post"

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
method = "post"

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
      common: CommonCli {
        log_level: Some("debug".to_string()),
        log_format: None,
        config: Some(path),
      },
      listen: None,
    };
    let config = Config::from_cli_and_file(cli).unwrap();
    assert!(matches!(config.log_level, LogLevel::Debug));
  }

  #[test]
  fn route_without_method_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("no_method.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
route = "/test"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::RouteMissingMethod { .. }));
  }

  #[test]
  fn method_without_route_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("no_route.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
method = "post"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let err = Config::from_cli_and_file(cli).unwrap_err();
    assert!(matches!(err, ConfigError::MethodWithoutRoute { .. }));
  }

  #[test]
  fn delegate_path_and_method_are_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delegate.toml");
    std::fs::write(
      &path,
      r#"
[queues.tags]
route = "/api/tags"
method = "get"
mode = "broadcast"
combiner_jq_exp = "."
delegate_path = "/api/tags"
delegate_method = "get"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let config = Config::from_cli_and_file(cli).unwrap();
    let q = &config.queues["tags"];
    assert_eq!(q.delegate_path.as_deref(), Some("/api/tags"));
    assert_eq!(q.delegate_method, Some(Method::Get));
  }

  #[test]
  fn delegate_path_only_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delegate_path_only.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
route = "/test"
method = "post"
delegate_path = "/custom"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let config = Config::from_cli_and_file(cli).unwrap();
    let q = &config.queues["test"];
    assert_eq!(q.delegate_path.as_deref(), Some("/custom"));
    assert!(q.delegate_method.is_none());
  }

  #[test]
  fn intake_timeout_secs_is_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("timeout.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
route = "/test"
method = "post"
intake_timeout_secs = 30
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let config = Config::from_cli_and_file(cli).unwrap();
    let q = &config.queues["test"];
    assert_eq!(q.intake_timeout_secs, Some(30));
  }

  #[test]
  fn delegate_method_only_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delegate_method_only.toml");
    std::fs::write(
      &path,
      r#"
[queues.test]
route = "/test"
method = "post"
delegate_method = "put"
"#,
    )
    .unwrap();

    let cli = cli_with_config(path.to_str().unwrap());
    let config = Config::from_cli_and_file(cli).unwrap();
    let q = &config.queues["test"];
    assert!(q.delegate_path.is_none());
    assert_eq!(q.delegate_method, Some(Method::Put));
  }
}
