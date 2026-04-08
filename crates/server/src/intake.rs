use crate::config::{
  ExtractorConfig, ExtractorKind, JqSource, QueueConfig, QueueMode,
};
use garage_queue_lib::capability::CapabilityRequirement;
use garage_queue_lib::extractor::{Extractor, ExtractorError};
use garage_queue_lib::jq::{eval_jq, JqEvalError};
use serde_json::json;
use thiserror::Error;

/// A compiled set of extractors for a single queue, ready to apply to payloads.
pub struct CompiledQueue {
  pub extractors: Vec<Extractor>,
  pub mode: QueueMode,
  /// The jq expression used to combine broadcast responses.  Present only
  /// when mode is Broadcast.
  pub combiner: Option<String>,
}

#[derive(Debug, Error)]
pub enum IntakeError {
  #[error("Extractor build failed: {0}")]
  ExtractorBuild(#[from] ExtractorError),

  #[error("Failed to read combiner script at '{path}': {source}")]
  CombinerScriptRead {
    path: String,
    source: std::io::Error,
  },
}

#[derive(Debug, Error)]
pub enum CombinerError {
  #[error("Queue '{queue}' has no combiner configured")]
  NotConfigured { queue: String },

  #[error("Combiner evaluation failed for queue '{queue}': {message}")]
  EvalFailed { queue: String, message: String },
}

impl CompiledQueue {
  pub fn build(config: &QueueConfig) -> Result<Self, IntakeError> {
    let extractors = config
      .extractors
      .iter()
      .map(build_extractor)
      .collect::<Result<Vec<_>, _>>()?;

    let combiner = match &config.combiner {
      Some(JqSource::Inline(exp)) => Some(exp.clone()),
      Some(JqSource::File(path)) => {
        let contents = std::fs::read_to_string(path).map_err(|source| {
          IntakeError::CombinerScriptRead {
            path: path.display().to_string(),
            source,
          }
        })?;
        Some(contents)
      }
      None => None,
    };

    Ok(Self {
      extractors,
      mode: config.mode,
      combiner,
    })
  }

  /// Extract all capability requirements from a payload.
  pub fn extract(
    &self,
    payload: &serde_json::Value,
  ) -> Result<Vec<CapabilityRequirement>, ExtractorError> {
    self.extractors.iter().map(|e| e.extract(payload)).collect()
  }

  /// Combine broadcast worker responses into a single value using the
  /// configured jq combiner.
  ///
  /// The input to the combiner is an array of objects, each with
  /// `worker_id` and `response` fields.
  pub fn combine(
    &self,
    queue_name: &str,
    responses: &[(String, serde_json::Value)],
  ) -> Result<serde_json::Value, CombinerError> {
    let expression =
      self
        .combiner
        .as_ref()
        .ok_or_else(|| CombinerError::NotConfigured {
          queue: queue_name.to_string(),
        })?;

    let input: serde_json::Value = responses
      .iter()
      .map(|(wid, resp)| json!({ "worker_id": wid, "response": resp }))
      .collect::<Vec<_>>()
      .into();

    eval_jq(expression, input).map_err(|e| {
      let message = match e {
        JqEvalError::Parse(m)
        | JqEvalError::Compile(m)
        | JqEvalError::Runtime(m) => m,
        JqEvalError::NoOutput => "expression produced no output".to_string(),
      };
      CombinerError::EvalFailed {
        queue: queue_name.to_string(),
        message,
      }
    })
  }
}

fn build_extractor(cfg: &ExtractorConfig) -> Result<Extractor, IntakeError> {
  let capability = cfg.capability.clone();
  match (&cfg.kind, &cfg.source) {
    (ExtractorKind::Tag, JqSource::Inline(exp)) => {
      Ok(Extractor::tag_from_exp(capability, exp))
    }
    (ExtractorKind::Tag, JqSource::File(path)) => {
      Ok(Extractor::tag_from_file(capability, path)?)
    }
    (ExtractorKind::Scalar, JqSource::Inline(exp)) => {
      Ok(Extractor::scalar_from_exp(capability, exp))
    }
    (ExtractorKind::Scalar, JqSource::File(path)) => {
      Ok(Extractor::scalar_from_file(capability, path)?)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn combine_identity_combiner() {
    let queue = CompiledQueue {
      extractors: vec![],
      mode: QueueMode::Broadcast,
      combiner: Some(".".to_string()),
    };

    let responses = vec![
      ("w1".to_string(), json!({"data": 1})),
      ("w2".to_string(), json!({"data": 2})),
    ];

    let result = queue.combine("test", &responses).unwrap();
    assert!(result.is_array());
    assert_eq!(result.as_array().unwrap().len(), 2);
  }

  #[test]
  fn combine_aggregation_combiner() {
    let queue = CompiledQueue {
      extractors: vec![],
      mode: QueueMode::Broadcast,
      combiner: Some("[.[] | .response.value] | add".to_string()),
    };

    let responses = vec![
      ("w1".to_string(), json!({"value": 10})),
      ("w2".to_string(), json!({"value": 20})),
    ];

    let result = queue.combine("test", &responses).unwrap();
    assert_eq!(result, json!(30));
  }

  #[test]
  fn combine_missing_combiner_returns_error() {
    let queue = CompiledQueue {
      extractors: vec![],
      mode: QueueMode::Exclusive,
      combiner: None,
    };

    let err = queue.combine("test", &[]).unwrap_err();
    assert!(matches!(err, CombinerError::NotConfigured { .. }));
  }
}
