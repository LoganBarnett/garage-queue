use crate::capability::CapabilityRequirement;
use crate::jq::{eval_jq, JqEvalError};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractorError {
  #[error("Failed to read jq script at '{path}': {source}")]
  ScriptRead {
    path: String,
    source: std::io::Error,
  },

  #[error(
    "jq expression for capability '{capability}' failed to parse: {message}"
  )]
  Parse { capability: String, message: String },

  #[error(
    "jq expression for capability '{capability}' failed to compile: {message}"
  )]
  Compile { capability: String, message: String },

  #[error("jq expression for capability '{capability}' produced no output")]
  NoOutput { capability: String },

  #[error(
    "jq expression for capability '{capability}' produced unexpected type: \
         expected {expected}, got {actual}"
  )]
  TypeMismatch {
    capability: String,
    expected: &'static str,
    actual: &'static str,
  },

  #[error("jq runtime error for capability '{capability}': {message}")]
  Runtime { capability: String, message: String },
}

/// Whether the extractor produces a tag string or a numeric scalar.
#[derive(Debug, Clone, Copy)]
pub enum ExtractorKind {
  Tag,
  Scalar,
}

/// A compiled extractor that evaluates a jq expression against a payload and
/// produces a single CapabilityRequirement.
///
/// The expression is re-compiled per evaluation.  For the throughput expected
/// of a local work queue this is negligible; pre-compilation can be revisited
/// if profiling shows otherwise.
#[derive(Debug)]
pub struct Extractor {
  pub capability: String,
  pub kind: ExtractorKind,
  expression: String,
}

impl Extractor {
  /// Build a tag extractor from an inline expression.
  pub fn tag_from_exp(
    capability: impl Into<String>,
    expression: impl Into<String>,
  ) -> Self {
    Self {
      capability: capability.into(),
      kind: ExtractorKind::Tag,
      expression: expression.into(),
    }
  }

  /// Build a scalar extractor from an inline expression.
  pub fn scalar_from_exp(
    capability: impl Into<String>,
    expression: impl Into<String>,
  ) -> Self {
    Self {
      capability: capability.into(),
      kind: ExtractorKind::Scalar,
      expression: expression.into(),
    }
  }

  /// Build a tag extractor by reading the expression from a file.
  pub fn tag_from_file(
    capability: impl Into<String>,
    path: impl AsRef<Path>,
  ) -> Result<Self, ExtractorError> {
    let path = path.as_ref();
    let expression = std::fs::read_to_string(path).map_err(|source| {
      ExtractorError::ScriptRead {
        path: path.display().to_string(),
        source,
      }
    })?;
    Ok(Self::tag_from_exp(capability, expression))
  }

  /// Build a scalar extractor by reading the expression from a file.
  pub fn scalar_from_file(
    capability: impl Into<String>,
    path: impl AsRef<Path>,
  ) -> Result<Self, ExtractorError> {
    let path = path.as_ref();
    let expression = std::fs::read_to_string(path).map_err(|source| {
      ExtractorError::ScriptRead {
        path: path.display().to_string(),
        source,
      }
    })?;
    Ok(Self::scalar_from_exp(capability, expression))
  }

  /// Evaluate this extractor against a payload, returning the corresponding
  /// CapabilityRequirement.
  pub fn extract(
    &self,
    payload: &serde_json::Value,
  ) -> Result<CapabilityRequirement, ExtractorError> {
    let output =
      eval_jq(&self.expression, payload.clone()).map_err(|e| match e {
        JqEvalError::Parse(msg) => ExtractorError::Parse {
          capability: self.capability.clone(),
          message: msg,
        },
        JqEvalError::Compile(msg) => ExtractorError::Compile {
          capability: self.capability.clone(),
          message: msg,
        },
        JqEvalError::NoOutput => ExtractorError::NoOutput {
          capability: self.capability.clone(),
        },
        JqEvalError::Runtime(msg) => ExtractorError::Runtime {
          capability: self.capability.clone(),
          message: msg,
        },
      })?;

    match self.kind {
      ExtractorKind::Tag => {
        let value = output
          .as_str()
          .ok_or_else(|| ExtractorError::TypeMismatch {
            capability: self.capability.clone(),
            expected: "string",
            actual: json_type_name(&output),
          })?
          .to_string();
        Ok(CapabilityRequirement::Tag { value })
      }

      ExtractorKind::Scalar => {
        let value =
          output
            .as_f64()
            .ok_or_else(|| ExtractorError::TypeMismatch {
              capability: self.capability.clone(),
              expected: "number",
              actual: json_type_name(&output),
            })?;
        Ok(CapabilityRequirement::Scalar {
          name: self.capability.clone(),
          value,
        })
      }
    }
  }
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
  match v {
    serde_json::Value::Null => "null",
    serde_json::Value::Bool(_) => "bool",
    serde_json::Value::Number(_) => "number",
    serde_json::Value::String(_) => "string",
    serde_json::Value::Array(_) => "array",
    serde_json::Value::Object(_) => "object",
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::capability::CapabilityRequirement;
  use serde_json::json;

  #[test]
  fn tag_extraction_from_string_field() {
    let extractor = Extractor::tag_from_exp("model", ".model");
    let payload = json!({ "model": "llama3.2:8b", "prompt": "hello" });
    let result = extractor.extract(&payload).unwrap();
    assert_eq!(
      result,
      CapabilityRequirement::Tag {
        value: "llama3.2:8b".to_string()
      }
    );
  }

  #[test]
  fn scalar_extraction_from_numeric_field() {
    let extractor = Extractor::scalar_from_exp("vram_mb", ".vram_mb");
    let payload = json!({ "vram_mb": 8192.0 });
    let result = extractor.extract(&payload).unwrap();
    assert_eq!(
      result,
      CapabilityRequirement::Scalar {
        name: "vram_mb".to_string(),
        value: 8192.0
      }
    );
  }

  #[test]
  fn tag_extractor_on_numeric_field_returns_type_mismatch() {
    let extractor = Extractor::tag_from_exp("model", ".count");
    let payload = json!({ "count": 42 });
    let err = extractor.extract(&payload).unwrap_err();
    assert!(matches!(
      err,
      ExtractorError::TypeMismatch {
        expected: "string",
        actual: "number",
        ..
      }
    ));
  }

  #[test]
  fn scalar_extractor_on_string_field_returns_type_mismatch() {
    let extractor = Extractor::scalar_from_exp("vram_mb", ".name");
    let payload = json!({ "name": "foo" });
    let err = extractor.extract(&payload).unwrap_err();
    assert!(matches!(
      err,
      ExtractorError::TypeMismatch {
        expected: "number",
        actual: "string",
        ..
      }
    ));
  }

  #[test]
  fn expression_matching_nothing_returns_no_output() {
    let extractor = Extractor::tag_from_exp("model", ".nonexistent // empty");
    let payload = json!({ "other": "value" });
    let err = extractor.extract(&payload).unwrap_err();
    assert!(matches!(err, ExtractorError::NoOutput { .. }));
  }

  #[test]
  fn invalid_jq_syntax_returns_parse_error() {
    let extractor = Extractor::tag_from_exp("model", ".foo ||| bar");
    let payload = json!({});
    let err = extractor.extract(&payload).unwrap_err();
    assert!(matches!(err, ExtractorError::Parse { .. }));
  }

  #[test]
  fn file_based_tag_extractor() {
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("extract.jq");
    std::fs::write(&script_path, ".model").unwrap();

    let extractor = Extractor::tag_from_file("model", &script_path).unwrap();
    let payload = json!({ "model": "gemma2:9b" });
    let result = extractor.extract(&payload).unwrap();
    assert_eq!(
      result,
      CapabilityRequirement::Tag {
        value: "gemma2:9b".to_string()
      }
    );
  }

  #[test]
  fn file_based_scalar_extractor() {
    let dir = tempfile::tempdir().unwrap();
    let script_path = dir.path().join("vram.jq");
    std::fs::write(&script_path, ".vram_mb").unwrap();

    let extractor =
      Extractor::scalar_from_file("vram_mb", &script_path).unwrap();
    let payload = json!({ "vram_mb": 4096.0 });
    let result = extractor.extract(&payload).unwrap();
    assert_eq!(
      result,
      CapabilityRequirement::Scalar {
        name: "vram_mb".to_string(),
        value: 4096.0
      }
    );
  }

  #[test]
  fn missing_script_file_returns_script_read_error() {
    let err =
      Extractor::tag_from_file("model", "/nonexistent/path.jq").unwrap_err();
    assert!(matches!(err, ExtractorError::ScriptRead { .. }));
  }

  #[test]
  fn scalar_from_integer_json_value() {
    let extractor = Extractor::scalar_from_exp("count", ".n");
    let payload = json!({ "n": 7 });
    let result = extractor.extract(&payload).unwrap();
    assert_eq!(
      result,
      CapabilityRequirement::Scalar {
        name: "count".to_string(),
        value: 7.0
      }
    );
  }
}
