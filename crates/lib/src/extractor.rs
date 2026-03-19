use crate::capability::CapabilityRequirement;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractorError {
    #[error("Failed to read jq script at '{path}': {source}")]
    ScriptRead { path: String, source: std::io::Error },

    #[error("jq expression for capability '{capability}' failed to parse: {message}")]
    Parse { capability: String, message: String },

    #[error("jq expression for capability '{capability}' failed to compile: {message}")]
    Compile { capability: String, message: String },

    #[error("jq expression for capability '{capability}' produced no output")]
    NoOutput { capability: String },

    #[error(
        "jq expression for capability '{capability}' produced unexpected type: \
         expected {expected}, got {actual}"
    )]
    TypeMismatch { capability: String, expected: &'static str, actual: &'static str },

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
pub struct Extractor {
    pub capability: String,
    pub kind: ExtractorKind,
    expression: String,
}

impl Extractor {
    /// Build a tag extractor from an inline expression.
    pub fn tag_from_exp(capability: impl Into<String>, expression: impl Into<String>) -> Self {
        Self {
            capability: capability.into(),
            kind: ExtractorKind::Tag,
            expression: expression.into(),
        }
    }

    /// Build a scalar extractor from an inline expression.
    pub fn scalar_from_exp(capability: impl Into<String>, expression: impl Into<String>) -> Self {
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
        let expression =
            std::fs::read_to_string(path).map_err(|source| ExtractorError::ScriptRead {
                path: path.display().to_string(),
                source,
            })?;
        Ok(Self::tag_from_exp(capability, expression))
    }

    /// Build a scalar extractor by reading the expression from a file.
    pub fn scalar_from_file(
        capability: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<Self, ExtractorError> {
        let path = path.as_ref();
        let expression =
            std::fs::read_to_string(path).map_err(|source| ExtractorError::ScriptRead {
                path: path.display().to_string(),
                source,
            })?;
        Ok(Self::scalar_from_exp(capability, expression))
    }

    /// Evaluate this extractor against a payload, returning the corresponding
    /// CapabilityRequirement.
    pub fn extract(
        &self,
        payload: &serde_json::Value,
    ) -> Result<CapabilityRequirement, ExtractorError> {
        let output = eval_jq(&self.expression, payload.clone()).map_err(|e| match e {
            JqError::Parse(msg) => ExtractorError::Parse {
                capability: self.capability.clone(),
                message: msg,
            },
            JqError::Compile(msg) => ExtractorError::Compile {
                capability: self.capability.clone(),
                message: msg,
            },
            JqError::NoOutput => ExtractorError::NoOutput {
                capability: self.capability.clone(),
            },
            JqError::Runtime(msg) => ExtractorError::Runtime {
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

// ── jq evaluation ────────────────────────────────────────────────────────────

#[derive(Debug)]
enum JqError {
    Parse(String),
    Compile(String),
    NoOutput,
    Runtime(String),
}

/// Evaluate a jq expression against a JSON value, returning the first output.
///
/// Expressions that produce no output are an error; expressions that produce
/// multiple outputs use only the first.
fn eval_jq(expression: &str, input: serde_json::Value) -> Result<serde_json::Value, JqError> {
    use jaq_core::{load, Ctx, RcIter};
    use jaq_json::Val;

    let loader = load::Loader::new(jaq_std::defs().chain(jaq_json::defs()));
    let arena = load::Arena::default();

    let modules = loader
        .load(&arena, load::File { code: expression, path: "(expr)" })
        .map_err(|errors| JqError::Parse(format!("{errors:?}")))?;

    // `inputs` must be declared before `filter` so that it is dropped after
    // `filter`.  The iterator returned by `filter.run()` borrows from both,
    // and Rust drops locals in reverse declaration order.
    let inputs = RcIter::new(core::iter::empty::<Result<Val, _>>());

    let filter = jaq_core::Compiler::default()
        .with_funs(jaq_std::funs().chain(jaq_json::funs()))
        .compile(modules)
        .map_err(|errors| JqError::Compile(format!("{errors:?}")))?;

    let ctx = Ctx::new([], &inputs);

    // Bind the first output to a named local.  The temporary iterator created
    // by filter.run() is dropped at the semicolon, releasing the borrows on
    // `filter` and `inputs` before those locals go out of scope.
    let first = filter.run((ctx, Val::from(input))).next();

    match first {
        Some(Ok(val)) => Ok(serde_json::Value::from(val)),
        Some(Err(e)) => Err(JqError::Runtime(e.to_string())),
        None => Err(JqError::NoOutput),
    }
}
