use thiserror::Error;

#[derive(Debug, Error)]
pub enum JqEvalError {
  #[error("jq expression failed to parse: {0}")]
  Parse(String),

  #[error("jq expression failed to compile: {0}")]
  Compile(String),

  #[error("jq expression produced no output")]
  NoOutput,

  #[error("jq runtime error: {0}")]
  Runtime(String),
}

/// Evaluate a jq expression against a JSON value, returning the first output.
///
/// Expressions that produce no output are an error; expressions that produce
/// multiple outputs use only the first.
pub fn eval_jq(
  expression: &str,
  input: serde_json::Value,
) -> Result<serde_json::Value, JqEvalError> {
  use jaq_core::{load, Ctx, RcIter};
  use jaq_json::Val;

  let loader = load::Loader::new(jaq_std::defs().chain(jaq_json::defs()));
  let arena = load::Arena::default();

  let modules = loader
    .load(
      &arena,
      load::File {
        code: expression,
        path: "(expr)",
      },
    )
    .map_err(|errors| JqEvalError::Parse(format!("{errors:?}")))?;

  // `inputs` must be declared before `filter` so that it is dropped after
  // `filter`.  The iterator returned by `filter.run()` borrows from both,
  // and Rust drops locals in reverse declaration order.
  let inputs = RcIter::new(core::iter::empty::<Result<Val, _>>());

  let filter = jaq_core::Compiler::default()
    .with_funs(jaq_std::funs().chain(jaq_json::funs()))
    .compile(modules)
    .map_err(|errors| JqEvalError::Compile(format!("{errors:?}")))?;

  let ctx = Ctx::new([], &inputs);

  // Bind the first output to a named local.  The temporary iterator created
  // by filter.run() is dropped at the semicolon, releasing the borrows on
  // `filter` and `inputs` before those locals go out of scope.
  let first = filter.run((ctx, Val::from(input))).next();

  match first {
    Some(Ok(val)) => Ok(serde_json::Value::from(val)),
    Some(Err(e)) => Err(JqEvalError::Runtime(e.to_string())),
    None => Err(JqEvalError::NoOutput),
  }
}
