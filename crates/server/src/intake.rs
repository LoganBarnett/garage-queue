use crate::config::{ExtractorConfig, ExtractorKind, JqSource, QueueConfig};
use garage_queue_lib::capability::CapabilityRequirement;
use garage_queue_lib::extractor::{Extractor, ExtractorError};
use thiserror::Error;

/// A compiled set of extractors for a single queue, ready to apply to payloads.
pub struct CompiledQueue {
  pub extractors: Vec<Extractor>,
}

#[derive(Debug, Error)]
pub enum IntakeError {
  #[error("Extractor build failed: {0}")]
  ExtractorBuild(#[from] ExtractorError),
}

impl CompiledQueue {
  pub fn build(config: &QueueConfig) -> Result<Self, IntakeError> {
    let extractors = config
      .extractors
      .iter()
      .map(build_extractor)
      .collect::<Result<Vec<_>, _>>()?;
    Ok(Self { extractors })
  }

  /// Extract all capability requirements from a payload.
  pub fn extract(
    &self,
    payload: &serde_json::Value,
  ) -> Result<Vec<CapabilityRequirement>, ExtractorError> {
    self.extractors.iter().map(|e| e.extract(payload)).collect()
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
