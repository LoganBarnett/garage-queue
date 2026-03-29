use crate::capability::{CapabilityRequirement, WorkerCapabilities};

/// Returns true if the worker's capabilities satisfy every requirement of the
/// item.  All requirements must be met — there is no OR logic.
pub fn satisfies(
  requirements: &[CapabilityRequirement],
  capabilities: &WorkerCapabilities,
) -> bool {
  requirements
    .iter()
    .all(|req| satisfies_one(req, capabilities))
}

fn satisfies_one(
  req: &CapabilityRequirement,
  caps: &WorkerCapabilities,
) -> bool {
  match req {
    CapabilityRequirement::Tag { value } => caps.has_tag(value),
    CapabilityRequirement::Scalar { name, value } => {
      caps.scalar(name).map(|cap| cap >= *value).unwrap_or(false)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn worker(tags: &[&str], scalars: &[(&str, f64)]) -> WorkerCapabilities {
    WorkerCapabilities {
      tags: tags.iter().map(|s| s.to_string()).collect(),
      scalars: scalars.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
    }
  }

  fn tag(value: &str) -> CapabilityRequirement {
    CapabilityRequirement::Tag {
      value: value.to_string(),
    }
  }

  fn scalar(name: &str, value: f64) -> CapabilityRequirement {
    CapabilityRequirement::Scalar {
      name: name.to_string(),
      value,
    }
  }

  #[test]
  fn tag_present() {
    assert!(satisfies(&[tag("llama3.2:8b")], &worker(&["llama3.2:8b"], &[])));
  }

  #[test]
  fn tag_absent() {
    assert!(!satisfies(&[tag("gemma2:9b")], &worker(&["llama3.2:8b"], &[])));
  }

  #[test]
  fn scalar_sufficient() {
    assert!(satisfies(
      &[scalar("vram_mb", 8192.0)],
      &worker(&[], &[("vram_mb", 16384.0)])
    ));
  }

  #[test]
  fn scalar_exact() {
    assert!(satisfies(
      &[scalar("vram_mb", 8192.0)],
      &worker(&[], &[("vram_mb", 8192.0)])
    ));
  }

  #[test]
  fn scalar_insufficient() {
    assert!(!satisfies(
      &[scalar("vram_mb", 8192.0)],
      &worker(&[], &[("vram_mb", 4096.0)])
    ));
  }

  #[test]
  fn scalar_missing() {
    assert!(!satisfies(&[scalar("vram_mb", 8192.0)], &worker(&[], &[])));
  }

  #[test]
  fn all_requirements_must_match() {
    // Tag present but scalar insufficient — item should not be assigned.
    assert!(!satisfies(
      &[tag("llama3.2:8b"), scalar("vram_mb", 8192.0)],
      &worker(&["llama3.2:8b"], &[("vram_mb", 4096.0)])
    ));
  }

  #[test]
  fn empty_requirements_matches_any_worker() {
    assert!(satisfies(&[], &worker(&[], &[])));
    assert!(satisfies(&[], &worker(&["anything"], &[("foo", 1.0)])));
  }
}
