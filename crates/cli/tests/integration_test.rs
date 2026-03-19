// Integration tests for the CLI binary.
//
// Tests that require a running server are not included here.  These tests
// only exercise the binary's CLI interface (flags, subcommands, help text).

use std::{path::PathBuf, process::Command};

fn binary_path() -> PathBuf {
  let mut path =
    std::env::current_exe().expect("Failed to get current executable path");
  path.pop(); // remove test executable name
  path.pop(); // remove deps dir
  path.push("garage-queue-cli");
  if !path.exists() {
    path.pop();
    path.pop();
    path.push("debug");
    path.push("garage-queue-cli");
  }
  path
}

#[test]
fn test_help_flag() {
  let output = Command::new(binary_path())
    .arg("--help")
    .output()
    .expect("Failed to execute binary");
  assert!(
    output.status.success(),
    "Expected success exit code, got: {:?}",
    output.status.code()
  );
  let stdout = String::from_utf8_lossy(&output.stdout);
  assert!(stdout.contains("Usage:"), "Expected help text to contain 'Usage:': {stdout}");
}

#[test]
fn test_version_flag() {
  let output = Command::new(binary_path())
    .arg("--version")
    .output()
    .expect("Failed to execute binary");
  assert!(
    output.status.success(),
    "Expected success exit code, got: {:?}",
    output.status.code()
  );
  let stdout = String::from_utf8_lossy(&output.stdout);
  assert!(
    stdout.contains("garage-queue-cli"),
    "Expected version to contain 'garage-queue-cli': {stdout}"
  );
}

#[test]
fn test_health_subcommand_help() {
  let output = Command::new(binary_path())
    .args(["health", "--help"])
    .output()
    .expect("Failed to execute binary");
  assert!(
    output.status.success(),
    "Expected success exit code, got: {:?}",
    output.status.code()
  );
}

#[test]
fn test_generate_subcommand_help() {
  let output = Command::new(binary_path())
    .args(["generate", "--help"])
    .output()
    .expect("Failed to execute binary");
  assert!(
    output.status.success(),
    "Expected success exit code, got: {:?}",
    output.status.code()
  );
}
