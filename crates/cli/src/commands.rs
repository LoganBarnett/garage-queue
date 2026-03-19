use std::io::Read;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CommandError {
  #[error("Failed to read payload from stdin: {0}")]
  StdinRead(std::io::Error),

  #[error("Payload is not valid JSON: {0}")]
  PayloadParse(serde_json::Error),

  #[error("Request to {url} failed: {message}")]
  Request { url: String, message: String },

  #[error("Server returned {status}: {body}")]
  ServerError { status: u16, body: String },
}

/// GET /healthz — print the server health response and exit.
pub async fn health(server_url: &str) -> Result<(), CommandError> {
  let url = format!("{server_url}/healthz");
  let response = reqwest::get(&url)
    .await
    .map_err(|e| CommandError::Request { url: url.clone(), message: e.to_string() })?;

  let status = response.status();
  let body = response
    .text()
    .await
    .map_err(|e| CommandError::Request { url: url.clone(), message: e.to_string() })?;

  if status.is_success() {
    println!("{body}");
    Ok(())
  } else {
    Err(CommandError::ServerError { status: status.as_u16(), body })
  }
}

/// POST /api/generate — submit a payload and print the response.
///
/// The payload is read from the `--payload` flag if given, otherwise from
/// stdin.  Either way it must be valid JSON.
pub async fn generate(
  server_url: &str,
  payload_arg: Option<String>,
) -> Result<(), CommandError> {
  let raw = match payload_arg {
    Some(s) => s,
    None => {
      let mut buf = String::new();
      std::io::stdin()
        .read_to_string(&mut buf)
        .map_err(CommandError::StdinRead)?;
      buf
    }
  };

  let payload: serde_json::Value =
    serde_json::from_str(&raw).map_err(CommandError::PayloadParse)?;

  let url = format!("{server_url}/api/generate");
  let client = reqwest::Client::new();
  let response = client
    .post(&url)
    .json(&payload)
    .send()
    .await
    .map_err(|e| CommandError::Request { url: url.clone(), message: e.to_string() })?;

  let status = response.status();
  let body = response
    .text()
    .await
    .map_err(|e| CommandError::Request { url: url.clone(), message: e.to_string() })?;

  if status.is_success() {
    println!("{body}");
    Ok(())
  } else {
    Err(CommandError::ServerError { status: status.as_u16(), body })
  }
}
