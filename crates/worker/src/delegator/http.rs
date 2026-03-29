use super::DelegateError;

/// Forwards the payload as a JSON POST to a configured URL and returns the
/// response body parsed as JSON.
///
/// The delegator forces `stream: false` on any payload that contains a stream
/// field, since the queue system does not support streaming responses.
pub struct HttpDelegator {
  client: reqwest::Client,
  url: String,
}

impl HttpDelegator {
  pub fn new(url: impl Into<String>) -> Self {
    Self {
      client: reqwest::Client::new(),
      url: url.into(),
    }
  }

  pub async fn delegate(
    &self,
    payload: &serde_json::Value,
  ) -> Result<serde_json::Value, DelegateError> {
    let mut body = payload.clone();
    // Streaming responses are not supported through the queue.
    if let Some(obj) = body.as_object_mut() {
      obj.insert("stream".to_string(), serde_json::Value::Bool(false));
    }

    self
      .client
      .post(&self.url)
      .json(&body)
      .send()
      .await?
      .json::<serde_json::Value>()
      .await
      .map_err(DelegateError::Http)
  }
}
