use super::DelegateError;

/// Forwards the payload to a configured URL and returns the response body
/// parsed as JSON.
///
/// When `delegate_path` or `delegate_method` are provided (from QueueItem
/// hints), the delegator overrides its default URL path and/or HTTP method.
/// GET and DELETE requests send no body; POST, PUT, and PATCH send the
/// payload with `stream: false` injected.
pub struct HttpDelegator {
  client: reqwest::Client,
  url: String,
}

/// Prepare a JSON body for methods that carry one: clone the payload and
/// force `stream: false` (the queue system does not support streaming).
fn prepare_body(payload: &serde_json::Value) -> serde_json::Value {
  let mut body = payload.clone();
  if let Some(obj) = body.as_object_mut() {
    obj.insert("stream".to_string(), serde_json::Value::Bool(false));
  }
  body
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
    delegate_path: Option<&str>,
    delegate_method: Option<&str>,
  ) -> Result<serde_json::Value, DelegateError> {
    let url = match delegate_path {
      Some(path) => {
        let mut parsed = reqwest::Url::parse(&self.url).map_err(|e| {
          DelegateError::InvalidUrl(format!("{}: {e}", self.url))
        })?;
        parsed.set_path(path);
        parsed.to_string()
      }
      None => self.url.clone(),
    };

    let method = delegate_method.unwrap_or("post");

    let response = match method {
      "get" | "GET" => self.client.get(&url).send().await?,
      "delete" | "DELETE" => self.client.delete(&url).send().await?,
      "put" | "PUT" => {
        let body = prepare_body(payload);
        self.client.put(&url).json(&body).send().await?
      }
      "patch" | "PATCH" => {
        let body = prepare_body(payload);
        self.client.patch(&url).json(&body).send().await?
      }
      // POST is the default for unrecognised methods too.
      _ => {
        let body = prepare_body(payload);
        self.client.post(&url).json(&body).send().await?
      }
    };

    response
      .json::<serde_json::Value>()
      .await
      .map_err(DelegateError::Http)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::{routing, Json, Router};
  use serde_json::json;
  use std::net::SocketAddr;

  /// Start a small axum server that records what it receives and returns
  /// a canned response.  Returns the base URL.
  async fn mock_server() -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
      .route(
        "/api/generate",
        routing::post(|Json(body): Json<serde_json::Value>| async move {
          Json(
            json!({ "route": "/api/generate", "method": "post", "body": body }),
          )
        }),
      )
      .route(
        "/api/tags",
        routing::get(|| async {
          Json(json!({ "route": "/api/tags", "method": "get" }))
        }),
      )
      .route(
        "/custom",
        routing::put(|Json(body): Json<serde_json::Value>| async move {
          Json(json!({ "route": "/custom", "method": "put", "body": body }))
        }),
      );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
      axum::serve(listener, app).await.ok();
    });

    (format!("http://{addr}/api/generate"), handle)
  }

  #[tokio::test]
  async fn none_none_preserves_default_post_with_stream_false() {
    let (base_url, _handle) = mock_server().await;
    let delegator = HttpDelegator::new(&base_url);

    let payload = json!({ "model": "test", "prompt": "hi" });
    let result = delegator.delegate(&payload, None, None).await.unwrap();

    assert_eq!(result["method"], "post");
    assert_eq!(result["route"], "/api/generate");
    assert_eq!(result["body"]["stream"], false);
    assert_eq!(result["body"]["prompt"], "hi");
  }

  #[tokio::test]
  async fn path_override_changes_url() {
    let (base_url, _handle) = mock_server().await;
    let delegator = HttpDelegator::new(&base_url);

    let result = delegator
      .delegate(&json!({}), Some("/api/tags"), Some("get"))
      .await
      .unwrap();

    assert_eq!(result["method"], "get");
    assert_eq!(result["route"], "/api/tags");
    // GET sends no body, so no "body" key in the response.
    assert!(result.get("body").is_none());
  }

  #[tokio::test]
  async fn get_sends_no_body() {
    let (base_url, _handle) = mock_server().await;
    let delegator = HttpDelegator::new(&base_url);

    let result = delegator
      .delegate(&json!({"some": "data"}), Some("/api/tags"), Some("get"))
      .await
      .unwrap();

    assert_eq!(result["method"], "get");
    assert!(result.get("body").is_none());
  }

  #[tokio::test]
  async fn method_only_override_keeps_configured_url() {
    let (base_url, _handle) = mock_server().await;
    let delegator = HttpDelegator::new(&base_url);

    // Override method to POST (same as default) but no path override.
    let payload = json!({ "model": "test" });
    let result = delegator
      .delegate(&payload, None, Some("post"))
      .await
      .unwrap();

    assert_eq!(result["route"], "/api/generate");
    assert_eq!(result["method"], "post");
  }

  #[tokio::test]
  async fn put_method_sends_body() {
    let (base_url, _handle) = mock_server().await;
    let delegator = HttpDelegator::new(&base_url);

    let payload = json!({ "data": "value" });
    let result = delegator
      .delegate(&payload, Some("/custom"), Some("put"))
      .await
      .unwrap();

    assert_eq!(result["method"], "put");
    assert_eq!(result["route"], "/custom");
    assert_eq!(result["body"]["stream"], false);
    assert_eq!(result["body"]["data"], "value");
  }
}
