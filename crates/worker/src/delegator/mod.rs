pub mod http;

pub use http::HttpDelegator;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DelegateError {
    #[error("HTTP delegator request failed: {0}")]
    Http(#[from] reqwest::Error),
}
