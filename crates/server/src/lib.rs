pub mod config;
pub mod dispatch;
pub mod intake;
pub mod logging;
pub mod registry;
pub mod routes;
pub mod state;

use axum::{routing::get, routing::post, Router};
use state::AppState;
use tower_http::trace::TraceLayer;

/// Assemble the application router with all routes and middleware attached.
/// The caller is responsible for binding a listener and calling axum::serve.
pub fn build_router(state: AppState) -> Router {
  Router::new()
    .route("/healthz", get(routes::health::healthz))
    .route("/api/work/connect", post(routes::sse::connect))
    .route("/api/work/poll", post(routes::work::poll))
    .route("/api/work/result", post(routes::work::result))
    // Catch-all for queue intake: each queue declares its own route in
    // config.  More specific routes above take precedence.
    .route("/*path", post(routes::intake::handle_intake))
    .layer(TraceLayer::new_for_http())
    .with_state(state)
}
