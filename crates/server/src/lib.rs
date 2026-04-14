pub mod config;
pub mod dispatch;
pub mod intake;
pub mod logging;
pub mod registry;
pub mod routes;
pub mod state;

use axum::{routing::get, routing::post, Router};
use config::{Config, Method};
use state::AppState;
use tower_http::trace::TraceLayer;

/// Assemble the application router with all routes and middleware attached.
/// The caller is responsible for binding a listener and calling axum::serve.
pub fn build_router(state: AppState, config: &Config) -> Router {
  let mut router = Router::new()
    .route("/healthz", get(routes::health::healthz))
    .route("/api/work/connect", post(routes::sse::connect))
    .route("/api/work/poll", post(routes::work::poll))
    .route("/api/work/result", post(routes::work::result));

  for queue_cfg in config.queues.values() {
    if let (Some(ref path), Some(method)) = (&queue_cfg.route, queue_cfg.method)
    {
      let method_router = match method {
        Method::Get => get(routes::intake::handle_intake),
        Method::Post => post(routes::intake::handle_intake),
        Method::Put => axum::routing::put(routes::intake::handle_intake),
        Method::Patch => axum::routing::patch(routes::intake::handle_intake),
        Method::Delete => axum::routing::delete(routes::intake::handle_intake),
      };
      router = router.route(path, method_router);
    }
  }

  router.layer(TraceLayer::new_for_http()).with_state(state)
}
