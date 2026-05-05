use axum::{Router, routing::get};
use sqlx::PgPool;
use tower_http::trace::TraceLayer;

use crate::routes;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route(
            "/assignments/{slug}",
            get(routes::assignments::get_assignment),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}
