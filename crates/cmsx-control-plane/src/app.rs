use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use sqlx::PgPool;
use tower_http::trace::TraceLayer;

use crate::{config::CmsxConfig, routes, storage::Storage};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub storage: Storage,
    pub cmsx: CmsxConfig,
}

pub fn router(state: AppState) -> Router {
    let max_body_bytes = state.cmsx.max_body_bytes;

    Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route(
            "/assignments/{slug}",
            get(routes::assignments::get_assignment),
        )
        .route(
            "/cmsx/a/{slug}/submit",
            post(routes::cmsx::submit).layer(DefaultBodyLimit::max(max_body_bytes)),
        )
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}
