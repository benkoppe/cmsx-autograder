use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use sqlx::PgPool;
use tower_http::trace::TraceLayer;

use crate::{
    config::{AdminConfig, CmsxConfig},
    routes,
    storage::Storage,
};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub storage: Storage,
    pub cmsx: CmsxConfig,
    pub admin: AdminConfig,
}

pub fn router(state: AppState) -> Router {
    let max_body_bytes = state.cmsx.max_body_bytes;

    Router::new()
        .merge(routes::health::router())
        .merge(routes::assignments::router())
        .merge(routes::cmsx::router(max_body_bytes))
        .merge(routes::workers::router())
        .merge(routes::admin::router())
        .merge(routes::inspection::router())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}
