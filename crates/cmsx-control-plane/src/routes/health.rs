use axum::{Router, routing::get};

use crate::app::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/healthz", get(healthz))
}

pub async fn healthz() -> &'static str {
    "ok"
}
