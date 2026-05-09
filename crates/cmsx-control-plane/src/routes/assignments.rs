use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::{app::AppState, error::ApiError};

pub fn router() -> Router<AppState> {
    Router::new().route("/assignments/{slug}", get(get_assignment))
}

#[derive(Debug, Serialize)]
pub struct AssignmentResponse {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub max_score: f64,
    pub execution_config: Value,
    pub runner_config: Value,
    pub capabilities: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub async fn get_assignment(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<AssignmentResponse>, ApiError> {
    let assignment = sqlx::query!(
        r#"
        SELECT
            id,
            slug,
            name,
            max_score,
            execution_config AS "execution_config: SqlxJson<Value>",
            runner_config AS "runner_config: SqlxJson<Value>",
            capabilities AS "capabilities: SqlxJson<Value>",
            created_at,
            updated_at
        FROM assignments
        WHERE slug = $1
        LIMIT 1
        "#,
        slug
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?;

    let Some(assignment) = assignment else {
        return Err(ApiError::not_found("assignment not found"));
    };

    Ok(Json(AssignmentResponse {
        id: assignment.id,
        slug: assignment.slug,
        name: assignment.name,
        max_score: assignment.max_score,
        execution_config: assignment.execution_config.0,
        runner_config: assignment.runner_config.0,
        capabilities: assignment.capabilities.0,
        created_at: assignment.created_at,
        updated_at: assignment.updated_at,
    }))
}
