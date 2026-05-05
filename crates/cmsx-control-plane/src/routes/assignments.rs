use axum::{
    Json,
    extract::{Path, State},
};
use serde::Serialize;
use serde_json::Value;

use crate::{app::AppState, error::ApiError};

#[derive(Debug, Serialize)]
pub struct AssignmentResponse {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub cmsx_assignment_id: String,
    pub max_score: f64,
    pub execution_config: Value,
    pub runner_config: Value,
    pub capabilities: Value,
    pub created_at: String,
    pub updated_at: String,
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
            cmsx_assignment_id,
            max_score,
            execution_config_json,
            runner_config_json,
            capabilities_json,
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

    let execution_config =
        serde_json::from_str(&assignment.execution_config_json).map_err(ApiError::internal)?;
    let runner_config =
        serde_json::from_str(&assignment.runner_config_json).map_err(ApiError::internal)?;
    let capabilities =
        serde_json::from_str(&assignment.capabilities_json).map_err(ApiError::internal)?;

    Ok(Json(AssignmentResponse {
        id: assignment.id,
        slug: assignment.slug,
        name: assignment.name,
        cmsx_assignment_id: assignment.cmsx_assignment_id,
        max_score: assignment.max_score,
        execution_config,
        runner_config,
        capabilities,
        created_at: assignment.created_at,
        updated_at: assignment.updated_at,
    }))
}
