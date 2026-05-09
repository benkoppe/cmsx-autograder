use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::{
    app::AppState,
    error::ApiError,
    routes::{admin::AdminAuth, common::bounded_limit},
};

const DEFAULT_SUBMISSION_LIMIT: i64 = 100;
const MAX_SUBMISSION_LIMIT: i64 = 500;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/assignments/{slug}/submissions",
            get(list_assignment_submissions),
        )
        .route(
            "/submissions/{submission_id}/results",
            get(get_submission_results),
        )
}

#[derive(Debug, Deserialize)]
pub struct SubmissionQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SubmissionResponse {
    pub id: Uuid,
    pub assignment_id: Uuid,
    pub assignment_slug: String,
    pub cmsx_group_id: String,
    pub cmsx_assignment_id: String,
    pub cmsx_assignment_name: String,
    pub netids_raw: String,
    pub received_at: DateTime<Utc>,
    pub file_count: i64,
    pub latest_job: Option<SubmissionJobSummary>,
}

#[derive(Debug, Serialize)]
pub struct SubmissionJobSummary {
    pub id: Uuid,
    pub status: String,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct SubmissionResultResponse {
    pub job_id: Uuid,
    pub job_status: String,
    pub attempts: i32,
    pub result_status: String,
    pub score: f64,
    pub max_score: f64,
    pub feedback: Option<String>,
    pub tests: Value,
    pub result: Value,
    pub stdout_summary: Option<String>,
    pub stderr_summary: Option<String>,
    pub duration_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
}

pub async fn list_assignment_submissions(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(slug): Path<String>,
    Query(query): Query<SubmissionQuery>,
) -> Result<Json<Vec<SubmissionResponse>>, ApiError> {
    let assignment_id = sqlx::query_scalar!(
        r#"
        SELECT id
        FROM assignments
        WHERE slug = $1
        "#,
        slug,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("assignment not found"))?;

    let limit = bounded_limit(
        query.limit,
        DEFAULT_SUBMISSION_LIMIT,
        MAX_SUBMISSION_LIMIT,
        "limit",
    )?;

    let rows = sqlx::query!(
        r#"
        SELECT
            submissions.id,
            submissions.assignment_id,
            assignments.slug AS assignment_slug,
            submissions.cmsx_group_id,
            submissions.cmsx_assignment_id,
            submissions.cmsx_assignment_name,
            submissions.netids_raw,
            submissions.received_at,
            COUNT(submission_files.id) AS "file_count!",
            latest_job.id AS "latest_job_id?",
            latest_job.status AS "latest_job_status?",
            latest_job.queued_at AS "latest_job_queued_at?",
            latest_job.started_at AS "latest_job_started_at?",
            latest_job.finished_at AS "latest_job_finished_at?"
        FROM submissions
        JOIN assignments ON assignments.id = submissions.assignment_id
        LEFT JOIN submission_files ON submission_files.submission_id = submissions.id
        LEFT JOIN LATERAL (
            SELECT id, status, queued_at, started_at, finished_at
            FROM grading_jobs
            WHERE grading_jobs.submission_id = submissions.id
            ORDER BY queued_at DESC, id DESC
            LIMIT 1
        ) AS latest_job ON true
        WHERE submissions.assignment_id = $1
        GROUP BY
            submissions.id,
            assignments.slug,
            latest_job.id,
            latest_job.status,
            latest_job.queued_at,
            latest_job.started_at,
            latest_job.finished_at
        ORDER BY submissions.received_at DESC, submissions.id DESC
        LIMIT $2
        "#,
        assignment_id,
        limit,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(
        rows.into_iter()
            .map(|row| SubmissionResponse {
                id: row.id,
                assignment_id: row.assignment_id,
                assignment_slug: row.assignment_slug,
                cmsx_group_id: row.cmsx_group_id,
                cmsx_assignment_id: row.cmsx_assignment_id,
                cmsx_assignment_name: row.cmsx_assignment_name,
                netids_raw: row.netids_raw,
                received_at: row.received_at,
                file_count: row.file_count,
                latest_job: match (
                    row.latest_job_id,
                    row.latest_job_status,
                    row.latest_job_queued_at,
                ) {
                    (Some(id), Some(status), Some(queued_at)) => Some(SubmissionJobSummary {
                        id,
                        status,
                        queued_at,
                        started_at: row.latest_job_started_at,
                        finished_at: row.latest_job_finished_at,
                    }),
                    _ => None,
                },
            })
            .collect(),
    ))
}

pub async fn get_submission_results(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(submission_id): Path<Uuid>,
) -> Result<Json<Vec<SubmissionResultResponse>>, ApiError> {
    ensure_submission_exists(&state, submission_id).await?;

    let rows = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id AS job_id,
            grading_jobs.status AS job_status,
            grading_jobs.attempts,
            grading_results.status AS result_status,
            grading_results.score,
            grading_results.max_score,
            grading_results.feedback,
            grading_results.tests AS "tests: SqlxJson<Value>",
            grading_results.result AS "result: SqlxJson<Value>",
            grading_results.stdout_summary,
            grading_results.stderr_summary,
            grading_results.duration_ms,
            grading_results.created_at
        FROM grading_results
        JOIN grading_jobs ON grading_jobs.id = grading_results.job_id
        WHERE grading_jobs.submission_id = $1
        ORDER BY grading_results.created_at DESC, grading_results.id DESC
        "#,
        submission_id,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(
        rows.into_iter()
            .map(|row| SubmissionResultResponse {
                job_id: row.job_id,
                job_status: row.job_status,
                attempts: row.attempts,
                result_status: row.result_status,
                score: row.score,
                max_score: row.max_score,
                feedback: row.feedback,
                tests: row.tests.0,
                result: row.result.0,
                stdout_summary: row.stdout_summary,
                stderr_summary: row.stderr_summary,
                duration_ms: row.duration_ms,
                created_at: row.created_at,
            })
            .collect(),
    ))
}

async fn ensure_submission_exists(state: &AppState, submission_id: Uuid) -> Result<(), ApiError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM submissions
            WHERE id = $1
        )
        "#,
        submission_id,
    )
    .fetch_one(&state.db)
    .await
    .map_err(ApiError::internal)?
    .unwrap_or(false);

    if !exists {
        return Err(ApiError::not_found("submission not found"));
    }

    Ok(())
}
