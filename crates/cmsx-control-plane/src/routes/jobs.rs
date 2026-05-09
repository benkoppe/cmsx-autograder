use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
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

const DEFAULT_EVENT_LIMIT: i64 = 500;
const MAX_EVENT_LIMIT: i64 = 1000;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/jobs/{job_id}", get(get_job))
        .route("/jobs/{job_id}/events", get(get_job_events))
        .route("/jobs/{job_id}/cancel", post(cancel_job))
}

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    pub after_sequence: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct JobResponse {
    pub id: Uuid,
    pub submission_id: Uuid,
    pub assignment_id: Uuid,
    pub assignment_slug: String,
    pub assignment_name: String,
    pub worker_id: Option<Uuid>,
    pub worker_name: Option<String>,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub queued_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub failure_reason: Option<String>,
    pub failure_message: Option<String>,
    pub failure_retryable: Option<bool>,
    pub result: Option<JobResultSummary>,
}

#[derive(Debug, Serialize)]
pub struct JobResultSummary {
    pub status: String,
    pub score: f64,
    pub max_score: f64,
    pub duration_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct JobEventsResponse {
    pub events: Vec<JobEventResponse>,
    pub next_after_sequence: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct JobEventResponse {
    pub sequence: i64,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub stream: String,
    pub visibility: String,
    pub message: String,
    pub data: Value,
}

pub async fn get_job(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(job_id): Path<Uuid>,
) -> Result<Json<JobResponse>, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id,
            grading_jobs.submission_id,
            grading_jobs.assignment_id,
            assignments.slug AS assignment_slug,
            assignments.name AS assignment_name,
            grading_jobs.worker_id,
            workers.name AS "worker_name?",
            grading_jobs.status,
            grading_jobs.attempts,
            grading_jobs.max_attempts,
            grading_jobs.queued_at,
            grading_jobs.claimed_at,
            grading_jobs.started_at,
            grading_jobs.finished_at,
            grading_jobs.lease_expires_at,
            grading_jobs.last_heartbeat_at,
            grading_jobs.cancel_requested_at,
            grading_jobs.failure_reason,
            grading_jobs.failure_message,
            grading_jobs.failure_retryable,
            grading_results.status AS "result_status?",
            grading_results.score AS "result_score?",
            grading_results.max_score AS "result_max_score?",
            grading_results.duration_ms AS "result_duration_ms?",
            grading_results.created_at AS "result_created_at?"
        FROM grading_jobs
        JOIN assignments ON assignments.id = grading_jobs.assignment_id
        LEFT JOIN workers ON workers.id = grading_jobs.worker_id
        LEFT JOIN grading_results ON grading_results.job_id = grading_jobs.id
        WHERE grading_jobs.id = $1
        "#,
        job_id,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("job not found"))?;

    Ok(Json(JobResponse {
        id: row.id,
        submission_id: row.submission_id,
        assignment_id: row.assignment_id,
        assignment_slug: row.assignment_slug,
        assignment_name: row.assignment_name,
        worker_id: row.worker_id,
        worker_name: row.worker_name,
        status: row.status,
        attempts: row.attempts,
        max_attempts: row.max_attempts,
        queued_at: row.queued_at,
        claimed_at: row.claimed_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        lease_expires_at: row.lease_expires_at,
        last_heartbeat_at: row.last_heartbeat_at,
        cancel_requested_at: row.cancel_requested_at,
        failure_reason: row.failure_reason,
        failure_message: row.failure_message,
        failure_retryable: row.failure_retryable,
        result: match (
            row.result_status,
            row.result_score,
            row.result_max_score,
            row.result_created_at,
        ) {
            (Some(status), Some(score), Some(max_score), Some(created_at)) => {
                Some(JobResultSummary {
                    status,
                    score,
                    max_score,
                    duration_ms: row.result_duration_ms,
                    created_at,
                })
            }
            _ => None,
        },
    }))
}

pub async fn get_job_events(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(job_id): Path<Uuid>,
    Query(query): Query<EventQuery>,
) -> Result<Json<JobEventsResponse>, ApiError> {
    ensure_job_exists(&state, job_id).await?;

    let after_sequence = match query.after_sequence {
        Some(sequence) if sequence < 0 => {
            return Err(ApiError::bad_request("after_sequence must be nonnegative"));
        }
        Some(sequence) => sequence,
        None => -1,
    };

    let limit = bounded_limit(query.limit, DEFAULT_EVENT_LIMIT, MAX_EVENT_LIMIT, "limit")?;

    let rows = sqlx::query!(
        r#"
        SELECT
            sequence,
            timestamp,
            type,
            stream,
            visibility,
            message,
            data AS "data: SqlxJson<Value>"
        FROM job_events
        WHERE job_id = $1
          AND sequence > $2
        ORDER BY sequence ASC
        LIMIT $3
        "#,
        job_id,
        after_sequence,
        limit,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    let events: Vec<JobEventResponse> = rows
        .into_iter()
        .map(|row| JobEventResponse {
            sequence: row.sequence,
            timestamp: row.timestamp,
            event_type: row.r#type,
            stream: row.stream,
            visibility: row.visibility,
            message: row.message,
            data: row.data.0,
        })
        .collect();

    let next_after_sequence = events
        .last()
        .map(|event| event.sequence)
        .or(query.after_sequence);

    Ok(Json(JobEventsResponse {
        events,
        next_after_sequence,
    }))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(job_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let now = Utc::now();

    let update = sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET cancel_requested_at = $2
        WHERE id = $1
          AND status IN ('queued', 'claimed', 'running')
          AND cancel_requested_at IS NULL
        "#,
        job_id,
        now,
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() == 1 {
        return Ok(StatusCode::NO_CONTENT);
    }

    let row = sqlx::query!(
        r#"
        SELECT status, cancel_requested_at
        FROM grading_jobs
        WHERE id = $1
        "#,
        job_id,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("job not found"))?;

    if matches!(row.status.as_str(), "queued" | "claimed" | "running")
        && row.cancel_requested_at.is_some()
    {
        return Ok(StatusCode::NO_CONTENT);
    }

    Err(ApiError::conflict("job is already terminal"))
}

async fn ensure_job_exists(state: &AppState, job_id: Uuid) -> Result<(), ApiError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM grading_jobs
            WHERE id = $1
        )
        "#,
        job_id,
    )
    .fetch_one(&state.db)
    .await
    .map_err(ApiError::internal)?
    .unwrap_or(false);

    if !exists {
        return Err(ApiError::not_found("job not found"));
    }

    Ok(())
}
