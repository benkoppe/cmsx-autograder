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

use crate::{app::AppState, error::ApiError, routes::admin::AdminAuth};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/jobs/{job_id}", get(get_job))
        .route("/jobs/{job_id}/events", get(get_job_events))
        .route("/jobs/{job_id}/cancel", post(cancel_job))
        .route(
            "/assignments/{slug}/submissions",
            get(list_assignment_submissions),
        )
        .route(
            "/submissions/{submission_id}/results",
            get(get_submission_results),
        )
}

const DEFAULT_EVENT_LIMIT: i64 = 500;
const MAX_EVENT_LIMIT: i64 = 1000;
const DEFAULT_SUBMISSION_LIMIT: i64 = 100;
const MAX_SUBMISSION_LIMIT: i64 = 500;

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    pub after_sequence: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SubmissionQuery {
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

fn bounded_limit(
    value: Option<i64>,
    default: i64,
    max: i64,
    field: &'static str,
) -> Result<i64, ApiError> {
    match value {
        Some(value) if value <= 0 => {
            Err(ApiError::bad_request(format!("{field} must be positive")))
        }
        Some(value) => Ok(value.min(max)),
        None => Ok(default),
    }
}
