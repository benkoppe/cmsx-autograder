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

use cmsx_core::{GradingResult, JobStatus};

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
    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    let row = sqlx::query!(
        r#"
        SELECT status, cancel_requested_at
        FROM grading_jobs
        WHERE id = $1
        FOR UPDATE
        "#,
        job_id
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("job not found"))?;

    let status = row
        .status
        .parse::<JobStatus>()
        .map_err(|_| ApiError::internal("invalid job status in database"))?;

    match status {
        JobStatus::Queued => {
            // terminalize immediately
            sqlx::query!(
                r#"
                UPDATE grading_jobs
                SET status = 'cancelled',
                    cancel_requested_at = COALESCE(cancel_requested_at, $2),
                    finished_at = $2,
                    lease_expires_at = NULL,
                    worker_id = NULL,
                    claimed_at = NULL,
                    started_at = NULL,
                    last_heartbeat_at = NULL
                WHERE id = $1
                "#,
                job_id,
                now,
            )
            .execute(&mut *tx)
            .await
            .map_err(ApiError::internal)?;
            insert_cancelled_result(&mut tx, job_id, now).await?;
        }
        JobStatus::Claimed | JobStatus::Running if row.cancel_requested_at.is_none() => {
            // request active cancellation
            sqlx::query!(
                r#"
                UPDATE grading_jobs
                SET cancel_requested_at = $2
                WHERE id = $1
                "#,
                job_id,
                now,
            )
            .execute(&mut *tx)
            .await
            .map_err(ApiError::internal)?;
        }
        JobStatus::Claimed | JobStatus::Running | JobStatus::Cancelled => {
            // idempotent cancellation request
        }
        JobStatus::Succeeded | JobStatus::Failed | JobStatus::Error => {
            return Err(ApiError::conflict("job is already terminal"));
        }
    }

    tx.commit().await.map_err(ApiError::internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn insert_cancelled_result(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), ApiError> {
    let result = GradingResult::cancelled();
    let result_json = serde_json::to_value(&result).map_err(ApiError::internal)?;
    let tests_json = serde_json::to_value(&result.tests).map_err(ApiError::internal)?;

    sqlx::query!(
        r#"
        INSERT INTO grading_results (
            id,
            job_id,
            status,
            score,
            max_score,
            feedback,
            tests,
            result,
            stdout_summary,
            stderr_summary,
            duration_ms,
            created_at
        )
        VALUES ($1, $2, 'cancelled', 0, 0, $3, $4, $5, NULL, NULL, 0, $6)
        ON CONFLICT (job_id) DO NOTHING
        "#,
        Uuid::now_v7(),
        job_id,
        result.feedback,
        SqlxJson(tests_json) as _,
        SqlxJson(result_json) as _,
        now,
    )
    .execute(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    Ok(())
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

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;
    use uuid::Uuid;

    use cmsx_core::{GradingResult, JobResultRequest, WorkerHeartbeatRequest, WorkerStatus};

    use crate::test_support;

    fn cancelled_result_request() -> JobResultRequest {
        JobResultRequest {
            result: GradingResult::cancelled(),
            duration_ms: Some(0),
            stdout_summary: None,
            stderr_summary: None,
        }
    }

    #[tokio::test]
    async fn job_routes_reject_missing_admin_token() {
        let app = test_support::test_app().await;
        let job_id = Uuid::now_v7();

        let response = test_support::get(&app.app, &format!("/jobs/{job_id}")).await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn get_job_returns_queued_job() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response = test_support::admin_get(&app.app, &format!("/jobs/{}", setup.job_id)).await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["id"], setup.job_id.to_string());
        assert_eq!(body["submission_id"], setup.submission_id.to_string());
        assert_eq!(body["assignment_slug"], test_support::TEST_ASSIGNMENT_SLUG);
        assert_eq!(body["status"], "queued");
        assert!(body["worker_id"].is_null());
        assert!(body["result"].is_null());
    }

    #[tokio::test]
    async fn get_job_returns_claimed_job_with_worker() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_claimed_job(&app).await;

        let response = test_support::admin_get(&app.app, &format!("/jobs/{}", setup.job_id)).await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "claimed");
        assert_eq!(body["worker_name"], test_support::TEST_WORKER_NAME);
        assert_eq!(body["attempts"], 1);
        assert!(!body["worker_id"].is_null());
        assert!(!body["lease_expires_at"].is_null());
    }

    #[tokio::test]
    async fn get_job_returns_completed_job_with_result_summary() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_completed_job(&app).await;

        let response = test_support::admin_get(&app.app, &format!("/jobs/{}", setup.job_id)).await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded");
        assert_eq!(body["result"]["status"], "passed");
        assert_eq!(body["result"]["score"], 100.0);
        assert_eq!(body["result"]["max_score"], 100.0);
        assert_eq!(body["result"]["duration_ms"], 123);
    }

    #[tokio::test]
    async fn get_job_events_returns_sequence_order() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_claimed_job(&app).await;

        test_support::post_test_job_events(
            &app,
            &setup,
            vec![
                test_support::test_event(0, "first"),
                test_support::test_event(1, "second"),
                test_support::test_event(2, "third"),
            ],
        )
        .await;

        let response =
            test_support::admin_get(&app.app, &format!("/jobs/{}/events", setup.job_id)).await;
        let (status, body) = test_support::response_json(response).await;
        let events = body["events"]
            .as_array()
            .expect("events should be an array");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["sequence"], 0);
        assert_eq!(events[1]["sequence"], 1);
        assert_eq!(events[2]["sequence"], 2);
        assert_eq!(body["next_after_sequence"], 2);
    }

    #[tokio::test]
    async fn get_job_events_after_sequence_skips_old_events() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_claimed_job(&app).await;

        test_support::post_test_job_events(
            &app,
            &setup,
            vec![
                test_support::test_event(0, "first"),
                test_support::test_event(1, "second"),
                test_support::test_event(2, "third"),
            ],
        )
        .await;

        let response = test_support::admin_get(
            &app.app,
            &format!("/jobs/{}/events?after_sequence=0", setup.job_id),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;
        let events = body["events"]
            .as_array()
            .expect("events should be an array");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["sequence"], 1);
        assert_eq!(events[1]["sequence"], 2);
        assert_eq!(body["next_after_sequence"], 2);
    }

    #[tokio::test]
    async fn get_job_events_rejects_negative_after_sequence() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response = test_support::admin_get(
            &app.app,
            &format!("/jobs/{}/events?after_sequence=-1", setup.job_id),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_job_events_rejects_nonpositive_limit() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response =
            test_support::admin_get(&app.app, &format!("/jobs/{}/events?limit=0", setup.job_id))
                .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_job_events_caps_large_limit() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_claimed_job(&app).await;

        test_support::post_test_job_events(
            &app,
            &setup,
            vec![test_support::test_event(0, "first")],
        )
        .await;

        let response = test_support::admin_get(
            &app.app,
            &format!("/jobs/{}/events?limit=999999", setup.job_id),
        )
        .await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancel_queued_job_sets_cancel_requested_at() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let cancel_requested_at = sqlx::query_scalar!(
            r#"
            SELECT cancel_requested_at
            FROM grading_jobs
            WHERE id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load job cancellation");

        assert!(cancel_requested_at.is_some());

        let repeated = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;

        assert_eq!(repeated.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn cancel_queued_job_terminalizes_immediately() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;

        let response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let row = sqlx::query!(
            r#"
            SELECT grading_jobs.status, grading_jobs.finished_at, grading_results.status AS "result_status?"
            FROM grading_jobs
            LEFT JOIN grading_results ON grading_results.job_id = grading_jobs.id
            WHERE grading_jobs.id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load cancelled job");

        assert_eq!(row.status, "cancelled");
        assert!(row.finished_at.is_some());
        assert_eq!(row.result_status.as_deref(), Some("cancelled"));
    }

    #[tokio::test]
    async fn cancelled_queued_job_is_not_claimed() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_queued_job(&app).await;
        let private_key = test_support::create_test_worker(&app).await;

        test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;

        let response = test_support::worker_post_json(
            &app.app,
            &private_key,
            "/workers/jobs/claim",
            &cmsx_core::ClaimJobRequest {
                available_slots: 1,
                wait_seconds: Some(0),
            },
        )
        .await;

        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert!(body["jobs"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn heartbeat_extends_lease_for_cancelled_active_job() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_running_job(&app).await;

        test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;

        let before = sqlx::query_scalar!(
            r#"
            SELECT lease_expires_at
            FROM grading_jobs
            WHERE id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .unwrap()
        .unwrap();

        let heartbeat = cmsx_core::WorkerHeartbeatRequest {
            version: "0.1.0".to_string(),
            status: cmsx_core::WorkerStatus::Online,
            running_jobs: 1,
            max_jobs: 1,
            active_job_ids: vec![setup.job_id],
        };

        let response = test_support::worker_post_json(
            &app.app,
            &setup.private_key,
            "/workers/heartbeat",
            &heartbeat,
        )
        .await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["cancelled_job_ids"][0], setup.job_id.to_string());

        let after = sqlx::query_scalar!(
            r#"
            SELECT lease_expires_at
            FROM grading_jobs
            WHERE id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .unwrap()
        .unwrap();

        assert!(after > before);
    }

    #[tokio::test]
    async fn cancel_running_job_is_observed_by_heartbeat() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_running_job(&app).await;

        let response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let heartbeat = WorkerHeartbeatRequest {
            version: "0.1.0".to_string(),
            status: WorkerStatus::Online,
            running_jobs: 1,
            max_jobs: 1,
            active_job_ids: vec![setup.job_id],
        };

        let response = test_support::worker_post_json(
            &app.app,
            &setup.private_key,
            "/workers/heartbeat",
            &heartbeat,
        )
        .await;
        let (status, body) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["cancelled_job_ids"][0], setup.job_id.to_string());
    }

    #[tokio::test]
    async fn cancel_terminal_job_returns_conflict() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_completed_job(&app).await;

        let response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn missing_job_returns_not_found() {
        let app = test_support::test_app().await;
        let job_id = Uuid::now_v7();

        let get_job = test_support::admin_get(&app.app, &format!("/jobs/{job_id}")).await;
        let cancel_job =
            test_support::admin_post_json(&app.app, &format!("/jobs/{job_id}/cancel"), &json!({}))
                .await;
        let get_events = test_support::admin_get(&app.app, &format!("/jobs/{job_id}/events")).await;

        assert_eq!(get_job.status(), StatusCode::NOT_FOUND);
        assert_eq!(cancel_job.status(), StatusCode::NOT_FOUND);
        assert_eq!(get_events.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancelled_claimed_job_rejects_start_but_accepts_cancelled_result() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_claimed_job(&app).await;

        let cancel_response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;
        assert_eq!(cancel_response.status(), StatusCode::NO_CONTENT);

        let start_response = test_support::worker_post_json(
            &app.app,
            &setup.private_key,
            &format!("/workers/jobs/{}/started", setup.job_id),
            &json!({}),
        )
        .await;
        assert_eq!(start_response.status(), StatusCode::CONFLICT);

        let result_response = test_support::worker_post_json(
            &app.app,
            &setup.private_key,
            &format!("/workers/jobs/{}/result", setup.job_id),
            &cancelled_result_request(),
        )
        .await;
        assert_eq!(result_response.status(), StatusCode::NO_CONTENT);

        let row = sqlx::query!(
            r#"
            SELECT grading_jobs.status, grading_results.status AS "result_status?"
            FROM grading_jobs
            LEFT JOIN grading_results ON grading_results.job_id = grading_jobs.id
            WHERE grading_jobs.id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load cancelled claimed job");

        assert_eq!(row.status, "cancelled");
        assert_eq!(row.result_status.as_deref(), Some("cancelled"));
    }

    #[tokio::test]
    async fn cancelled_running_job_rejects_non_cancelled_result() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_running_job(&app).await;

        let cancel_response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;
        assert_eq!(cancel_response.status(), StatusCode::NO_CONTENT);

        let result_response = test_support::worker_post_json(
            &app.app,
            &setup.private_key,
            &format!("/workers/jobs/{}/result", setup.job_id),
            &test_support::test_job_result_request(),
        )
        .await;

        assert_eq!(result_response.status(), StatusCode::CONFLICT);

        let row = sqlx::query!(
            r#"
            SELECT grading_jobs.status, grading_jobs.cancel_requested_at, grading_results.status AS "result_status?"
            FROM grading_jobs
            LEFT JOIN grading_results ON grading_results.job_id = grading_jobs.id
            WHERE grading_jobs.id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load cancelled running job");

        assert_eq!(row.status, "running");
        assert!(row.cancel_requested_at.is_some());
        assert!(row.result_status.is_none());
    }

    #[tokio::test]
    async fn expired_cancelled_active_job_gets_cancelled_result() {
        let app = test_support::test_app().await;
        let setup = test_support::setup_running_job(&app).await;

        let cancel_response = test_support::admin_post_json(
            &app.app,
            &format!("/jobs/{}/cancel", setup.job_id),
            &json!({}),
        )
        .await;
        assert_eq!(cancel_response.status(), StatusCode::NO_CONTENT);

        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET lease_expires_at = $2
            WHERE id = $1
            "#,
            setup.job_id,
            chrono::Utc::now() - chrono::Duration::seconds(1),
        )
        .execute(&app.db)
        .await
        .expect("failed to expire cancelled job lease");

        let claim_response = test_support::worker_post_json(
            &app.app,
            &setup.private_key,
            "/workers/jobs/claim",
            &cmsx_core::ClaimJobRequest {
                available_slots: 1,
                wait_seconds: Some(0),
            },
        )
        .await;
        assert_eq!(claim_response.status(), StatusCode::OK);

        let row = sqlx::query!(
            r#"
            SELECT grading_jobs.status, grading_results.status AS "result_status?"
            FROM grading_jobs
            LEFT JOIN grading_results ON grading_results.job_id = grading_jobs.id
            WHERE grading_jobs.id = $1
            "#,
            setup.job_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load expired cancelled job");

        assert_eq!(row.status, "cancelled");
        assert_eq!(row.result_status.as_deref(), Some("cancelled"));
    }
}
