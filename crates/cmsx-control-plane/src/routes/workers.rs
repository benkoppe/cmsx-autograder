use std::collections::HashSet;

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{FromRef, FromRequest, FromRequestParts, Path, Request, State},
    http::{HeaderMap, StatusCode, header, request::Parts},
    response::Response,
};
use chrono::{Duration, Utc};
use jwt_simple::prelude::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, ClaimedJob, ClaimedJobFile, JobEventBatchRequest,
    JobFailureRequest, JobResultRequest, StartedJobRequest, WorkerAuthClaims,
    WorkerHeartbeatRequest, WorkerHeartbeatResponse,
};

use crate::{app::AppState, error::ApiError};

const WORKER_REQUEST_MAX_BYTES: usize = 1024 * 1024;
const WORKER_EVENT_BATCH_MAX_EVENTS: usize = 512;
const WORKER_EVENT_MESSAGE_MAX_BYTES: usize = 64 * 1024;

const WORKER_AUDIENCE: &str = "cmsx-control-plane";
const LEASE_SECONDS: i64 = 60;

#[derive(Debug, Clone)]
pub struct AuthenticatedWorker {
    pub id: Uuid,
}

pub struct WorkerJson<T> {
    pub worker: AuthenticatedWorker,
    pub value: T,
}

impl<S> FromRequestParts<S> for AuthenticatedWorker
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        let method = parts.method.as_str();
        let path = parts.uri.path();

        authenticate_worker(&app_state, method, path, &parts.headers, &[]).await
    }
}

impl<S, T> FromRequest<S> for WorkerJson<T>
where
    AppState: axum::extract::FromRef<S>,
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        let method = req.method().as_str().to_owned();
        let path = req.uri().path().to_owned();
        let headers = req.headers().clone();

        let body = Bytes::from_request(req, state)
            .await
            .map_err(ApiError::internal)?;

        if body.len() > WORKER_REQUEST_MAX_BYTES {
            return Err(ApiError::payload_too_large("worker request body too large"));
        }

        let worker = authenticate_worker(&app_state, &method, &path, &headers, &body).await?;
        let value =
            serde_json::from_slice(&body).map_err(|_| ApiError::bad_request("invalid json"))?;

        Ok(Self { worker, value })
    }
}

async fn authenticate_worker(
    state: &AppState,
    method: &str,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<AuthenticatedWorker, ApiError> {
    let token = worker_token_from_headers(headers)?;
    let body_sha256 = hex::encode(Sha256::digest(body));

    verify_worker_jwt(state, token, method, path, &body_sha256).await
}

fn worker_token_from_headers(headers: &HeaderMap) -> Result<&str, ApiError> {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::unauthorized("missing worker authorization"))?;

    auth_header
        .strip_prefix("WorkerJWT ")
        .ok_or_else(|| ApiError::unauthorized("invalid worker authorization scheme"))
}

async fn verify_worker_jwt(
    state: &AppState,
    token: &str,
    method: &str,
    path: &str,
    body_sha256: &str,
) -> Result<AuthenticatedWorker, ApiError> {
    let metadata = Token::decode_metadata(token)
        .map_err(|_| ApiError::unauthorized("invalid worker token"))?;

    let fingerprint = metadata
        .key_id()
        .ok_or_else(|| ApiError::unauthorized("missing worker key id"))?
        .to_string();

    let keys = sqlx::query!(
        r#"
        SELECT worker_keys.worker_id, worker_keys.public_key
        FROM worker_keys
        JOIN workers ON workers.id = worker_keys.worker_id
        WHERE worker_keys.public_key_fingerprint = $1
          AND worker_keys.revoked_at IS NULL
          AND workers.status != 'disabled'
        "#,
        fingerprint
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    for key in keys {
        let worker_id = key.worker_id;
        let public_key = Ed25519PublicKey::from_pem(&key.public_key).map_err(ApiError::internal)?;

        let options = VerificationOptions {
            allowed_audiences: Some(HashSet::from([WORKER_AUDIENCE.to_string()])),
            required_key_id: Some(fingerprint.clone()),
            time_tolerance: Some(jwt_simple::prelude::Duration::from_secs(60)),
            max_validity: Some(jwt_simple::prelude::Duration::from_secs(60)),
            ..Default::default()
        };

        let Ok(claims) = public_key.verify_token::<WorkerAuthClaims>(token, Some(options)) else {
            continue;
        };

        let custom = claims.custom;

        if custom.iss != format!("worker-key:{fingerprint}") {
            continue;
        }
        if custom.aud != WORKER_AUDIENCE {
            continue;
        }
        if custom.method != method {
            continue;
        }
        if custom.path != path {
            continue;
        }
        if custom.body_sha256 != body_sha256 {
            continue;
        }

        record_worker_jti(state, worker_id, custom.jti).await?;

        return Ok(AuthenticatedWorker { id: worker_id });
    }

    Err(ApiError::unauthorized("worker authentication failed"))
}

async fn record_worker_jti(state: &AppState, worker_id: Uuid, jti: Uuid) -> Result<(), ApiError> {
    let now = Utc::now();
    let expires_at = now + Duration::seconds(120);

    sqlx::query!(
        r#"
        DELETE FROM worker_request_nonces
        WHERE expires_at <= $1
        "#,
        now
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    let insert = sqlx::query!(
        r#"
        INSERT INTO worker_request_nonces (worker_id, jti, expires_at)
        VALUES ($1, $2, $3)
        "#,
        worker_id,
        jti,
        expires_at
    )
    .execute(&state.db)
    .await;

    if let Err(error) = insert {
        if is_unique_violation(&error) {
            return Err(ApiError::unauthorized("replayed worker token"));
        }

        return Err(ApiError::internal(error));
    }

    Ok(())
}

pub async fn heartbeat(
    State(state): State<AppState>,
    WorkerJson { worker, value }: WorkerJson<WorkerHeartbeatRequest>,
) -> Result<Json<WorkerHeartbeatResponse>, ApiError> {
    let now = Utc::now();

    validate_heartbeat_request(&value)?;

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    record_worker_heartbeat(&mut tx, worker.id, &value, now).await?;
    let reconciliation =
        reconcile_active_jobs(&mut tx, worker.id, &value.active_job_ids, now).await?;

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(Json(WorkerHeartbeatResponse {
        worker_id: worker.id,
        lease_seconds: LEASE_SECONDS,
        renewed_job_ids: reconciliation.renewed_job_ids,
        cancelled_job_ids: reconciliation.cancelled_job_ids,
        unknown_job_ids: reconciliation.unknown_job_ids,
    }))
}

struct HeartbeatReconciliation {
    renewed_job_ids: Vec<Uuid>,
    cancelled_job_ids: Vec<Uuid>,
    unknown_job_ids: Vec<Uuid>,
}

async fn record_worker_heartbeat(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    worker_id: Uuid,
    value: &WorkerHeartbeatRequest,
    now: chrono::DateTime<Utc>,
) -> Result<(), ApiError> {
    sqlx::query!(
        r#"
        UPDATE workers
        SET status = 'online',
            version = $2,
            last_seen_at = $3
        WHERE id = $1
        "#,
        worker_id,
        value.version,
        now
    )
    .execute(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    sqlx::query!(
        r#"
        INSERT INTO worker_heartbeats (
            id,
            worker_id,
            status,
            version,
            running_jobs,
            max_jobs,
            reported_at
        )
        VALUES ($1, $2, 'online', $3, $4, $5, $6)
        "#,
        Uuid::now_v7(),
        worker_id,
        value.version,
        value.running_jobs,
        value.max_jobs,
        now
    )
    .execute(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    Ok(())
}

async fn reconcile_active_jobs(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    worker_id: Uuid,
    active_job_ids: &[Uuid],
    now: chrono::DateTime<Utc>,
) -> Result<HeartbeatReconciliation, ApiError> {
    let mut renewed_job_ids = Vec::new();
    let mut cancelled_job_ids = Vec::new();
    let mut unknown_job_ids = Vec::new();

    for job_id in active_job_ids {
        let row = sqlx::query!(
            r#"
            SELECT status, worker_id, lease_expires_at, cancel_requested_at
            FROM grading_jobs
            WHERE id = $1
            "#,
            job_id
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(ApiError::internal)?;

        let Some(row) = row else {
            unknown_job_ids.push(*job_id);
            continue;
        };

        if row.worker_id != Some(worker_id)
            || !matches!(row.status.as_str(), "claimed" | "running")
            || row
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= now)
        {
            unknown_job_ids.push(*job_id);
            continue;
        }

        if row.cancel_requested_at.is_some() {
            cancelled_job_ids.push(*job_id);
            continue;
        }

        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET lease_expires_at = $3,
                last_heartbeat_at = $4
            WHERE id = $1
              AND worker_id = $2
              AND status IN ('claimed', 'running')
              AND lease_expires_at > $4
              AND cancel_requested_at IS NULL
            "#,
            job_id,
            worker_id,
            now + Duration::seconds(LEASE_SECONDS),
            now
        )
        .execute(&mut **tx)
        .await
        .map_err(ApiError::internal)?;

        renewed_job_ids.push(*job_id);
    }

    Ok(HeartbeatReconciliation {
        renewed_job_ids,
        cancelled_job_ids,
        unknown_job_ids,
    })
}

pub async fn claim_job(
    State(state): State<AppState>,
    WorkerJson { worker, value }: WorkerJson<ClaimJobRequest>,
) -> Result<Json<ClaimJobResponse>, ApiError> {
    validate_claim_request(&value)?;

    if value.available_slots <= 0 {
        return Ok(Json(ClaimJobResponse { jobs: Vec::new() }));
    }

    let jobs = claim_available_jobs(&state, worker.id, &value).await?;
    Ok(Json(ClaimJobResponse { jobs }))
}

async fn claim_available_jobs(
    state: &AppState,
    worker_id: Uuid,
    request: &ClaimJobRequest,
) -> Result<Vec<ClaimedJob>, ApiError> {
    let now = Utc::now();
    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    mark_exhausted_expired_jobs(&mut tx, now).await?;

    let rows = sqlx::query!(
        r#"
        WITH candidate AS (
            SELECT grading_jobs.id
            FROM grading_jobs
            WHERE (
                grading_jobs.status = 'queued'
                OR (
                    grading_jobs.status IN ('claimed', 'running')
                    AND grading_jobs.lease_expires_at <= $2
                    AND grading_jobs.attempts < grading_jobs.max_attempts
                    AND grading_jobs.cancel_requested_at IS NULL
                )
            )
            ORDER BY grading_jobs.queued_at
            FOR UPDATE SKIP LOCKED
            LIMIT $3
        )
        UPDATE grading_jobs
        SET status = 'claimed',
            worker_id = $1,
            attempts = attempts + 1,
            claimed_at = $2,
            started_at = NULL,
            finished_at = NULL,
            lease_expires_at = $4,
            last_heartbeat_at = $2,
            failure_reason = NULL,
            failure_message = NULL,
            failure_retryable = NULL
        FROM candidate
        WHERE grading_jobs.id = candidate.id
        RETURNING
            grading_jobs.id
        "#,
        worker_id,
        now,
        i64::from(request.available_slots),
        now + Duration::seconds(LEASE_SECONDS)
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    let mut jobs = Vec::with_capacity(rows.len());

    for row in rows {
        jobs.push(load_claimed_job_in_tx(&mut tx, worker_id, row.id).await?);
    }

    tx.commit().await.map_err(ApiError::internal)?;
    Ok(jobs)
}

async fn mark_exhausted_expired_jobs(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: chrono::DateTime<Utc>,
) -> Result<(), ApiError> {
    sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = 'error',
            finished_at = $1,
            lease_expires_at = NULL,
            failure_reason = 'lease_expired',
            failure_message = 'job lease expired and max attempts were exhausted',
            failure_retryable = false
        WHERE status IN ('claimed', 'running')
          AND lease_expires_at <= $1
          AND attempts >= max_attempts
        "#,
        now
    )
    .execute(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    Ok(())
}

async fn load_claimed_job_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    worker_id: Uuid,
    job_id: Uuid,
) -> Result<ClaimedJob, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id,
            grading_jobs.submission_id,
            grading_jobs.assignment_id,
            grading_jobs.lease_expires_at,
            grading_jobs.attempts,
            assignments.execution_config AS "execution_config: SqlxJson<Value>",
            assignments.runner_config AS "runner_config: SqlxJson<Value>",
            assignments.capabilities AS "capabilities: SqlxJson<Value>",
            submissions.raw_metadata AS "raw_metadata: SqlxJson<Value>"
        FROM grading_jobs
        JOIN assignments ON assignments.id = grading_jobs.assignment_id
        JOIN submissions ON submissions.id = grading_jobs.submission_id
        WHERE grading_jobs.id = $1
          AND grading_jobs.worker_id = $2
          AND grading_jobs.status IN ('claimed', 'running')
        "#,
        job_id,
        worker_id
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    let files = load_claimed_job_files_in_tx(tx, row.submission_id).await?;

    Ok(ClaimedJob {
        id: row.id,
        submission_id: row.submission_id,
        assignment_id: row.assignment_id,
        lease_expires_at: row
            .lease_expires_at
            .ok_or_else(|| ApiError::internal("active job has no lease"))?,
        attempt: row.attempts,
        execution_config: row.execution_config.0,
        runner_config: row.runner_config.0,
        capabilities: row.capabilities.0,
        submission_metadata: row.raw_metadata.0,
        files,
    })
}

async fn load_claimed_job_files_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    submission_id: Uuid,
) -> Result<Vec<ClaimedJobFile>, ApiError> {
    let files = sqlx::query!(
        r#"
        SELECT id, problem_name, original_filename, safe_filename, content_sha256, size_bytes
        FROM submission_files
        WHERE submission_id = $1
        ORDER BY created_at, id
        "#,
        submission_id
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(ApiError::internal)?
    .into_iter()
    .map(|file| ClaimedJobFile {
        id: file.id,
        problem_name: file.problem_name,
        original_filename: file.original_filename,
        safe_filename: file.safe_filename,
        content_sha256: file.content_sha256,
        size_bytes: file.size_bytes,
    })
    .collect();

    Ok(files)
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    worker: AuthenticatedWorker,
) -> Result<Json<ClaimedJob>, ApiError> {
    let job = load_owned_job(&state, worker.id, job_id).await?;
    Ok(Json(job))
}

async fn load_owned_job(
    state: &AppState,
    worker_id: Uuid,
    job_id: Uuid,
) -> Result<ClaimedJob, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id,
            grading_jobs.submission_id,
            grading_jobs.assignment_id,
            grading_jobs.lease_expires_at,
            grading_jobs.attempts,
            assignments.execution_config AS "execution_config: SqlxJson<Value>",
            assignments.runner_config AS "runner_config: SqlxJson<Value>",
            assignments.capabilities AS "capabilities: SqlxJson<Value>",
            submissions.raw_metadata AS "raw_metadata: SqlxJson<Value>"
        FROM grading_jobs
        JOIN assignments ON assignments.id = grading_jobs.assignment_id
        JOIN submissions ON submissions.id = grading_jobs.submission_id
        WHERE grading_jobs.id = $1
          AND grading_jobs.worker_id = $2
          AND grading_jobs.status IN ('claimed', 'running')
        "#,
        job_id,
        worker_id
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("job not found"))?;

    let files = sqlx::query!(
        r#"
        SELECT id, problem_name, original_filename, safe_filename, content_sha256, size_bytes
        FROM submission_files
        WHERE submission_id = $1
        ORDER BY created_at, id
        "#,
        row.submission_id
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?
    .into_iter()
    .map(|file| ClaimedJobFile {
        id: file.id,
        problem_name: file.problem_name,
        original_filename: file.original_filename,
        safe_filename: file.safe_filename,
        content_sha256: file.content_sha256,
        size_bytes: file.size_bytes,
    })
    .collect();

    Ok(ClaimedJob {
        id: row.id,
        submission_id: row.submission_id,
        assignment_id: row.assignment_id,
        lease_expires_at: row
            .lease_expires_at
            .ok_or_else(|| ApiError::internal("active job has no lease"))?,
        attempt: row.attempts,
        execution_config: row.execution_config.0,
        runner_config: row.runner_config.0,
        capabilities: row.capabilities.0,
        submission_metadata: row.raw_metadata.0,
        files,
    })
}

pub async fn post_started(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value: _ }: WorkerJson<StartedJobRequest>,
) -> Result<StatusCode, ApiError> {
    let now = Utc::now();

    let update = sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = 'running',
            started_at = $3,
            lease_expires_at = $4,
            last_heartbeat_at = $3
        WHERE id = $1
          AND worker_id = $2
          AND status = 'claimed'
          AND lease_expires_at > $3
          AND cancel_requested_at IS NULL
        "#,
        job_id,
        worker.id,
        now,
        now + Duration::seconds(LEASE_SECONDS)
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() != 1 {
        return Err(ApiError::not_found("startable job not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_job_file(
    State(state): State<AppState>,
    Path((job_id, file_id)): Path<(Uuid, Uuid)>,
    worker: AuthenticatedWorker,
) -> Result<Response, ApiError> {
    let now = Utc::now();

    let file = sqlx::query!(
        r#"
        SELECT submission_files.storage_path, submission_files.original_filename
        FROM grading_jobs
        JOIN submission_files ON submission_files.submission_id = grading_jobs.submission_id
        WHERE grading_jobs.id = $1
          AND grading_jobs.worker_id = $2
          AND grading_jobs.status IN ('claimed', 'running')
          AND grading_jobs.lease_expires_at > $3
          AND submission_files.id = $4
        "#,
        job_id,
        worker.id,
        now,
        file_id
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("job file not found"))?;

    let bytes = state
        .storage
        .get(&file.storage_path)
        .await
        .map_err(ApiError::internal)?;

    let response = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::CONTENT_DISPOSITION,
            format!(
                "attachment; filename=\"{}\"",
                file.original_filename.replace('"', "")
            ),
        )
        .body(Body::from(bytes))
        .map_err(ApiError::internal)?;

    Ok(response)
}

pub async fn post_events(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobEventBatchRequest>,
) -> Result<StatusCode, ApiError> {
    validate_event_batch(&value)?;
    ensure_active_job_owner(&state, worker.id, job_id).await?;

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    for event in value.events {
        let result = sqlx::query!(
            r#"
            INSERT INTO job_events (
                id, job_id, sequence, timestamp, type, stream, visibility, message, data
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
            Uuid::now_v7(),
            job_id,
            event.sequence,
            event.timestamp,
            event.event_type,
            event.stream,
            event.visibility,
            event.message,
            SqlxJson(event.data) as _
        )
        .execute(&mut *tx)
        .await;

        if let Err(error) = result {
            if is_unique_violation(&error) {
                return Err(ApiError::bad_request("duplicate event sequence"));
            }

            return Err(ApiError::internal(error));
        }
    }

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT)
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    let Some(db_error) = error.as_database_error() else {
        return false;
    };

    db_error.code().as_deref() == Some("23505")
}

pub async fn post_result(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobResultRequest>,
) -> Result<StatusCode, ApiError> {
    validate_result_request(&value)?;

    let now = Utc::now();

    let result_status = match value.result.status {
        cmsx_core::ResultStatus::Passed => "passed",
        cmsx_core::ResultStatus::Failed => "failed",
        cmsx_core::ResultStatus::Error => "error",
        cmsx_core::ResultStatus::Cancelled => "cancelled",
    };

    let job_status = match value.result.status {
        cmsx_core::ResultStatus::Passed => "succeeded",
        cmsx_core::ResultStatus::Failed => "failed",
        cmsx_core::ResultStatus::Error => "error",
        cmsx_core::ResultStatus::Cancelled => "cancelled",
    };

    let result_json = serde_json::to_value(&value.result).map_err(ApiError::internal)?;
    let tests_json = serde_json::to_value(&value.result.tests).map_err(ApiError::internal)?;
    let feedback = value.result.feedback.clone();

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    let is_cancelled_result = matches!(value.result.status, cmsx_core::ResultStatus::Cancelled);

    let update = sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = $3,
            finished_at = $4,
            lease_expires_at = NULL
        WHERE id = $1
          AND worker_id = $2
          AND lease_expires_at > $4
          AND (
            status = 'running'
            OR (
                $5
                AND status = 'claimed'
                AND cancel_requested_at IS NOT NULL
            )
          )
        "#,
        job_id,
        worker.id,
        job_status,
        now,
        is_cancelled_result
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() != 1 {
        return Err(ApiError::not_found("active job not found"));
    }

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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#,
        Uuid::now_v7(),
        job_id,
        result_status,
        value.result.score,
        value.result.max_score,
        feedback,
        SqlxJson(tests_json) as _,
        SqlxJson(result_json) as _,
        value.stdout_summary,
        value.stderr_summary,
        value.duration_ms,
        now
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn post_failed(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobFailureRequest>,
) -> Result<StatusCode, ApiError> {
    validate_failure_request(&value)?;

    let now = Utc::now();
    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    let row = sqlx::query!(
        r#"
        SELECT attempts, max_attempts
        FROM grading_jobs
        WHERE id = $1
          AND worker_id = $2
          AND status IN ('claimed', 'running')
          AND lease_expires_at > $3
        FOR UPDATE
        "#,
        job_id,
        worker.id,
        now
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("active job not found"))?;

    if value.retryable && row.attempts < row.max_attempts {
        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET status = 'queued',
                worker_id = NULL,
                claimed_at = NULL,
                started_at = NULL,
                lease_expires_at = NULL,
                last_heartbeat_at = NULL,
                failure_reason = $3,
                failure_message = $4,
                failure_retryable = true
            WHERE id = $1
              AND worker_id = $2
            "#,
            job_id,
            worker.id,
            value.reason,
            value.message
        )
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;
    } else {
        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET status = 'error',
                failure_reason = $3,
                failure_message = $4,
                failure_retryable = $5,
                finished_at = $6,
                lease_expires_at = NULL
            WHERE id = $1
              AND worker_id = $2
            "#,
            job_id,
            worker.id,
            value.reason,
            value.message,
            value.retryable,
            now
        )
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;
    }

    tx.commit().await.map_err(ApiError::internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ensure_active_job_owner(
    state: &AppState,
    worker_id: Uuid,
    job_id: Uuid,
) -> Result<(), ApiError> {
    let now = Utc::now();

    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM grading_jobs
            WHERE id = $1
              AND worker_id = $2
              AND status IN ('claimed', 'running')
              AND lease_expires_at > $3
        )
        "#,
        job_id,
        worker_id,
        now
    )
    .fetch_one(&state.db)
    .await
    .map_err(ApiError::internal)?
    .unwrap_or(false);

    if !exists {
        return Err(ApiError::not_found("active job not found"));
    }

    Ok(())
}

fn validate_heartbeat_request(value: &WorkerHeartbeatRequest) -> Result<(), ApiError> {
    if value.running_jobs < 0 {
        return Err(ApiError::bad_request("running_jobs must be nonnegative"));
    }
    if value.max_jobs < 0 {
        return Err(ApiError::bad_request("max_jobs must be nonnegative"));
    }
    if value.running_jobs > value.max_jobs {
        return Err(ApiError::bad_request(
            "running_jobs must not exceed max_jobs",
        ));
    }
    if value.active_job_ids.len() > value.max_jobs as usize {
        return Err(ApiError::bad_request(
            "active_job_ids length must not exceed max_jobs",
        ));
    }

    Ok(())
}

fn validate_claim_request(value: &ClaimJobRequest) -> Result<(), ApiError> {
    if value.available_slots < 0 {
        return Err(ApiError::bad_request("available_slots must be nonnegative"));
    }
    if value.wait_seconds.unwrap_or(0) > 30 {
        return Err(ApiError::bad_request("wait_seconds must be <= 30"));
    }

    Ok(())
}

fn validate_event_batch(value: &JobEventBatchRequest) -> Result<(), ApiError> {
    if value.events.is_empty() {
        return Err(ApiError::bad_request("events must not be empty"));
    }
    if value.events.len() > WORKER_EVENT_BATCH_MAX_EVENTS {
        return Err(ApiError::bad_request("too many events in batch"));
    }

    let mut sequences = HashSet::new();

    for event in &value.events {
        if event.sequence < 0 {
            return Err(ApiError::bad_request("event sequence must be nonnegative"));
        }
        if !sequences.insert(event.sequence) {
            return Err(ApiError::bad_request("duplicate event sequence in batch"));
        }
        if event.event_type.trim().is_empty() {
            return Err(ApiError::bad_request("event type must not be empty"));
        }
        if event.message.len() > WORKER_EVENT_MESSAGE_MAX_BYTES {
            return Err(ApiError::bad_request("event message too large"));
        }
        if !matches!(
            event.stream.as_str(),
            "stdout" | "stderr" | "worker" | "resource"
        ) {
            return Err(ApiError::bad_request("invalid event stream"));
        }
        if !matches!(event.visibility.as_str(), "student" | "staff" | "internal") {
            return Err(ApiError::bad_request("invalid event visibility"));
        }
    }

    Ok(())
}

fn validate_result_request(value: &JobResultRequest) -> Result<(), ApiError> {
    const MAX_FEEDBACK_BYTES: usize = 64 * 1024;
    const MAX_SUMMARY_BYTES: usize = 64 * 1024;
    const MAX_TESTS: usize = 512;
    const MAX_ARTIFACTS: usize = 128;

    if value.result.schema_version != "1" {
        return Err(ApiError::bad_request("unsupported result schema_version"));
    }
    if !value.result.score.is_finite() || !value.result.max_score.is_finite() {
        return Err(ApiError::bad_request("result scores must be finite"));
    }
    if value.result.score < 0.0 || value.result.max_score < 0.0 {
        return Err(ApiError::bad_request("result scores must be nonnegative"));
    }
    if value.result.score > value.result.max_score {
        return Err(ApiError::bad_request(
            "result score must not exceed max_score",
        ));
    }
    if value.result.tests.len() > MAX_TESTS {
        return Err(ApiError::bad_request("too many tests in result"));
    }
    if value.result.artifacts.len() > MAX_ARTIFACTS {
        return Err(ApiError::bad_request("too many artifacts in result"));
    }
    if let Some(feedback) = &value.result.feedback
        && feedback.len() > MAX_FEEDBACK_BYTES
    {
        return Err(ApiError::bad_request("result feedback too large"));
    }
    if let Some(summary) = &value.stdout_summary
        && summary.len() > MAX_SUMMARY_BYTES
    {
        return Err(ApiError::bad_request("stdout summary too large"));
    }
    if let Some(summary) = &value.stderr_summary
        && summary.len() > MAX_SUMMARY_BYTES
    {
        return Err(ApiError::bad_request("stderr summary too large"));
    }
    if let Some(duration_ms) = value.duration_ms
        && duration_ms < 0
    {
        return Err(ApiError::bad_request("duration_ms must be nonnegative"));
    }

    for test in &value.result.tests {
        if test.name.trim().is_empty() {
            return Err(ApiError::bad_request("test name must not be empty"));
        }
        if !test.score.is_finite() || !test.max_score.is_finite() {
            return Err(ApiError::bad_request("test scores must be finite"));
        }
        if test.score < 0.0 || test.max_score < 0.0 {
            return Err(ApiError::bad_request("test scores must be nonnegative"));
        }
        if test.score > test.max_score {
            return Err(ApiError::bad_request(
                "test score must not exceed max_score",
            ));
        }
    }

    Ok(())
}

fn validate_failure_request(value: &JobFailureRequest) -> Result<(), ApiError> {
    if value.reason.trim().is_empty() {
        return Err(ApiError::bad_request("failure reason must not be empty"));
    }
    if value.reason.len() > 128 {
        return Err(ApiError::bad_request("failure reason too large"));
    }
    if value.message.len() > 64 * 1024 {
        return Err(ApiError::bad_request("failure message too large"));
    }

    Ok(())
}
