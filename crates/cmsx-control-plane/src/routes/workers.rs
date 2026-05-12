use std::collections::HashSet;
use std::str::FromStr;

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{FromRef, FromRequest, FromRequestParts, Path, Request, State},
    http::{HeaderMap, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use bytes::BytesMut;
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use jwt_simple::prelude::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, ClaimedJob, ClaimedJobFile, JobEventBatchRequest,
    JobFailureRequest, JobResultRequest, JobStatus, ResultStatus, StartedJobRequest,
    WorkerAuthClaims, WorkerHeartbeatRequest, WorkerHeartbeatResponse, WorkerStatus,
    protocol::{
        ARTIFACT_MAX_BYTES, ARTIFACT_MAX_COUNT, ArtifactVisibility, GRADING_RESULT_MAX_TESTS,
        GRADING_RESULT_SCHEMA_VERSION, JOB_EVENT_BATCH_MAX_EVENTS, JOB_EVENT_MESSAGE_MAX_BYTES,
        JOB_FAILURE_REASON_MAX_BYTES, JOB_LEASE_SECONDS, JobEventStream, JobEventVisibility,
        MAX_CLAIM_WAIT_SECONDS, WORKER_AUTH_SCHEME, WORKER_JWT_AUDIENCE,
        WORKER_JWT_MAX_VALIDITY_SECONDS, WORKER_JWT_TIME_TOLERANCE_SECONDS, WORKER_MAX_ACTIVE_JOBS,
        WORKER_MAX_CLAIM_JOBS, WORKER_REQUEST_NONCE_RETENTION_SECONDS,
        artifact_name_from_relative_path, decode_artifact_relative_path, validate_artifact_label,
        validate_artifact_relative_path, validate_artifact_sha256,
    },
};

use crate::{app::AppState, db, error::ApiError, job_maintenance::sweep_expired_jobs_in_tx};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/workers/heartbeat", post(heartbeat))
        .route("/workers/jobs/claim", post(claim_job))
        .route("/workers/jobs/{job_id}", get(get_job))
        .route("/workers/jobs/{job_id}/events", post(post_events))
        .route("/workers/jobs/{job_id}/result", post(post_result))
        .route("/workers/jobs/{job_id}/started", post(post_started))
        .route("/workers/jobs/{job_id}/failed", post(post_failed))
        .route("/workers/jobs/{job_id}/files/{file_id}", get(get_job_file))
        .route(
            "/workers/jobs/{job_id}/artifacts/{artifact_id}",
            put(put_artifact),
        )
}

const WORKER_REQUEST_MAX_BYTES: usize = 1024 * 1024;

const LEASE_SECONDS: i64 = JOB_LEASE_SECONDS;

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

    let Some((scheme, token)) = auth_header.split_once(' ') else {
        return Err(ApiError::unauthorized(
            "invalid worker authorization scheme",
        ));
    };

    if scheme != WORKER_AUTH_SCHEME || token.trim().is_empty() || token.contains(' ') {
        return Err(ApiError::unauthorized(
            "invalid worker authorization scheme",
        ));
    }

    Ok(token)
}

async fn verify_worker_jwt(
    state: &AppState,
    token: &str,
    method: &str,
    path: &str,
    body_sha256: &str,
) -> Result<AuthenticatedWorker, ApiError> {
    let disabled = WorkerStatus::Disabled.as_str();

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
          AND workers.status != $2
        "#,
        fingerprint,
        disabled
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    for key in keys {
        let worker_id = key.worker_id;
        let public_key = Ed25519PublicKey::from_pem(&key.public_key).map_err(ApiError::internal)?;

        let options = VerificationOptions {
            allowed_audiences: Some(HashSet::from([WORKER_JWT_AUDIENCE.to_string()])),
            required_key_id: Some(fingerprint.clone()),
            time_tolerance: Some(jwt_simple::prelude::Duration::from_secs(
                WORKER_JWT_TIME_TOLERANCE_SECONDS,
            )),
            max_validity: Some(jwt_simple::prelude::Duration::from_secs(
                WORKER_JWT_MAX_VALIDITY_SECONDS,
            )),
            ..Default::default()
        };

        let Ok(claims) = public_key.verify_token::<WorkerAuthClaims>(token, Some(options)) else {
            continue;
        };

        if claims.issuer.as_deref() != Some(&format!("worker-key:{fingerprint}")) {
            continue;
        }

        let Some(jwt_id) = claims.jwt_id else {
            continue;
        };

        let Ok(jti) = Uuid::parse_str(&jwt_id) else {
            continue;
        };

        let custom = claims.custom;

        if custom.method != method {
            continue;
        }
        if custom.path != path {
            continue;
        }
        if custom.body_sha256 != body_sha256 {
            continue;
        }

        record_worker_jti(state, worker_id, jti).await?;

        return Ok(AuthenticatedWorker { id: worker_id });
    }

    Err(ApiError::unauthorized("worker authentication failed"))
}

async fn record_worker_jti(state: &AppState, worker_id: Uuid, jti: Uuid) -> Result<(), ApiError> {
    let now = Utc::now();
    let expires_at = now + Duration::seconds(WORKER_REQUEST_NONCE_RETENTION_SECONDS);

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
        if db::is_unique_violation(&error) {
            return Err(ApiError::unauthorized("replayed worker token"));
        }

        return Err(ApiError::internal(error));
    }

    Ok(())
}

#[derive(Debug)]
struct ArtifactRouteError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ArtifactRouteError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn invalid_metadata(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "artifact_invalid_metadata",
            message,
        )
    }

    fn hash_mismatch(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "artifact_hash_mismatch", message)
    }

    fn too_large(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, "artifact_too_large", message)
    }

    fn not_active(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "job_not_active", message)
    }

    fn cancellation_requested(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "job_cancellation_requested", message)
    }

    fn duplicate(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "artifact_duplicate", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "artifact_conflict", message)
    }

    fn upload_failed(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "artifact_upload_failed",
            message,
        )
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "worker_unauthorized", message)
    }
}

impl IntoResponse for ArtifactRouteError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "error": self.message,
            "code": self.code,
        }));
        (self.status, body).into_response()
    }
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
    let online = WorkerStatus::Online.as_str();

    sqlx::query!(
        r#"
        UPDATE workers
        SET status = $2,
            version = $3,
            last_seen_at = $4
        WHERE id = $1
        "#,
        worker_id,
        online,
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
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
        Uuid::now_v7(),
        worker_id,
        online,
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

        let status = row
            .status
            .parse::<JobStatus>()
            .map_err(|_| ApiError::internal("invalid job status in database"))?;

        if row.worker_id != Some(worker_id)
            || !matches!(status, JobStatus::Claimed | JobStatus::Running)
        {
            unknown_job_ids.push(*job_id);
            continue;
        }

        if row.cancel_requested_at.is_some() {
            let claimed = JobStatus::Claimed.as_str();
            let running = JobStatus::Running.as_str();

            sqlx::query!(
                r#"
                UPDATE grading_jobs
                SET lease_expires_at = $5,
                    last_heartbeat_at = $6
                WHERE id = $1
                  AND worker_id = $2
                  AND status IN ($3, $4)
                  AND cancel_requested_at IS NOT NULL
                "#,
                job_id,
                worker_id,
                claimed,
                running,
                now + Duration::seconds(LEASE_SECONDS),
                now
            )
            .execute(&mut **tx)
            .await
            .map_err(ApiError::internal)?;
            cancelled_job_ids.push(*job_id);
            continue;
        }

        if row
            .lease_expires_at
            .is_none_or(|expires_at| expires_at <= now)
        {
            unknown_job_ids.push(*job_id);
            continue;
        }

        let claimed = JobStatus::Claimed.as_str();
        let running = JobStatus::Running.as_str();

        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET lease_expires_at = $5,
                last_heartbeat_at = $6
            WHERE id = $1
              AND worker_id = $2
              AND status IN ($3, $4)
              AND lease_expires_at > $6
              AND cancel_requested_at IS NULL
            "#,
            job_id,
            worker_id,
            claimed,
            running,
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
    let queued = JobStatus::Queued.as_str();
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();
    let next_status = JobStatus::Claimed.as_str();

    sweep_expired_jobs_in_tx(&mut tx, now).await?;

    let rows = sqlx::query!(
        r#"
        WITH candidate AS (
            SELECT grading_jobs.id
            FROM grading_jobs
            WHERE (
                (
                    grading_jobs.status = $5
                    AND grading_jobs.cancel_requested_at IS NULL
                )
                OR (
                    grading_jobs.status IN ($6, $7)
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
        SET status = $8,
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
        now + Duration::seconds(LEASE_SECONDS),
        queued,
        claimed,
        running,
        next_status
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

async fn load_claimed_job_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    worker_id: Uuid,
    job_id: Uuid,
) -> Result<ClaimedJob, ApiError> {
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();

    let row = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id,
            grading_jobs.submission_id,
            grading_jobs.assignment_id,
            assignments.slug AS assignment_slug,
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
          AND grading_jobs.status IN ($3, $4)
        "#,
        job_id,
        worker_id,
        claimed,
        running
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    let files = load_claimed_job_files_in_tx(tx, row.submission_id).await?;

    Ok(ClaimedJob {
        id: row.id,
        submission_id: row.submission_id,
        assignment_id: row.assignment_id,
        assignment_slug: row.assignment_slug,
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
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();
    let now = Utc::now();

    let row = sqlx::query!(
        r#"
        SELECT
            grading_jobs.id,
            grading_jobs.submission_id,
            grading_jobs.assignment_id,
            assignments.slug AS assignment_slug,
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
          AND grading_jobs.status IN ($3, $4)
          AND grading_jobs.lease_expires_at > $5
        "#,
        job_id,
        worker_id,
        claimed,
        running,
        now
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
        assignment_slug: row.assignment_slug,
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
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();

    let update = sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = $3,
            started_at = $4,
            lease_expires_at = $5,
            last_heartbeat_at = $4
        WHERE id = $1
          AND worker_id = $2
          AND status = $6
          AND lease_expires_at > $4
          AND cancel_requested_at IS NULL
        "#,
        job_id,
        worker.id,
        running,
        now,
        now + Duration::seconds(LEASE_SECONDS),
        claimed
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() != 1 {
        let cancellation_requested = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM grading_jobs
                WHERE id = $1
                  AND worker_id = $2
                  AND status = $4
                  AND lease_expires_at > $3
                  AND cancel_requested_at IS NOT NULL
            )
            "#,
            job_id,
            worker.id,
            now,
            claimed,
        )
        .fetch_one(&state.db)
        .await
        .map_err(ApiError::internal)?
        .unwrap_or(false);
        if cancellation_requested {
            return Err(ApiError::conflict("job cancellation requested"));
        }
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
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();

    let file = sqlx::query!(
        r#"
        SELECT submission_files.storage_path, submission_files.safe_filename
        FROM grading_jobs
        JOIN submission_files ON submission_files.submission_id = grading_jobs.submission_id
        WHERE grading_jobs.id = $1
          AND grading_jobs.worker_id = $2
          AND grading_jobs.status IN ($3, $4)
          AND grading_jobs.lease_expires_at > $5
          AND submission_files.id = $6
        "#,
        job_id,
        worker.id,
        claimed,
        running,
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
            format!("attachment; filename=\"{}\"", file.safe_filename),
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
            if db::is_unique_violation(&error) {
                return Err(ApiError::bad_request("duplicate event sequence"));
            }

            return Err(ApiError::internal(error));
        }
    }

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn post_result(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobResultRequest>,
) -> Result<StatusCode, ApiError> {
    validate_result_request(&value)?;

    let now = Utc::now();
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();
    let is_cancelled_result = matches!(value.result.status, ResultStatus::Cancelled);

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    let row = sqlx::query!(
        r#"
        SELECT
            grading_jobs.status,
            grading_jobs.worker_id,
            grading_jobs.attempts,
            grading_jobs.lease_expires_at,
            grading_jobs.cancel_requested_at,
            assignments.max_score AS assignment_max_score
        FROM grading_jobs
        JOIN assignments ON assignments.id = grading_jobs.assignment_id
        WHERE grading_jobs.id = $1
        FOR UPDATE
        "#,
        job_id
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("active job not found"))?;

    if row.worker_id != Some(worker.id) || row.lease_expires_at.is_none_or(|lease| lease <= now) {
        return Err(ApiError::not_found("active job not found"));
    }

    let allowed = if row.status == running {
        row.cancel_requested_at.is_none() || is_cancelled_result
    } else if row.status == claimed {
        is_cancelled_result && row.cancel_requested_at.is_some()
    } else {
        false
    };

    if !allowed {
        if row.cancel_requested_at.is_some() && !is_cancelled_result {
            return Err(ApiError::conflict("job cancellation requested"));
        }

        return Err(ApiError::not_found("active job not found"));
    }

    if value.result.max_score > row.assignment_max_score {
        return Err(ApiError::bad_request(
            "result max_score must not exceed assignment max_score",
        ));
    }

    if value.result.score > row.assignment_max_score {
        return Err(ApiError::bad_request(
            "result score must not exceed assignment max_score",
        ));
    }

    validate_result_artifact_refs_in_tx(&mut tx, job_id, row.attempts, &value.result.artifacts)
        .await?;

    let result_status = value.result.status.as_str();
    let job_status = value.result.status.terminal_job_status().as_str();
    let result_json = serde_json::to_value(&value.result).map_err(ApiError::internal)?;
    let tests_json = serde_json::to_value(&value.result.tests).map_err(ApiError::internal)?;
    let feedback = value.result.feedback.clone();

    sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = $3,
            finished_at = $4,
            lease_expires_at = NULL
        WHERE id = $1
          AND worker_id = $2
        "#,
        job_id,
        worker.id,
        job_status,
        now
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

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

async fn validate_result_artifact_refs_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job_id: Uuid,
    attempt: i32,
    refs: &[cmsx_core::ResultArtifactRef],
) -> Result<(), ApiError> {
    if refs.len() > ARTIFACT_MAX_COUNT {
        return Err(ApiError::bad_request("too many artifacts in result"));
    }

    let mut seen = std::collections::HashSet::new();

    for artifact in refs {
        validate_artifact_relative_path(&artifact.path)
            .map_err(|error| ApiError::bad_request(error.to_string()))?;

        if !seen.insert(artifact.path.clone()) {
            return Err(ApiError::bad_request("duplicate artifact reference"));
        }

        if let Some(label) = &artifact.label {
            cmsx_core::protocol::validate_artifact_label(label)
                .map_err(|error| ApiError::bad_request(error.to_string()))?;
        }
    }

    let rows = sqlx::query_scalar!(
        r#"
        SELECT relative_path
        FROM artifacts
        WHERE job_id = $1
          AND attempt = $2
        "#,
        job_id,
        attempt
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(ApiError::internal)?;

    let uploaded: std::collections::HashSet<String> = rows.into_iter().collect();

    for artifact in refs {
        if !uploaded.contains(&artifact.path) {
            return Err(ApiError::bad_request("artifact reference was not uploaded"));
        }
    }

    Ok(())
}

pub async fn post_failed(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobFailureRequest>,
) -> Result<StatusCode, ApiError> {
    validate_failure_request(&value)?;

    let now = Utc::now();
    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();

    let row = sqlx::query!(
        r#"
        SELECT attempts, max_attempts, cancel_requested_at
        FROM grading_jobs
        WHERE id = $1
          AND worker_id = $2
          AND status IN ($3, $4)
          AND lease_expires_at > $5
        FOR UPDATE
        "#,
        job_id,
        worker.id,
        claimed,
        running,
        now
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("active job not found"))?;

    if row.cancel_requested_at.is_some() {
        return Err(ApiError::conflict("job cancellation requested"));
    }

    if value.retryable && row.attempts < row.max_attempts {
        let queued = JobStatus::Queued.as_str();

        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET status = $3,
                worker_id = NULL,
                claimed_at = NULL,
                started_at = NULL,
                lease_expires_at = NULL,
                last_heartbeat_at = NULL,
                failure_reason = $4,
                failure_message = $5,
                failure_retryable = true
            WHERE id = $1
              AND worker_id = $2
            "#,
            job_id,
            worker.id,
            queued,
            value.reason,
            value.message
        )
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;
    } else {
        let error = JobStatus::Error.as_str();

        sqlx::query!(
            r#"
            UPDATE grading_jobs
            SET status = $3,
                failure_reason = $4,
                failure_message = $5,
                failure_retryable = $6,
                finished_at = $7,
                lease_expires_at = NULL
            WHERE id = $1
              AND worker_id = $2
            "#,
            job_id,
            worker.id,
            error,
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
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();

    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM grading_jobs
            WHERE id = $1
              AND worker_id = $2
              AND status IN ($3, $4)
              AND lease_expires_at > $5
        )
        "#,
        job_id,
        worker_id,
        claimed,
        running,
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

pub async fn put_artifact(
    State(state): State<AppState>,
    Path((job_id, artifact_id)): Path<(Uuid, Uuid)>,
    request: Request,
) -> Response {
    match put_artifact_inner(state, job_id, artifact_id, request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

struct ArtifactMetadata {
    relative_path: String,
    name: String,
    size_bytes: i64,
    sha256: String,
    visibility: ArtifactVisibility,
    storage_path: String,
}

#[derive(Debug)]
struct ActiveArtifactJob {
    attempt: i32,
}

async fn put_artifact_inner(
    state: AppState,
    job_id: Uuid,
    artifact_id: Uuid,
    request: Request,
) -> Result<(), ArtifactRouteError> {
    let method = request.method().as_str().to_string();
    let path = request.uri().path().to_string();
    let headers = request.headers().clone();

    let token = worker_token_from_headers(&headers)
        .map_err(|_| ArtifactRouteError::unauthorized("invalid worker authorization"))?;

    if let Some(content_length) = headers.get(header::CONTENT_LENGTH) {
        let content_length = content_length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| ArtifactRouteError::invalid_metadata("invalid content-length"))?;

        if content_length > ARTIFACT_MAX_BYTES {
            return Err(ArtifactRouteError::too_large("artifact body too large"));
        }
    }

    let encoded_relative_path = required_header(&headers, "x-cmsx-artifact-relative-path")?;
    let declared_size = required_header(&headers, "x-cmsx-artifact-size-bytes")?;
    let declared_sha256 = required_header(&headers, "x-cmsx-artifact-sha256")?;
    let visibility = required_header(&headers, "x-cmsx-artifact-visibility")?;

    let body = read_artifact_body(request.into_body()).await?;
    let body_sha256 = hex::encode(Sha256::digest(&body));

    let worker = verify_worker_jwt(&state, token, &method, &path, &body_sha256)
        .await
        .map_err(|_| ArtifactRouteError::unauthorized("worker authentication failed"))?;

    let relative_path = decode_artifact_relative_path(encoded_relative_path)
        .map_err(|error| ArtifactRouteError::invalid_metadata(error.to_string()))?;

    let name = artifact_name_from_relative_path(&relative_path)
        .map_err(|error| ArtifactRouteError::invalid_metadata(error.to_string()))?
        .to_string();

    validate_artifact_sha256(declared_sha256)
        .map_err(|error| ArtifactRouteError::invalid_metadata(error.to_string()))?;

    if declared_sha256 != body_sha256 {
        return Err(ArtifactRouteError::hash_mismatch(
            "artifact sha256 mismatch",
        ));
    }

    let size_bytes = declared_size
        .parse::<i64>()
        .map_err(|_| ArtifactRouteError::invalid_metadata("invalid artifact size"))?;

    if size_bytes < 0 || size_bytes as usize != body.len() {
        return Err(ArtifactRouteError::invalid_metadata(
            "artifact size does not match body length",
        ));
    }

    let visibility = ArtifactVisibility::from_str(visibility)
        .map_err(|error| ArtifactRouteError::invalid_metadata(error.to_string()))?;

    let active = load_active_artifact_job(&state, worker.id, job_id).await?;
    let storage_path =
        state
            .storage
            .artifact_key(job_id, active.attempt, artifact_id, declared_sha256);

    let metadata = ArtifactMetadata {
        relative_path,
        name,
        size_bytes,
        sha256: declared_sha256.to_string(),
        visibility,
        storage_path,
    };

    if let Some(outcome) =
        artifact_preflight(&state, job_id, artifact_id, active.attempt, &metadata).await?
    {
        return outcome;
    }

    state
        .storage
        .put_artifact_bytes(job_id, active.attempt, artifact_id, &metadata.sha256, body)
        .await
        .map_err(|error| {
            tracing::warn!(
                %job_id,
                %artifact_id,
                attempt = active.attempt,
                path = %metadata.relative_path,
                storage_path = %metadata.storage_path,
                ?error,
                "artifact storage upload failed"
            );
            ArtifactRouteError::upload_failed("artifact storage upload failed")
        })?;

    insert_artifact_after_upload(
        &state,
        worker.id,
        job_id,
        artifact_id,
        active.attempt,
        &metadata,
    )
    .await
}

async fn read_artifact_body(mut body: Body) -> Result<Bytes, ArtifactRouteError> {
    let mut bytes = BytesMut::new();
    let limit = ARTIFACT_MAX_BYTES as usize;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            ArtifactRouteError::upload_failed(format!("failed to read artifact body: {error}"))
        })?;

        let Some(data) = frame.data_ref() else {
            continue;
        };

        if bytes.len().saturating_add(data.len()) > limit {
            return Err(ArtifactRouteError::too_large("artifact body too large"));
        }

        bytes.extend_from_slice(data);
    }

    Ok(bytes.freeze())
}

fn required_header<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<&'a str, ArtifactRouteError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ArtifactRouteError::invalid_metadata(format!("missing {name} header")))
}

async fn load_active_artifact_job(
    state: &AppState,
    worker_id: Uuid,
    job_id: Uuid,
) -> Result<ActiveArtifactJob, ArtifactRouteError> {
    let now = Utc::now();
    let claimed = JobStatus::Claimed.as_str();
    let running = JobStatus::Running.as_str();

    let row = sqlx::query!(
        r#"
        SELECT status, worker_id, attempts, lease_expires_at, cancel_requested_at
        FROM grading_jobs
        WHERE id = $1
        "#,
        job_id
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|error| {
        tracing::warn!(%job_id, ?error, "failed to load artifact upload job state");
        ArtifactRouteError::upload_failed("failed to load artifact upload job state")
    })?
    .ok_or_else(|| ArtifactRouteError::not_active("job is not active"))?;

    if row.worker_id != Some(worker_id)
        || (row.status != claimed && row.status != running)
        || row.lease_expires_at.is_none_or(|lease| lease <= now)
    {
        return Err(ArtifactRouteError::not_active("job is not active"));
    }

    if row.cancel_requested_at.is_some() {
        return Err(ArtifactRouteError::cancellation_requested(
            "job cancellation requested",
        ));
    }

    Ok(ActiveArtifactJob {
        attempt: row.attempts,
    })
}

enum ExistingArtifactOutcome {
    Success,
    Conflict(ArtifactRouteError),
}

async fn artifact_preflight(
    state: &AppState,
    job_id: Uuid,
    artifact_id: Uuid,
    attempt: i32,
    metadata: &ArtifactMetadata,
) -> Result<Option<Result<(), ArtifactRouteError>>, ArtifactRouteError> {
    match classify_existing_artifact(state, job_id, artifact_id, attempt, metadata).await? {
        Some(ExistingArtifactOutcome::Success) => Ok(Some(Ok(()))),
        Some(ExistingArtifactOutcome::Conflict(error)) => Ok(Some(Err(error))),
        None => Ok(None),
    }
}

async fn classify_existing_artifact(
    state: &AppState,
    job_id: Uuid,
    artifact_id: Uuid,
    attempt: i32,
    metadata: &ArtifactMetadata,
) -> Result<Option<ExistingArtifactOutcome>, ArtifactRouteError> {
    let by_id = sqlx::query!(
        r#"
        SELECT job_id, attempt, relative_path, name, storage_path, content_type, size_bytes, sha256, visibility
        FROM artifacts
        WHERE id = $1
        "#,
        artifact_id
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|error| {
        tracing::warn!(%job_id, %artifact_id, ?error, "failed checking existing artifact id");
        ArtifactRouteError::upload_failed("failed checking existing artifact")
    })?;

    if let Some(row) = by_id {
        let identical = row.job_id == job_id
            && row.attempt == attempt
            && row.relative_path == metadata.relative_path
            && row.name == metadata.name
            && row.storage_path == metadata.storage_path
            && row.content_type.is_none()
            && row.size_bytes == metadata.size_bytes
            && row.sha256 == metadata.sha256
            && row.visibility == metadata.visibility.as_str();

        if identical {
            return Ok(Some(ExistingArtifactOutcome::Success));
        }

        return Ok(Some(ExistingArtifactOutcome::Conflict(
            ArtifactRouteError::conflict("artifact id conflicts with existing artifact"),
        )));
    }

    let by_path = sqlx::query_scalar!(
        r#"
        SELECT id
        FROM artifacts
        WHERE job_id = $1
          AND attempt = $2
          AND relative_path = $3
        "#,
        job_id,
        attempt,
        metadata.relative_path
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|error| {
        tracing::warn!(%job_id, %artifact_id, ?error, "failed checking existing artifact path");
        ArtifactRouteError::upload_failed("failed checking existing artifact")
    })?;

    if by_path.is_some() {
        return Ok(Some(ExistingArtifactOutcome::Conflict(
            ArtifactRouteError::duplicate("artifact relative path already exists"),
        )));
    }

    Ok(None)
}

async fn insert_artifact_after_upload(
    state: &AppState,
    worker_id: Uuid,
    job_id: Uuid,
    artifact_id: Uuid,
    attempt: i32,
    metadata: &ArtifactMetadata,
) -> Result<(), ArtifactRouteError> {
    let active = load_active_artifact_job(state, worker_id, job_id).await?;

    if active.attempt != attempt {
        return Err(ArtifactRouteError::not_active("job attempt changed"));
    }

    if let Some(outcome) = artifact_preflight(state, job_id, artifact_id, attempt, metadata).await?
    {
        return outcome;
    }

    let insert = sqlx::query!(
        r#"
        INSERT INTO artifacts (
            id,
            job_id,
            attempt,
            relative_path,
            name,
            storage_path,
            content_type,
            size_bytes,
            sha256,
            visibility,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10)
        "#,
        artifact_id,
        job_id,
        attempt,
        metadata.relative_path,
        metadata.name,
        metadata.storage_path,
        metadata.size_bytes,
        metadata.sha256,
        metadata.visibility.as_str(),
        Utc::now()
    )
    .execute(&state.db)
    .await;

    match insert {
        Ok(_) => Ok(()),
        Err(error) if db::is_unique_violation(&error) => {
            match classify_existing_artifact(state, job_id, artifact_id, attempt, metadata).await? {
                Some(ExistingArtifactOutcome::Success) => Ok(()),
                Some(ExistingArtifactOutcome::Conflict(error)) => Err(error),
                None => Err(ArtifactRouteError::conflict("artifact unique conflict")),
            }
        }
        Err(error) => {
            tracing::warn!(%job_id, %artifact_id, ?error, "failed inserting artifact row");
            Err(ArtifactRouteError::upload_failed(
                "failed inserting artifact row",
            ))
        }
    }
}

fn validate_heartbeat_request(value: &WorkerHeartbeatRequest) -> Result<(), ApiError> {
    if value.running_jobs < 0 {
        return Err(ApiError::bad_request("running_jobs must be nonnegative"));
    }
    if value.max_jobs < 0 {
        return Err(ApiError::bad_request("max_jobs must be nonnegative"));
    }
    if value.max_jobs as usize > WORKER_MAX_ACTIVE_JOBS {
        return Err(ApiError::bad_request(format!(
            "max_jobs must be <= {WORKER_MAX_ACTIVE_JOBS}"
        )));
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
    if value.active_job_ids.len() > WORKER_MAX_ACTIVE_JOBS {
        return Err(ApiError::bad_request(format!(
            "active_job_ids length must be <= {WORKER_MAX_ACTIVE_JOBS}"
        )));
    }

    Ok(())
}

fn validate_claim_request(value: &ClaimJobRequest) -> Result<(), ApiError> {
    if value.available_slots < 0 {
        return Err(ApiError::bad_request("available_slots must be nonnegative"));
    }
    if value.available_slots > WORKER_MAX_CLAIM_JOBS {
        return Err(ApiError::bad_request(format!(
            "available_slots must be <= {WORKER_MAX_CLAIM_JOBS}"
        )));
    }
    if value.wait_seconds.unwrap_or(0) > MAX_CLAIM_WAIT_SECONDS {
        return Err(ApiError::bad_request(format!(
            "wait_seconds must be <= {MAX_CLAIM_WAIT_SECONDS}"
        )));
    }

    Ok(())
}

fn validate_event_batch(value: &JobEventBatchRequest) -> Result<(), ApiError> {
    if value.events.is_empty() {
        return Err(ApiError::bad_request("events must not be empty"));
    }
    if value.events.len() > JOB_EVENT_BATCH_MAX_EVENTS {
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
        if event.message.len() > JOB_EVENT_MESSAGE_MAX_BYTES {
            return Err(ApiError::bad_request("event message too large"));
        }
        if !JobEventStream::is_valid(&event.stream) {
            return Err(ApiError::bad_request("invalid event stream"));
        }
        if !JobEventVisibility::is_valid(&event.visibility) {
            return Err(ApiError::bad_request("invalid event visibility"));
        }
    }

    Ok(())
}

fn validate_result_request(value: &JobResultRequest) -> Result<(), ApiError> {
    const MAX_FEEDBACK_BYTES: usize = JOB_EVENT_MESSAGE_MAX_BYTES;
    const MAX_SUMMARY_BYTES: usize = JOB_EVENT_MESSAGE_MAX_BYTES;
    if value.result.schema_version != GRADING_RESULT_SCHEMA_VERSION {
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
    if value.result.tests.len() > GRADING_RESULT_MAX_TESTS {
        return Err(ApiError::bad_request("too many tests in result"));
    }
    if value.result.artifacts.len() > ARTIFACT_MAX_COUNT {
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

    let mut artifact_paths = HashSet::new();

    for artifact in &value.result.artifacts {
        validate_artifact_relative_path(&artifact.path)
            .map_err(|error| ApiError::bad_request(error.to_string()))?;

        if !artifact_paths.insert(artifact.path.as_str()) {
            return Err(ApiError::bad_request("duplicate artifact reference"));
        }

        if let Some(label) = &artifact.label {
            validate_artifact_label(label)
                .map_err(|error| ApiError::bad_request(error.to_string()))?;
        }
    }

    Ok(())
}

fn validate_failure_request(value: &JobFailureRequest) -> Result<(), ApiError> {
    if value.reason.trim().is_empty() {
        return Err(ApiError::bad_request("failure reason must not be empty"));
    }
    if value.reason.len() > JOB_FAILURE_REASON_MAX_BYTES {
        return Err(ApiError::bad_request("failure reason too large"));
    }
    if value.message.len() > JOB_EVENT_MESSAGE_MAX_BYTES {
        return Err(ApiError::bad_request("failure message too large"));
    }

    Ok(())
}
