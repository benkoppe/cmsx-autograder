use std::collections::HashSet;

use axum::{
    Json,
    body::Bytes,
    extract::{FromRef, FromRequest, FromRequestParts, Path, Request, State},
    http::{HeaderMap, StatusCode, header, request::Parts},
};
use chrono::{Duration, Utc};
use jwt_simple::prelude::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, ClaimedJob, ClaimedJobFile, JobEventBatchRequest,
    JobFailureRequest, JobResultRequest, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
};

use crate::{app::AppState, error::ApiError};

const WORKER_REQUEST_MAX_BYTES: usize = 1024 * 1024;
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

    let claims = verify_worker_jwt(state, token, method, path, &body_sha256).await?;

    Ok(AuthenticatedWorker { id: claims.sub })
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

#[derive(Debug, serde::Deserialize)]
struct WorkerClaims {
    iss: String,
    sub: Uuid,
    aud: String,
    jti: Uuid,
    method: String,
    path: String,
    body_sha256: String,
}

async fn verify_worker_jwt(
    state: &AppState,
    token: &str,
    method: &str,
    path: &str,
    body_sha256: &str,
) -> Result<WorkerClaims, ApiError> {
    let metadata = Token::decode_metadata(token)
        .map_err(|_| ApiError::unauthorized("invalid worker token"))?;

    let key_id = metadata
        .key_id()
        .ok_or_else(|| ApiError::unauthorized("missing worker key id"))?;

    let worker_id =
        Uuid::parse_str(key_id).map_err(|_| ApiError::unauthorized("invalid worker key id"))?;

    let keys = sqlx::query!(
        r#"
        SELECT worker_keys.public_key
        FROM worker_keys
        JOIN workers ON workers.id = worker_keys.worker_id
        WHERE worker_keys.worker_id = $1
          AND worker_keys.revoked_at IS NULL
          AND workers.status != 'disabled'
        "#,
        worker_id
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    for key in keys {
        let public_key = Ed25519PublicKey::from_pem(&key.public_key).map_err(ApiError::internal)?;

        let options = VerificationOptions {
            allowed_audiences: Some(HashSet::from([WORKER_AUDIENCE.to_string()])),
            required_subject: Some(worker_id.to_string()),
            required_key_id: Some(worker_id.to_string()),
            time_tolerance: Some(jwt_simple::prelude::Duration::from_secs(60)),
            max_validity: Some(jwt_simple::prelude::Duration::from_secs(60)),
            ..Default::default()
        };

        let Ok(claims) = public_key.verify_token::<WorkerClaims>(token, Some(options)) else {
            continue;
        };

        let custom = claims.custom;

        if custom.iss != format!("worker:{worker_id}") {
            continue;
        }
        if custom.aud != WORKER_AUDIENCE {
            continue;
        }
        if custom.sub != worker_id {
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

        return Ok(custom);
    }

    Err(ApiError::unauthorized("worker authentication failed"))
}

pub async fn heartbeat(
    State(state): State<AppState>,
    WorkerJson { worker, value }: WorkerJson<WorkerHeartbeatRequest>,
) -> Result<Json<WorkerHeartbeatResponse>, ApiError> {
    let now = Utc::now();

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    sqlx::query!(
        r#"
        UPDATE workers
        SET status = 'online',
            version = $2,
            last_seen_at = $3
        WHERE id = $1
        "#,
        worker.id,
        value.version,
        now
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    sqlx::query!(
        r#"
        INSERT INTO worker_heartbeats (
            id,
            worker_id,
            status,
            version,
            executor_backends,
            runner_images,
            running_jobs,
            max_jobs,
            reported_at
        )
        VALUES ($1, $2, 'online', $3, $4, $5, $6, $7, $8)
        "#,
        Uuid::now_v7(),
        worker.id,
        value.version,
        SqlxJson(value.executor_backends) as _,
        SqlxJson(value.runner_images) as _,
        value.running_jobs,
        value.max_jobs,
        now
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET lease_expires_at = $2,
            last_heartbeat_at = $3
        WHERE worker_id = $1
          AND status IN ('claimed', 'running')
        "#,
        worker.id,
        now + Duration::seconds(LEASE_SECONDS),
        now
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    let cancelled = sqlx::query_scalar!(
        r#"
        SELECT id
        FROM grading_jobs
        WHERE worker_id = $1
          AND status IN ('claimed', 'running')
          AND cancel_requested_at IS NOT NULL
        "#,
        worker.id
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(Json(WorkerHeartbeatResponse {
        worker_id: worker.id,
        lease_seconds: LEASE_SECONDS,
        cancelled_job_ids: cancelled,
    }))
}

pub async fn claim_job(
    State(state): State<AppState>,
    WorkerJson { worker, value }: WorkerJson<ClaimJobRequest>,
) -> Result<Json<ClaimJobResponse>, ApiError> {
    if value.available_slots <= 0 {
        return Ok(Json(ClaimJobResponse { jobs: Vec::new() }));
    }

    let Some(job) = try_claim_job(&state, worker.id, &value).await? else {
        return Ok(Json(ClaimJobResponse { jobs: Vec::new() }));
    };

    Ok(Json(ClaimJobResponse { jobs: vec![job] }))
}

async fn try_claim_job(
    state: &AppState,
    worker_id: Uuid,
    request: &ClaimJobRequest,
) -> Result<Option<ClaimedJob>, ApiError> {
    let now = Utc::now();
    let lease_expires_at = now + Duration::seconds(LEASE_SECONDS);

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    let row = sqlx::query!(
        r#"
        WITH candidate AS (
            SELECT grading_jobs.id
            FROM grading_jobs
            JOIN assignments ON assignments.id = grading_jobs.assignment_id
            WHERE grading_jobs.status = 'queued'
              AND assignments.execution_config->>'backend' = ANY($2)
              AND (
                assignments.runner_config->>'environment' = ANY($3)
                OR assignments.runner_config->>'image' = ANY($3)
              )
            ORDER BY grading_jobs.queued_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE grading_jobs
        SET status = 'claimed',
            worker_id = $1,
            attempts = attempts + 1,
            claimed_at = $4,
            lease_expires_at = $5 
        FROM candidate
        WHERE grading_jobs.id = candidate.id
        RETURNING
            grading_jobs.id,
            grading_jobs.submission_id,
            grading_jobs.assignment_id
        "#,
        worker_id,
        &request.executor_backends,
        &request.runner_images,
        now,
        lease_expires_at
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    let Some(row) = row else {
        tx.commit().await.map_err(ApiError::internal)?;
        return Ok(None);
    };

    let assignment = sqlx::query!(
        r#"
        SELECT
            execution_config AS "execution_config: SqlxJson<Value>",
            runner_config AS "runner_config: SqlxJson<Value>",
            capabilities AS "capabilities: SqlxJson<Value>"
        FROM assignments
        WHERE id = $1
        "#,
        row.assignment_id
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    let files = sqlx::query!(
        r#"
        SELECT id, problem_name, original_filename, safe_filename, size_bytes
        FROM submission_files
        WHERE submission_id = $1
        ORDER BY created_at, id
        "#,
        row.submission_id
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiError::internal)?
    .into_iter()
    .map(|file| ClaimedJobFile {
        id: file.id,
        problem_name: file.problem_name,
        original_filename: file.original_filename,
        safe_filename: file.safe_filename,
        size_bytes: file.size_bytes,
    })
    .collect();

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(Some(ClaimedJob {
        id: row.id,
        submission_id: row.submission_id,
        assignment_id: row.assignment_id,
        execution_config: assignment.execution_config.0,
        runner_config: assignment.runner_config.0,
        capabilities: assignment.capabilities.0,
        files,
    }))
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
            assignments.execution_config AS "execution_config: SqlxJson<Value>",
            assignments.runner_config AS "runner_config: SqlxJson<Value>",
            assignments.capabilities AS "capabilities: SqlxJson<Value>"
        FROM grading_jobs
        JOIN assignments ON assignments.id = grading_jobs.assignment_id
        WHERE grading_jobs.id = $1
          AND grading_jobs.worker_id = $2
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
        SELECT id, problem_name, original_filename, safe_filename, size_bytes
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
        size_bytes: file.size_bytes,
    })
    .collect();

    Ok(ClaimedJob {
        id: row.id,
        submission_id: row.submission_id,
        assignment_id: row.assignment_id,
        execution_config: row.execution_config.0,
        runner_config: row.runner_config.0,
        capabilities: row.capabilities.0,
        files,
    })
}

pub async fn post_events(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobEventBatchRequest>,
) -> Result<StatusCode, ApiError> {
    ensure_job_owner(&state, worker.id, job_id).await?;

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

    for event in value.events {
        sqlx::query!(
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
        .await
        .map_err(ApiError::internal)?;
    }

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn post_result(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    WorkerJson { worker, value }: WorkerJson<JobResultRequest>,
) -> Result<StatusCode, ApiError> {
    ensure_job_owner(&state, worker.id, job_id).await?;

    let now = Utc::now();
    let status = match value.result.status {
        cmsx_core::ResultStatus::Passed => "succeeded",
        cmsx_core::ResultStatus::Failed => "failed",
        cmsx_core::ResultStatus::Error => "error",
        cmsx_core::ResultStatus::Cancelled => "cancelled",
    };

    let mut tx = state.db.begin().await.map_err(ApiError::internal)?;

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
        format!("{:?}", value.result.status).to_lowercase(),
        value.result.score,
        value.result.max_score,
        value.result.feedback,
        SqlxJson(value.result.tests) as _,
        SqlxJson(serde_json::to_value(&value.result).map_err(ApiError::internal)?) as _,
        value.stdout_summary,
        value.stderr_summary,
        value.duration_ms,
        now
    )
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = $2,
            finished_at = $3,
            lease_expires_at = NULL
        WHERE id = $1
        "#,
        job_id,
        status,
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
    ensure_job_owner(&state, worker.id, job_id).await?;

    sqlx::query!(
        r#"
        UPDATE grading_jobs
        SET status = 'error',
            error_message = $2,
            finished_at = $3,
            lease_expires_at = NULL
        WHERE id = $1
        "#,
        job_id,
        format!("{}: {}", value.reason, value.message),
        Utc::now()
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn ensure_job_owner(state: &AppState, worker_id: Uuid, job_id: Uuid) -> Result<(), ApiError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM grading_jobs
            WHERE id = $1
              AND worker_id = $2
              AND status IN ('claimed', 'running')
        )
        "#,
        job_id,
        worker_id
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
