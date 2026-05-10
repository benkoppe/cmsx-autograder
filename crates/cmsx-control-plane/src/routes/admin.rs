use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::{
    Json, Router,
    extract::{FromRef, FromRequestParts, Path, State},
    http::{StatusCode, header, request::Parts},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use jwt_simple::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use cmsx_core::{
    WorkerStatus,
    protocol::{ASSIGNMENT_NAME_MAX_BYTES, ASSIGNMENT_SLUG_MAX_BYTES, WORKER_NAME_MAX_BYTES},
};

use crate::{app::AppState, db, error::ApiError, workers};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/admin/assignments",
            get(list_assignments).post(create_assignment),
        )
        .route(
            "/admin/assignments/{slug}",
            get(get_assignment).patch(update_assignment),
        )
        .route(
            "/admin/assignments/{slug}/tokens",
            get(list_assignment_tokens).post(create_assignment_token),
        )
        .route(
            "/admin/assignments/{slug}/tokens/{token_id}/revoke",
            post(revoke_assignment_token),
        )
        .route("/admin/workers", get(list_workers).post(create_worker))
        .route("/admin/workers/{worker_id}", get(get_worker))
        .route(
            "/admin/workers/{worker_id}/keys",
            get(list_worker_keys).post(create_worker_key),
        )
        .route(
            "/admin/workers/{worker_id}/keys/{key_id}/revoke",
            post(revoke_worker_key),
        )
        .route("/admin/workers/{worker_id}/disable", post(disable_worker))
}

const DEFAULT_WORKER_VERSION: &str = "0.1.0";

pub struct AdminAuth;

impl<S> FromRequestParts<S> for AdminAuth
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let state = AppState::from_ref(state);

        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| ApiError::unauthorized("missing admin authorization"))?;

        if state.admin.bootstrap_token_hashes.is_empty() {
            return Err(ApiError::forbidden("admin API is not configured"));
        }

        for hash in &state.admin.bootstrap_token_hashes {
            let Ok(parsed_hash) = PasswordHash::new(hash) else {
                tracing::warn!("ignoring malformed admin token hash");
                continue;
            };

            if Argon2::default()
                .verify_password(token.as_bytes(), &parsed_hash)
                .is_ok()
            {
                return Ok(Self);
            }
        }

        Err(ApiError::unauthorized("invalid admin token"))
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateAssignmentRequest {
    pub slug: String,
    pub name: String,
    pub max_score: f64,
    #[serde(default = "empty_object")]
    pub execution_config: Value,
    #[serde(default = "empty_object")]
    pub runner_config: Value,
    #[serde(default = "empty_object")]
    pub capabilities: Value,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAssignmentRequest {
    pub name: Option<String>,
    pub max_score: Option<f64>,
    pub execution_config: Option<Value>,
    pub runner_config: Option<Value>,
    pub capabilities: Option<Value>,
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

#[derive(Debug)]
struct AssignmentRow {
    id: Uuid,
    slug: String,
    name: String,
    max_score: f64,
    execution_config: SqlxJson<Value>,
    runner_config: SqlxJson<Value>,
    capabilities: SqlxJson<Value>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateAssignmentTokenRequest {}

#[derive(Debug, Serialize)]
pub struct CreatedAssignmentTokenResponse {
    pub id: Uuid,
    pub assignment_slug: String,
    pub token: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct AssignmentTokenResponse {
    pub id: Uuid,
    pub assignment_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateWorkerRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct CreatedWorkerResponse {
    pub worker_id: Uuid,
    pub name: String,
    pub key_id: Uuid,
    pub public_key_fingerprint: String,
    pub private_key_base64: String,
    pub worker_config: WorkerConfigResponse,
    pub worker_config_toml: String,
}

#[derive(Debug, Serialize)]
pub struct WorkerConfigResponse {
    pub control_plane_url: String,
    pub private_key_base64: String,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct WorkerResponse {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub version: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct WorkerRow {
    id: Uuid,
    name: String,
    status: String,
    version: Option<String>,
    created_at: DateTime<Utc>,
    last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct WorkerKeyResponse {
    pub id: Uuid,
    pub worker_id: Uuid,
    pub public_key_fingerprint: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct CreatedWorkerKeyResponse {
    pub key_id: Uuid,
    pub worker_id: Uuid,
    pub public_key_fingerprint: String,
    pub private_key_base64: String,
    pub worker_config: WorkerConfigResponse,
    pub worker_config_toml: String,
}

fn empty_object() -> Value {
    json!({})
}

pub async fn create_assignment(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Json(value): Json<CreateAssignmentRequest>,
) -> Result<Json<AssignmentResponse>, ApiError> {
    validate_assignment_create(&value)?;

    let now = Utc::now();
    let assignment_id = Uuid::now_v7();

    let row = sqlx::query_as!(
        AssignmentRow,
        r#"
        INSERT INTO assignments (
            id,
            slug,
            name,
            max_score,
            execution_config,
            runner_config,
            capabilities,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
        RETURNING
            id,
            slug,
            name,
            max_score,
            execution_config AS "execution_config: SqlxJson<Value>",
            runner_config AS "runner_config: SqlxJson<Value>",
            capabilities AS "capabilities: SqlxJson<Value>",
            created_at,
            updated_at
        "#,
        assignment_id,
        value.slug,
        value.name,
        value.max_score,
        SqlxJson(value.execution_config) as _,
        SqlxJson(value.runner_config) as _,
        SqlxJson(value.capabilities) as _,
        now,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        if db::is_unique_violation(&error) {
            ApiError::conflict("assignment slug already exists")
        } else {
            ApiError::internal(error)
        }
    })?;

    Ok(Json(assignment_response(row)))
}

pub async fn list_assignments(
    State(state): State<AppState>,
    _admin: AdminAuth,
) -> Result<Json<Vec<AssignmentResponse>>, ApiError> {
    let rows = sqlx::query_as!(
        AssignmentRow,
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
        ORDER BY created_at DESC, id DESC
        "#
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(rows.into_iter().map(assignment_response).collect()))
}

pub async fn get_assignment(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(slug): Path<String>,
) -> Result<Json<AssignmentResponse>, ApiError> {
    let row = sqlx::query_as!(
        AssignmentRow,
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
        "#,
        slug,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("assignment not found"))?;

    Ok(Json(assignment_response(row)))
}

pub async fn update_assignment(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(slug): Path<String>,
    Json(value): Json<UpdateAssignmentRequest>,
) -> Result<Json<AssignmentResponse>, ApiError> {
    validate_assignment_update(&value)?;

    let current = sqlx::query_as!(
        AssignmentRow,
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
        "#,
        slug,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("assignment not found"))?;

    let name = value.name.unwrap_or(current.name);
    let max_score = value.max_score.unwrap_or(current.max_score);
    let execution_config = value.execution_config.unwrap_or(current.execution_config.0);
    let runner_config = value.runner_config.unwrap_or(current.runner_config.0);
    let capabilities = value.capabilities.unwrap_or(current.capabilities.0);
    let now = Utc::now();

    let row = sqlx::query_as!(
        AssignmentRow,
        r#"
        UPDATE assignments
        SET name = $2,
            max_score = $3,
            execution_config = $4,
            runner_config = $5,
            capabilities = $6,
            updated_at = $7
        WHERE id = $1
        RETURNING
            id,
            slug,
            name,
            max_score,
            execution_config AS "execution_config: SqlxJson<Value>",
            runner_config AS "runner_config: SqlxJson<Value>",
            capabilities AS "capabilities: SqlxJson<Value>",
            created_at,
            updated_at
        "#,
        current.id,
        name,
        max_score,
        SqlxJson(execution_config) as _,
        SqlxJson(runner_config) as _,
        SqlxJson(capabilities) as _,
        now,
    )
    .fetch_one(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(assignment_response(row)))
}

pub async fn create_assignment_token(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(slug): Path<String>,
    Json(_value): Json<CreateAssignmentTokenRequest>,
) -> Result<Json<CreatedAssignmentTokenResponse>, ApiError> {
    let assignment_id = assignment_id_for_slug(&state, &slug).await?;

    let token_id = Uuid::now_v7();
    let token = generate_secret("ca");
    let token_hash = hash_secret(&token)?;
    let now = Utc::now();

    sqlx::query!(
        r#"
        INSERT INTO assignment_tokens (id, assignment_id, token_hash, created_at)
        VALUES ($1, $2, $3, $4)
        "#,
        token_id,
        assignment_id,
        token_hash,
        now,
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(CreatedAssignmentTokenResponse {
        id: token_id,
        assignment_slug: slug,
        token,
        created_at: now,
    }))
}

pub async fn list_assignment_tokens(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(slug): Path<String>,
) -> Result<Json<Vec<AssignmentTokenResponse>>, ApiError> {
    let assignment_id = assignment_id_for_slug(&state, &slug).await?;

    let rows = sqlx::query!(
        r#"
        SELECT id, assignment_id, created_at, revoked_at
        FROM assignment_tokens
        WHERE assignment_id = $1
        ORDER BY created_at DESC, id DESC
        "#,
        assignment_id,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(
        rows.into_iter()
            .map(|row| AssignmentTokenResponse {
                id: row.id,
                assignment_id: row.assignment_id,
                created_at: row.created_at,
                revoked_at: row.revoked_at,
            })
            .collect(),
    ))
}

pub async fn revoke_assignment_token(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path((slug, token_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, ApiError> {
    let assignment_id = assignment_id_for_slug(&state, &slug).await?;
    let now = Utc::now();

    let update = sqlx::query!(
        r#"
        UPDATE assignment_tokens
        SET revoked_at = $3
        WHERE id = $1
          AND assignment_id = $2
          AND revoked_at IS NULL
        "#,
        token_id,
        assignment_id,
        now,
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() != 1 {
        return Err(ApiError::not_found("active assignment token not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn create_worker(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Json(value): Json<CreateWorkerRequest>,
) -> Result<Json<CreatedWorkerResponse>, ApiError> {
    validate_worker_name(&value.name)?;

    let provisioned = workers::provision_worker(&state.db, &value.name)
        .await
        .map_err(ApiError::internal)?;

    let worker_config = worker_config_response(&state, &provisioned.private_key_base64);
    let worker_config_toml = worker_config_toml(&worker_config);

    Ok(Json(CreatedWorkerResponse {
        worker_id: provisioned.worker_id,
        name: provisioned.name,
        key_id: provisioned.key_id,
        public_key_fingerprint: provisioned.public_key_fingerprint,
        private_key_base64: provisioned.private_key_base64,
        worker_config,
        worker_config_toml,
    }))
}

pub async fn list_workers(
    State(state): State<AppState>,
    _admin: AdminAuth,
) -> Result<Json<Vec<WorkerResponse>>, ApiError> {
    let rows = sqlx::query_as!(
        WorkerRow,
        r#"
        SELECT id, name, status, version, created_at, last_seen_at
        FROM workers
        ORDER BY created_at DESC, id DESC
        "#
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(rows.into_iter().map(worker_response).collect()))
}

pub async fn get_worker(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(worker_id): Path<Uuid>,
) -> Result<Json<WorkerResponse>, ApiError> {
    let row = sqlx::query_as!(
        WorkerRow,
        r#"
        SELECT id, name, status, version, created_at, last_seen_at
        FROM workers
        WHERE id = $1
        "#,
        worker_id,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("worker not found"))?;

    Ok(Json(worker_response(row)))
}

pub async fn list_worker_keys(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(worker_id): Path<Uuid>,
) -> Result<Json<Vec<WorkerKeyResponse>>, ApiError> {
    ensure_worker_exists(&state, worker_id).await?;

    let rows = sqlx::query!(
        r#"
        SELECT id, worker_id, public_key_fingerprint, created_at, revoked_at
        FROM worker_keys
        WHERE worker_id = $1
        ORDER BY created_at DESC, id DESC
        "#,
        worker_id,
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(
        rows.into_iter()
            .map(|row| WorkerKeyResponse {
                id: row.id,
                worker_id: row.worker_id,
                public_key_fingerprint: row.public_key_fingerprint,
                created_at: row.created_at,
                revoked_at: row.revoked_at,
            })
            .collect(),
    ))
}

pub async fn create_worker_key(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(worker_id): Path<Uuid>,
) -> Result<Json<CreatedWorkerKeyResponse>, ApiError> {
    ensure_worker_exists(&state, worker_id).await?;

    let key_id = Uuid::now_v7();
    let now = Utc::now();

    let key_pair = Ed25519KeyPair::generate();
    let private_key_pem = key_pair.to_pem();
    let private_key_base64 = STANDARD.encode(private_key_pem.as_bytes());
    let public_key_pem = key_pair.public_key().to_pem();
    let public_key_fingerprint = workers::public_key_fingerprint(&public_key_pem);

    sqlx::query!(
        r#"
        INSERT INTO worker_keys (
            id,
            worker_id,
            public_key,
            public_key_fingerprint,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5)
        "#,
        key_id,
        worker_id,
        public_key_pem,
        public_key_fingerprint,
        now,
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    let worker_config = worker_config_response(&state, &private_key_base64);
    let worker_config_toml = worker_config_toml(&worker_config);

    Ok(Json(CreatedWorkerKeyResponse {
        key_id,
        worker_id,
        public_key_fingerprint,
        private_key_base64,
        worker_config,
        worker_config_toml,
    }))
}

pub async fn revoke_worker_key(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path((worker_id, key_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    let now = Utc::now();

    let update = sqlx::query!(
        r#"
        UPDATE worker_keys
        SET revoked_at = $3
        WHERE id = $1
          AND worker_id = $2
          AND revoked_at IS NULL
        "#,
        key_id,
        worker_id,
        now,
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() != 1 {
        return Err(ApiError::not_found("active worker key not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn disable_worker(
    State(state): State<AppState>,
    _admin: AdminAuth,
    Path(worker_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let disabled = WorkerStatus::Disabled.as_str();

    let update = sqlx::query!(
        r#"
        UPDATE workers
        SET status = $2
        WHERE id = $1
          AND status != $2
        "#,
        worker_id,
        disabled,
    )
    .execute(&state.db)
    .await
    .map_err(ApiError::internal)?;

    if update.rows_affected() != 1 {
        return Err(ApiError::not_found("enabled worker not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

fn validate_assignment_create(value: &CreateAssignmentRequest) -> Result<(), ApiError> {
    validate_slug(&value.slug)?;
    validate_assignment_name(&value.name)?;
    validate_score(value.max_score)?;
    validate_json_object(&value.execution_config, "execution_config")?;
    validate_json_object(&value.runner_config, "runner_config")?;
    validate_json_object(&value.capabilities, "capabilities")?;
    Ok(())
}

fn validate_assignment_update(value: &UpdateAssignmentRequest) -> Result<(), ApiError> {
    if let Some(name) = &value.name {
        validate_assignment_name(name)?;
    }
    if let Some(score) = value.max_score {
        validate_score(score)?;
    }
    if let Some(config) = &value.execution_config {
        validate_json_object(config, "execution_config")?;
    }
    if let Some(config) = &value.runner_config {
        validate_json_object(config, "runner_config")?;
    }
    if let Some(config) = &value.capabilities {
        validate_json_object(config, "capabilities")?;
    }

    Ok(())
}

fn validate_slug(slug: &str) -> Result<(), ApiError> {
    if slug.is_empty() || slug.len() > ASSIGNMENT_SLUG_MAX_BYTES {
        return Err(ApiError::bad_request(format!(
            "slug must be 1-{ASSIGNMENT_SLUG_MAX_BYTES} characters"
        )));
    }

    if !slug
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ApiError::bad_request(
            "slug must contain only lowercase letters, digits, and hyphens",
        ));
    }

    if slug.starts_with('-') || slug.ends_with('-') || slug.contains("--") {
        return Err(ApiError::bad_request("slug has invalid hyphen placement"));
    }

    Ok(())
}

fn validate_assignment_name(name: &str) -> Result<(), ApiError> {
    if name.trim().is_empty() || name.len() > ASSIGNMENT_NAME_MAX_BYTES {
        return Err(ApiError::bad_request(format!(
            "assignment name must be 1-{ASSIGNMENT_NAME_MAX_BYTES} characters"
        )));
    }

    Ok(())
}

fn validate_score(score: f64) -> Result<(), ApiError> {
    if !score.is_finite() || score < 0.0 {
        return Err(ApiError::bad_request(
            "max_score must be a finite nonnegative number",
        ));
    }

    Ok(())
}

fn validate_json_object(value: &Value, field: &str) -> Result<(), ApiError> {
    if !value.is_object() {
        return Err(ApiError::bad_request(format!("{field} must be an object")));
    }

    Ok(())
}

fn validate_worker_name(name: &str) -> Result<(), ApiError> {
    if name.trim().is_empty() || name.len() > WORKER_NAME_MAX_BYTES {
        return Err(ApiError::bad_request(format!(
            "worker name must be 1-{WORKER_NAME_MAX_BYTES} characters"
        )));
    }

    Ok(())
}

async fn assignment_id_for_slug(state: &AppState, slug: &str) -> Result<Uuid, ApiError> {
    sqlx::query_scalar!(
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
    .ok_or_else(|| ApiError::not_found("assignment not found"))
}

async fn ensure_worker_exists(state: &AppState, worker_id: Uuid) -> Result<(), ApiError> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM workers
            WHERE id = $1
        )
        "#,
        worker_id,
    )
    .fetch_one(&state.db)
    .await
    .map_err(ApiError::internal)?
    .unwrap_or(false);

    if !exists {
        return Err(ApiError::not_found("worker not found"));
    }

    Ok(())
}

fn hash_secret(secret: &str) -> Result<String, ApiError> {
    let salt = SaltString::generate(&mut OsRng);

    Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(ApiError::internal)
}

fn generate_secret(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

fn worker_config_response(state: &AppState, private_key_base64: &str) -> WorkerConfigResponse {
    WorkerConfigResponse {
        control_plane_url: state.admin.public_url.trim_end_matches('/').to_string(),
        private_key_base64: private_key_base64.to_string(),
        version: DEFAULT_WORKER_VERSION.to_string(),
    }
}

#[derive(serde::Serialize)]
struct WorkerConfigToml<'a> {
    control_plane_url: &'a str,
    private_key_base64: &'a str,
    version: &'a str,
    executor: WorkerExecutorConfigToml,
}

#[derive(serde::Serialize)]
struct WorkerExecutorConfigToml {
    backend: &'static str,
    workspace_root: &'static str,
    grader_root: &'static str,
    keep_workspaces: bool,
    python_command: &'static str,
}

fn worker_config_toml(config: &WorkerConfigResponse) -> String {
    let toml_config = WorkerConfigToml {
        control_plane_url: &config.control_plane_url,
        private_key_base64: &config.private_key_base64,
        version: &config.version,
        executor: WorkerExecutorConfigToml {
            backend: "in-worker",
            workspace_root: "data/worker",
            grader_root: "examples/assignments",
            keep_workspaces: false,
            python_command: "python3",
        },
    };

    toml::to_string(&toml_config).expect("worker config TOML should serialize")
}

fn assignment_response(row: AssignmentRow) -> AssignmentResponse {
    AssignmentResponse {
        id: row.id,
        slug: row.slug,
        name: row.name,
        max_score: row.max_score,
        execution_config: row.execution_config.0,
        runner_config: row.runner_config.0,
        capabilities: row.capabilities.0,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn worker_response(row: WorkerRow) -> WorkerResponse {
    WorkerResponse {
        id: row.id,
        name: row.name,
        status: row.status,
        version: row.version,
        created_at: row.created_at,
        last_seen_at: row.last_seen_at,
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;
    use uuid::Uuid;

    use cmsx_core::{
        ClaimJobRequest, JobEventBatchRequest, JobStatus, ResultStatus, WorkerHeartbeatRequest,
        WorkerStatus, protocol::JOB_LEASE_SECONDS,
    };

    use crate::test_support;

    #[tokio::test]
    async fn admin_routes_reject_missing_bearer_token() {
        let app = test_support::test_app().await;

        let response = test_support::get(&app.app, "/admin/assignments").await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_routes_accept_valid_bootstrap_token() {
        let app = test_support::test_app().await;

        let response = test_support::admin_get(&app.app, "/admin/assignments").await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_assignment_persists_assignment() {
        let app = test_support::test_app().await;

        let response = create_assignment(&app, "python-intro").await;
        let (status, json) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["slug"], "python-intro");
        assert_eq!(json["name"], "Python Intro");
        assert_eq!(json["max_score"], 100.0);

        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM assignments
            WHERE slug = 'python-intro'
            "#
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to count assignments")
        .unwrap_or(0);

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn create_assignment_rejects_duplicate_slug() {
        let app = test_support::test_app().await;

        let first = create_assignment(&app, "python-intro").await;
        assert_eq!(first.status(), StatusCode::OK);

        let second = create_assignment(&app, "python-intro").await;
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn update_assignment_persists_mutable_fields() {
        let app = test_support::test_app().await;

        let created = create_assignment(&app, "python-intro").await;
        assert_eq!(created.status(), StatusCode::OK);

        let response = test_support::admin_patch_json(
            &app.app,
            "/admin/assignments/python-intro",
            &json!({
                "name": "Updated Python Intro",
                "max_score": 50.0,
                "execution_config": {"timeout_seconds": 30},
                "runner_config": {"environment": "python"},
                "capabilities": {"read_files": true}
            }),
        )
        .await;

        let (status, json) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["slug"], "python-intro");
        assert_eq!(json["name"], "Updated Python Intro");
        assert_eq!(json["max_score"], 50.0);
        assert_eq!(json["execution_config"]["timeout_seconds"], 30);
        assert_eq!(json["runner_config"]["environment"], "python");
        assert_eq!(json["capabilities"]["read_files"], true);
    }

    #[tokio::test]
    async fn create_assignment_token_returns_plaintext_once_and_stores_hash() {
        let app = test_support::test_app().await;

        let assignment = create_assignment(&app, "python-intro").await;
        assert_eq!(assignment.status(), StatusCode::OK);

        let response = test_support::admin_post_json(
            &app.app,
            "/admin/assignments/python-intro/tokens",
            &json!({}),
        )
        .await;

        let (status, json) = test_support::response_json(response).await;
        assert_eq!(status, StatusCode::OK);

        let token = json["token"]
            .as_str()
            .expect("token should be returned")
            .to_string();

        assert!(token.starts_with("ca_"));
        assert!(token.chars().count() <= 36);

        let row = sqlx::query!(
            r#"
            SELECT token_hash
            FROM assignment_tokens
            WHERE id = $1
            "#,
            Uuid::parse_str(json["id"].as_str().unwrap()).unwrap(),
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load assignment token");

        assert_ne!(row.token_hash, token);
        assert!(row.token_hash.starts_with("$argon2"));
    }

    #[tokio::test]
    async fn revoked_assignment_token_cannot_submit_cmsx() {
        let app = test_support::test_app().await;

        assert_eq!(
            create_assignment(&app, "python-intro").await.status(),
            StatusCode::OK
        );

        let token_response = test_support::admin_post_json(
            &app.app,
            "/admin/assignments/python-intro/tokens",
            &json!({}),
        )
        .await;
        let (_, token_json) = test_support::response_json(token_response).await;

        let token_id = token_json["id"].as_str().unwrap();
        let token = token_json["token"].as_str().unwrap();

        let revoke_response = test_support::admin_post_json(
            &app.app,
            &format!("/admin/assignments/python-intro/tokens/{token_id}/revoke"),
            &json!({}),
        )
        .await;
        assert_eq!(revoke_response.status(), StatusCode::NO_CONTENT);

        let submit_response = test_support::submit_cmsx(&app.app, "python-intro", token).await;
        assert_eq!(submit_response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn create_worker_returns_config_and_stores_public_key_only() {
        let app = test_support::test_app().await;

        let response = create_worker(&app, "worker-1").await;
        let (status, json) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["name"], "worker-1");
        assert_eq!(
            json["worker_config"]["control_plane_url"],
            "http://control-plane.test"
        );
        assert!(json["private_key_base64"].as_str().unwrap().len() > 100);
        assert!(
            json["worker_config_toml"]
                .as_str()
                .unwrap()
                .contains("private_key_base64")
        );

        let worker_id = Uuid::parse_str(json["worker_id"].as_str().unwrap()).unwrap();

        let worker = sqlx::query!(
            r#"
            SELECT name, status
            FROM workers
            WHERE id = $1
            "#,
            worker_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load worker");

        assert_eq!(worker.name, "worker-1");
        assert_eq!(worker.status, WorkerStatus::Offline.as_str());

        let key = sqlx::query!(
            r#"
            SELECT public_key, public_key_fingerprint
            FROM worker_keys
            WHERE worker_id = $1
            "#,
            worker_id,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load worker key");

        assert!(key.public_key.contains("PUBLIC KEY"));
        assert!(!key.public_key.contains("PRIVATE KEY"));
        assert_eq!(
            key.public_key_fingerprint,
            json["public_key_fingerprint"].as_str().unwrap()
        );
    }

    #[tokio::test]
    async fn provisioned_worker_key_can_heartbeat() {
        let app = test_support::test_app().await;

        let worker_response = create_worker(&app, "worker-1").await;
        let (_, worker_json) = test_support::response_json(worker_response).await;
        let private_key = worker_json["private_key_base64"].as_str().unwrap();
        assert_eq!(
            test_support::worker_public_key_fingerprint(private_key),
            worker_json["public_key_fingerprint"].as_str().unwrap()
        );

        let heartbeat = WorkerHeartbeatRequest {
            version: "0.1.0".to_string(),
            status: WorkerStatus::Online,
            running_jobs: 0,
            max_jobs: 1,
            active_job_ids: vec![],
        };

        let response =
            test_support::worker_post_json(&app.app, private_key, "/workers/heartbeat", &heartbeat)
                .await;

        let (status, json) = test_support::response_json(response).await;

        assert_eq!(status, StatusCode::OK, "body={json}");
        assert_eq!(json["worker_id"], worker_json["worker_id"]);
        assert_eq!(json["lease_seconds"], JOB_LEASE_SECONDS);
    }

    #[tokio::test]
    async fn revoked_worker_key_cannot_heartbeat() {
        let app = test_support::test_app().await;

        let worker_response = create_worker(&app, "worker-1").await;
        let (_, worker_json) = test_support::response_json(worker_response).await;

        let worker_id = worker_json["worker_id"].as_str().unwrap();
        let key_id = worker_json["key_id"].as_str().unwrap();
        let private_key = worker_json["private_key_base64"].as_str().unwrap();

        let revoke_response = test_support::admin_post_json(
            &app.app,
            &format!("/admin/workers/{worker_id}/keys/{key_id}/revoke"),
            &json!({}),
        )
        .await;
        assert_eq!(revoke_response.status(), StatusCode::NO_CONTENT);

        let heartbeat = WorkerHeartbeatRequest {
            version: "0.1.0".to_string(),
            status: WorkerStatus::Online,
            running_jobs: 0,
            max_jobs: 1,
            active_job_ids: vec![],
        };

        let response =
            test_support::worker_post_json(&app.app, private_key, "/workers/heartbeat", &heartbeat)
                .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn disabled_worker_cannot_heartbeat() {
        let app = test_support::test_app().await;

        let worker_response = create_worker(&app, "worker-1").await;
        let (_, worker_json) = test_support::response_json(worker_response).await;

        let worker_id = worker_json["worker_id"].as_str().unwrap();
        let private_key = worker_json["private_key_base64"].as_str().unwrap();

        let disable_response = test_support::admin_post_json(
            &app.app,
            &format!("/admin/workers/{worker_id}/disable"),
            &json!({}),
        )
        .await;
        assert_eq!(disable_response.status(), StatusCode::NO_CONTENT);

        let heartbeat = WorkerHeartbeatRequest {
            version: "0.1.0".to_string(),
            status: WorkerStatus::Online,
            running_jobs: 0,
            max_jobs: 1,
            active_job_ids: vec![],
        };

        let response =
            test_support::worker_post_json(&app.app, private_key, "/workers/heartbeat", &heartbeat)
                .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn provisioning_smoke_test_reaches_stored_result() {
        let app = test_support::test_app().await;

        assert_eq!(
            create_assignment(&app, "python-intro").await.status(),
            StatusCode::OK
        );

        let token_response = test_support::admin_post_json(
            &app.app,
            "/admin/assignments/python-intro/tokens",
            &json!({}),
        )
        .await;
        let (_, token_json) = test_support::response_json(token_response).await;
        let assignment_token = token_json["token"].as_str().unwrap();

        let worker_response = create_worker(&app, "worker-1").await;
        let (_, worker_json) = test_support::response_json(worker_response).await;
        let private_key = worker_json["private_key_base64"].as_str().unwrap();

        let submit_response =
            test_support::submit_cmsx(&app.app, "python-intro", assignment_token).await;
        assert_eq!(submit_response.status(), StatusCode::NO_CONTENT);

        let claim = ClaimJobRequest {
            available_slots: 1,
            wait_seconds: None,
        };

        let claim_response =
            test_support::worker_post_json(&app.app, private_key, "/workers/jobs/claim", &claim)
                .await;
        let (claim_status, claim_json) = test_support::response_json(claim_response).await;

        assert_eq!(claim_status, StatusCode::OK);
        assert_eq!(claim_json["jobs"].as_array().unwrap().len(), 1);

        let job_id = claim_json["jobs"][0]["id"].as_str().unwrap();

        let started_response = test_support::worker_post_json(
            &app.app,
            private_key,
            &format!("/workers/jobs/{job_id}/started"),
            &json!({}),
        )
        .await;
        assert_eq!(started_response.status(), StatusCode::NO_CONTENT);

        let events = JobEventBatchRequest {
            events: vec![test_support::test_event(0, "Job started")],
        };

        let events_response = test_support::worker_post_json(
            &app.app,
            private_key,
            &format!("/workers/jobs/{job_id}/events"),
            &events,
        )
        .await;
        assert_eq!(events_response.status(), StatusCode::NO_CONTENT);

        let result = test_support::test_job_result_request();

        let result_response = test_support::worker_post_json(
            &app.app,
            private_key,
            &format!("/workers/jobs/{job_id}/result"),
            &result,
        )
        .await;
        assert_eq!(result_response.status(), StatusCode::NO_CONTENT);

        let job_uuid = Uuid::parse_str(job_id).unwrap();

        let stored = sqlx::query!(
            r#"
            SELECT
                grading_jobs.status AS job_status,
                grading_results.status AS result_status,
                grading_results.score,
                grading_results.max_score
            FROM grading_jobs
            JOIN grading_results ON grading_results.job_id = grading_jobs.id
            WHERE grading_jobs.id = $1
            "#,
            job_uuid,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to load stored result");

        assert_eq!(stored.job_status, JobStatus::Succeeded.as_str());
        assert_eq!(stored.result_status, ResultStatus::Passed.as_str());
        assert_eq!(stored.score, 100.0);
        assert_eq!(stored.max_score, 100.0);

        let event_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM job_events
            WHERE job_id = $1
            "#,
            job_uuid,
        )
        .fetch_one(&app.db)
        .await
        .expect("failed to count job events")
        .unwrap_or(0);

        assert_eq!(event_count, 1);
    }

    async fn create_assignment(
        app: &test_support::TestApp,
        slug: &str,
    ) -> axum::response::Response {
        test_support::admin_post_json(
            &app.app,
            "/admin/assignments",
            &json!({
                "slug": slug,
                "name": "Python Intro",
                "max_score": 100.0,
                "execution_config": {},
                "runner_config": {},
                "capabilities": {
                    "read_files": true,
                    "run_commands": false,
                    "execute_student_code": false,
                    "network": false
                }
            }),
        )
        .await
    }

    async fn create_worker(app: &test_support::TestApp, name: &str) -> axum::response::Response {
        test_support::admin_post_json(
            &app.app,
            "/admin/workers",
            &json!({
                "name": name
            }),
        )
        .await
    }
}
