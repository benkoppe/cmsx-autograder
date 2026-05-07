use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::{
    Json,
    extract::{FromRef, FromRequestParts, Path, State},
    http::{StatusCode, header, request::Parts},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use jwt_simple::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::{app::AppState, error::ApiError, workers};

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
        if is_unique_violation(&error) {
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
    let token = generate_secret("cmsx_assignment");
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
    let update = sqlx::query!(
        r#"
        UPDATE workers
        SET status = 'disabled'
        WHERE id = $1
          AND status != 'disabled'
        "#,
        worker_id,
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
    if slug.is_empty() || slug.len() > 128 {
        return Err(ApiError::bad_request("slug must be 1-128 characters"));
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
    if name.trim().is_empty() || name.len() > 256 {
        return Err(ApiError::bad_request(
            "assignment name must be 1-256 characters",
        ));
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
    if name.trim().is_empty() || name.len() > 128 {
        return Err(ApiError::bad_request(
            "worker name must be 1-128 characters",
        ));
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
    format!(
        "{prefix}_{}_{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

fn worker_config_response(state: &AppState, private_key_base64: &str) -> WorkerConfigResponse {
    WorkerConfigResponse {
        control_plane_url: state.admin.public_url.trim_end_matches('/').to_string(),
        private_key_base64: private_key_base64.to_string(),
        version: DEFAULT_WORKER_VERSION.to_string(),
    }
}

fn worker_config_toml(config: &WorkerConfigResponse) -> String {
    format!(
        r#"control_plane_url = "{}"
private_key_base64 = "{}"
version = "{}"
[executor]
backend = "in-worker"
workspace_root = "data/worker"
grader_root = "examples/assignments"
keep_workspaces = false
python_command = "python3"
"#,
        config.control_plane_url, config.private_key_base64, config.version
    )
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

fn is_unique_violation(error: &sqlx::Error) -> bool {
    let Some(db_error) = error.as_database_error() else {
        return false;
    };

    db_error.code().as_deref() == Some("23505")
}
