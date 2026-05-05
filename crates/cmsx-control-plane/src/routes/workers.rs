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
