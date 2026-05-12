use std::{fmt, pin::Pin};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::AsyncRead;
use tokio::time::Duration;
use tokio_util::io::StreamReader;
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, ClaimedJob, JobEventBatchRequest, JobFailureRequest,
    JobResultRequest, StartedJobRequest, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
    protocol::{JOB_EVENT_MESSAGE_MAX_BYTES, encode_artifact_relative_path},
};

use crate::auth::WorkerSigner;

pub const ERROR_BODY_MAX_BYTES: usize = JOB_EVENT_MESSAGE_MAX_BYTES;

pub type ClientResult<T> = std::result::Result<T, ClientError>;

#[derive(Debug)]
pub enum ClientError {
    Request(reqwest::Error),
    Status { status: StatusCode, body: String },
    Decode(anyhow::Error),
    Auth(anyhow::Error),
}

impl ClientError {
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Status { status, .. } => Some(*status),
            _ => None,
        }
    }

    pub fn is_status(&self, expected: StatusCode) -> bool {
        self.status() == Some(expected)
    }

    pub fn bounded_body(&self) -> Option<&str> {
        match self {
            Self::Status { body, .. } => Some(body.as_str()),
            _ => None,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => write!(f, "request failed: {error}"),
            Self::Status { status, body } => {
                if body.is_empty() {
                    write!(f, "control plane returned status {status}")
                } else {
                    write!(f, "control plane returned status {status}: {body}")
                }
            }
            Self::Decode(error) => write!(f, "failed to decode response: {error}"),
            Self::Auth(error) => write!(f, "failed to sign request: {error}"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone, Deserialize)]
pub struct ArtifactUploadErrorBody {
    pub code: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ArtifactUploadOutcome {
    Uploaded,
    JobNotActive,
    CancellationRequested,
    NonRetryableFailure { body: String },
    RetryableFailure { body: String },
}

pub struct ArtifactUpload<'a> {
    pub artifact_id: Uuid,
    pub relative_path: &'a str,
    pub sha256: &'a str,
    pub size_bytes: u64,
    pub bytes: Bytes,
}

pub struct JobFileDownload {
    pub reader: Pin<Box<dyn AsyncRead + Send>>,
}

#[derive(Clone)]
pub struct ControlPlaneClient {
    base_url: String,
    http: Client,
    signer: WorkerSigner,
}

impl ControlPlaneClient {
    pub fn new(base_url: String, signer: WorkerSigner) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: Client::new(),
            signer,
        }
    }

    pub async fn heartbeat(
        &self,
        request: &WorkerHeartbeatRequest,
    ) -> ClientResult<WorkerHeartbeatResponse> {
        self.post_json("/workers/heartbeat", request).await
    }

    pub async fn claim_job(&self, request: &ClaimJobRequest) -> ClientResult<ClaimJobResponse> {
        self.post_json("/workers/jobs/claim", request).await
    }

    #[allow(dead_code)] // Protocol helper reserved for future job refresh/reconciliation flows.
    pub async fn get_job(&self, job_id: Uuid) -> ClientResult<ClaimedJob> {
        self.get_json(&format!("/workers/jobs/{job_id}")).await
    }

    pub async fn post_events(
        &self,
        job_id: Uuid,
        request: &JobEventBatchRequest,
    ) -> ClientResult<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/events"), request)
            .await
    }

    pub async fn post_result(&self, job_id: Uuid, request: &JobResultRequest) -> ClientResult<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/result"), request)
            .await
    }

    pub async fn post_started(&self, job_id: Uuid) -> ClientResult<()> {
        self.post_empty(
            &format!("/workers/jobs/{job_id}/started"),
            &StartedJobRequest {},
        )
        .await
    }

    pub async fn post_failed(&self, job_id: Uuid, request: &JobFailureRequest) -> ClientResult<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/failed"), request)
            .await
    }

    pub async fn get_job_file_stream(
        &self,
        job_id: Uuid,
        file_id: Uuid,
    ) -> ClientResult<JobFileDownload> {
        let path = format!("/workers/jobs/{job_id}/files/{file_id}");
        let auth = self
            .signer
            .authorization_header("GET", &path, &[])
            .map_err(ClientError::Auth)?;

        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .send()
            .await
            .map_err(ClientError::Request)?;

        let response = ensure_success(response).await?;

        let stream = response.bytes_stream().map_err(std::io::Error::other);

        let reader = StreamReader::new(stream);

        Ok(JobFileDownload {
            reader: Box::pin(reader),
        })
    }

    pub async fn put_artifact(
        &self,
        job_id: Uuid,
        artifact: ArtifactUpload<'_>,
    ) -> Result<ArtifactUploadOutcome, ClientError> {
        let path = format!("/workers/jobs/{job_id}/artifacts/{}", artifact.artifact_id);
        let auth = self
            .signer
            .authorization_header("PUT", &path, &artifact.bytes)
            .map_err(ClientError::Auth)?;

        let encoded_path = encode_artifact_relative_path(artifact.relative_path)
            .map_err(|error| ClientError::Decode(anyhow::Error::new(error)))?;

        let response = self
            .http
            .put(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header("x-cmsx-artifact-relative-path", encoded_path)
            .header(
                "x-cmsx-artifact-size-bytes",
                artifact.size_bytes.to_string(),
            )
            .header("x-cmsx-artifact-sha256", artifact.sha256)
            .header("x-cmsx-artifact-visibility", "staff")
            .body(artifact.bytes)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(ClientError::Request)?;

        artifact_upload_outcome(response).await
    }

    #[allow(dead_code)] // Used by get_job, which is intentionally retained for future flows.
    async fn get_json<R>(&self, path: &str) -> ClientResult<R>
    where
        R: DeserializeOwned,
    {
        let auth = self
            .signer
            .authorization_header("GET", path, &[])
            .map_err(ClientError::Auth)?;

        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .send()
            .await
            .map_err(ClientError::Request)?;

        let response = ensure_success(response).await?;

        response
            .json()
            .await
            .map_err(|error| ClientError::Decode(anyhow::Error::new(error)))
    }

    async fn post_json<T, R>(&self, path: &str, value: &T) -> ClientResult<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let body = serde_json::to_vec(value).map_err(|error| {
            ClientError::Decode(anyhow::Error::new(error).context("failed to encode request body"))
        })?;

        let auth = self
            .signer
            .authorization_header("POST", path, &body)
            .map_err(ClientError::Auth)?;

        let response = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(ClientError::Request)?;

        let response = ensure_success(response).await?;

        response
            .json()
            .await
            .map_err(|error| ClientError::Decode(anyhow::Error::new(error)))
    }

    async fn post_empty<T>(&self, path: &str, value: &T) -> ClientResult<()>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(value).map_err(|error| {
            ClientError::Decode(anyhow::Error::new(error).context("failed to encode request body"))
        })?;

        let auth = self
            .signer
            .authorization_header("POST", path, &body)
            .map_err(ClientError::Auth)?;

        let response = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(ClientError::Request)?;

        ensure_success(response).await?;
        Ok(())
    }
}

async fn ensure_success(response: reqwest::Response) -> ClientResult<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let body = read_bounded_error_body(response).await;

    Err(ClientError::Status { status, body })
}

async fn read_bounded_error_body(response: reqwest::Response) -> String {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                if bytes.is_empty() {
                    return format!("failed to read error body: {error}");
                }

                tracing::warn!(?error, "failed to finish reading error body");
                break;
            }
        };

        let remaining = ERROR_BODY_MAX_BYTES.saturating_sub(bytes.len());

        if remaining == 0 {
            break;
        }

        if chunk.len() <= remaining {
            bytes.extend_from_slice(&chunk);
        } else {
            bytes.extend_from_slice(&chunk[..remaining]);
            break;
        }
    }

    String::from_utf8_lossy(&bytes).into_owned()
}

async fn artifact_upload_outcome(
    response: reqwest::Response,
) -> Result<ArtifactUploadOutcome, ClientError> {
    if response.status().is_success() {
        return Ok(ArtifactUploadOutcome::Uploaded);
    }

    let status = response.status();
    let body = read_bounded_error_body(response).await;

    Ok(classify_artifact_upload_error(status, &body))
}

fn classify_artifact_upload_error(status: StatusCode, body: &str) -> ArtifactUploadOutcome {
    let parsed = serde_json::from_str::<ArtifactUploadErrorBody>(body).ok();
    let code = parsed.and_then(|body| body.code);

    match (status, code.as_deref()) {
        (StatusCode::NOT_FOUND, Some("job_not_active")) => ArtifactUploadOutcome::JobNotActive,
        (StatusCode::CONFLICT, Some("job_cancellation_requested")) => {
            ArtifactUploadOutcome::CancellationRequested
        }
        (StatusCode::CONFLICT, Some("artifact_duplicate" | "artifact_conflict"))
        | (StatusCode::BAD_REQUEST, Some("artifact_invalid_metadata" | "artifact_hash_mismatch"))
        | (StatusCode::PAYLOAD_TOO_LARGE, Some("artifact_too_large"))
        | (StatusCode::UNAUTHORIZED, _) => ArtifactUploadOutcome::NonRetryableFailure {
            body: body.to_string(),
        },
        (status, Some("artifact_upload_failed")) if status.is_server_error() => {
            ArtifactUploadOutcome::RetryableFailure {
                body: body.to_string(),
            }
        }
        (status, _) if status.is_server_error() => ArtifactUploadOutcome::RetryableFailure {
            body: body.to_string(),
        },
        _ => ArtifactUploadOutcome::NonRetryableFailure {
            body: body.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_error_exposes_status() {
        let error = ClientError::Status {
            status: StatusCode::NOT_FOUND,
            body: "missing".to_string(),
        };

        assert_eq!(error.status(), Some(StatusCode::NOT_FOUND));
        assert!(error.is_status(StatusCode::NOT_FOUND));
        assert_eq!(error.bounded_body(), Some("missing"));
    }

    // --- artifact upload outcome classification ---

    fn coded_body(code: &str) -> String {
        serde_json::json!({"error": "test", "code": code}).to_string()
    }

    #[test]
    fn artifact_outcome_job_not_active() {
        let body = coded_body("job_not_active");
        let outcome = classify_artifact_upload_error(StatusCode::NOT_FOUND, &body);

        assert!(matches!(outcome, ArtifactUploadOutcome::JobNotActive));
    }

    #[test]
    fn artifact_outcome_cancellation_requested() {
        let body = coded_body("job_cancellation_requested");
        let outcome = classify_artifact_upload_error(StatusCode::CONFLICT, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::CancellationRequested
        ));
    }

    #[test]
    fn artifact_outcome_duplicate_is_non_retryable() {
        let body = coded_body("artifact_duplicate");
        let outcome = classify_artifact_upload_error(StatusCode::CONFLICT, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_conflict_is_non_retryable() {
        let body = coded_body("artifact_conflict");
        let outcome = classify_artifact_upload_error(StatusCode::CONFLICT, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_invalid_metadata_is_non_retryable() {
        let body = coded_body("artifact_invalid_metadata");
        let outcome = classify_artifact_upload_error(StatusCode::BAD_REQUEST, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_hash_mismatch_is_non_retryable() {
        let body = coded_body("artifact_hash_mismatch");
        let outcome = classify_artifact_upload_error(StatusCode::BAD_REQUEST, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_too_large_is_non_retryable() {
        let body = coded_body("artifact_too_large");
        let outcome = classify_artifact_upload_error(StatusCode::PAYLOAD_TOO_LARGE, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_unauthorized_is_non_retryable() {
        let outcome = classify_artifact_upload_error(StatusCode::UNAUTHORIZED, "unauthorized");

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_server_error_with_upload_failed_code_is_retryable() {
        let body = coded_body("artifact_upload_failed");
        let outcome = classify_artifact_upload_error(StatusCode::INTERNAL_SERVER_ERROR, &body);

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::RetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_generic_server_error_is_retryable() {
        let outcome = classify_artifact_upload_error(StatusCode::INTERNAL_SERVER_ERROR, "not json");

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::RetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_502_is_retryable() {
        let outcome = classify_artifact_upload_error(StatusCode::BAD_GATEWAY, "gateway error");

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::RetryableFailure { .. }
        ));
    }

    #[test]
    fn artifact_outcome_unknown_4xx_is_non_retryable() {
        let outcome = classify_artifact_upload_error(StatusCode::FORBIDDEN, "forbidden");

        assert!(matches!(
            outcome,
            ArtifactUploadOutcome::NonRetryableFailure { .. }
        ));
    }
}
