use std::{fmt, pin::Pin};

use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use reqwest::{Client, StatusCode};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, ClaimedJob, JobEventBatchRequest, JobFailureRequest,
    JobResultRequest, StartedJobRequest, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
};

use crate::auth::WorkerSigner;

pub const ERROR_BODY_MAX_BYTES: usize = 64 * 1024;

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
    match response.bytes().await {
        Ok(bytes) => {
            let bounded = if bytes.len() > ERROR_BODY_MAX_BYTES {
                &bytes[..ERROR_BODY_MAX_BYTES]
            } else {
                &bytes
            };

            String::from_utf8_lossy(bounded).into_owned()
        }
        Err(error) => format!("failed to read error body: {error}"),
    }
}

pub fn cap_message_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }

    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_message_preserves_short_message() {
        assert_eq!(cap_message_bytes("abc", 10), "abc");
    }

    #[test]
    fn cap_message_truncates_on_char_boundary() {
        let value = "ééé";
        let capped = cap_message_bytes(value, 3);
        assert_eq!(capped, "é");
        assert!(capped.len() <= 3);
    }

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
}
