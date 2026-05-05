use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Serialize, de::DeserializeOwned};

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, ClaimedJob, JobEventBatchRequest, JobFailureRequest,
    JobResultRequest, StartedJobRequest, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
};

use crate::auth::WorkerSigner;

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
    ) -> Result<WorkerHeartbeatResponse> {
        self.post_json("/workers/heartbeat", request).await
    }

    pub async fn claim_job(&self, request: &ClaimJobRequest) -> Result<ClaimJobResponse> {
        self.post_json("/workers/jobs/claim", request).await
    }

    pub async fn get_job(&self, job_id: uuid::Uuid) -> Result<ClaimedJob> {
        self.get_json(&format!("/workers/jobs/{job_id}")).await
    }

    pub async fn post_events(
        &self,
        job_id: uuid::Uuid,
        request: &JobEventBatchRequest,
    ) -> Result<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/events"), request)
            .await
    }

    pub async fn post_result(&self, job_id: uuid::Uuid, request: &JobResultRequest) -> Result<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/result"), request)
            .await
    }

    pub async fn post_started(&self, job_id: uuid::Uuid) -> Result<()> {
        self.post_empty(
            &format!("/workers/jobs/{job_id}/started"),
            &StartedJobRequest {},
        )
        .await
    }

    pub async fn post_failed(&self, job_id: uuid::Uuid, request: &JobFailureRequest) -> Result<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/failed"), request)
            .await
    }

    pub async fn get_job_file(
        &self,
        job_id: uuid::Uuid,
        file_id: uuid::Uuid,
    ) -> Result<bytes::Bytes> {
        let path = format!("/workers/jobs/{job_id}/files/{file_id}");
        let auth = self.signer.authorization_header("GET", &path, &[])?;

        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .send()
            .await
            .context("request failed")?
            .error_for_status()
            .context("control plane returned error")?;

        response
            .bytes()
            .await
            .context("failed to read response body")
    }

    async fn get_json<R>(&self, path: &str) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let auth = self.signer.authorization_header("GET", path, &[])?;

        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .send()
            .await
            .context("request failed")?
            .error_for_status()
            .context("control plane returned error")?;

        response.json().await.context("failed to decode response")
    }

    async fn post_json<T, R>(&self, path: &str, value: &T) -> Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let body = serde_json::to_vec(value).context("failed to encode request body")?;
        let auth = self.signer.authorization_header("POST", path, &body)?;

        let response = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .context("request failed")?
            .error_for_status()
            .context("control plane returned error")?;

        response.json().await.context("failed to decode response")
    }

    async fn post_empty<T>(&self, path: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(value).context("failed to encode request body")?;
        let auth = self.signer.authorization_header("POST", path, &body)?;

        self.http
            .post(format!("{}{}", self.base_url, path))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .context("request failed")?
            .error_for_status()
            .context("control plane returned error")?;

        Ok(())
    }
}
