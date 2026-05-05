use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Serialize, de::DeserializeOwned};

use cmsx_core::{
    ClaimJobRequest, ClaimJobResponse, JobEventBatchRequest, JobFailureRequest, JobResultRequest,
    WorkerHeartbeatRequest, WorkerHeartbeatResponse,
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

    pub async fn post_failed(&self, job_id: uuid::Uuid, request: &JobFailureRequest) -> Result<()> {
        self.post_empty(&format!("/workers/jobs/{job_id}/failed"), request)
            .await
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
