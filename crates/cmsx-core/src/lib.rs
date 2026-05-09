pub mod protocol;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub max_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Submission {
    pub id: Uuid,
    pub assignment_id: Uuid,
    pub netids_raw: String,
    pub received_at: DateTime<Utc>,
    pub raw_metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GradingJob {
    pub id: Uuid,
    pub submission_id: Uuid,
    pub assignment_id: Uuid,
    pub status: JobStatus,
    pub queued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Error,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = ();
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "claimed" => Ok(Self::Claimed),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "error" => Ok(Self::Error),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub job_id: Uuid,
    pub sequence: i64,
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub stream: String,
    pub visibility: String,
    pub message: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GradingResult {
    pub schema_version: String,
    pub status: ResultStatus,
    pub score: f64,
    pub max_score: f64,
    pub feedback: Option<String>,
    pub tests: Vec<TestResult>,
    pub artifacts: Vec<String>,
}

impl GradingResult {
    pub fn cancelled() -> Self {
        Self {
            schema_version: protocol::GRADING_RESULT_SCHEMA_VERSION.to_string(),
            status: ResultStatus::Cancelled,
            score: 0.0,
            max_score: 0.0,
            feedback: Some("Job cancelled".to_string()),
            tests: Vec::new(),
            artifacts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultStatus {
    Passed,
    Failed,
    Error,
    Cancelled,
}

impl ResultStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn terminal_job_status(&self) -> JobStatus {
        match self {
            Self::Passed => JobStatus::Succeeded,
            Self::Failed => JobStatus::Failed,
            Self::Error => JobStatus::Error,
            Self::Cancelled => JobStatus::Cancelled,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub name: String,
    pub status: ResultStatus,
    pub score: f64,
    pub max_score: f64,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerAuthClaims {
    pub method: String,
    pub path: String,
    pub body_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Online,
    Offline,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeatRequest {
    pub version: String,
    pub status: WorkerStatus,
    pub running_jobs: i32,
    pub max_jobs: i32,
    pub active_job_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeatResponse {
    pub worker_id: Uuid,
    pub lease_seconds: i64,
    pub renewed_job_ids: Vec<Uuid>,
    pub cancelled_job_ids: Vec<Uuid>,
    pub unknown_job_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimJobRequest {
    pub available_slots: i32,
    pub wait_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimJobResponse {
    pub jobs: Vec<ClaimedJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub submission_id: Uuid,
    pub assignment_id: Uuid,
    pub assignment_slug: String,
    pub lease_expires_at: DateTime<Utc>,
    pub attempt: i32,
    pub execution_config: serde_json::Value,
    pub runner_config: serde_json::Value,
    pub capabilities: serde_json::Value,
    pub submission_metadata: serde_json::Value,
    pub files: Vec<ClaimedJobFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedJobFile {
    pub id: Uuid,
    pub problem_name: Option<String>,
    pub original_filename: String,
    pub safe_filename: String,
    pub content_sha256: String,
    pub size_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartedJobRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEventBatchRequest {
    pub events: Vec<JobEventPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEventPayload {
    pub sequence: i64,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub stream: String,
    pub visibility: String,
    pub message: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResultRequest {
    pub result: GradingResult,
    pub duration_ms: Option<i64>,
    pub stdout_summary: Option<String>,
    pub stderr_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFailureRequest {
    pub reason: String,
    pub message: String,
    pub retryable: bool,
}
