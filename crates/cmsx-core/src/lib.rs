use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub cmsx_assignment_id: String,
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
    Running,
    Succeeded,
    Failed,
    Error,
    Cancelled,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultStatus {
    Passed,
    Failed,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub name: String,
    pub status: ResultStatus,
    pub score: f64,
    pub max_score: f64,
    pub message: Option<String>,
}
