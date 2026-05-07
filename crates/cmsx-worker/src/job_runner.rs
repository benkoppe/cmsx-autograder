use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use cmsx_core::ClaimedJob;

use crate::{
    client::ControlPlaneClient, config::WorkerConfig, executor::Executor,
    worker::CancellationReason,
};

pub const FAILURE_INPUT_DOWNLOAD: &str = "input_download_failed";
pub const FAILURE_INPUT_HASH: &str = "input_hash_mismatch";
pub const FAILURE_GRADER_MISSING: &str = "grader_missing";
pub const FAILURE_EXECUTOR_ERROR: &str = "executor_error";
pub const FAILURE_RESULT_MISSING: &str = "result_missing";
pub const FAILURE_RESULT_INVALID: &str = "result_invalid";
pub const FAILURE_CANCELLED_BEFORE_START: &str = "cancelled_before_start";
pub const FAILURE_TIMEOUT: &str = "timeout";
pub const FAILURE_LEASE_LOST: &str = "lease_lost";
pub const FAILURE_WORKSPACE_ERROR: &str = "workspace_error";

pub const FAILURE_MESSAGE_MAX_BYTES: usize = 64 * 1024;

pub async fn run_job(
    _config: WorkerConfig,
    _client: ControlPlaneClient,
    _executor: Executor,
    job: ClaimedJob,
    _cancel: CancellationToken,
    _reason: Arc<RwLock<CancellationReason>>,
) -> Result<()> {
    tracing::info!(job_id = %job.id, "claimed job; full lifecycle not wired yet");
    Ok(())
}
