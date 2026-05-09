pub mod in_worker;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use cmsx_core::ClaimedJob;

use crate::workspace::JobWorkspace;

pub use in_worker::InWorkerExecutor;

#[derive(Clone)]
pub enum Executor {
    InWorker(InWorkerExecutor),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutput {
    pub status: ExecutionStatus,
    pub duration_ms: i64,
    pub stdout_summary: Option<String>,
    pub stderr_summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStatus {
    Exited { code: Option<i32> },
    TimedOut,
    Cancelled,
}

impl Executor {
    pub async fn run(
        &self,
        job: &ClaimedJob,
        workspace: &JobWorkspace,
        cancel: CancellationToken,
    ) -> Result<ExecutionOutput> {
        match self {
            Self::InWorker(executor) => executor.run(job, workspace, cancel).await,
        }
    }
}
