pub mod docker_socket;
pub mod in_worker;
pub mod utils;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use cmsx_core::ClaimedJob;

use crate::{events::ExecutorEventSink, workspace::JobWorkspace};

pub use docker_socket::DockerSocketExecutor;
pub use in_worker::InWorkerExecutor;

#[derive(Clone)]
pub enum Executor {
    DockerSocket(Box<DockerSocketExecutor>),
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
        event_sink: ExecutorEventSink,
    ) -> Result<ExecutionOutput> {
        match self {
            Self::DockerSocket(executor) => executor.run(job, workspace, cancel, event_sink).await,
            Self::InWorker(executor) => executor.run(job, workspace, cancel, event_sink).await,
        }
    }

    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::DockerSocket(_) => "docker-socket",
            Self::InWorker(_) => "in-worker",
        }
    }
}
