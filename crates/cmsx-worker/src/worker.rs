use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use tokio::sync::{RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cmsx_core::{ClaimJobRequest, WorkerHeartbeatRequest, WorkerHeartbeatResponse, WorkerStatus};

use crate::{
    auth::WorkerSigner,
    capacity,
    client::ControlPlaneClient,
    config::{ExecutorConfig, WorkerConfig},
    executor::{Executor, InWorkerExecutor},
    job_runner,
};

type ActiveJobs = Arc<RwLock<HashMap<Uuid, ActiveJobHandle>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    None,
    ControlPlaneCancelled,
    LeaseLost,
}

#[derive(Clone)]
pub struct ActiveJobHandle {
    pub cancel: CancellationToken,
    pub reason: Arc<RwLock<CancellationReason>>,
}

pub fn apply_cancellation_reason(current: &mut CancellationReason, next: CancellationReason) {
    match (*current, next) {
        (_, CancellationReason::LeaseLost) => *current = CancellationReason::LeaseLost,
        (CancellationReason::None, CancellationReason::ControlPlaneCancelled) => {
            *current = CancellationReason::ControlPlaneCancelled;
        }
        _ => {}
    }
}

pub fn decrement_running_jobs(running_jobs: &AtomicUsize) {
    let previous = running_jobs.fetch_sub(1, Ordering::Relaxed);
    debug_assert!(previous > 0, "running_jobs underflow");
}

pub async fn run(config: WorkerConfig) -> Result<()> {
    let max_jobs = capacity::max_jobs(config.executor.max_jobs());
    let running_jobs = Arc::new(AtomicUsize::new(0));
    let active_jobs = Arc::new(RwLock::new(HashMap::new()));
    let semaphore = Arc::new(Semaphore::new(max_jobs));

    let signer = WorkerSigner::from_base64_pem(&config.private_key_base64)?;
    let client = ControlPlaneClient::new(config.control_plane_url.clone(), signer);
    let executor = executor_from_config(&config.executor);

    tokio::spawn(heartbeat_loop(
        config.clone(),
        client.clone(),
        running_jobs.clone(),
        active_jobs.clone(),
        max_jobs,
    ));

    claim_loop(
        config,
        client,
        executor,
        semaphore,
        running_jobs,
        active_jobs,
        max_jobs,
    )
    .await
}

fn executor_from_config(config: &ExecutorConfig) -> Executor {
    match config {
        ExecutorConfig::InWorker(config) => Executor::InWorker(InWorkerExecutor::new(config)),
        ExecutorConfig::DockerSocket(_) => {
            tracing::warn!(
                "docker-socket executor is not implemented; using in-worker executor config defaults"
            );
            let fallback = crate::config::InWorkerExecutorConfig {
                workspace_root: config.workspace_root().to_path_buf(),
                grader_root: config.grader_root().to_path_buf(),
                max_jobs: config.max_jobs(),
                keep_workspaces: config.keep_workspaces(),
                python_command: Some("python3".to_string()),
            };
            executor_from_config(&ExecutorConfig::InWorker(fallback))
        }
    }
}

async fn heartbeat_loop(
    config: WorkerConfig,
    client: ControlPlaneClient,
    running_jobs: Arc<AtomicUsize>,
    active_jobs: ActiveJobs,
    max_jobs: usize,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        let active_job_ids = {
            let active_jobs = active_jobs.read().await;
            active_jobs.keys().copied().collect()
        };

        let request = WorkerHeartbeatRequest {
            version: config.version.clone(),
            status: WorkerStatus::Online,
            running_jobs: running_jobs.load(Ordering::Relaxed) as i32,
            max_jobs: max_jobs as i32,
            active_job_ids,
        };

        match client.heartbeat(&request).await {
            Ok(response) => {
                reconcile_heartbeat_response(&active_jobs, response).await;
            }
            Err(error) => {
                tracing::warn!(?error, "worker heartbeat failed");
            }
        }
    }
}

async fn reconcile_heartbeat_response(active_jobs: &ActiveJobs, response: WorkerHeartbeatResponse) {
    for job_id in response.cancelled_job_ids {
        cancel_active_job(
            active_jobs,
            job_id,
            CancellationReason::ControlPlaneCancelled,
            "control plane requested job cancellation",
        )
        .await;
    }

    for job_id in response.unknown_job_ids {
        cancel_active_job(
            active_jobs,
            job_id,
            CancellationReason::LeaseLost,
            "control plane no longer recognizes active job ownership",
        )
        .await;
    }

    for job_id in response.renewed_job_ids {
        tracing::debug!(%job_id, "job lease renewed");
    }
}

pub async fn cancel_active_job(
    active_jobs: &ActiveJobs,
    job_id: Uuid,
    reason: CancellationReason,
    message: &'static str,
) {
    let handle = {
        let active_jobs = active_jobs.read().await;
        active_jobs.get(&job_id).cloned()
    };

    let Some(handle) = handle else {
        tracing::debug!(%job_id, ?reason, "heartbeat referenced inactive job");
        return;
    };

    {
        let mut current = handle.reason.write().await;
        apply_cancellation_reason(&mut current, reason);
    }

    tracing::info!(%job_id, ?reason, message);
    handle.cancel.cancel();
}

async fn claim_loop(
    config: WorkerConfig,
    client: ControlPlaneClient,
    executor: Executor,
    semaphore: Arc<Semaphore>,
    running_jobs: Arc<AtomicUsize>,
    active_jobs: ActiveJobs,
    max_jobs: usize,
) -> Result<()> {
    loop {
        let current_running = running_jobs.load(Ordering::Relaxed);
        let available_slots = max_jobs.saturating_sub(current_running);

        if available_slots == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        let request = ClaimJobRequest {
            available_slots: available_slots as i32,
            wait_seconds: Some(20),
        };

        let response = match client.claim_job(&request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(?error, "job claim failed");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        if response.jobs.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        for job in response.jobs {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "capacity semaphore closed; remaining already-claimed jobs will expire or be reclaimed by lease"
                    );
                    return Ok(());
                }
            };

            let cancel = CancellationToken::new();
            let reason = Arc::new(RwLock::new(CancellationReason::None));
            let handle = ActiveJobHandle {
                cancel: cancel.clone(),
                reason: reason.clone(),
            };

            {
                let mut active_jobs = active_jobs.write().await;
                active_jobs.insert(job.id, handle);
            }

            running_jobs.fetch_add(1, Ordering::Relaxed);

            let config = config.clone();
            let client = client.clone();
            let executor = executor.clone();
            let running_jobs = running_jobs.clone();
            let active_jobs = active_jobs.clone();

            tokio::spawn(async move {
                let _permit = permit;
                let job_id = job.id;

                let result =
                    job_runner::run_job(config, client, executor, job, cancel, reason).await;

                if let Err(error) = result {
                    tracing::error!(%job_id, ?error, "job runner failed");
                }

                {
                    let mut active_jobs = active_jobs.write().await;
                    active_jobs.remove(&job_id);
                }

                decrement_running_jobs(&running_jobs);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_reason_prioritizes_lease_lost() {
        let mut reason = CancellationReason::None;
        apply_cancellation_reason(&mut reason, CancellationReason::ControlPlaneCancelled);
        assert_eq!(reason, CancellationReason::ControlPlaneCancelled);

        apply_cancellation_reason(&mut reason, CancellationReason::LeaseLost);
        assert_eq!(reason, CancellationReason::LeaseLost);

        apply_cancellation_reason(&mut reason, CancellationReason::ControlPlaneCancelled);
        assert_eq!(reason, CancellationReason::LeaseLost);
    }

    #[test]
    fn cancellation_reason_none_to_lease_lost() {
        let mut reason = CancellationReason::None;
        apply_cancellation_reason(&mut reason, CancellationReason::LeaseLost);
        assert_eq!(reason, CancellationReason::LeaseLost);
    }

    #[test]
    fn cancellation_reason_none_to_control_plane_cancelled() {
        let mut reason = CancellationReason::None;
        apply_cancellation_reason(&mut reason, CancellationReason::ControlPlaneCancelled);
        assert_eq!(reason, CancellationReason::ControlPlaneCancelled);
    }

    #[test]
    fn decrement_running_jobs_decrements() {
        let running_jobs = AtomicUsize::new(1);

        decrement_running_jobs(&running_jobs);

        assert_eq!(running_jobs.load(Ordering::Relaxed), 0);
    }
}
