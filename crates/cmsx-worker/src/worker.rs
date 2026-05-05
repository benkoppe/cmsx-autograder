use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use tokio::sync::{RwLock, Semaphore};
use uuid::Uuid;

use cmsx_core::{
    ClaimJobRequest, ClaimedJob, JobFailureRequest, WorkerHeartbeatRequest, WorkerStatus,
};

use crate::{auth::WorkerSigner, capacity, client::ControlPlaneClient, config::WorkerConfig};

type ActiveJobs = Arc<RwLock<HashSet<Uuid>>>;

pub async fn run(config: WorkerConfig) -> Result<()> {
    let max_jobs = capacity::max_jobs(config.max_jobs);
    let running_jobs = Arc::new(AtomicUsize::new(0));
    let active_jobs = Arc::new(RwLock::new(HashSet::new()));

    let signer = WorkerSigner::from_pem(config.worker_id, &config.private_key_pem)?;
    let client = ControlPlaneClient::new(config.control_plane_url.clone(), signer);

    tokio::spawn(heartbeat_loop(
        config,
        client,
        running_jobs,
        active_jobs,
        max_jobs,
    ));

    tracing::warn!(
        "worker executor is not implemented yet; heartbeat loop is running but jobs will not be claimed"
    );

    std::future::pending::<()>().await;
    Ok(())
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

        let claimed_job_ids = {
            let active_jobs = active_jobs.read().await;
            active_jobs.iter().copied().collect()
        };

        let request = WorkerHeartbeatRequest {
            worker_name: config.worker_name.clone(),
            version: config.version.clone(),
            status: WorkerStatus::Online,
            executor_backends: config.executor_backends.clone(),
            runner_images: config.runner_images.clone(),
            running_jobs: running_jobs.load(Ordering::Relaxed) as i32,
            max_jobs: max_jobs as i32,
            claimed_job_ids,
        };

        if let Err(error) = client.heartbeat(&request).await {
            tracing::warn!(?error, "worker heartbeat failed");
        }
    }
}

#[allow(dead_code)]
async fn claim_loop(
    config: WorkerConfig,
    client: ControlPlaneClient,
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
            executor_backends: config.executor_backends.clone(),
            runner_images: config.runner_images.clone(),
            max_jobs: max_jobs as i32,
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
            continue;
        }

        for job in response.jobs {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = client.clone();
            let running_jobs = running_jobs.clone();
            let active_jobs = active_jobs.clone();

            {
                let mut active_jobs = active_jobs.write().await;
                active_jobs.insert(job.id);
            }

            running_jobs.fetch_add(1, Ordering::Relaxed);

            tokio::spawn(async move {
                let _permit = permit;

                if let Err(error) = run_job(client.clone(), job.clone()).await {
                    tracing::error!(job_id = %job.id, ?error, "job failed");

                    let failure = JobFailureRequest {
                        reason: "worker_error".to_string(),
                        message: error.to_string(),
                        retryable: false,
                    };

                    let _ = client.post_failed(job.id, &failure).await;
                }

                {
                    let mut active_jobs = active_jobs.write().await;
                    active_jobs.remove(&job.id);
                }

                running_jobs.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }
}

#[allow(dead_code)]
async fn run_job(client: ControlPlaneClient, job: ClaimedJob) -> Result<()> {
    tracing::info!(job_id = %job.id, "claimed job");

    let failure = JobFailureRequest {
        reason: "executor_not_implemented".to_string(),
        message: "in-worker executor has not been implemented yet".to_string(),
        retryable: false,
    };

    client.post_failed(job.id, &failure).await?;

    Ok(())
}
