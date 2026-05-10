use std::{collections::HashMap, path::Path, time::Instant};

use anyhow::{Context, Result, bail};
use bollard::{
    Docker,
    container::LogOutput,
    models::{ContainerCreateBody, ContainerCreateResponse, HostConfig, Mount, MountType},
    query_parameters::{
        CreateContainerOptionsBuilder, KillContainerOptionsBuilder, LogsOptionsBuilder,
        RemoveContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::{task::JoinHandle, time::Duration};
use tokio_util::sync::CancellationToken;

use cmsx_core::{ClaimedJob, protocol::job_event_type};

use crate::{
    config::DockerSocketExecutorConfig,
    events::{ExecutorEvent, ExecutorEventSink},
    executor::{
        ExecutionOutput, ExecutionStatus,
        utils::{OutputSummaries, normalize_timeout_seconds},
    },
    job_contract,
    workspace::JobWorkspace,
};

const RUNNER_USER: &str = "10001:10001";
const RUNNER_HOME: &str = "/tmp";
const CONTAINER_PYTHON_COMMAND: &str = "python";
const DOCKER_NETWORK_BRIDGE: &str = "bridge";
const DOCKER_NETWORK_NONE: &str = "none";
const DOCKER_DROP_ALL_CAPS: &str = "ALL";
const DOCKER_NO_NEW_PRIVILEGES: &str = "no-new-privileges:true";
const DOCKER_KILL_SIGNAL: &str = "SIGKILL";
const DOCKER_LOG_TAIL_ALL: &str = "all";
const LOG_READER_JOIN_TIMEOUT_SECONDS: u64 = 2;
const BYTES_PER_MEBIBYTE: i64 = 1024 * 1024;

#[derive(Clone)]
pub struct DockerSocketExecutor {
    docker: Docker,
    config: DockerSocketExecutorConfig,
}

#[derive(Debug, Deserialize)]
struct ExecutionConfig {
    timeout_seconds: Option<u64>,
    memory_mb: Option<i64>,
    cpus: Option<f64>,
    pids_limit: Option<i64>,
    network: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RunnerConfig {
    image: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DockerJobConfig {
    pub image: String,
    pub timeout_seconds: u64,
    pub memory_bytes: Option<i64>,
    pub nano_cpus: Option<i64>,
    pub pids_limit: Option<i64>,
    pub network_enabled: bool,
}

impl DockerSocketExecutor {
    pub fn new(config: &DockerSocketExecutorConfig) -> Result<Self> {
        let docker = connect_docker(config.docker_host.as_deref())?;

        Ok(Self {
            docker,
            config: config.clone(),
        })
    }

    pub async fn run(
        &self,
        job: &ClaimedJob,
        workspace: &JobWorkspace,
        cancel: CancellationToken,
        event_sink: ExecutorEventSink,
    ) -> Result<ExecutionOutput> {
        let config = parse_docker_job_config(job, &self.config)?;
        let started = Instant::now();
        let container_name = container_name(job);

        let container = self
            .create_container(&container_name, &config, workspace)
            .await?;

        event_sink.emit(ExecutorEvent::worker(
            job_event_type::EXECUTOR_CONTAINER_CREATED,
            "Docker container created",
            json!({
                "container_id": container.id,
                "container_name": container_name,
            }),
        ));

        let container_id = container.id;
        let logs_task = tokio::spawn(collect_logs(
            self.docker.clone(),
            container_id.clone(),
            event_sink.clone(),
        ));

        event_sink.emit(ExecutorEvent::worker(
            job_event_type::EXECUTOR_CONTAINER_STARTED,
            "Docker container started",
            json!({
                "container_id": container_id,
                "container_name": container_name,
            }),
        ));

        let status = match self
            .start_and_wait(&container_id, config.timeout_seconds, cancel)
            .await
        {
            Ok(status) => status,
            Err(error) => {
                cleanup_container(&self.docker, &container_id).await;
                let _ = logs_task.await;
                return Err(error);
            }
        };

        let summaries = join_logs(logs_task).await;
        cleanup_container(&self.docker, &container_id).await;

        Ok(ExecutionOutput {
            status,
            duration_ms: started.elapsed().as_millis().min(i64::MAX as u128) as i64,
            stdout_summary: summaries.stdout.into_summary_string(),
            stderr_summary: summaries.stderr.into_summary_string(),
        })
    }

    async fn create_container(
        &self,
        container_name: &str,
        config: &DockerJobConfig,
        workspace: &JobWorkspace,
    ) -> Result<ContainerCreateResponse> {
        let input_dir = canonical_host_path(&workspace.input_dir)?;
        let grader_dir = canonical_host_path(&workspace.grader_dir)?;
        let work_dir = canonical_host_path(&workspace.work_dir)?;
        let output_dir = canonical_host_path(&workspace.output_dir)?;

        let host_config = HostConfig {
            mounts: Some(vec![
                bind_mount(&input_dir, job_contract::CONTAINER_INPUT_DIR, true),
                bind_mount(&grader_dir, job_contract::CONTAINER_GRADER_DIR, true),
                bind_mount(&work_dir, job_contract::CONTAINER_WORK_DIR, false),
                bind_mount(&output_dir, job_contract::CONTAINER_OUTPUT_DIR, false),
            ]),
            tmpfs: Some(default_tmpfs_mounts()),
            network_mode: Some(if config.network_enabled {
                DOCKER_NETWORK_BRIDGE.to_string()
            } else {
                DOCKER_NETWORK_NONE.to_string()
            }),
            memory: config.memory_bytes,
            memory_swap: config.memory_bytes,
            nano_cpus: config.nano_cpus,
            pids_limit: config.pids_limit,
            readonly_rootfs: Some(true),
            cap_drop: Some(vec![DOCKER_DROP_ALL_CAPS.to_string()]),
            security_opt: Some(vec![DOCKER_NO_NEW_PRIVILEGES.to_string()]),
            privileged: Some(false),
            publish_all_ports: Some(false),
            auto_remove: Some(false),
            init: Some(true),
            ..Default::default()
        };

        let body = ContainerCreateBody {
            image: Some(config.image.clone()),
            user: Some(RUNNER_USER.to_string()),
            cmd: Some(vec![
                CONTAINER_PYTHON_COMMAND.to_string(),
                "-m".to_string(),
                job_contract::SDK_MODULE.to_string(),
                format!(
                    "{}/{}",
                    job_contract::CONTAINER_GRADER_DIR,
                    job_contract::GRADE_PY
                ),
            ]),
            working_dir: Some(job_contract::CONTAINER_WORK_DIR.to_string()),
            env: Some(vec![
                format!("HOME={RUNNER_HOME}"),
                format!(
                    "{}={}",
                    job_contract::ENV_INPUT_DIR,
                    job_contract::CONTAINER_INPUT_DIR
                ),
                format!(
                    "{}={}",
                    job_contract::ENV_WORK_DIR,
                    job_contract::CONTAINER_WORK_DIR
                ),
                format!(
                    "{}={}",
                    job_contract::ENV_OUTPUT_DIR,
                    job_contract::CONTAINER_OUTPUT_DIR
                ),
            ]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            network_disabled: Some(!config.network_enabled),
            host_config: Some(host_config),
            ..Default::default()
        };

        let options = CreateContainerOptionsBuilder::default()
            .name(container_name)
            .build();

        self.docker
            .create_container(Some(options), body)
            .await
            .with_context(|| format!("failed to create Docker container {container_name}"))
    }

    async fn start_and_wait(
        &self,
        container_id: &str,
        timeout_seconds: u64,
        cancel: CancellationToken,
    ) -> Result<ExecutionStatus> {
        self.docker
            .start_container(container_id, None)
            .await
            .with_context(|| format!("failed to start Docker container {container_id}"))?;

        tokio::select! {
            wait = wait_for_container(&self.docker, container_id) => {
                wait
            }
            _ = tokio::time::sleep(Duration::from_secs(timeout_seconds)) => {
                kill_container(&self.docker, container_id).await;
                wait_after_kill(&self.docker, container_id).await;
                Ok(ExecutionStatus::TimedOut)
            }
            _ = cancel.cancelled() => {
                kill_container(&self.docker, container_id).await;
                wait_after_kill(&self.docker, container_id).await;
                Ok(ExecutionStatus::Cancelled)
            }
        }
    }
}

pub fn parse_docker_job_config(
    job: &ClaimedJob,
    defaults: &DockerSocketExecutorConfig,
) -> Result<DockerJobConfig> {
    let runner = serde_json::from_value::<RunnerConfig>(job.runner_config.clone())
        .context("invalid runner_config for docker-socket executor")?;

    let execution = serde_json::from_value::<ExecutionConfig>(job.execution_config.clone())
        .unwrap_or(ExecutionConfig {
            timeout_seconds: None,
            memory_mb: None,
            cpus: None,
            pids_limit: None,
            network: None,
        });

    let image = runner
        .image
        .map(|image| image.trim().to_string())
        .filter(|image| !image.is_empty())
        .unwrap_or_else(|| defaults.default_image.trim().to_string());

    if image.is_empty() {
        bail!("executor.default_image must not be empty");
    }

    Ok(DockerJobConfig {
        image,
        timeout_seconds: execution
            .timeout_seconds
            .or(defaults.default_timeout_seconds)
            .map(Option::Some)
            .map(normalize_timeout_seconds)
            .unwrap_or_else(|| normalize_timeout_seconds(None)),
        memory_bytes: normalize_memory_bytes(execution.memory_mb.or(defaults.default_memory_mb))?,
        nano_cpus: normalize_nano_cpus(execution.cpus.or(defaults.default_cpus))?,
        pids_limit: normalize_pids_limit(execution.pids_limit.or(defaults.default_pids_limit))?,
        network_enabled: execution
            .network
            .or(defaults.default_network)
            .unwrap_or(false),
    })
}

pub fn normalize_memory_bytes(memory_mb: Option<i64>) -> Result<Option<i64>> {
    let Some(memory_mb) = memory_mb else {
        return Ok(None);
    };

    if memory_mb <= 0 {
        bail!("execution_config.memory_mb must be positive");
    }

    memory_mb
        .checked_mul(BYTES_PER_MEBIBYTE)
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("execution_config.memory_mb is too large"))
}

pub fn normalize_nano_cpus(cpus: Option<f64>) -> Result<Option<i64>> {
    let Some(cpus) = cpus else {
        return Ok(None);
    };

    if !cpus.is_finite() || cpus <= 0.0 {
        bail!("execution_config.cpus must be a positive finite number");
    }

    let nano_cpus = cpus * 1_000_000_000.0;

    if nano_cpus > i64::MAX as f64 {
        bail!("execution_config.cpus is too large");
    }

    Ok(Some(nano_cpus.round() as i64))
}

pub fn normalize_pids_limit(pids_limit: Option<i64>) -> Result<Option<i64>> {
    let Some(pids_limit) = pids_limit else {
        return Ok(None);
    };

    if pids_limit <= 0 {
        bail!("execution_config.pids_limit must be positive");
    }

    Ok(Some(pids_limit))
}

pub fn container_name(job: &ClaimedJob) -> String {
    format!("cmsx-job-{}-attempt-{}", job.id, job.attempt)
}

fn canonical_host_path(path: &Path) -> Result<String> {
    path.canonicalize()
        .with_context(|| {
            format!(
                "failed to canonicalize Docker bind mount path {}",
                path.display()
            )
        })
        .map(|path| path.display().to_string())
}

fn bind_mount(source: &str, target: &str, read_only: bool) -> Mount {
    Mount {
        target: Some(target.to_string()),
        source: Some(source.to_string()),
        typ: Some(MountType::BIND),
        read_only: Some(read_only),
        ..Default::default()
    }
}

fn default_tmpfs_mounts() -> HashMap<String, String> {
    HashMap::from([
        (
            "/tmp".to_string(),
            "rw,noexec,nosuid,nodev,size=64m,mode=1777".to_string(),
        ),
        (
            "/run".to_string(),
            "rw,noexec,nosuid,nodev,size=16m,mode=755".to_string(),
        ),
    ])
}

async fn wait_for_container(docker: &Docker, container_id: &str) -> Result<ExecutionStatus> {
    let mut stream = docker.wait_container(container_id, None);

    let result = stream
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Docker wait stream ended without a result"))?
        .with_context(|| format!("failed waiting for Docker container {container_id}"))?;

    Ok(ExecutionStatus::Exited {
        code: Some(result.status_code as i32),
    })
}

async fn wait_after_kill(docker: &Docker, container_id: &str) {
    if let Err(error) = wait_for_container(docker, container_id).await {
        tracing::debug!(
            container_id,
            ?error,
            "failed waiting for Docker container after kill"
        );
    }
}

async fn kill_container(docker: &Docker, container_id: &str) {
    let options = KillContainerOptionsBuilder::default()
        .signal(DOCKER_KILL_SIGNAL)
        .build();

    if let Err(error) = docker.kill_container(container_id, Some(options)).await {
        tracing::debug!(
            container_id,
            ?error,
            "failed to kill Docker container; it may have already exited"
        );
    }
}

async fn cleanup_container(docker: &Docker, container_id: &str) {
    let options = RemoveContainerOptionsBuilder::default()
        .force(true)
        .v(true)
        .link(false)
        .build();

    if let Err(error) = docker.remove_container(container_id, Some(options)).await {
        tracing::warn!(container_id, ?error, "failed to remove Docker container");
    }
}

async fn collect_logs(
    docker: Docker,
    container_id: String,
    event_sink: ExecutorEventSink,
) -> OutputSummaries {
    let options = LogsOptionsBuilder::default()
        .follow(true)
        .stdout(true)
        .stderr(true)
        .timestamps(false)
        .tail(DOCKER_LOG_TAIL_ALL)
        .build();

    let mut summaries = OutputSummaries::default();
    let mut logs = docker.logs(&container_id, Some(options));

    while let Some(item) = logs.next().await {
        match item {
            Ok(LogOutput::StdOut { message }) => {
                summaries.stdout.push(&message);
                event_sink.emit(ExecutorEvent::stdout(
                    String::from_utf8_lossy(&message).into_owned(),
                ));
            }
            Ok(LogOutput::StdErr { message }) => {
                summaries.stderr.push(&message);
                event_sink.emit(ExecutorEvent::stderr(
                    String::from_utf8_lossy(&message).into_owned(),
                ));
            }
            Ok(LogOutput::Console { message }) => {
                summaries.stdout.push(&message);
                event_sink.emit(ExecutorEvent::stdout(
                    String::from_utf8_lossy(&message).into_owned(),
                ));
            }
            Ok(LogOutput::StdIn { .. }) => {}
            Err(error) => {
                tracing::debug!(container_id, ?error, "Docker log stream ended with error");
                break;
            }
        }
    }

    summaries
}

async fn join_logs(task: JoinHandle<OutputSummaries>) -> OutputSummaries {
    match tokio::time::timeout(Duration::from_secs(LOG_READER_JOIN_TIMEOUT_SECONDS), task).await {
        Ok(Ok(summaries)) => summaries,
        Ok(Err(error)) => {
            tracing::warn!(?error, "Docker log collection task failed");
            OutputSummaries::default()
        }
        Err(_) => {
            tracing::warn!("timed out waiting for Docker log collection task");
            OutputSummaries::default()
        }
    }
}

fn connect_docker(docker_host: Option<&str>) -> Result<Docker> {
    if let Some(docker_host) = docker_host.map(str::trim).filter(|host| !host.is_empty()) {
        return Docker::connect_with_host(docker_host)
            .with_context(|| format!("failed to connect to Docker daemon at {docker_host}"));
    }

    Docker::connect_with_defaults()
        .context("failed to connect to Docker daemon; set executor.docker_host or DOCKER_HOST")
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use uuid::Uuid;

    fn test_job(
        execution_config: serde_json::Value,
        runner_config: serde_json::Value,
    ) -> ClaimedJob {
        ClaimedJob {
            id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            submission_id: Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
            assignment_id: Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap(),
            assignment_slug: "intro".to_string(),
            lease_expires_at: chrono::Utc::now(),
            attempt: 2,
            execution_config,
            runner_config,
            capabilities: json!({}),
            submission_metadata: json!({}),
            files: Vec::new(),
        }
    }

    fn default_executor_config() -> DockerSocketExecutorConfig {
        DockerSocketExecutorConfig {
            workspace_root: "data/worker".into(),
            grader_root: "examples/assignments".into(),
            max_jobs: Some(1),
            keep_workspaces: false,
            docker_host: None,
            default_image: "cmsx-runner-python:latest".to_string(),
            default_timeout_seconds: Some(60),
            default_memory_mb: Some(512),
            default_cpus: Some(1.0),
            default_pids_limit: Some(128),
            default_network: Some(false),
        }
    }

    #[test]
    fn docker_job_config_requires_image() {
        let job = test_job(json!({}), json!({}));

        let config = parse_docker_job_config(&job, &default_executor_config()).unwrap();

        assert_eq!(config.image, "cmsx-runner-python:latest");
    }

    #[test]
    fn docker_job_config_allows_image_override() {
        let job = test_job(json!({}), json!({ "image": "custom-runner:latest" }));

        let config = parse_docker_job_config(&job, &default_executor_config()).unwrap();

        assert_eq!(config.image, "custom-runner:latest");
    }

    #[test]
    fn docker_job_config_empty_image_uses_default() {
        let job = test_job(json!({}), json!({ "image": "   " }));

        let config = parse_docker_job_config(&job, &default_executor_config()).unwrap();

        assert_eq!(config.image, "cmsx-runner-python:latest");
    }

    #[test]
    fn docker_job_config_uses_worker_defaults() {
        let job = test_job(json!({}), json!({}));

        let config = parse_docker_job_config(&job, &default_executor_config()).unwrap();

        assert_eq!(config.timeout_seconds, 60);
        assert_eq!(config.memory_bytes, Some(512 * BYTES_PER_MEBIBYTE));
        assert_eq!(config.nano_cpus, Some(1_000_000_000));
        assert_eq!(config.pids_limit, Some(128));
        assert!(!config.network_enabled);
    }

    #[test]
    fn docker_job_config_assignment_overrides_worker_defaults() {
        let job = test_job(
            json!({
                "timeout_seconds": 30,
                "memory_mb": 256,
                "cpus": 1.5,
                "pids_limit": 64,
                "network": true,
            }),
            json!({}),
        );

        let config = parse_docker_job_config(&job, &default_executor_config()).unwrap();

        assert_eq!(config.timeout_seconds, 30);
        assert_eq!(config.memory_bytes, Some(256 * BYTES_PER_MEBIBYTE));
        assert_eq!(config.nano_cpus, Some(1_500_000_000));
        assert_eq!(config.pids_limit, Some(64));
        assert!(config.network_enabled);
    }

    #[test]
    fn docker_job_config_parses_resource_limits() {
        let job = test_job(
            json!({
                "timeout_seconds": 30,
                "memory_mb": 512,
                "cpus": 1.5,
                "pids_limit": 128,
                "network": true,
            }),
            json!({ "image": "runner:latest" }),
        );

        let config = parse_docker_job_config(&job, &default_executor_config()).unwrap();

        assert_eq!(config.timeout_seconds, 30);
        assert_eq!(config.memory_bytes, Some(512 * BYTES_PER_MEBIBYTE));
        assert_eq!(config.nano_cpus, Some(1_500_000_000));
        assert_eq!(config.pids_limit, Some(128));
        assert!(config.network_enabled);
    }

    #[test]
    fn memory_must_be_positive() {
        assert!(normalize_memory_bytes(Some(0)).is_err());
        assert!(normalize_memory_bytes(Some(-1)).is_err());
    }

    #[test]
    fn cpus_must_be_positive_finite() {
        assert!(normalize_nano_cpus(Some(0.0)).is_err());
        assert!(normalize_nano_cpus(Some(-1.0)).is_err());
        assert!(normalize_nano_cpus(Some(f64::NAN)).is_err());
    }

    #[test]
    fn pids_limit_must_be_positive() {
        assert!(normalize_pids_limit(Some(0)).is_err());
        assert!(normalize_pids_limit(Some(-1)).is_err());
    }

    #[test]
    fn container_name_includes_job_and_attempt() {
        let job = test_job(json!({}), json!({ "image": "runner:latest" }));

        assert_eq!(
            container_name(&job),
            "cmsx-job-00000000-0000-0000-0000-000000000001-attempt-2"
        );
    }
}
