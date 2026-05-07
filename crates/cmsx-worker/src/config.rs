use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub control_plane_url: String,
    pub private_key_base64: String,
    pub version: String,
    pub executor: ExecutorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case")]
pub enum ExecutorConfig {
    DockerSocket(DockerSocketExecutorConfig),
    InWorker(InWorkerExecutorConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerSocketExecutorConfig {
    pub workspace_root: PathBuf,
    pub grader_root: PathBuf,
    pub max_jobs: Option<usize>,
    pub keep_workspaces: bool,
    pub docker_host: Option<String>,

    pub default_image: String,
    pub default_timeout_seconds: Option<u64>,
    pub default_memory_mb: Option<i64>,
    pub default_cpus: Option<f64>,
    pub default_pids_limit: Option<i64>,
    pub default_network: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InWorkerExecutorConfig {
    pub workspace_root: PathBuf,
    pub grader_root: PathBuf,
    pub max_jobs: Option<usize>,
    pub keep_workspaces: bool,
    pub python_command: Option<String>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            control_plane_url: "http://127.0.0.1:3000".to_string(),
            private_key_base64: String::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            executor: ExecutorConfig::InWorker(InWorkerExecutorConfig {
                workspace_root: PathBuf::from("data/worker"),
                grader_root: PathBuf::from("examples/assignments"),
                max_jobs: None,
                keep_workspaces: false,
                python_command: Some("python3".to_string()),
            }),
        }
    }
}

impl WorkerConfig {
    pub fn load() -> Result<Self> {
        let config_path =
            std::env::var("CMSX_WORKER_CONFIG").unwrap_or_else(|_| "cmsx-worker.toml".to_string());

        let config: Self = Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file(config_path))
            .merge(Env::prefixed("CMSX_WORKER_").split("__"))
            .extract()
            .context("failed to load worker config")?;

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.control_plane_url.trim().is_empty() {
            bail!("control_plane_url must be set");
        }
        if self.private_key_base64.trim().is_empty() {
            bail!("private_key_base64 must be set");
        }

        self.executor.validate()?;

        Ok(())
    }
}

impl ExecutorConfig {
    pub fn max_jobs(&self) -> Option<usize> {
        match self {
            Self::DockerSocket(config) => config.max_jobs,
            Self::InWorker(config) => config.max_jobs,
        }
    }

    pub fn workspace_root(&self) -> &Path {
        match self {
            Self::DockerSocket(config) => &config.workspace_root,
            Self::InWorker(config) => &config.workspace_root,
        }
    }

    pub fn grader_root(&self) -> &Path {
        match self {
            Self::DockerSocket(config) => &config.grader_root,
            Self::InWorker(config) => &config.grader_root,
        }
    }

    pub fn keep_workspaces(&self) -> bool {
        match self {
            Self::DockerSocket(config) => config.keep_workspaces,
            Self::InWorker(config) => config.keep_workspaces,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.workspace_root().as_os_str().is_empty() {
            bail!("executor.workspace_root must be set");
        }
        if self.grader_root().as_os_str().is_empty() {
            bail!("executor.grader_root must be set");
        }
        match self {
            Self::DockerSocket(config) => config.validate()?,
            Self::InWorker(config) => config.validate()?,
        }

        Ok(())
    }
}

impl DockerSocketExecutorConfig {
    fn validate(&self) -> Result<()> {
        if matches!(self.docker_host.as_deref(), Some(host) if host.trim().is_empty()) {
            bail!("executor.docker_host must not be empty when set");
        }
        if self.default_image.trim().is_empty() {
            bail!("executor.default_image must not be empty");
        }
        if matches!(self.default_timeout_seconds, Some(0)) {
            bail!("executor.default_timeout_seconds must be positive");
        }
        if matches!(self.default_memory_mb, Some(memory_mb) if memory_mb <= 0) {
            bail!("executor.default_memory_mb must be positive");
        }
        if matches!(self.default_cpus, Some(cpus) if !cpus.is_finite() || cpus <= 0.0) {
            bail!("executor.default_cpus must be a positive finite number");
        }
        if matches!(self.default_pids_limit, Some(pids_limit) if pids_limit <= 0) {
            bail!("executor.default_pids_limit must be positive");
        }

        Ok(())
    }
}

impl InWorkerExecutorConfig {
    pub fn python_command(&self) -> &str {
        self.python_command.as_deref().unwrap_or("python3")
    }

    fn validate(&self) -> Result<()> {
        if self.python_command().trim().is_empty() {
            bail!("executor.python_command must not be empty");
        }

        Ok(())
    }
}
