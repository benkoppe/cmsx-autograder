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
    pub max_jobs: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InWorkerExecutorConfig {
    pub workspace_root: PathBuf,
    pub max_jobs: Option<usize>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            control_plane_url: "http://127.0.0.1:3000".to_string(),
            private_key_base64: String::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            executor: ExecutorConfig::InWorker(InWorkerExecutorConfig {
                workspace_root: PathBuf::from("data/worker"),
                max_jobs: None,
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
}
