use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub worker_id: Uuid,
    pub worker_name: String,
    pub control_plane_url: String,
    pub private_key_pem: String,
    pub version: String,
    pub executor_backends: Vec<String>,
    pub runner_images: Vec<String>,
    pub max_jobs: Option<usize>,
    pub workspace_root: PathBuf,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: Uuid::nil(),
            worker_name: "worker".to_string(),
            control_plane_url: "http://127.0.0.1:3000".to_string(),
            private_key_pem: String::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            executor_backends: vec!["in-worker".to_string()],
            runner_images: vec!["python".to_string()],
            max_jobs: None,
            workspace_root: PathBuf::from("data/worker"),
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
        if self.worker_id.is_nil() {
            bail!("worker_id must be set");
        }
        if self.worker_name.trim().is_empty() {
            bail!("worker_name must be set");
        }
        if self.control_plane_url.trim().is_empty() {
            bail!("control_plane_url must be set");
        }
        if self.private_key_pem.trim().is_empty() {
            bail!("private_key_pem must be set");
        }
        if self.executor_backends.is_empty() {
            bail!("at least one executor backend must be configured");
        }
        if self.runner_images.is_empty() {
            bail!("at least one runner image must be configured");
        }
        Ok(())
    }
}
