use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case")]
pub enum StorageConfig {
    Local {
        root: PathBuf,
        #[serde(default)]
        prefix: String,
    },
    S3 {
        bucket: String,
        region: String,
        #[serde(default)]
        endpoint: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        allow_http: bool,
    },
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3000"
                .parse()
                .expect("default bind addr should parse"),
            database_url: String::new(),
            storage: StorageConfig::Local {
                root: PathBuf::from("data/storage"),
                prefix: String::new(),
            },
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path = env::var("CMSX_CONFIG").unwrap_or_else(|_| "cmsx.toml".to_string());

        let config: Self = Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file(config_path))
            .merge(Env::prefixed("CMSX_").split("__"))
            .extract()
            .context("failed to load configuration")?;

        config.validate()?;

        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.database_url.trim().is_empty() {
            bail!("CMSX_DATABASE_URL must be set");
        }

        self.storage.validate().context("invalid storage config")?;

        Ok(())
    }
}

impl StorageConfig {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Local { root, .. } => {
                if root.as_os_str().is_empty() {
                    bail!("local root must not be empty");
                }
            }
            Self::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                ..
            } => {
                if bucket.trim().is_empty() {
                    bail!("s3 bucket must be set");
                }
                if region.trim().is_empty() {
                    bail!("s3 region must be set");
                }
                if access_key_id.trim().is_empty() {
                    bail!("s3 access_key_id must be set");
                }
                if secret_access_key.trim().is_empty() {
                    bail!("s3 secret_access_key must be set");
                }
            }
        }
        Ok(())
    }
}
