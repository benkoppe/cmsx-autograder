use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

const KIB: usize = 1024;
const GIB: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub storage: StorageConfig,
    #[serde(default)]
    pub cmsx: CmsxConfig,
    pub admin: AdminConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminConfig {
    #[serde(default = "default_public_url")]
    pub public_url: String,

    #[serde(default)]
    pub bootstrap_token_hashes: Vec<String>,
}

fn default_public_url() -> String {
    "http://127.0.0.1:3000".to_string()
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            public_url: default_public_url(),
            bootstrap_token_hashes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CmsxConfig {
    pub max_body_bytes: usize,
    pub max_field_bytes: usize,
    pub max_file_bytes: i64,
    pub max_files: usize,
}

impl Default for CmsxConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 8 * GIB,
            max_field_bytes: 64 * KIB,
            max_file_bytes: (2 * GIB) as i64,
            max_files: 512,
        }
    }
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
            cmsx: CmsxConfig::default(),
            admin: AdminConfig::default(),
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

        self.admin.validate().context("invalid admin config")?;
        self.storage.validate().context("invalid storage config")?;
        self.cmsx.validate().context("invalid CMSX config")?;

        Ok(())
    }
}

impl AdminConfig {
    fn validate(&self) -> Result<()> {
        if self.public_url.trim().is_empty() {
            bail!("admin.public_url must not be empty");
        }

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

impl CmsxConfig {
    fn validate(&self) -> Result<()> {
        if self.max_body_bytes == 0 {
            bail!("cmsx.max_body_bytes must be greater than zero");
        }
        if self.max_field_bytes == 0 {
            bail!("cmsx.max_field_bytes must be greater than zero");
        }
        if self.max_file_bytes <= 0 {
            bail!("cmsx.max_file_bytes must be greater than zero");
        }
        if self.max_files == 0 {
            bail!("cmsx.max_files must be greater than zero");
        }
        if self.max_body_bytes < self.max_file_bytes as usize {
            bail!("cmsx.max_body_bytes must be at least cmsx.max_file_bytes");
        }

        Ok(())
    }
}
