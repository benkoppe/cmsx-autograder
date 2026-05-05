use anyhow::{Context, Result};
use std::{env, net::SocketAddr};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("CMSX_BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse()
            .context("failed to parse CMSX_BIND_ADDR")?;

        let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;

        Ok(Self {
            bind_addr,
            database_url,
        })
    }
}
