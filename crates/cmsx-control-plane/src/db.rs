use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::str::FromStr;

pub async fn connect(database_url: &str) -> Result<PgPool> {
    let options =
        PgConnectOptions::from_str(database_url).context("failed to parse CMSX_DATABASE_URL")?;

    let pool = PgPool::connect_with(options)
        .await
        .context("failed to connect to Postgres database")?;

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("failed to run database migrations")?;

    Ok(pool)
}
