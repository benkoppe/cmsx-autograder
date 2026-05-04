use anyhow::{Context, Result};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use std::{path::Path, str::FromStr};

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    ensure_sqlite_parent_dir(database_url)?;

    let options = SqliteConnectOptions::from_str(database_url)
        .context("failed to parse DATABASE_URL")?
        .create_if_missing(true)
        .foreign_keys(true);

    let pool = SqlitePool::connect_with(options)
        .await
        .context("failed to connect to SQLite database")?;

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("failed to run database migrations")?;

    Ok(pool)
}

fn ensure_sqlite_parent_dir(database_url: &str) -> Result<()> {
    let Some(path) = database_url.strip_prefix("sqlite://") else {
        return Ok(());
    };

    if path == ":memory:" {
        return Ok(());
    }

    let path = Path::new(path);

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create SQLite database parent directory {}",
                parent.display()
            )
        })?;
    }

    Ok(())
}
