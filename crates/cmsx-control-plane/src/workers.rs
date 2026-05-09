use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::Utc;
use jwt_simple::prelude::*;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use cmsx_core::WorkerStatus;

pub struct ProvisionedWorker {
    pub worker_id: Uuid,
    pub key_id: Uuid,
    pub name: String,
    pub private_key_base64: String,
    pub public_key_fingerprint: String,
}

pub async fn provision_worker(db: &PgPool, name: &str) -> Result<ProvisionedWorker> {
    let worker_id = Uuid::now_v7();
    let key_id = Uuid::now_v7();
    let now = Utc::now();

    let key_pair = Ed25519KeyPair::generate();
    let private_key_pem = key_pair.to_pem();
    let private_key_base64 = STANDARD.encode(private_key_pem.as_bytes());
    let public_key = key_pair.public_key();
    let public_key_pem = public_key.to_pem();
    let public_key_fingerprint = public_key_fingerprint(&public_key_pem);

    let mut tx = db.begin().await.context("failed to begin transaction")?;
    let offline = WorkerStatus::Offline.as_str();

    sqlx::query!(
        r#"
        INSERT INTO workers (
            id,
            name,
            status,
            version,
            created_at,
            last_seen_at
        )
        VALUES ($1, $2, $3, NULL, $4, NULL)
        "#,
        worker_id,
        name,
        offline,
        now
    )
    .execute(&mut *tx)
    .await
    .context("failed to insert worker")?;

    sqlx::query!(
        r#"
        INSERT INTO worker_keys (
            id,
            worker_id,
            public_key,
            public_key_fingerprint,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5)
        "#,
        key_id,
        worker_id,
        public_key_pem,
        public_key_fingerprint,
        now
    )
    .execute(&mut *tx)
    .await
    .context("failed to insert worker key")?;

    tx.commit()
        .await
        .context("failed to commit worker provisioning")?;

    Ok(ProvisionedWorker {
        worker_id,
        key_id,
        name: name.to_string(),
        private_key_base64,
        public_key_fingerprint,
    })
}

pub fn public_key_fingerprint(public_key_pem: &str) -> String {
    hex::encode(Sha256::digest(public_key_pem.as_bytes()))
}
