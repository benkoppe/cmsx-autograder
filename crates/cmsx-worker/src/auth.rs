use anyhow::{Context, Result};
use jwt_simple::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const WORKER_AUDIENCE: &str = "cmsx-control-plane";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerClaims {
    iss: String,
    sub: Uuid,
    aud: String,
    jti: Uuid,
    method: String,
    path: String,
    body_sha256: String,
}

#[derive(Clone)]
pub struct WorkerSigner {
    worker_id: Uuid,
    key: Ed25519KeyPair,
}

impl WorkerSigner {
    pub fn from_pem(worker_id: Uuid, pem: &str) -> Result<Self> {
        let key = Ed25519KeyPair::from_pem(pem)
            .context("invalid worker private key")?
            .with_key_id(&worker_id.to_string());
        Ok(Self { worker_id, key })
    }

    pub fn authorization_header(&self, method: &str, path: &str, body: &[u8]) -> Result<String> {
        let body_sha256 = hex::encode(Sha256::digest(body));
        let custom = WorkerClaims {
            iss: format!("worker:{}", self.worker_id),
            sub: self.worker_id,
            aud: WORKER_AUDIENCE.to_string(),
            jti: Uuid::now_v7(),
            method: method.to_string(),
            path: path.to_string(),
            body_sha256,
        };

        let claims =
            Claims::with_custom_claims(custom, jwt_simple::prelude::Duration::from_secs(30))
                .with_audience(WORKER_AUDIENCE)
                .with_subject(self.worker_id.to_string());

        let token = self.key.sign(claims).context("failed to sign worker jwt")?;
        Ok(format!("WorkerJWT {token}"))
    }
}
