use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use jwt_simple::prelude::*;
use sha2::{Digest, Sha256};

use cmsx_core::{
    WorkerAuthClaims,
    protocol::{WORKER_AUTH_SCHEME, WORKER_JWT_AUDIENCE, WORKER_JWT_VALIDITY_SECONDS},
};

#[derive(Clone)]
pub struct WorkerSigner {
    fingerprint: String,
    key: Ed25519KeyPair,
}

impl WorkerSigner {
    pub fn from_base64_pem(private_key_base64: &str) -> Result<Self> {
        let pem_bytes = STANDARD
            .decode(private_key_base64.trim())
            .context("invalid base64 worker private key")?;

        let pem = String::from_utf8(pem_bytes).context("worker private key is not valid UTF-8")?;

        let key = Ed25519KeyPair::from_pem(&pem).context("invalid worker private key")?;
        let public_key = key.public_key();
        let fingerprint = public_key_fingerprint(&public_key);
        let key = key.with_key_id(&fingerprint);

        Ok(Self { fingerprint, key })
    }

    pub fn authorization_header(&self, method: &str, path: &str, body: &[u8]) -> Result<String> {
        let body_sha256 = hex::encode(Sha256::digest(body));
        let issuer = format!("worker-key:{}", self.fingerprint);
        let jti = uuid::Uuid::now_v7();

        let custom = WorkerAuthClaims {
            method: method.to_string(),
            path: path.to_string(),
            body_sha256,
        };

        let claims = Claims::with_custom_claims(
            custom,
            jwt_simple::prelude::Duration::from_secs(WORKER_JWT_VALIDITY_SECONDS),
        )
        .with_audience(WORKER_JWT_AUDIENCE)
        .with_issuer(issuer)
        .with_jwt_id(jti.to_string());

        let token = self.key.sign(claims).context("failed to sign worker jwt")?;
        Ok(format!("{WORKER_AUTH_SCHEME} {token}"))
    }
}

fn public_key_fingerprint(public_key: &Ed25519PublicKey) -> String {
    let pem = public_key.to_pem();
    hex::encode(Sha256::digest(pem.as_bytes()))
}
