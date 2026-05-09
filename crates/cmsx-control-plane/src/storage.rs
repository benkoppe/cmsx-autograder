use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path,
};
use uuid::Uuid;

use crate::config::StorageConfig;

#[derive(Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

pub struct SubmissionFileUpload {
    key: String,
    upload: Box<dyn MultipartUpload>,
}

impl Storage {
    pub fn from_config(config: &StorageConfig) -> Result<Self> {
        match config {
            StorageConfig::Local { root, prefix } => {
                let store = LocalFileSystem::new_with_prefix(root)
                    .context("failed to initialize local storage")?;

                Ok(Self {
                    store: Arc::new(store),
                    prefix: normalize_prefix(prefix),
                })
            }
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                prefix,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key)
                    .with_allow_http(*allow_http)
                    .with_virtual_hosted_style_request(false);

                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }

                let store = builder
                    .build()
                    .context("failed to initialize S3-compatible storage")?;

                Ok(Self {
                    store: Arc::new(store),
                    prefix: normalize_prefix(prefix),
                })
            }
        }
    }

    pub async fn begin_submission_file_upload(
        &self,
        submission_id: Uuid,
        submission_file_id: Uuid,
    ) -> Result<SubmissionFileUpload> {
        let key = self.submission_file_key(submission_id, submission_file_id);
        let path = Path::from(key.clone());

        let upload = self
            .store
            .as_ref()
            .put_multipart(&path)
            .await
            .context("failed to begin submission file upload")?;

        Ok(SubmissionFileUpload { key, upload })
    }

    pub fn submission_file_key(&self, submission_id: Uuid, submission_file_id: Uuid) -> String {
        self.key(format!("submissions/{submission_id}/{submission_file_id}"))
    }

    fn key(&self, suffix: String) -> String {
        if self.prefix.is_empty() {
            suffix
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    pub async fn get(&self, key: &str) -> Result<Bytes> {
        let bytes = self
            .store
            .as_ref()
            .get(&Path::from(key))
            .await
            .context("failed to get stored object")?
            .bytes()
            .await
            .context("failed to read stored object")?;

        Ok(bytes)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.store
            .as_ref()
            .delete(&Path::from(key))
            .await
            .context("failed to delete stored object")?;

        Ok(())
    }
}

impl SubmissionFileUpload {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn put_part(&mut self, bytes: Bytes) -> Result<()> {
        self.upload
            .put_part(PutPayload::from_bytes(bytes))
            .await
            .context("failed to upload submission file part")?;

        Ok(())
    }

    pub async fn complete(&mut self) -> Result<()> {
        self.upload
            .complete()
            .await
            .context("failed to complete submission file upload")?;

        Ok(())
    }

    pub async fn abort(&mut self) -> Result<()> {
        self.upload
            .abort()
            .await
            .context("failed to abort submission file upload")?;

        Ok(())
    }
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}
