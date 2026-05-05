use std::collections::HashMap;

use argon2::{
    Argon2, PasswordVerifier,
    password_hash::{Error as PasswordHashError, PasswordHash},
};
use axum::{
    Json,
    extract::{Multipart, Path, State, multipart::Field},
};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::{app::AppState, error::ApiError};

#[derive(Debug, Serialize)]
pub struct SubmissionResponse {
    pub submission_id: Uuid,
    pub job_id: Uuid,
    pub status: &'static str,
}

#[derive(Debug, Default)]
struct CmsxSubmission {
    fields: HashMap<String, String>,
    files: Vec<StoredSubmissionFile>,
}

const UPLOAD_PART_BYTES: usize = 8 * 1024 * 1024;

pub async fn submit(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    multipart: Multipart,
) -> Result<Json<SubmissionResponse>, ApiError> {
    let assignment = sqlx::query!(
        r#"
        SELECT id
        FROM assignments
        WHERE slug = $1
        LIMIT 1
        "#,
        slug
    )
    .fetch_optional(&state.db)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("assignment not found"))?;

    let token_rows = sqlx::query!(
        r#"
        SELECT token_hash
        FROM assignment_tokens
        WHERE assignment_id = $1
          AND revoked_at IS NULL
        "#,
        assignment.id
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    let token_hashes: Vec<String> = token_rows.into_iter().map(|row| row.token_hash).collect();

    let submission_id = Uuid::now_v7();
    let parsed = parse_multipart(&state, submission_id, multipart, &token_hashes).await?;

    let job_id = Uuid::now_v7();
    let now = Utc::now();

    let cmsx_assignment_id = parsed.fields.get("assignment_id").cloned();
    let cmsx_assignment_name = parsed.fields.get("assignment_name").cloned();
    let cmsx_group_id = parsed.fields.get("group_id").cloned();
    let netids_raw = parsed.fields.get("netids").cloned().unwrap_or_default();
    let netids_json = parse_netids_json(&netids_raw);
    let raw_metadata = raw_metadata_json(&parsed.fields);
    let file_metadata = file_metadata(&parsed.fields);

    let mut stored_files = parsed.files;

    for file in &mut stored_files {
        file.problem_name = file_metadata
            .get(&file.field_name)
            .and_then(|metadata| metadata.problem_name.clone());
    }

    let db_result: Result<(), sqlx::Error> = async {
        let mut tx = state.db.begin().await?;

        sqlx::query!(
            r#"
        INSERT INTO submissions (
            id,
            assignment_id,
            cmsx_group_id,
            cmsx_assignment_id,
            cmsx_assignment_name,
            netids_raw,
            netids_json,
            received_at,
            raw_metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
            submission_id,
            assignment.id,
            cmsx_group_id,
            cmsx_assignment_id,
            cmsx_assignment_name,
            netids_raw,
            SqlxJson(netids_json) as _,
            now,
            SqlxJson(raw_metadata) as _
        )
        .execute(&mut *tx)
        .await?;

        for file in &stored_files {
            sqlx::query!(
                r#"
            INSERT INTO submission_files (
                id,
                submission_id,
                problem_name,
                cmsx_file_field_name,
                original_filename,
                storage_path,
                content_sha256,
                size_bytes,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
                file.id,
                submission_id,
                file.problem_name,
                file.field_name,
                file.original_filename,
                file.storage_key,
                file.content_sha256,
                file.size_bytes,
                now
            )
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query!(
            r#"
        INSERT INTO grading_jobs (
            id,
            submission_id,
            assignment_id,
            status,
            attempts,
            queued_at
        )
        VALUES ($1, $2, $3, 'queued', 0, $4)
        "#,
            job_id,
            submission_id,
            assignment.id,
            now
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }
    .await;

    if let Err(error) = db_result {
        cleanup_stored_files(&state, &stored_files).await;
        return Err(ApiError::internal(error));
    }

    Ok(Json(SubmissionResponse {
        submission_id,
        job_id,
        status: "queued",
    }))
}

#[derive(Debug)]
struct StoredSubmissionFile {
    id: Uuid,
    problem_name: Option<String>,
    field_name: String,
    original_filename: String,
    storage_key: String,
    content_sha256: String,
    size_bytes: i64,
}

async fn parse_multipart(
    state: &AppState,
    submission_id: Uuid,
    mut multipart: Multipart,
    token_hashes: &[String],
) -> Result<CmsxSubmission, ApiError> {
    let mut parsed = CmsxSubmission::default();
    let mut authorized = false;

    loop {
        let next_field = match multipart.next_field().await {
            Ok(next_field) => next_field,
            Err(error) => {
                cleanup_stored_files(state, &parsed.files).await;
                return Err(ApiError::internal(error));
            }
        };

        let Some(mut field) = next_field else {
            break;
        };

        let Some(name) = field.name().map(str::to_owned) else {
            continue;
        };

        if let Some(filename) = field.file_name().map(str::to_owned) {
            if !authorized {
                cleanup_stored_files(state, &parsed.files).await;
                return Err(ApiError::bad_request(
                    "auth_token must appear before uploaded files",
                ));
            }

            match stream_submission_file(state, submission_id, name, filename, &mut field).await {
                Ok(file) => parsed.files.push(file),
                Err(error) => {
                    cleanup_stored_files(state, &parsed.files).await;
                    return Err(error);
                }
            }
        } else {
            let value = match field.text().await {
                Ok(value) => value,
                Err(error) => {
                    cleanup_stored_files(state, &parsed.files).await;
                    return Err(ApiError::internal(error));
                }
            };

            if name == "auth_token" {
                authorized = verify_any_token(&value, token_hashes)?;

                if !authorized {
                    cleanup_stored_files(state, &parsed.files).await;
                    return Err(ApiError::unauthorized("invalid auth_token"));
                }
            }

            parsed.fields.insert(name, value);
        }
    }

    if !authorized {
        cleanup_stored_files(state, &parsed.files).await;
        return Err(ApiError::bad_request("missing auth_token"));
    }

    if let Some(num_files) = parsed.fields.get("num_files") {
        let expected = match num_files.parse::<usize>() {
            Ok(expected) => expected,
            Err(_) => {
                cleanup_stored_files(state, &parsed.files).await;
                return Err(ApiError::bad_request("num_files must be an integer"));
            }
        };

        if expected != parsed.files.len() {
            cleanup_stored_files(state, &parsed.files).await;
            return Err(ApiError::bad_request(
                "num_files does not match uploaded file count",
            ));
        }
    }

    Ok(parsed)
}

async fn stream_submission_file(
    state: &AppState,
    submission_id: Uuid,
    field_name: String,
    original_filename: String,
    field: &mut Field<'_>,
) -> Result<StoredSubmissionFile, ApiError> {
    let submission_file_id = Uuid::now_v7();

    let mut upload = state
        .storage
        .begin_submission_file_upload(submission_id, submission_file_id)
        .await
        .map_err(ApiError::internal)?;

    let storage_key = upload.key().to_string();
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_i64;
    let mut part = BytesMut::with_capacity(UPLOAD_PART_BYTES);

    loop {
        let chunk = match field.chunk().await {
            Ok(chunk) => chunk,
            Err(error) => {
                abort_upload(upload).await;
                return Err(ApiError::internal(error));
            }
        };

        let Some(chunk) = chunk else {
            break;
        };

        hasher.update(&chunk);

        let chunk_len = i64::try_from(chunk.len())
            .map_err(|error| ApiError::internal(format!("uploaded file is too large: {error}")))?;

        size_bytes = size_bytes
            .checked_add(chunk_len)
            .ok_or_else(|| ApiError::internal("uploaded file is too large"))?;

        part.extend_from_slice(&chunk);

        while part.len() >= UPLOAD_PART_BYTES {
            let bytes = part.split_to(UPLOAD_PART_BYTES).freeze();

            if let Err(error) = upload.put_part(bytes).await {
                abort_upload(upload).await;
                return Err(ApiError::internal(error));
            }
        }
    }

    if !part.is_empty()
        && let Err(error) = upload.put_part(part.freeze()).await
    {
        abort_upload(upload).await;
        return Err(ApiError::internal(error));
    }

    if size_bytes == 0
        && let Err(error) = upload.put_part(Bytes::new()).await
    {
        abort_upload(upload).await;
        return Err(ApiError::internal(error));
    }

    if let Err(error) = upload.complete().await {
        abort_upload(upload).await;
        return Err(ApiError::internal(error));
    }

    Ok(StoredSubmissionFile {
        id: submission_file_id,
        problem_name: None,
        field_name,
        original_filename,
        storage_key,
        content_sha256: hex::encode(hasher.finalize()),
        size_bytes,
    })
}

async fn abort_upload(mut upload: crate::storage::SubmissionFileUpload) {
    if let Err(error) = upload.abort().await {
        tracing::warn!(error = %error, "failed to abort submission file upload");
    }
}

async fn cleanup_stored_files(state: &AppState, stored_files: &[StoredSubmissionFile]) {
    for file in stored_files {
        if let Err(error) = state.storage.delete(&file.storage_key).await {
            tracing::warn!(
                error = %error,
                storage_key = %file.storage_key,
                "failed to clean up stored submission file"
            );
        }
    }
}

fn verify_any_token(token: &str, token_hashes: &[String]) -> Result<bool, ApiError> {
    for token_hash in token_hashes {
        if verify_token(token, token_hash)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn verify_token(token: &str, token_hash: &str) -> Result<bool, ApiError> {
    let parsed_hash = PasswordHash::new(token_hash).map_err(map_password_hash_error)?;

    Ok(Argon2::default()
        .verify_password(token.as_bytes(), &parsed_hash)
        .is_ok())
}

fn map_password_hash_error(error: PasswordHashError) -> ApiError {
    match error {
        PasswordHashError::Password => ApiError::unauthorized("invalid auth_token"),
        _ => ApiError::internal(error),
    }
}

fn raw_metadata_json(fields: &HashMap<String, String>) -> Value {
    let mut raw = Map::new();

    for (key, value) in fields {
        if key != "auth_token" {
            raw.insert(key.clone(), Value::String(value.clone()));
        }
    }

    Value::Object(raw)
}

fn parse_netids_json(netids_raw: &str) -> Option<Value> {
    let netids: Vec<Value> = netids_raw
        .split(|ch: char| ch == '_' || ch == ',' || ch == ';' || ch.is_whitespace())
        .map(str::trim)
        .filter(|netid| !netid.is_empty())
        .map(|netid| Value::String(netid.to_string()))
        .collect();

    if netids.is_empty() {
        None
    } else {
        Some(Value::Array(netids))
    }
}

#[derive(Debug, Clone)]
struct CmsxFileMetadata {
    problem_name: Option<String>,
}

fn file_metadata(fields: &HashMap<String, String>) -> HashMap<String, CmsxFileMetadata> {
    let mut by_field_name = HashMap::new();

    for (key, file_field_name) in fields {
        let Some(index) = key.strip_prefix("file_name_") else {
            continue;
        };

        let problem_name = fields.get(&format!("problem_name_{index}")).cloned();

        by_field_name.insert(file_field_name.clone(), CmsxFileMetadata { problem_name });
    }

    by_field_name
}
