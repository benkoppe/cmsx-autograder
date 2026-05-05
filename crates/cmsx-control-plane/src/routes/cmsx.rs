use std::collections::{HashMap, HashSet};

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

#[derive(Debug)]
struct AssignmentTokenHash {
    id: Uuid,
    token_hash: String,
}

const UPLOAD_PART_BYTES: usize = 8 * 1024 * 1024;
const CMSX_AUTH_TOKEN_MAX_BYTES: usize = 36;

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
        SELECT id, token_hash
        FROM assignment_tokens
        WHERE assignment_id = $1
          AND revoked_at IS NULL
        "#,
        assignment.id
    )
    .fetch_all(&state.db)
    .await
    .map_err(ApiError::internal)?;

    let token_hashes: Vec<AssignmentTokenHash> = token_rows
        .into_iter()
        .map(|row| AssignmentTokenHash {
            id: row.id,
            token_hash: row.token_hash,
        })
        .collect();

    let submission_id = Uuid::now_v7();
    let parsed = parse_multipart(&state, submission_id, multipart, &token_hashes).await?;

    let job_id = Uuid::now_v7();
    let now = Utc::now();

    let mut stored_files = parsed.files;

    let metadata =
        match validate_stored_submission(&parsed.fields, &mut stored_files, state.cmsx.max_files) {
            Ok(metadata) => metadata,
            Err(error) => {
                cleanup_stored_files(&state, &stored_files).await;
                return Err(error);
            }
        };

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
            metadata.cmsx_group_id,
            metadata.cmsx_assignment_id,
            metadata.cmsx_assignment_name,
            metadata.netids_raw,
            SqlxJson(metadata.netids_json) as _,
            now,
            SqlxJson(metadata.raw_metadata) as _
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
                safe_filename,
                storage_path,
                content_sha256,
                size_bytes,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
                file.id,
                submission_id,
                file.problem_name,
                file.field_name,
                file.original_filename,
                file.safe_filename
                    .as_ref()
                    .expect("validated submission files should have safe filenames"),
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
    safe_filename: Option<String>,
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
    token_hashes: &[AssignmentTokenHash],
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
            // CMSX's autograder guide lists auth_token before uploaded file parts.
            // We intentionally require that order for now so unauthenticated requests
            // never write to object storage. Revisit after testing against real CMSX.
            if !authorized {
                cleanup_stored_files(state, &parsed.files).await;
                return Err(ApiError::bad_request(
                    "auth_token must appear before uploaded file parts",
                ));
            }

            if parsed.files.len() >= state.cmsx.max_files {
                cleanup_stored_files(state, &parsed.files).await;
                return Err(ApiError::payload_too_large(format!(
                    "too many uploaded files; limit is {} files",
                    state.cmsx.max_files
                )));
            }

            match stream_submission_file(
                state,
                submission_id,
                name,
                filename,
                &mut field,
                state.cmsx.max_file_bytes,
            )
            .await
            {
                Ok(file) => parsed.files.push(file),
                Err(error) => {
                    cleanup_stored_files(state, &parsed.files).await;
                    return Err(error);
                }
            }
        } else {
            let value = match read_text_field_limited(field, state.cmsx.max_field_bytes).await {
                Ok(value) => value,
                Err(error) => {
                    cleanup_stored_files(state, &parsed.files).await;
                    return Err(error);
                }
            };

            if name == "auth_token" {
                validate_auth_token_shape(&value)?;

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

    Ok(parsed)
}

async fn stream_submission_file(
    state: &AppState,
    submission_id: Uuid,
    field_name: String,
    original_filename: String,
    field: &mut Field<'_>,
    max_file_bytes: i64,
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
            .ok_or_else(|| file_too_large_error(max_file_bytes))?;

        if size_bytes > max_file_bytes {
            abort_upload(upload).await;
            return Err(file_too_large_error(max_file_bytes));
        }

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
        safe_filename: None,
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

fn verify_any_token(token: &str, token_hashes: &[AssignmentTokenHash]) -> Result<bool, ApiError> {
    let mut usable_hashes = 0_usize;

    for token_hash in token_hashes {
        let parsed_hash = match PasswordHash::new(&token_hash.token_hash) {
            Ok(parsed_hash) => parsed_hash,
            Err(error) => {
                tracing::warn!(
                    token_id = %token_hash.id,
                    error = %error,
                    "ignoring malformed assignment token hash"
                );
                continue;
            }
        };

        usable_hashes += 1;

        match Argon2::default().verify_password(token.as_bytes(), &parsed_hash) {
            Ok(()) => return Ok(true),
            Err(PasswordHashError::Password) => {}
            Err(error) => {
                tracing::warn!(
                    token_id = %token_hash.id,
                    error = %error,
                    "ignoring unusable assignment token hash"
                );
            }
        }
    }

    if usable_hashes == 0 {
        return Err(ApiError::internal("assignment has no usable token hashes"));
    }

    Ok(false)
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

async fn read_text_field_limited(
    mut field: Field<'_>,
    max_bytes: usize,
) -> Result<String, ApiError> {
    let mut bytes = BytesMut::new();

    loop {
        let chunk = field.chunk().await.map_err(ApiError::internal)?;

        let Some(chunk) = chunk else {
            break;
        };

        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(ApiError::payload_too_large(format!(
                "multipart field is too large; limit is {}",
                human_readable_bytes(max_bytes)
            )));
        }

        bytes.extend_from_slice(&chunk);
    }

    String::from_utf8(bytes.to_vec())
        .map_err(|_| ApiError::bad_request("multipart text field is not valid UTF-8"))
}

fn required_field<'a>(
    fields: &'a HashMap<String, String>,
    name: &str,
) -> Result<&'a str, ApiError> {
    fields
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("missing {name}")))
}

#[derive(Debug)]
struct ValidatedSubmissionMetadata {
    cmsx_assignment_id: String,
    cmsx_assignment_name: String,
    cmsx_group_id: String,
    netids_raw: String,
    netids_json: Option<Value>,
    raw_metadata: Value,
}

fn validate_stored_submission(
    fields: &HashMap<String, String>,
    stored_files: &mut [StoredSubmissionFile],
    max_files: usize,
) -> Result<ValidatedSubmissionMetadata, ApiError> {
    let file_metadata = validate_cmsx_metadata(fields, stored_files, max_files)?;

    for file in stored_files {
        let metadata = file_metadata
            .get(&file.field_name)
            .ok_or_else(|| ApiError::bad_request("missing uploaded file metadata"))?;

        file.problem_name = Some(metadata.problem_name.clone());
        file.safe_filename = Some(metadata.safe_filename.clone());
    }

    let cmsx_assignment_id = required_field(fields, "assignment_id")?.to_string();
    let cmsx_assignment_name = required_field(fields, "assignment_name")?.to_string();
    let cmsx_group_id = required_field(fields, "group_id")?.to_string();
    let netids_raw = required_field(fields, "netids")?.to_string();

    Ok(ValidatedSubmissionMetadata {
        cmsx_assignment_id,
        cmsx_assignment_name,
        cmsx_group_id,
        netids_json: parse_netids_json(&netids_raw),
        netids_raw,
        raw_metadata: raw_metadata_json(fields),
    })
}

#[derive(Debug, Clone)]
struct CmsxFileMetadata {
    problem_name: String,
    safe_filename: String,
}

fn validate_cmsx_metadata(
    fields: &HashMap<String, String>,
    files: &[StoredSubmissionFile],
    max_files: usize,
) -> Result<HashMap<String, CmsxFileMetadata>, ApiError> {
    required_field(fields, "netids")?;
    required_field(fields, "group_id")?;
    required_field(fields, "assignment_id")?;
    required_field(fields, "assignment_name")?;

    let num_files = required_field(fields, "num_files")?
        .parse::<usize>()
        .map_err(|_| ApiError::bad_request("num_files must be an integer"))?;

    if num_files > max_files {
        return Err(ApiError::payload_too_large(format!(
            "too many uploaded files; limit is {max_files} files"
        )));
    }

    if files.len() != num_files {
        return Err(ApiError::bad_request(
            "num_files does not match uploaded file count",
        ));
    }

    let mut by_field_name = HashMap::with_capacity(num_files);
    let mut expected_field_names = HashSet::with_capacity(num_files);
    let mut safe_filenames = HashSet::with_capacity(num_files);

    for index in 0..num_files {
        let problem_name_key = format!("problem_name_{index}");
        let file_name_key = format!("file_name_{index}");

        let problem_name = required_field(fields, &problem_name_key)?.to_string();
        let field_name = required_field(fields, &file_name_key)?.to_string();
        let safe_filename = safe_submission_filename(&field_name)?;

        if !expected_field_names.insert(field_name.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate uploaded file field name {field_name}"
            )));
        }

        if !safe_filenames.insert(safe_filename.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate uploaded filename after sanitization: {safe_filename}"
            )));
        }

        by_field_name.insert(
            field_name,
            CmsxFileMetadata {
                problem_name,
                safe_filename,
            },
        );
    }

    let mut seen_uploaded_field_names = HashSet::with_capacity(files.len());

    for file in files {
        if !seen_uploaded_field_names.insert(file.field_name.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate uploaded file part {}",
                file.field_name
            )));
        }

        if !expected_field_names.contains(&file.field_name) {
            return Err(ApiError::bad_request(format!(
                "unexpected uploaded file part {}",
                file.field_name
            )));
        }
    }

    Ok(by_field_name)
}

fn safe_submission_filename(filename: &str) -> Result<String, ApiError> {
    let trimmed = filename.trim();

    if trimmed.is_empty() {
        return Err(ApiError::bad_request("uploaded filename must not be empty"));
    }

    if trimmed == "." || trimmed == ".." {
        return Err(ApiError::bad_request(
            "uploaded filename must not be . or ..",
        ));
    }

    let sanitized = sanitize_filename::sanitize(trimmed);

    if sanitized != trimmed {
        return Err(ApiError::bad_request(format!(
            "uploaded filename is not safe: {filename}"
        )));
    }

    Ok(sanitized)
}

fn validate_auth_token_shape(token: &str) -> Result<(), ApiError> {
    if token.len() > CMSX_AUTH_TOKEN_MAX_BYTES {
        return Err(ApiError::bad_request(format!(
            "auth_token must be at most {CMSX_AUTH_TOKEN_MAX_BYTES} bytes"
        )));
    }

    Ok(())
}

fn file_too_large_error(max_file_bytes: i64) -> ApiError {
    ApiError::payload_too_large(format!(
        "uploaded file is too large; limit is {}",
        human_readable_bytes(max_file_bytes as usize)
    ))
}

fn human_readable_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = KIB * 1024;
    const GIB: usize = MIB * 1024;

    if bytes >= GIB && bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= KIB && bytes.is_multiple_of(KIB) {
        format!("{} KiB", bytes / KIB)
    } else {
        format!("{bytes} bytes")
    }
}
