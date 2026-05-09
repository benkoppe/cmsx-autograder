use std::collections::{HashMap, HashSet};

use argon2::{
    Argon2, PasswordVerifier,
    password_hash::{Error as PasswordHashError, PasswordHash},
};
use axum::{
    extract::{Multipart, Path, State, multipart::Field},
    http::StatusCode,
};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::{app::AppState, error::ApiError};

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
const CMSX_AUTH_TOKEN_MAX_CHARS: usize = 36;

pub async fn submit(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    multipart: Multipart,
) -> Result<StatusCode, ApiError> {
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

    Ok(StatusCode::NO_CONTENT)
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
            // CMSX's autograder guide shows auth_token before uploaded file parts.
            // We require that ordering so unauthenticated requests cannot stream file
            // data into object storage before token validation.
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
    if token.chars().count() > CMSX_AUTH_TOKEN_MAX_CHARS {
        return Err(ApiError::bad_request(format!(
            "auth_token must be at most {CMSX_AUTH_TOKEN_MAX_CHARS} characters"
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

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode, header},
        response::Response,
    };
    use serde_json::{Value, json};
    use sqlx::{PgPool, Row, types::Json as SqlxJson};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{config::CmsxConfig, test_support};

    const TEST_SLUG: &str = "intro";
    const TEST_TOKEN: &str = "super_secret";

    struct TestApp {
        app: Router,
        db: PgPool,
        storage_root: TempDir,
        assignment_id: Uuid,
        _test_database: test_support::TestDatabase,
    }

    struct CmsxMultipart {
        boundary: String,
        body: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    struct CmsxFilePart {
        problem_name: String,
        field_name: String,
        upload_field_name: Option<String>,
        filename: String,
        bytes: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    enum MultipartPart {
        Text {
            name: String,
            value: String,
        },
        File {
            name: String,
            filename: String,
            content_type: String,
            bytes: Vec<u8>,
        },
    }

    #[derive(Clone, Debug)]
    struct CmsxSubmissionBuilder {
        token: Option<String>,
        netids: String,
        group_id: String,
        assignment_id: String,
        assignment_name: String,
        explicit_num_files: Option<String>,
        files: Vec<CmsxFilePart>,
        file_parts_before_auth: bool,
        omitted_fields: Vec<&'static str>,
    }

    #[derive(Debug)]
    struct SubmissionRow {
        id: Uuid,
        assignment_id: Uuid,
        cmsx_group_id: String,
        cmsx_assignment_id: String,
        cmsx_assignment_name: String,
        netids_raw: String,
        netids_json: Option<Value>,
        raw_metadata: Value,
    }

    #[derive(Debug)]
    struct SubmissionFileRow {
        problem_name: Option<String>,
        cmsx_file_field_name: String,
        original_filename: String,
        safe_filename: String,
        storage_path: String,
        content_sha256: String,
        size_bytes: i64,
    }

    impl CmsxFilePart {
        fn new(
            problem_name: impl Into<String>,
            field_name: impl Into<String>,
            filename: impl Into<String>,
            bytes: impl Into<Vec<u8>>,
        ) -> Self {
            Self {
                problem_name: problem_name.into(),
                field_name: field_name.into(),
                upload_field_name: None,
                filename: filename.into(),
                bytes: bytes.into(),
            }
        }

        fn with_upload_field_name(mut self, upload_field_name: impl Into<String>) -> Self {
            self.upload_field_name = Some(upload_field_name.into());
            self
        }

        fn upload_field_name(&self) -> &str {
            self.upload_field_name
                .as_deref()
                .unwrap_or(&self.field_name)
        }

        fn to_file_part(&self) -> MultipartPart {
            MultipartPart::File {
                name: self.upload_field_name().to_string(),
                filename: self.filename.clone(),
                content_type: "application/octet-stream".to_string(),
                bytes: self.bytes.clone(),
            }
        }
    }

    impl Default for CmsxSubmissionBuilder {
        fn default() -> Self {
            Self {
                token: Some(TEST_TOKEN.to_string()),
                netids: "acm22_ath55_gn93".to_string(),
                group_id: "341".to_string(),
                assignment_id: "21".to_string(),
                assignment_name: "Assignment 1: Intro to CMSX".to_string(),
                explicit_num_files: None,
                files: vec![CmsxFilePart::new(
                    "Part 1 of the Assignment",
                    "Part_1_of_the_Assignment",
                    "Part_1_of_the_Assignment.py",
                    b"print('hello')\n".to_vec(),
                )],
                file_parts_before_auth: false,
                omitted_fields: Vec::new(),
            }
        }
    }

    impl CmsxSubmissionBuilder {
        fn with_token(mut self, token: impl Into<String>) -> Self {
            self.token = Some(token.into());
            self
        }

        fn without_auth_token(mut self) -> Self {
            self.token = None;
            self
        }

        fn with_files(mut self, files: Vec<CmsxFilePart>) -> Self {
            self.files = files;
            self
        }

        fn with_num_files(mut self, num_files: impl Into<String>) -> Self {
            self.explicit_num_files = Some(num_files.into());
            self
        }

        fn with_file_parts_before_auth(mut self) -> Self {
            self.file_parts_before_auth = true;
            self
        }

        fn without_field(mut self, field: &'static str) -> Self {
            self.omitted_fields.push(field);
            self
        }

        fn includes_field(&self, field: &str) -> bool {
            !self.omitted_fields.contains(&field)
        }

        fn multipart(&self) -> CmsxMultipart {
            let boundary = "cmsx-test-boundary";
            let mut parts = Vec::new();

            if self.file_parts_before_auth
                && let Some(file) = self.files.first()
            {
                parts.push(file.to_file_part());
            }

            if let Some(token) = &self.token
                && self.includes_field("auth_token")
            {
                parts.push(text_part("auth_token", token));
            }

            if self.includes_field("netids") {
                parts.push(text_part("netids", &self.netids));
            }
            if self.includes_field("group_id") {
                parts.push(text_part("group_id", &self.group_id));
            }
            if self.includes_field("assignment_id") {
                parts.push(text_part("assignment_id", &self.assignment_id));
            }
            if self.includes_field("assignment_name") {
                parts.push(text_part("assignment_name", &self.assignment_name));
            }
            if self.includes_field("num_files") {
                parts.push(text_part(
                    "num_files",
                    self.explicit_num_files
                        .as_deref()
                        .unwrap_or(&self.files.len().to_string()),
                ));
            }

            for (index, file) in self.files.iter().enumerate() {
                let problem_name_key = format!("problem_name_{index}");
                let file_name_key = format!("file_name_{index}");

                if self.includes_field(&problem_name_key) {
                    parts.push(text_part(&problem_name_key, &file.problem_name));
                }
                if self.includes_field(&file_name_key) {
                    parts.push(text_part(&file_name_key, &file.field_name));
                }
            }

            for (index, file) in self.files.iter().enumerate() {
                if self.file_parts_before_auth && index == 0 {
                    continue;
                }

                parts.push(file.to_file_part());
            }

            encode_multipart(boundary, &parts)
        }
    }

    fn text_part(name: &str, value: &str) -> MultipartPart {
        MultipartPart::Text {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn encode_multipart(boundary: &str, parts: &[MultipartPart]) -> CmsxMultipart {
        let mut body = Vec::new();

        for part in parts {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());

            match part {
                MultipartPart::Text { name, value } => {
                    body.extend_from_slice(
                        format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n")
                            .as_bytes(),
                    );
                    body.extend_from_slice(value.as_bytes());
                    body.extend_from_slice(b"\r\n");
                }
                MultipartPart::File {
                    name,
                    filename,
                    content_type,
                    bytes,
                } => {
                    body.extend_from_slice(
                        format!(
                            "Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n"
                        )
                        .as_bytes(),
                    );
                    body.extend_from_slice(
                        format!("Content-Type: {content_type}\r\n\r\n").as_bytes(),
                    );
                    body.extend_from_slice(bytes);
                    body.extend_from_slice(b"\r\n");
                }
            }
        }

        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        CmsxMultipart {
            boundary: boundary.to_string(),
            body,
        }
    }

    async fn test_app(configure_cmsx: impl FnOnce(&mut CmsxConfig)) -> TestApp {
        let shared = test_support::test_app_with_cmsx(configure_cmsx).await;

        let assignment_id = insert_assignment(&shared.db).await;
        insert_assignment_token(&shared.db, assignment_id, TEST_TOKEN).await;

        TestApp {
            app: shared.app,
            db: shared.db,
            storage_root: shared.storage_root,
            assignment_id,
            _test_database: shared._test_database,
        }
    }

    async fn insert_assignment(db: &PgPool) -> Uuid {
        let assignment_id = Uuid::now_v7();
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO assignments (
                id,
                slug,
                name,
                max_score,
                execution_config,
                runner_config,
                capabilities,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(assignment_id)
        .bind(TEST_SLUG)
        .bind("Intro Assignment")
        .bind(100.0_f64)
        .bind(SqlxJson(json!({"backend": "in-worker"})))
        .bind(SqlxJson(json!({"environment": "python"})))
        .bind(SqlxJson(json!({"read_files": true})))
        .bind(now)
        .bind(now)
        .execute(db)
        .await
        .expect("failed to insert test assignment");

        assignment_id
    }

    async fn insert_assignment_token(db: &PgPool, assignment_id: Uuid, token: &str) {
        let token_id = Uuid::now_v7();
        let now = Utc::now();
        let token_hash = test_support::hash_test_token(token);

        sqlx::query(
            r#"
            INSERT INTO assignment_tokens (
                id,
                assignment_id,
                token_hash,
                created_at
            )
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(token_id)
        .bind(assignment_id)
        .bind(token_hash)
        .bind(now)
        .execute(db)
        .await
        .expect("failed to insert test assignment token");
    }

    impl TestApp {
        async fn submit(&self, slug: &str, multipart: CmsxMultipart) -> Response {
            let request = Request::builder()
                .method("POST")
                .uri(format!("/cmsx/a/{slug}/submit"))
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={}", multipart.boundary),
                )
                .body(Body::from(multipart.body))
                .expect("failed to build request");

            test_support::request(&self.app, request).await
        }
    }

    async fn response_json(response: Response) -> (StatusCode, Value) {
        test_support::response_json(response).await
    }

    async fn response_body(response: Response) -> (StatusCode, bytes::Bytes) {
        test_support::response_body(response).await
    }

    async fn assert_no_content_success(response: Response) {
        let (status, body) = response_body(response).await;

        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());
    }

    async fn table_count(db: &PgPool, table: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {table}");

        sqlx::query_scalar::<_, i64>(&sql)
            .fetch_one(db)
            .await
            .unwrap_or_else(|error| panic!("failed to count table {table}: {error}"))
    }

    async fn assert_ingestion_counts(db: &PgPool, submissions: i64, files: i64, jobs: i64) {
        assert_eq!(table_count(db, "submissions").await, submissions);
        assert_eq!(table_count(db, "submission_files").await, files);
        assert_eq!(table_count(db, "grading_jobs").await, jobs);
    }

    async fn assert_no_ingestion_writes(db: &PgPool) {
        assert_ingestion_counts(db, 0, 0, 0).await;
    }

    async fn fetch_submission(db: &PgPool) -> SubmissionRow {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                assignment_id,
                cmsx_group_id,
                cmsx_assignment_id,
                cmsx_assignment_name,
                netids_raw,
                netids_json,
                raw_metadata
            FROM submissions
            LIMIT 1
            "#,
        )
        .fetch_one(db)
        .await
        .expect("failed to fetch submission");

        SubmissionRow {
            id: row.try_get("id").expect("missing id"),
            assignment_id: row.try_get("assignment_id").expect("missing assignment_id"),
            cmsx_group_id: row.try_get("cmsx_group_id").expect("missing cmsx_group_id"),
            cmsx_assignment_id: row
                .try_get("cmsx_assignment_id")
                .expect("missing cmsx_assignment_id"),
            cmsx_assignment_name: row
                .try_get("cmsx_assignment_name")
                .expect("missing cmsx_assignment_name"),
            netids_raw: row.try_get("netids_raw").expect("missing netids_raw"),
            netids_json: row.try_get("netids_json").expect("missing netids_json"),
            raw_metadata: row.try_get("raw_metadata").expect("missing raw_metadata"),
        }
    }

    async fn fetch_submission_files(db: &PgPool) -> Vec<SubmissionFileRow> {
        let rows = sqlx::query(
            r#"
            SELECT
                problem_name,
                cmsx_file_field_name,
                original_filename,
                safe_filename,
                storage_path,
                content_sha256,
                size_bytes
            FROM submission_files
            ORDER BY cmsx_file_field_name
            "#,
        )
        .fetch_all(db)
        .await
        .expect("failed to fetch submission files");

        rows.into_iter()
            .map(|row| SubmissionFileRow {
                problem_name: row.try_get("problem_name").expect("missing problem_name"),
                cmsx_file_field_name: row
                    .try_get("cmsx_file_field_name")
                    .expect("missing cmsx_file_field_name"),
                original_filename: row
                    .try_get("original_filename")
                    .expect("missing original_filename"),
                safe_filename: row.try_get("safe_filename").expect("missing safe_filename"),
                storage_path: row.try_get("storage_path").expect("missing storage_path"),
                content_sha256: row
                    .try_get("content_sha256")
                    .expect("missing content_sha256"),
                size_bytes: row.try_get("size_bytes").expect("missing size_bytes"),
            })
            .collect()
    }

    async fn fetch_job_submission_id(db: &PgPool) -> Uuid {
        sqlx::query_scalar::<_, Uuid>("SELECT submission_id FROM grading_jobs LIMIT 1")
            .fetch_one(db)
            .await
            .expect("failed to fetch grading job")
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn assert_storage_has_no_files(root: &Path) {
        let storage_path = root.join("storage");

        if !storage_path.exists() {
            return;
        }

        let file_count = count_regular_files(&storage_path);
        assert_eq!(file_count, 0, "expected storage to contain no files");
    }

    fn count_regular_files(path: &Path) -> usize {
        fs::read_dir(path)
            .unwrap_or_else(|error| panic!("failed to read directory {}: {error}", path.display()))
            .map(|entry| {
                let entry = entry.expect("failed to read directory entry");
                let path = entry.path();
                let metadata = entry
                    .metadata()
                    .unwrap_or_else(|error| panic!("failed to stat {}: {error}", path.display()));

                if metadata.is_dir() {
                    count_regular_files(&path)
                } else if metadata.is_file() {
                    1
                } else {
                    0
                }
            })
            .sum()
    }

    fn documented_sample_files() -> Vec<CmsxFilePart> {
        vec![
            CmsxFilePart::new(
                "Part 1 of the Assignment",
                "Part_1_of_the_Assignment",
                "Part_1_of_the_Assignment.py",
                b"print('part 1')\n".to_vec(),
            ),
            CmsxFilePart::new(
                "Part 2 of the Assignment",
                "Part_2_of_the_Assignment",
                "Part_2_of_the_Assignment.py",
                b"print('part 2')\n".to_vec(),
            ),
            CmsxFilePart::new(
                "Part 3 of the Assignment",
                "Part_3_of_the_Assignment",
                "Part_3_of_the_Assignment.py",
                b"print('part 3')\n".to_vec(),
            ),
        ]
    }

    fn documented_sample_submission() -> CmsxSubmissionBuilder {
        CmsxSubmissionBuilder::default().with_files(documented_sample_files())
    }

    fn assert_submission_file(
        file: &SubmissionFileRow,
        submission_id: &Uuid,
        problem_name: &str,
        field_name: &str,
        original_filename: &str,
        contents: &[u8],
    ) {
        assert_eq!(file.problem_name.as_deref(), Some(problem_name));
        assert_eq!(file.cmsx_file_field_name, field_name);
        assert_eq!(file.original_filename, original_filename);
        assert_eq!(file.safe_filename, field_name);
        assert_eq!(file.content_sha256, sha256_hex(contents));
        assert_eq!(file.size_bytes, contents.len() as i64);
        assert!(file.storage_path.contains(&submission_id.to_string()));
    }

    #[tokio::test]
    async fn accepts_valid_cmsx_submission() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(TEST_SLUG, documented_sample_submission().multipart())
            .await;

        assert_no_content_success(response).await;
        assert_ingestion_counts(&test.db, 1, 3, 1).await;

        let submission = fetch_submission(&test.db).await;
        assert_eq!(submission.assignment_id, test.assignment_id);
        assert_eq!(submission.cmsx_group_id, "341");
        assert_eq!(submission.cmsx_assignment_id, "21");
        assert_eq!(
            submission.cmsx_assignment_name,
            "Assignment 1: Intro to CMSX"
        );
        assert_eq!(submission.netids_raw, "acm22_ath55_gn93");
        assert_eq!(
            submission.netids_json,
            Some(json!(["acm22", "ath55", "gn93"]))
        );

        assert_eq!(submission.raw_metadata["group_id"], "341");
        assert_eq!(submission.raw_metadata["assignment_id"], "21");
        assert_eq!(
            submission.raw_metadata["assignment_name"],
            "Assignment 1: Intro to CMSX"
        );
        assert_eq!(submission.raw_metadata["num_files"], "3");
        assert!(submission.raw_metadata.get("auth_token").is_none());

        assert_eq!(fetch_job_submission_id(&test.db).await, submission.id);

        let files = fetch_submission_files(&test.db).await;
        assert_eq!(files.len(), 3);

        assert_submission_file(
            &files[0],
            &submission.id,
            "Part 1 of the Assignment",
            "Part_1_of_the_Assignment",
            "Part_1_of_the_Assignment.py",
            b"print('part 1')\n",
        );
        assert_submission_file(
            &files[1],
            &submission.id,
            "Part 2 of the Assignment",
            "Part_2_of_the_Assignment",
            "Part_2_of_the_Assignment.py",
            b"print('part 2')\n",
        );
        assert_submission_file(
            &files[2],
            &submission.id,
            "Part 3 of the Assignment",
            "Part_3_of_the_Assignment",
            "Part_3_of_the_Assignment.py",
            b"print('part 3')\n",
        );
    }

    #[tokio::test]
    async fn rejects_invalid_auth_token() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_token("wrong_secret")
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"], "invalid auth_token");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_missing_auth_token_when_no_file_parts_are_present() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .without_auth_token()
                    .with_files(Vec::new())
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "missing auth_token");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_uploaded_file_without_prior_auth_token() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .without_auth_token()
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "auth_token must appear before uploaded file parts"
        );
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_file_before_auth_token() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_file_parts_before_auth()
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "auth_token must appear before uploaded file parts"
        );
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_auth_token_longer_than_cmsx_limit() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_token("x".repeat(37))
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "auth_token must be at most 36 characters");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn accepts_36_character_non_ascii_auth_token_for_shape_validation() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_token("é".repeat(36))
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"], "invalid auth_token");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_auth_token_longer_than_36_non_ascii_characters() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_token("é".repeat(37))
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "auth_token must be at most 36 characters");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    async fn assert_missing_required_cmsx_metadata_field(
        field: &'static str,
        expected_error: &str,
    ) {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .without_field(field)
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "field={field}");
        assert_eq!(body["error"], expected_error, "field={field}");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_missing_required_cmsx_metadata_fields() {
        for (field, expected_error) in [
            ("netids", "missing netids"),
            ("group_id", "missing group_id"),
            ("assignment_id", "missing assignment_id"),
            ("assignment_name", "missing assignment_name"),
            ("num_files", "missing num_files"),
            ("problem_name_0", "missing problem_name_0"),
            ("file_name_0", "missing file_name_0"),
        ] {
            assert_missing_required_cmsx_metadata_field(field, expected_error).await;
        }
    }

    #[tokio::test]
    async fn rejects_mismatched_num_files() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_num_files("0")
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "num_files does not match uploaded file count"
        );
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_unexpected_uploaded_file_part() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_files(vec![
                        CmsxFilePart::new(
                            "Part 1 of the Assignment",
                            "Expected_File",
                            "Expected_File.py",
                            b"print('hello')\n".to_vec(),
                        )
                        .with_upload_field_name("Other_File"),
                    ])
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "unexpected uploaded file part Other_File");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_unsafe_file_name_metadata() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_files(vec![CmsxFilePart::new(
                        "Part 1 of the Assignment",
                        "../evil.py",
                        "evil.py",
                        b"print('hello')\n".to_vec(),
                    )])
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "uploaded filename is not safe: ../evil.py");
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn rejects_oversized_file() {
        let test = test_app(|cmsx| {
            cmsx.max_file_bytes = 3;
        })
        .await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_files(vec![CmsxFilePart::new(
                        "Part 1 of the Assignment",
                        "Part_1_of_the_Assignment",
                        "Part_1_of_the_Assignment.py",
                        b"1234".to_vec(),
                    )])
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            body["error"],
            "uploaded file is too large; limit is 3 bytes"
        );
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }

    #[tokio::test]
    async fn cleans_up_stored_files_when_late_validation_fails() {
        let test = test_app(|_| {}).await;

        let response = test
            .submit(
                TEST_SLUG,
                CmsxSubmissionBuilder::default()
                    .with_num_files("2")
                    .multipart(),
            )
            .await;
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "num_files does not match uploaded file count"
        );
        assert_no_ingestion_writes(&test.db).await;
        assert_storage_has_no_files(test.storage_root.path());
    }
}
