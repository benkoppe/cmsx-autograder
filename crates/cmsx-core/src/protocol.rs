use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

pub const TEXT_TRUNCATION_MARKER: &str = "\n...[truncated]";

pub const JOB_EVENT_MESSAGE_MAX_BYTES: usize = 64 * 1024;
pub const JOB_EVENT_BATCH_MAX_EVENTS: usize = 512;

pub const GRADING_RESULT_SCHEMA_VERSION: &str = "1";

pub const WORKER_JWT_AUDIENCE: &str = "cmsx-control-plane";
pub const WORKER_AUTH_SCHEME: &str = "WorkerJWT";
pub const WORKER_JWT_VALIDITY_SECONDS: u64 = 30;
pub const WORKER_JWT_MAX_VALIDITY_SECONDS: u64 = 60;
pub const WORKER_JWT_TIME_TOLERANCE_SECONDS: u64 = 60;
pub const WORKER_REQUEST_NONCE_RETENTION_SECONDS: i64 = 120;

pub const JOB_LEASE_SECONDS: i64 = 60;
pub const JOB_SWEEP_INTERVAL_SECONDS: u64 = 5;
pub const MAX_CLAIM_WAIT_SECONDS: u64 = 30;
pub const WORKER_MAX_CLAIM_JOBS: i32 = 128;
pub const WORKER_MAX_ACTIVE_JOBS: usize = 128;

pub const ASSIGNMENT_SLUG_MAX_BYTES: usize = 128;
pub const ASSIGNMENT_NAME_MAX_BYTES: usize = 256;
pub const WORKER_NAME_MAX_BYTES: usize = 128;
pub const JOB_FAILURE_REASON_MAX_BYTES: usize = 128;
pub const GRADING_RESULT_MAX_TESTS: usize = 512;
pub const GRADING_RESULT_MAX_ARTIFACTS: usize = ARTIFACT_MAX_COUNT;

pub const ARTIFACT_MAX_COUNT: usize = 128;
pub const ARTIFACT_MAX_BYTES: u64 = 10 * 1024 * 1024;
pub const ARTIFACT_TOTAL_MAX_BYTES: u64 = 50 * 1024 * 1024;
pub const ARTIFACT_RELATIVE_PATH_MAX_BYTES: usize = 1024;
pub const ARTIFACT_RELATIVE_PATH_MAX_ENCODED_BYTES: usize = 3 * ARTIFACT_RELATIVE_PATH_MAX_BYTES;
pub const ARTIFACT_NAME_MAX_BYTES: usize = 255;
pub const ARTIFACT_LABEL_MAX_BYTES: usize = 256;

pub mod failure_reason {
    pub const INPUT_DOWNLOAD_FAILED: &str = "input_download_failed";
    pub const INPUT_HASH_MISMATCH: &str = "input_hash_mismatch";
    pub const GRADER_MISSING: &str = "grader_missing";
    pub const EXECUTOR_ERROR: &str = "executor_error";
    pub const RESULT_MISSING: &str = "result_missing";
    pub const RESULT_INVALID: &str = "result_invalid";
    pub const ARTIFACT_INVALID: &str = "artifact_invalid";
    pub const ARTIFACT_UPLOAD_FAILED: &str = "artifact_upload_failed";
    pub const CANCELLED_BEFORE_START: &str = "cancelled_before_start";
    pub const TIMEOUT: &str = "timeout";
    pub const LEASE_LOST: &str = "lease_lost";
    pub const LEASE_EXPIRED: &str = "lease_expired";
    pub const WORKSPACE_ERROR: &str = "workspace_error";
}

pub mod job_event_type {
    pub const STDOUT: &str = "stdout";
    pub const STDERR: &str = "stderr";
    pub const JOB_INPUT_PREPARED: &str = "job.input.prepared";
    pub const JOB_STARTED: &str = "job.started";
    pub const EXECUTOR_STARTED: &str = "executor.started";
    pub const EXECUTOR_CONTAINER_CREATED: &str = "executor.container.created";
    pub const EXECUTOR_CONTAINER_STARTED: &str = "executor.container.started";
    pub const ARTIFACT_DISCOVERED: &str = "artifact.discovered";
    pub const ARTIFACT_UPLOADED: &str = "artifact.uploaded";
    pub const ARTIFACT_REJECTED: &str = "artifact.rejected";
    pub const ARTIFACT_UPLOAD_FAILED: &str = "artifact.upload_failed";
    pub const RESULT_READ: &str = "result.read";
    pub const JOB_TIMEOUT: &str = "job.timeout";
    pub const JOB_CANCELLED: &str = "job.cancelled";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobEventStream {
    Stdout,
    Stderr,
    Worker,
    Resource,
}

impl JobEventStream {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Worker => "worker",
            Self::Resource => "resource",
        }
    }

    pub fn is_valid(value: &str) -> bool {
        [
            Self::Stdout.as_str(),
            Self::Stderr.as_str(),
            Self::Worker.as_str(),
            Self::Resource.as_str(),
        ]
        .contains(&value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobEventVisibility {
    Student,
    Staff,
    Internal,
}

impl JobEventVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Student => "student",
            Self::Staff => "staff",
            Self::Internal => "internal",
        }
    }

    pub fn is_valid(value: &str) -> bool {
        [
            Self::Student.as_str(),
            Self::Staff.as_str(),
            Self::Internal.as_str(),
        ]
        .contains(&value)
    }
}

pub fn cap_text(value: &str, max_bytes: usize) -> String {
    cap_text_with_optional_marker(value, max_bytes, None)
}

pub fn cap_text_with_marker(value: String, max_bytes: usize) -> String {
    cap_text_with_optional_marker(&value, max_bytes, Some(TEXT_TRUNCATION_MARKER))
}

fn cap_text_with_optional_marker(value: &str, max_bytes: usize, marker: Option<&str>) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    if max_bytes == 0 {
        return String::new();
    }

    let marker = marker.unwrap_or("");

    if marker.is_empty() {
        return truncate_to_char_boundary(value, max_bytes).to_string();
    }

    let marker_len = marker.len();

    if max_bytes <= marker_len {
        return truncate_to_char_boundary(value, max_bytes).to_string();
    }

    let max_prefix = max_bytes - marker_len;
    let mut capped = truncate_to_char_boundary(value, max_prefix).to_string();
    capped.push_str(marker);
    capped
}

fn truncate_to_char_boundary(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());

    while !value.is_char_boundary(end) {
        end -= 1;
    }

    &value[..end]
}

const ARTIFACT_PATH_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactVisibility {
    Student,
    Staff,
    Internal,
}

impl ArtifactVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Student => "student",
            Self::Staff => "staff",
            Self::Internal => "internal",
        }
    }

    pub fn is_valid(value: &str) -> bool {
        matches!(value, "student" | "staff" | "internal")
    }
}

impl FromStr for ArtifactVisibility {
    type Err = ArtifactValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "student" => Ok(Self::Student),
            "staff" => Ok(Self::Staff),
            "internal" => Ok(Self::Internal),
            _ => Err(ArtifactValidationError::InvalidVisibility),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactValidationError {
    EmptyPath,
    AbsolutePath,
    PathTooLong,
    EmptyComponent,
    DotComponent,
    DotDotComponent,
    Backslash,
    ControlCharacter,
    NameTooLong,
    LabelTooLong,
    InvalidLabel,
    InvalidSha256,
    InvalidVisibility,
    EncodedPathTooLong,
    NonAsciiEncodedPath,
    InvalidPercentEncoding,
    InvalidUtf8,
}

impl fmt::Display for ArtifactValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyPath => "artifact path must not be empty",
            Self::AbsolutePath => "artifact path must be relative",
            Self::PathTooLong => "artifact path is too long",
            Self::EmptyComponent => "artifact path contains an empty component",
            Self::DotComponent => "artifact path must not contain '.' components",
            Self::DotDotComponent => "artifact path must not contain '..' components",
            Self::Backslash => "artifact path must not contain backslashes",
            Self::ControlCharacter => "artifact path contains a control character",
            Self::NameTooLong => "artifact filename is too long",
            Self::LabelTooLong => "artifact label is too long",
            Self::InvalidLabel => "artifact label contains an invalid character",
            Self::InvalidSha256 => "artifact sha256 must be 64 lowercase hex characters",
            Self::InvalidVisibility => "invalid artifact visibility",
            Self::EncodedPathTooLong => "encoded artifact path is too long",
            Self::NonAsciiEncodedPath => "encoded artifact path must be ASCII",
            Self::InvalidPercentEncoding => "invalid percent encoding in artifact path",
            Self::InvalidUtf8 => "artifact path must decode to UTF-8",
        };
        write!(f, "{message}")
    }
}

impl std::error::Error for ArtifactValidationError {}

pub fn validate_artifact_relative_path(value: &str) -> Result<(), ArtifactValidationError> {
    if value.is_empty() {
        return Err(ArtifactValidationError::EmptyPath);
    }
    if value.len() > ARTIFACT_RELATIVE_PATH_MAX_BYTES {
        return Err(ArtifactValidationError::PathTooLong);
    }
    if value.starts_with('/') {
        return Err(ArtifactValidationError::AbsolutePath);
    }
    if value.contains('\\') {
        return Err(ArtifactValidationError::Backslash);
    }
    if value
        .chars()
        .any(|ch| ch == '\0' || ch.is_ascii_control() || ch == '\u{7f}')
    {
        return Err(ArtifactValidationError::ControlCharacter);
    }

    let mut last_component = None;

    for component in value.split('/') {
        if component.is_empty() {
            return Err(ArtifactValidationError::EmptyComponent);
        }
        if component == "." {
            return Err(ArtifactValidationError::DotComponent);
        }
        if component == ".." {
            return Err(ArtifactValidationError::DotDotComponent);
        }

        last_component = Some(component);
    }

    let Some(name) = last_component else {
        return Err(ArtifactValidationError::EmptyPath);
    };

    if name.len() > ARTIFACT_NAME_MAX_BYTES {
        return Err(ArtifactValidationError::NameTooLong);
    }

    Ok(())
}

pub fn artifact_name_from_relative_path(value: &str) -> Result<&str, ArtifactValidationError> {
    validate_artifact_relative_path(value)?;
    Ok(value
        .rsplit('/')
        .next()
        .expect("validated non-empty path should have final component"))
}

pub fn validate_artifact_label(value: &str) -> Result<(), ArtifactValidationError> {
    if value.as_bytes().len() > ARTIFACT_LABEL_MAX_BYTES {
        return Err(ArtifactValidationError::LabelTooLong);
    }

    if value
        .chars()
        .any(|ch| ch == '\0' || ch.is_ascii_control() || ch == '\u{7f}')
    {
        return Err(ArtifactValidationError::InvalidLabel);
    }

    Ok(())
}

pub fn validate_artifact_sha256(value: &str) -> Result<(), ArtifactValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(ArtifactValidationError::InvalidSha256);
    }

    Ok(())
}

pub fn encode_artifact_relative_path(value: &str) -> Result<String, ArtifactValidationError> {
    validate_artifact_relative_path(value)?;
    Ok(utf8_percent_encode(value, ARTIFACT_PATH_ENCODE_SET).to_string())
}

pub fn decode_artifact_relative_path(value: &str) -> Result<String, ArtifactValidationError> {
    if value.len() > ARTIFACT_RELATIVE_PATH_MAX_ENCODED_BYTES {
        return Err(ArtifactValidationError::EncodedPathTooLong);
    }

    if !value.is_ascii() {
        return Err(ArtifactValidationError::NonAsciiEncodedPath);
    }

    validate_percent_escapes(value)?;

    let decoded = percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| ArtifactValidationError::InvalidUtf8)?
        .into_owned();

    validate_artifact_relative_path(&decoded)?;
    Ok(decoded)
}

fn validate_percent_escapes(value: &str) -> Result<(), ArtifactValidationError> {
    let bytes = value.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }

        if index + 2 >= bytes.len() {
            return Err(ArtifactValidationError::InvalidPercentEncoding);
        }

        if !bytes[index + 1].is_ascii_hexdigit() || !bytes[index + 2].is_ascii_hexdigit() {
            return Err(ArtifactValidationError::InvalidPercentEncoding);
        }

        index += 3;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_text_preserves_short_text() {
        assert_eq!(cap_text_with_marker("abc".to_string(), 10), "abc");
    }

    #[test]
    fn cap_text_adds_marker_within_limit() {
        let capped = cap_text_with_marker("a".repeat(100), 20);

        assert!(capped.len() <= 20);
        assert!(capped.ends_with(TEXT_TRUNCATION_MARKER));
    }

    #[test]
    fn cap_text_preserves_char_boundary() {
        let capped = cap_text_with_marker("é".repeat(100), 21);

        assert!(capped.len() <= 21);
        assert!(capped.is_char_boundary(capped.len()));
    }

    #[test]
    fn cap_text_handles_tiny_limit() {
        let capped = cap_text_with_marker("abcdef".to_string(), 3);

        assert_eq!(capped.len(), 3);
    }

    #[test]
    fn cap_text_truncates_without_marker() {
        let capped = cap_text("ééé", 3);

        assert_eq!(capped, "é");
        assert!(capped.len() <= 3);
    }
}
