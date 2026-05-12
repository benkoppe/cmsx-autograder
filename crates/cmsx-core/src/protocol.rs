use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

pub const TEXT_TRUNCATION_MARKER: &str = "\n...[truncated]";

pub const JOB_EVENT_MESSAGE_MAX_BYTES: usize = 64 * 1024;
pub const JOB_EVENT_BATCH_MAX_EVENTS: usize = 512;

pub const GRADING_RESULT_SCHEMA_VERSION: &str = "2";

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

    #[test]
    fn grading_result_schema_version_is_v2() {
        assert_eq!(GRADING_RESULT_SCHEMA_VERSION, "2");
    }

    // --- artifact path validation ---

    #[test]
    fn artifact_path_accepts_simple_relative_paths() {
        assert!(validate_artifact_relative_path("report.txt").is_ok());
        assert!(validate_artifact_relative_path("reports/summary.txt").is_ok());
        assert!(validate_artifact_relative_path("a/b/c/d.txt").is_ok());
        assert!(validate_artifact_relative_path("a-b_c.d~e/123.txt").is_ok());
    }

    #[test]
    fn artifact_path_rejects_dot_and_dotdot_components() {
        assert_eq!(
            validate_artifact_relative_path("."),
            Err(ArtifactValidationError::DotComponent)
        );
        assert_eq!(
            validate_artifact_relative_path(".."),
            Err(ArtifactValidationError::DotDotComponent)
        );
        assert_eq!(
            validate_artifact_relative_path("a/./b"),
            Err(ArtifactValidationError::DotComponent)
        );
        assert_eq!(
            validate_artifact_relative_path("a/../b"),
            Err(ArtifactValidationError::DotDotComponent)
        );
    }

    #[test]
    fn artifact_path_allows_longer_dot_only_components() {
        assert!(validate_artifact_relative_path("...").is_ok());
        assert!(validate_artifact_relative_path("....").is_ok());
        assert!(validate_artifact_relative_path(".....").is_ok());
        assert!(validate_artifact_relative_path("a/.../b.txt").is_ok());
        assert!(validate_artifact_relative_path("....../file").is_ok());
    }

    #[test]
    fn artifact_path_rejects_empty() {
        assert_eq!(
            validate_artifact_relative_path(""),
            Err(ArtifactValidationError::EmptyPath)
        );
    }

    #[test]
    fn artifact_path_rejects_absolute() {
        assert_eq!(
            validate_artifact_relative_path("/report.txt"),
            Err(ArtifactValidationError::AbsolutePath)
        );
    }

    #[test]
    fn artifact_path_rejects_empty_component() {
        assert_eq!(
            validate_artifact_relative_path("a//b"),
            Err(ArtifactValidationError::EmptyComponent)
        );
    }

    #[test]
    fn artifact_path_rejects_backslash() {
        assert_eq!(
            validate_artifact_relative_path("a\\b"),
            Err(ArtifactValidationError::Backslash)
        );
    }

    #[test]
    fn artifact_path_rejects_control_characters() {
        assert_eq!(
            validate_artifact_relative_path("a/\n/b"),
            Err(ArtifactValidationError::ControlCharacter)
        );
        assert_eq!(
            validate_artifact_relative_path("a/\x01b"),
            Err(ArtifactValidationError::ControlCharacter)
        );
        assert_eq!(
            validate_artifact_relative_path("a/\x7fb"),
            Err(ArtifactValidationError::ControlCharacter)
        );
        assert_eq!(
            validate_artifact_relative_path("\0"),
            Err(ArtifactValidationError::ControlCharacter)
        );
    }

    #[test]
    fn artifact_path_rejects_too_long_path() {
        let long = "a".repeat(ARTIFACT_RELATIVE_PATH_MAX_BYTES + 1);
        assert_eq!(
            validate_artifact_relative_path(&long),
            Err(ArtifactValidationError::PathTooLong)
        );
    }

    #[test]
    fn artifact_path_rejects_too_long_name() {
        let long_name = "a".repeat(ARTIFACT_NAME_MAX_BYTES + 1);
        let path = format!("reports/{long_name}");
        assert_eq!(
            validate_artifact_relative_path(&path),
            Err(ArtifactValidationError::NameTooLong)
        );
    }

    #[test]
    fn artifact_path_accepts_exactly_max_lengths() {
        let max_name = "a".repeat(ARTIFACT_NAME_MAX_BYTES);
        assert!(validate_artifact_relative_path(&max_name).is_ok());

        // path at max length with name within limit: prefix/ + max_name
        let prefix = "x/";
        let remaining = ARTIFACT_RELATIVE_PATH_MAX_BYTES - prefix.len();
        let name = "a".repeat(remaining.min(ARTIFACT_NAME_MAX_BYTES));
        let path = format!("{prefix}{name}");
        assert!(validate_artifact_relative_path(&path).is_ok());
    }

    #[test]
    fn artifact_name_from_path_extracts_last_component() {
        assert_eq!(
            artifact_name_from_relative_path("reports/summary.txt").unwrap(),
            "summary.txt"
        );
        assert_eq!(
            artifact_name_from_relative_path("report.txt").unwrap(),
            "report.txt"
        );
    }

    // --- percent encoding ---

    #[test]
    fn artifact_percent_encode_round_trips_with_slashes() {
        let original = "reports/hello world/é.txt";
        let encoded = encode_artifact_relative_path(original).unwrap();
        let decoded = decode_artifact_relative_path(&encoded).unwrap();

        assert_eq!(decoded, original);
        assert!(encoded.contains('/'));
    }

    #[test]
    fn artifact_percent_encode_leaves_unreserved_and_slash() {
        let original = "abc/def-ghi_jkl.mno~pqr/123";
        let encoded = encode_artifact_relative_path(original).unwrap();

        assert_eq!(encoded, original);
    }

    #[test]
    fn artifact_percent_decode_rejects_truncated_escape() {
        assert_eq!(
            decode_artifact_relative_path("%"),
            Err(ArtifactValidationError::InvalidPercentEncoding)
        );
        assert_eq!(
            decode_artifact_relative_path("%4"),
            Err(ArtifactValidationError::InvalidPercentEncoding)
        );
        assert_eq!(
            decode_artifact_relative_path("abc%2"),
            Err(ArtifactValidationError::InvalidPercentEncoding)
        );
    }

    #[test]
    fn artifact_percent_decode_rejects_non_hex_escape() {
        assert_eq!(
            decode_artifact_relative_path("%GG"),
            Err(ArtifactValidationError::InvalidPercentEncoding)
        );
        assert_eq!(
            decode_artifact_relative_path("%ZZ"),
            Err(ArtifactValidationError::InvalidPercentEncoding)
        );
    }

    #[test]
    fn artifact_percent_decode_consumes_only_two_hex_digits() {
        assert_eq!(decode_artifact_relative_path("%41A").unwrap(), "AA");
        assert_eq!(decode_artifact_relative_path("%61%62").unwrap(), "ab");
    }

    #[test]
    fn artifact_percent_decode_rejects_invalid_decoded_path() {
        assert!(decode_artifact_relative_path(".").is_err());
        assert!(decode_artifact_relative_path("..").is_err());
        assert!(decode_artifact_relative_path("a%2F..%2Fb").is_err());
    }

    #[test]
    fn artifact_percent_decode_rejects_non_ascii_input() {
        assert_eq!(
            decode_artifact_relative_path("café.txt"),
            Err(ArtifactValidationError::NonAsciiEncodedPath)
        );
    }

    #[test]
    fn artifact_percent_decode_rejects_overlong_encoded() {
        let long = "a".repeat(ARTIFACT_RELATIVE_PATH_MAX_ENCODED_BYTES + 1);
        assert_eq!(
            decode_artifact_relative_path(&long),
            Err(ArtifactValidationError::EncodedPathTooLong)
        );
    }

    // --- SHA-256 validation ---

    #[test]
    fn artifact_sha256_accepts_lowercase_hex() {
        let valid = "a".repeat(64);
        assert!(validate_artifact_sha256(&valid).is_ok());

        let valid_mixed = "0123456789abcdef".repeat(4);
        assert!(validate_artifact_sha256(&valid_mixed).is_ok());
    }

    #[test]
    fn artifact_sha256_rejects_uppercase_hex() {
        let upper = "A".repeat(64);
        assert_eq!(
            validate_artifact_sha256(&upper),
            Err(ArtifactValidationError::InvalidSha256)
        );

        let mixed = format!("{}{}", "a".repeat(32), "A".repeat(32));
        assert_eq!(
            validate_artifact_sha256(&mixed),
            Err(ArtifactValidationError::InvalidSha256)
        );
    }

    #[test]
    fn artifact_sha256_rejects_wrong_length() {
        assert_eq!(
            validate_artifact_sha256("abc"),
            Err(ArtifactValidationError::InvalidSha256)
        );
        assert_eq!(
            validate_artifact_sha256(&"a".repeat(63)),
            Err(ArtifactValidationError::InvalidSha256)
        );
        assert_eq!(
            validate_artifact_sha256(&"a".repeat(65)),
            Err(ArtifactValidationError::InvalidSha256)
        );
    }

    #[test]
    fn artifact_sha256_rejects_non_hex() {
        assert_eq!(
            validate_artifact_sha256(&"g".repeat(64)),
            Err(ArtifactValidationError::InvalidSha256)
        );
    }

    // --- label validation ---

    #[test]
    fn artifact_label_accepts_normal_text() {
        assert!(validate_artifact_label("Summary").is_ok());
        assert!(validate_artifact_label("Coverage Report (v2)").is_ok());
        assert!(validate_artifact_label("").is_ok());
    }

    #[test]
    fn artifact_label_rejects_control_characters() {
        assert_eq!(
            validate_artifact_label("bad\nlabel"),
            Err(ArtifactValidationError::InvalidLabel)
        );
        assert_eq!(
            validate_artifact_label("bad\x01label"),
            Err(ArtifactValidationError::InvalidLabel)
        );
        assert_eq!(
            validate_artifact_label("bad\x7flabel"),
            Err(ArtifactValidationError::InvalidLabel)
        );
    }

    #[test]
    fn artifact_label_rejects_too_long() {
        let long = "a".repeat(ARTIFACT_LABEL_MAX_BYTES + 1);
        assert_eq!(
            validate_artifact_label(&long),
            Err(ArtifactValidationError::LabelTooLong)
        );
    }

    #[test]
    fn artifact_label_accepts_exactly_max_length() {
        let max = "a".repeat(ARTIFACT_LABEL_MAX_BYTES);
        assert!(validate_artifact_label(&max).is_ok());
    }

    // --- visibility ---

    #[test]
    fn artifact_visibility_parses_valid_values() {
        assert_eq!(
            "student".parse::<ArtifactVisibility>().unwrap(),
            ArtifactVisibility::Student
        );
        assert_eq!(
            "staff".parse::<ArtifactVisibility>().unwrap(),
            ArtifactVisibility::Staff
        );
        assert_eq!(
            "internal".parse::<ArtifactVisibility>().unwrap(),
            ArtifactVisibility::Internal
        );
    }

    #[test]
    fn artifact_visibility_rejects_unknown() {
        assert!("unknown".parse::<ArtifactVisibility>().is_err());
        assert!("".parse::<ArtifactVisibility>().is_err());
    }

    #[test]
    fn artifact_visibility_as_str_round_trips() {
        for vis in [
            ArtifactVisibility::Student,
            ArtifactVisibility::Staff,
            ArtifactVisibility::Internal,
        ] {
            assert_eq!(vis.as_str().parse::<ArtifactVisibility>().unwrap(), vis);
        }
    }

    #[test]
    fn artifact_visibility_is_valid_matches_parse() {
        assert!(ArtifactVisibility::is_valid("student"));
        assert!(ArtifactVisibility::is_valid("staff"));
        assert!(ArtifactVisibility::is_valid("internal"));
        assert!(!ArtifactVisibility::is_valid("unknown"));
        assert!(!ArtifactVisibility::is_valid(""));
    }
}
