use std::{
    ffi::OsStr,
    fs,
    path::{Component, Path, PathBuf},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    fs as tokio_fs,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cmsx_core::{ClaimedJob, ClaimedJobFile, GradingResult};

use crate::job_contract;

pub const MAX_INPUT_FILE_BYTES: u64 = 64 * 1024 * 1024;
pub const RESULT_JSON_MAX_BYTES: u64 = 1024 * 1024;
const MATERIALIZATION_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct JobWorkspace {
    pub root: PathBuf,
    pub input_dir: PathBuf,
    pub files_dir: PathBuf,
    pub grader_dir: PathBuf,
    pub work_dir: PathBuf,
    pub output_dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub result_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("job attempt must be positive: {0}")]
    InvalidAttempt(i32),
    #[error("invalid safe component: {0}")]
    InvalidSafeComponent(String),
    #[error("grader bundle is missing or not a directory: {0}")]
    GraderMissing(String),
    #[error("grader bundle must contain grade.py: {0}")]
    GradePyMissing(String),
    #[error("grader destination is not empty: {0}")]
    GraderDestinationNotEmpty(String),
    #[error("grader bundle contains a symlink: {0}")]
    GraderSymlink(String),
    #[error("grader bundle contains an unsupported file type: {0}")]
    GraderUnsupportedFileType(String),
    #[error("invalid grader/workspace path: {0}")]
    InvalidTrustedPath(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum MaterializeInputError {
    #[error("input materialization was cancelled")]
    Cancelled,
    #[error("expected sha256 is invalid: {0}")]
    InvalidExpectedHash(String),
    #[error("input hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("input size mismatch: expected {expected}, actual {actual}")]
    SizeMismatch { expected: i64, actual: u64 },
    #[error("input too large: max {max}, actual {actual}")]
    TooLarge { max: u64, actual: u64 },
    #[error("invalid input filename: {0}")]
    InvalidFilename(String),
    #[error("input final path already exists: {0}")]
    FinalPathExists(String),
    #[error("expected input size is invalid: {0}")]
    InvalidExpectedSize(i64),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum ResultReadError {
    #[error("result.json is missing")]
    Missing,
    #[error("result.json is too large: max {max}, actual {actual}")]
    TooLarge { max: u64, actual: u64 },
    #[error("failed to read result.json: {0}")]
    Io(std::io::Error),
    #[error("result.json is invalid: {0}")]
    InvalidJson(serde_json::Error),
}

#[derive(Debug, Serialize)]
struct Metadata<'a> {
    job_id: Uuid,
    submission_id: Uuid,
    assignment_id: Uuid,
    assignment_slug: &'a str,
    attempt: i32,
    received: &'a serde_json::Value,
    files: Vec<MetadataFile<'a>>,
}

#[derive(Debug, Serialize)]
struct MetadataFile<'a> {
    id: Uuid,
    problem_name: &'a Option<String>,
    original_filename: &'a str,
    safe_filename: &'a str,
    sha256: &'a str,
    size_bytes: i64,
}

pub fn validate_safe_component(value: &str) -> Result<(), WorkspaceError> {
    if value.is_empty() || value.trim().is_empty() {
        return Err(WorkspaceError::InvalidSafeComponent(value.to_string()));
    }

    if value.trim() != value {
        return Err(WorkspaceError::InvalidSafeComponent(value.to_string()));
    }

    if value == "." || value == ".." {
        return Err(WorkspaceError::InvalidSafeComponent(value.to_string()));
    }

    if value.contains('/') || value.contains('\\') {
        return Err(WorkspaceError::InvalidSafeComponent(value.to_string()));
    }

    let path = Path::new(value);

    if path.is_absolute() {
        return Err(WorkspaceError::InvalidSafeComponent(value.to_string()));
    }

    let mut components = path.components();

    match (components.next(), components.next()) {
        (Some(Component::Normal(component)), None) if component == OsStr::new(value) => Ok(()),
        _ => Err(WorkspaceError::InvalidSafeComponent(value.to_string())),
    }
}

pub fn build_workspace_paths(
    workspace_root: &Path,
    job: &ClaimedJob,
) -> Result<JobWorkspace, WorkspaceError> {
    if job.attempt <= 0 {
        return Err(WorkspaceError::InvalidAttempt(job.attempt));
    }

    let root = workspace_root
        .join(job.id.to_string())
        .join(format!("attempt-{}", job.attempt));
    let input_dir = root.join(job_contract::INPUT_DIR);
    let files_dir = input_dir.join(job_contract::FILES_DIR);
    let grader_dir = root.join(job_contract::GRADER_DIR);
    let work_dir = root.join(job_contract::WORK_DIR);
    let output_dir = root.join(job_contract::OUTPUT_DIR);
    let artifacts_dir = output_dir.join(job_contract::ARTIFACTS_DIR);
    let result_path = output_dir.join(job_contract::RESULT_JSON);

    Ok(JobWorkspace {
        root,
        input_dir,
        files_dir,
        grader_dir,
        work_dir,
        output_dir,
        artifacts_dir,
        result_path,
    })
}

pub async fn prepare_attempt_workspace(
    workspace_root: &Path,
    job: &ClaimedJob,
) -> Result<JobWorkspace, WorkspaceError> {
    let workspace = build_workspace_paths(workspace_root, job)?;

    if workspace.root.exists() {
        tokio_fs::remove_dir_all(&workspace.root).await?;
    }

    tokio_fs::create_dir_all(&workspace.files_dir).await?;
    tokio_fs::create_dir_all(&workspace.grader_dir).await?;
    tokio_fs::create_dir_all(&workspace.work_dir).await?;
    tokio_fs::create_dir_all(&workspace.artifacts_dir).await?;

    set_runtime_workspace_permissions(&workspace)?;

    write_metadata(job, &workspace).await?;

    Ok(workspace)
}

pub fn set_runtime_workspace_permissions(workspace: &JobWorkspace) -> Result<(), WorkspaceError> {
    set_writable_runtime_permissions(&workspace.work_dir)?;
    set_writable_runtime_permissions(&workspace.output_dir)?;
    set_writable_runtime_permissions(&workspace.artifacts_dir)?;
    Ok(())
}

#[cfg(unix)]
fn set_writable_runtime_permissions(path: &Path) -> Result<(), WorkspaceError> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o777);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_writable_runtime_permissions(_path: &Path) -> Result<(), WorkspaceError> {
    Ok(())
}

pub async fn cleanup_attempt_workspace(workspace: &JobWorkspace) -> Result<(), WorkspaceError> {
    if workspace.root.exists() {
        tokio_fs::remove_dir_all(&workspace.root).await?;
    }

    Ok(())
}

async fn write_metadata(job: &ClaimedJob, workspace: &JobWorkspace) -> Result<(), WorkspaceError> {
    let metadata = Metadata {
        job_id: job.id,
        submission_id: job.submission_id,
        assignment_id: job.assignment_id,
        assignment_slug: &job.assignment_slug,
        attempt: job.attempt,
        received: &job.submission_metadata,
        files: job.files.iter().map(metadata_file).collect(),
    };

    let bytes = serde_json::to_vec_pretty(&metadata)?;
    tokio_fs::write(workspace.input_dir.join(job_contract::METADATA_JSON), bytes).await?;

    Ok(())
}

fn metadata_file(file: &ClaimedJobFile) -> MetadataFile<'_> {
    MetadataFile {
        id: file.id,
        problem_name: &file.problem_name,
        original_filename: &file.original_filename,
        safe_filename: &file.safe_filename,
        sha256: &file.content_sha256,
        size_bytes: file.size_bytes,
    }
}

pub struct MaterializeInputFileRequest<'a> {
    pub files_dir: &'a Path,
    pub file_id: Uuid,
    pub safe_filename: &'a str,
    pub expected_size_bytes: i64,
    pub expected_sha256: &'a str,
    pub max_bytes: u64,
    pub cancel: CancellationToken,
}

pub async fn materialize_input_file_from_async_read<R>(
    mut reader: R,
    request: MaterializeInputFileRequest<'_>,
) -> Result<(), MaterializeInputError>
where
    R: AsyncRead + Unpin,
{
    validate_safe_component(request.safe_filename)
        .map_err(|error| MaterializeInputError::InvalidFilename(error.to_string()))?;

    if request.expected_size_bytes < 0 {
        return Err(MaterializeInputError::InvalidExpectedSize(
            request.expected_size_bytes,
        ));
    }

    let expected_size = request.expected_size_bytes as u64;

    if expected_size > request.max_bytes {
        return Err(MaterializeInputError::TooLarge {
            max: request.max_bytes,
            actual: expected_size,
        });
    }

    validate_expected_sha256(request.expected_sha256)?;

    let temp_path = request
        .files_dir
        .join(format!(".download-{}.tmp", request.file_id));
    let final_path = request.files_dir.join(request.safe_filename);

    if final_path.exists() {
        return Err(MaterializeInputError::FinalPathExists(
            final_path.display().to_string(),
        ));
    }

    if temp_path.exists() {
        tokio_fs::remove_file(&temp_path).await?;
    }

    let result = materialize_to_temp(
        &mut reader,
        &temp_path,
        expected_size,
        request.expected_sha256,
        request.max_bytes,
        request.cancel,
    )
    .await;

    match result {
        Ok(()) => {
            if final_path.exists() {
                let _ = tokio_fs::remove_file(&temp_path).await;
                return Err(MaterializeInputError::FinalPathExists(
                    final_path.display().to_string(),
                ));
            }

            tokio_fs::rename(&temp_path, &final_path).await?;
            Ok(())
        }
        Err(error) => {
            if let Err(cleanup_error) = tokio_fs::remove_file(&temp_path).await
                && cleanup_error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(
                    path = %temp_path.display(),
                    ?cleanup_error,
                    "failed to remove temporary input file"
                );
            }

            Err(error)
        }
    }
}

async fn materialize_to_temp<R>(
    reader: &mut R,
    temp_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    max_bytes: u64,
    cancel: CancellationToken,
) -> Result<(), MaterializeInputError>
where
    R: AsyncRead + Unpin,
{
    let mut file = tokio_fs::File::create(temp_path).await?;
    let mut hasher = Sha256::new();
    let mut actual = 0_u64;
    let mut buffer = [0_u8; MATERIALIZATION_BUFFER_BYTES];

    loop {
        if cancel.is_cancelled() {
            return Err(MaterializeInputError::Cancelled);
        }

        let read = reader.read(&mut buffer).await?;

        if read == 0 {
            break;
        }

        actual = actual.saturating_add(read as u64);

        if actual > expected_size {
            return Err(MaterializeInputError::SizeMismatch {
                expected: expected_size as i64,
                actual,
            });
        }

        if actual > max_bytes {
            return Err(MaterializeInputError::TooLarge {
                max: max_bytes,
                actual,
            });
        }

        hasher.update(&buffer[..read]);
        file.write_all(&buffer[..read]).await?;
    }

    file.flush().await?;
    drop(file);

    if actual != expected_size {
        return Err(MaterializeInputError::SizeMismatch {
            expected: expected_size as i64,
            actual,
        });
    }

    let actual_sha256 = hex::encode(hasher.finalize());

    if actual_sha256 != expected_sha256.to_ascii_lowercase() {
        return Err(MaterializeInputError::HashMismatch {
            expected: expected_sha256.to_string(),
            actual: actual_sha256,
        });
    }

    Ok(())
}

pub fn validate_expected_sha256(value: &str) -> Result<(), MaterializeInputError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(MaterializeInputError::InvalidExpectedHash(
            value.to_string(),
        ));
    }

    Ok(())
}

pub fn install_grader_bundle(
    grader_root: &Path,
    assignment_slug: &str,
    workspace: &JobWorkspace,
) -> Result<(), WorkspaceError> {
    validate_safe_component(assignment_slug)?;

    if !grader_root.exists() {
        return Err(WorkspaceError::GraderMissing(
            grader_root.display().to_string(),
        ));
    }

    let canonical_grader_root = grader_root.canonicalize().map_err(|error| {
        WorkspaceError::InvalidTrustedPath(format!(
            "failed to canonicalize grader_root {}: {error}",
            grader_root.display()
        ))
    })?;

    let source = grader_root.join(assignment_slug);

    if !source.exists() || !source.is_dir() {
        return Err(WorkspaceError::GraderMissing(source.display().to_string()));
    }

    let canonical_source = source.canonicalize().map_err(|error| {
        WorkspaceError::InvalidTrustedPath(format!(
            "failed to canonicalize grader source {}: {error}",
            source.display()
        ))
    })?;

    if !canonical_source.starts_with(&canonical_grader_root) {
        return Err(WorkspaceError::InvalidTrustedPath(format!(
            "grader source escapes grader root: {}",
            canonical_source.display()
        )));
    }

    if !workspace.grader_dir.exists() {
        return Err(WorkspaceError::InvalidTrustedPath(format!(
            "grader destination does not exist: {}",
            workspace.grader_dir.display()
        )));
    }

    if workspace.grader_dir.read_dir()?.next().is_some() {
        return Err(WorkspaceError::GraderDestinationNotEmpty(
            workspace.grader_dir.display().to_string(),
        ));
    }

    let canonical_destination = workspace.grader_dir.canonicalize().map_err(|error| {
        WorkspaceError::InvalidTrustedPath(format!(
            "failed to canonicalize grader destination {}: {error}",
            workspace.grader_dir.display()
        ))
    })?;

    if canonical_source == canonical_destination {
        return Err(WorkspaceError::InvalidTrustedPath(
            "grader source and destination are the same path".to_string(),
        ));
    }

    if canonical_destination.starts_with(&canonical_source) {
        return Err(WorkspaceError::InvalidTrustedPath(format!(
            "grader destination is inside grader source: {}",
            canonical_destination.display()
        )));
    }

    copy_grader_dir(&canonical_source, &canonical_destination)?;

    let grade_py = canonical_destination.join(job_contract::GRADE_PY);
    if !grade_py.exists() || !grade_py.is_file() {
        return Err(WorkspaceError::GradePyMissing(
            grade_py.display().to_string(),
        ));
    }

    Ok(())
}

fn copy_grader_dir(source: &Path, destination: &Path) -> Result<(), WorkspaceError> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let metadata = fs::symlink_metadata(&source_path)?;

        if metadata.file_type().is_symlink() {
            return Err(WorkspaceError::GraderSymlink(
                source_path.display().to_string(),
            ));
        }

        let destination_path = destination.join(entry.file_name());

        if metadata.is_dir() {
            fs::create_dir(&destination_path)?;
            copy_grader_dir(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path)?;
        } else {
            return Err(WorkspaceError::GraderUnsupportedFileType(
                source_path.display().to_string(),
            ));
        }
    }

    Ok(())
}

pub async fn read_bounded_result_json(
    result_path: &Path,
) -> Result<GradingResult, ResultReadError> {
    let metadata = match tokio_fs::metadata(result_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ResultReadError::Missing);
        }
        Err(error) => return Err(ResultReadError::Io(error)),
    };

    let len = metadata.len();

    if len > RESULT_JSON_MAX_BYTES {
        return Err(ResultReadError::TooLarge {
            max: RESULT_JSON_MAX_BYTES,
            actual: len,
        });
    }

    let bytes = tokio_fs::read(result_path)
        .await
        .map_err(ResultReadError::Io)?;

    serde_json::from_slice(&bytes).map_err(ResultReadError::InvalidJson)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cmsx_core::{ClaimedJobFile, ResultStatus, protocol::GRADING_RESULT_SCHEMA_VERSION};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    fn test_job(attempt: i32) -> ClaimedJob {
        ClaimedJob {
            id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            submission_id: Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
            assignment_id: Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap(),
            assignment_slug: "intro".to_string(),
            lease_expires_at: chrono::Utc::now(),
            attempt,
            execution_config: json!({}),
            runner_config: json!({}),
            capabilities: json!({}),
            submission_metadata: json!({"cmsx": true}),
            files: vec![ClaimedJobFile {
                id: Uuid::parse_str("00000000-0000-0000-0000-000000000004").unwrap(),
                problem_name: Some("problem".to_string()),
                original_filename: "hello.py".to_string(),
                safe_filename: "hello.py".to_string(),
                content_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                    .to_string(),
                size_bytes: 5,
            }],
        }
    }

    fn test_workspace(temp: &TempDir) -> JobWorkspace {
        let root = temp.path().to_path_buf();
        let input_dir = root.join(job_contract::INPUT_DIR);
        let output_dir = root.join(job_contract::OUTPUT_DIR);

        JobWorkspace {
            root: root.clone(),
            input_dir: input_dir.clone(),
            files_dir: input_dir.join(job_contract::FILES_DIR),
            grader_dir: root.join(job_contract::GRADER_DIR),
            work_dir: root.join(job_contract::WORK_DIR),
            artifacts_dir: output_dir.join(job_contract::ARTIFACTS_DIR),
            result_path: output_dir.join(job_contract::RESULT_JSON),
            output_dir,
        }
    }

    #[test]
    fn safe_component_accepts_simple_name() {
        validate_safe_component("hello.py").unwrap();
        validate_safe_component("assignment-1").unwrap();
    }

    #[test]
    fn safe_component_rejects_empty_and_whitespace() {
        assert!(validate_safe_component("").is_err());
        assert!(validate_safe_component("   ").is_err());
    }

    #[test]
    fn safe_component_rejects_leading_or_trailing_whitespace() {
        assert!(validate_safe_component(" hello").is_err());
        assert!(validate_safe_component("hello ").is_err());
    }

    #[test]
    fn safe_component_rejects_dots_and_separators() {
        assert!(validate_safe_component(".").is_err());
        assert!(validate_safe_component("..").is_err());
        assert!(validate_safe_component("a/b").is_err());
        assert!(validate_safe_component("a\\b").is_err());
        assert!(validate_safe_component("/absolute").is_err());
    }

    #[test]
    fn workspace_layout_is_attempt_specific() {
        let root = PathBuf::from("data/worker/jobs");
        let job = test_job(2);
        let workspace = build_workspace_paths(&root, &job).unwrap();

        assert_eq!(
            workspace.root,
            root.join(job.id.to_string()).join("attempt-2")
        );
        assert_eq!(
            workspace.input_dir,
            workspace.root.join(job_contract::INPUT_DIR)
        );
        assert_eq!(workspace.files_dir, workspace.root.join("input/files"));
        assert_eq!(
            workspace.grader_dir,
            workspace.root.join(job_contract::GRADER_DIR)
        );
        assert_eq!(
            workspace.work_dir,
            workspace.root.join(job_contract::WORK_DIR)
        );
        assert_eq!(
            workspace.output_dir,
            workspace.root.join(job_contract::OUTPUT_DIR)
        );
        assert_eq!(
            workspace.artifacts_dir,
            workspace
                .root
                .join(job_contract::OUTPUT_DIR)
                .join(job_contract::ARTIFACTS_DIR)
        );
        assert_eq!(
            workspace.result_path,
            workspace
                .root
                .join(job_contract::OUTPUT_DIR)
                .join(job_contract::RESULT_JSON)
        );
    }

    #[test]
    fn workspace_rejects_invalid_attempt() {
        let root = PathBuf::from("data/worker/jobs");
        let error = build_workspace_paths(&root, &test_job(0)).unwrap_err();

        assert!(matches!(error, WorkspaceError::InvalidAttempt(0)));
    }

    #[tokio::test]
    async fn prepare_writes_metadata_shape() {
        let temp = TempDir::new().unwrap();
        let job = test_job(1);
        let workspace = prepare_attempt_workspace(temp.path(), &job).await.unwrap();

        let metadata_path = workspace.input_dir.join(job_contract::METADATA_JSON);
        let metadata: serde_json::Value =
            serde_json::from_slice(&tokio_fs::read(metadata_path).await.unwrap()).unwrap();

        assert_eq!(metadata["job_id"], job.id.to_string());
        assert_eq!(metadata["submission_id"], job.submission_id.to_string());
        assert_eq!(metadata["assignment_id"], job.assignment_id.to_string());
        assert_eq!(metadata["assignment_slug"], "intro");
        assert_eq!(metadata["attempt"], 1);
        assert_eq!(metadata["received"], json!({"cmsx": true}));
        assert_eq!(metadata["files"][0]["safe_filename"], "hello.py");
        assert_eq!(metadata["files"][0]["sha256"], job.files[0].content_sha256);
    }

    #[test]
    fn expected_sha256_validation() {
        validate_expected_sha256(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        )
        .unwrap();

        assert!(validate_expected_sha256("not-a-hash").is_err());
        assert!(
            validate_expected_sha256(
                "zzzz4dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            )
            .is_err()
        );
    }

    fn materialize_request<'a>(
        temp: &'a TempDir,
        file_id: Uuid,
        expected_size_bytes: i64,
        cancel: CancellationToken,
    ) -> MaterializeInputFileRequest<'a> {
        MaterializeInputFileRequest {
            files_dir: temp.path(),
            file_id,
            safe_filename: "hello.py",
            expected_size_bytes,
            expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            max_bytes: MAX_INPUT_FILE_BYTES,
            cancel,
        }
    }

    #[tokio::test]
    async fn materialization_writes_temp_then_final() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();

        materialize_input_file_from_async_read(
            &b"hello"[..],
            MaterializeInputFileRequest {
                files_dir: temp.path(),
                file_id,
                safe_filename: "hello.py",
                expected_size_bytes: 5,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                max_bytes: MAX_INPUT_FILE_BYTES,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            tokio_fs::read(temp.path().join("hello.py")).await.unwrap(),
            b"hello"
        );
        assert!(
            !temp
                .path()
                .join(format!(".download-{file_id}.tmp"))
                .exists()
        );
    }

    #[tokio::test]
    async fn materialization_removes_stale_temp() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();
        let temp_path = temp.path().join(format!(".download-{file_id}.tmp"));

        tokio_fs::write(&temp_path, b"stale").await.unwrap();

        materialize_input_file_from_async_read(
            &b"hello"[..],
            MaterializeInputFileRequest {
                files_dir: temp.path(),
                file_id,
                safe_filename: "hello.py",
                expected_size_bytes: 5,
                expected_sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                max_bytes: MAX_INPUT_FILE_BYTES,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .unwrap();

        assert!(!temp_path.exists());
    }

    #[tokio::test]
    async fn materialization_rejects_final_path_exists() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();

        tokio_fs::write(temp.path().join("hello.py"), b"existing")
            .await
            .unwrap();

        let error = materialize_input_file_from_async_read(
            &b"hello"[..],
            materialize_request(&temp, file_id, 5, CancellationToken::new()),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, MaterializeInputError::FinalPathExists(_)));
    }

    #[tokio::test]
    async fn materialization_rejects_negative_size() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();

        let error = materialize_input_file_from_async_read(
            &b"hello"[..],
            materialize_request(&temp, file_id, -1, CancellationToken::new()),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            MaterializeInputError::InvalidExpectedSize(-1)
        ));
    }

    #[tokio::test]
    async fn materialization_rejects_too_large_metadata() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();

        let error = materialize_input_file_from_async_read(
            &b"hello"[..],
            materialize_request(
                &temp,
                file_id,
                (MAX_INPUT_FILE_BYTES + 1) as i64,
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, MaterializeInputError::TooLarge { .. }));
    }

    #[tokio::test]
    async fn materialization_detects_size_mismatch() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();

        let error = materialize_input_file_from_async_read(
            &b"hello!"[..],
            materialize_request(&temp, file_id, 5, CancellationToken::new()),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, MaterializeInputError::SizeMismatch { .. }));
    }

    #[tokio::test]
    async fn materialization_detects_hash_mismatch() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();

        let error = materialize_input_file_from_async_read(
            &b"HELLO"[..],
            materialize_request(&temp, file_id, 5, CancellationToken::new()),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, MaterializeInputError::HashMismatch { .. }));
    }

    #[tokio::test]
    async fn materialization_detects_cancelled_token() {
        let temp = TempDir::new().unwrap();
        let file_id = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = materialize_input_file_from_async_read(
            &b"hello"[..],
            materialize_request(&temp, file_id, 5, cancel),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, MaterializeInputError::Cancelled));
    }

    #[test]
    fn grader_copy_copies_recursive_bundle() {
        let grader_temp = TempDir::new().unwrap();
        let workspace_temp = TempDir::new().unwrap();

        let source = grader_temp.path().join("intro");
        fs::create_dir_all(source.join("nested")).unwrap();
        fs::write(source.join(job_contract::GRADE_PY), "print('grade')").unwrap();
        fs::write(source.join("nested/helper.py"), "x = 1").unwrap();

        let workspace = test_workspace(&workspace_temp);

        fs::create_dir_all(&workspace.grader_dir).unwrap();

        install_grader_bundle(grader_temp.path(), "intro", &workspace).unwrap();

        assert!(workspace.grader_dir.join(job_contract::GRADE_PY).exists());
        assert!(workspace.grader_dir.join("nested/helper.py").exists());
    }

    #[test]
    fn grader_copy_rejects_missing_grade_py() {
        let grader_temp = TempDir::new().unwrap();
        let workspace_temp = TempDir::new().unwrap();

        fs::create_dir_all(grader_temp.path().join("intro")).unwrap();

        let workspace = test_workspace(&workspace_temp);

        fs::create_dir_all(&workspace.grader_dir).unwrap();

        let error = install_grader_bundle(grader_temp.path(), "intro", &workspace).unwrap_err();

        assert!(matches!(error, WorkspaceError::GradePyMissing(_)));
    }

    #[test]
    fn grader_copy_fails_if_destination_non_empty() {
        let grader_temp = TempDir::new().unwrap();
        let workspace_temp = TempDir::new().unwrap();

        let source = grader_temp.path().join("intro");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join(job_contract::GRADE_PY), "print('grade')").unwrap();

        let workspace = test_workspace(&workspace_temp);

        fs::create_dir_all(&workspace.grader_dir).unwrap();
        fs::write(workspace.grader_dir.join("stale"), "stale").unwrap();

        let error = install_grader_bundle(grader_temp.path(), "intro", &workspace).unwrap_err();

        assert!(matches!(
            error,
            WorkspaceError::GraderDestinationNotEmpty(_)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn grader_copy_rejects_symlink() {
        use std::os::unix::fs::symlink;

        let grader_temp = TempDir::new().unwrap();
        let workspace_temp = TempDir::new().unwrap();

        let source = grader_temp.path().join("intro");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join(job_contract::GRADE_PY), "print('grade')").unwrap();
        symlink(
            source.join(job_contract::GRADE_PY),
            source.join("linked.py"),
        )
        .unwrap();

        let workspace = test_workspace(&workspace_temp);

        fs::create_dir_all(&workspace.grader_dir).unwrap();

        let error = install_grader_bundle(grader_temp.path(), "intro", &workspace).unwrap_err();

        assert!(matches!(error, WorkspaceError::GraderSymlink(_)));
    }

    #[tokio::test]
    async fn result_read_missing() {
        let temp = TempDir::new().unwrap();

        let error = read_bounded_result_json(&temp.path().join(job_contract::RESULT_JSON))
            .await
            .unwrap_err();

        assert!(matches!(error, ResultReadError::Missing));
    }

    #[tokio::test]
    async fn result_read_invalid_json() {
        let temp = TempDir::new().unwrap();
        let result_path = temp.path().join(job_contract::RESULT_JSON);

        tokio_fs::write(&result_path, b"not-json").await.unwrap();

        let error = read_bounded_result_json(&result_path).await.unwrap_err();

        assert!(matches!(error, ResultReadError::InvalidJson(_)));
    }

    #[tokio::test]
    async fn result_read_too_large() {
        let temp = TempDir::new().unwrap();
        let result_path = temp.path().join(job_contract::RESULT_JSON);
        let file = tokio_fs::File::create(&result_path).await.unwrap();

        file.set_len(RESULT_JSON_MAX_BYTES + 1).await.unwrap();

        let error = read_bounded_result_json(&result_path).await.unwrap_err();

        assert!(matches!(error, ResultReadError::TooLarge { .. }));
    }

    #[tokio::test]
    async fn result_read_valid() {
        let temp = TempDir::new().unwrap();
        let result_path = temp.path().join(job_contract::RESULT_JSON);
        let result = json!({
            "schema_version": GRADING_RESULT_SCHEMA_VERSION,
            "status": ResultStatus::Passed.as_str(),
            "score": 1.0,
            "max_score": 1.0,
            "feedback": null,
            "tests": [],
            "artifacts": []
        });

        tokio_fs::write(&result_path, serde_json::to_vec(&result).unwrap())
            .await
            .unwrap();

        let parsed = read_bounded_result_json(&result_path).await.unwrap();

        assert!(matches!(parsed.status, ResultStatus::Passed));
        assert_eq!(parsed.score, 1.0);
    }

    #[tokio::test]
    async fn cleanup_removes_attempt_workspace() {
        let temp = TempDir::new().unwrap();
        let workspace = prepare_attempt_workspace(temp.path(), &test_job(1))
            .await
            .unwrap();

        assert!(workspace.root.exists());

        cleanup_attempt_workspace(&workspace).await.unwrap();

        assert!(!workspace.root.exists());
    }

    #[tokio::test]
    async fn prepare_removes_existing_attempt_workspace() {
        let temp = TempDir::new().unwrap();
        let workspace = prepare_attempt_workspace(temp.path(), &test_job(1))
            .await
            .unwrap();

        let stale_path = workspace.work_dir.join("stale");
        let mut stale = tokio_fs::File::create(&stale_path).await.unwrap();
        stale.write_all(b"stale").await.unwrap();

        let workspace = prepare_attempt_workspace(temp.path(), &test_job(1))
            .await
            .unwrap();

        assert!(!workspace.work_dir.join("stale").exists());
    }
}
