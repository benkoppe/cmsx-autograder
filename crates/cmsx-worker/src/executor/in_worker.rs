use std::{process::Stdio, time::Instant};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_util::sync::CancellationToken;

use cmsx_core::ClaimedJob;

use crate::{
    config::InWorkerExecutorConfig,
    executor::{
        ExecutionOutput, ExecutionStatus,
        utils::{SUMMARY_MAX_BYTES, bytes_to_bounded_summary, parse_timeout_seconds},
    },
    workspace::JobWorkspace,
};

#[derive(Clone)]
pub struct InWorkerExecutor {
    python_command: String,
}

impl InWorkerExecutor {
    pub fn new(config: &InWorkerExecutorConfig) -> Self {
        Self {
            python_command: config.python_command().to_string(),
        }
    }

    pub async fn run(
        &self,
        job: &ClaimedJob,
        workspace: &JobWorkspace,
        cancel: CancellationToken,
    ) -> Result<ExecutionOutput> {
        let timeout_seconds = parse_timeout_seconds(&job.execution_config);
        let started = Instant::now();

        let mut child = Command::new(&self.python_command)
            .arg("-m")
            .arg("cmsx_autograder")
            .arg(workspace.grader_dir.join("grade.py"))
            .current_dir(&workspace.work_dir)
            .env("CMSX_INPUT_DIR", &workspace.input_dir)
            .env("CMSX_WORK_DIR", &workspace.work_dir)
            .env("CMSX_OUTPUT_DIR", &workspace.output_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn {}", self.python_command))?;

        let stdout = child.stdout.take().context("child stdout was not piped")?;
        let stderr = child.stderr.take().context("child stderr was not piped")?;

        let stdout_task = tokio::spawn(read_summary(stdout));
        let stderr_task = tokio::spawn(read_summary(stderr));

        let status = tokio::select! {
            result = child.wait() => {
                let status = result.context("failed waiting for child process")?;
                ExecutionStatus::Exited { code: status.code() }
            }
            _ = tokio::time::sleep(Duration::from_secs(timeout_seconds)) => {
                kill_and_reap(&mut child).await;
                ExecutionStatus::TimedOut
            }
            _ = cancel.cancelled() => {
                kill_and_reap(&mut child).await;
                ExecutionStatus::Cancelled
            }
        };

        let stdout_summary = join_summary(stdout_task).await;
        let stderr_summary = join_summary(stderr_task).await;

        Ok(ExecutionOutput {
            status,
            duration_ms: started.elapsed().as_millis().min(i64::MAX as u128) as i64,
            stdout_summary,
            stderr_summary,
        })
    }
}

async fn kill_and_reap(child: &mut tokio::process::Child) {
    if let Err(error) = child.kill().await {
        tracing::warn!(?error, "failed to kill child process");
    }

    if let Err(error) = child.wait().await {
        tracing::warn!(?error, "failed to reap child process");
    }
}

async fn join_summary(task: JoinHandle<Option<String>>) -> Option<String> {
    match timeout(Duration::from_secs(2), task).await {
        Ok(Ok(summary)) => summary,
        Ok(Err(error)) => {
            tracing::warn!(?error, "output reader task failed");
            None
        }
        Err(_) => {
            tracing::warn!("timed out waiting for output reader task");
            None
        }
    }
}

async fn read_summary<R>(mut reader: R) -> Option<String>
where
    R: AsyncRead + Unpin,
{
    let mut retained = Vec::with_capacity(SUMMARY_MAX_BYTES);
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;

    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                let remaining = SUMMARY_MAX_BYTES.saturating_sub(retained.len());
                if n <= remaining {
                    retained.extend_from_slice(&buffer[..n]);
                } else {
                    if remaining > 0 {
                        retained.extend_from_slice(&buffer[..remaining]);
                    }
                    truncated = true;
                }
            }
            Err(error) => {
                tracing::warn!(?error, "failed reading process output");
                break;
            }
        }
    }

    if retained.is_empty() && !truncated {
        return None;
    }

    Some(bytes_to_bounded_summary(retained, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use serde_json::json;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use cmsx_core::ClaimedJob;

    fn test_job(timeout_seconds: u64) -> ClaimedJob {
        ClaimedJob {
            id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            submission_id: Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
            assignment_id: Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap(),
            assignment_slug: "intro".to_string(),
            lease_expires_at: chrono::Utc::now(),
            attempt: 1,
            execution_config: json!({ "timeout_seconds": timeout_seconds }),
            runner_config: json!({}),
            capabilities: json!({}),
            submission_metadata: json!({}),
            files: Vec::new(),
        }
    }

    fn test_workspace(temp: &TempDir) -> JobWorkspace {
        let root = temp.path().join("job");

        let workspace = JobWorkspace {
            root: root.clone(),
            input_dir: root.join("input"),
            files_dir: root.join("input/files"),
            grader_dir: root.join("grader"),
            work_dir: root.join("work"),
            output_dir: root.join("output"),
            artifacts_dir: root.join("output/artifacts"),
            result_path: root.join("output/result.json"),
        };

        fs::create_dir_all(&workspace.files_dir).unwrap();
        fs::create_dir_all(&workspace.grader_dir).unwrap();
        fs::create_dir_all(&workspace.work_dir).unwrap();
        fs::create_dir_all(&workspace.artifacts_dir).unwrap();
        fs::write(workspace.grader_dir.join("grade.py"), "print('unused')").unwrap();

        workspace
    }

    #[cfg(unix)]
    fn write_executable_script(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, contents).unwrap();

        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    #[cfg(unix)]
    fn script_path(temp: &TempDir, name: &str) -> PathBuf {
        temp.path().join(name)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_timeout_kills_and_reaps() {
        let temp = TempDir::new().unwrap();
        let workspace = test_workspace(&temp);
        let command = script_path(&temp, "fake-python-timeout.sh");

        write_executable_script(
            &command,
            r#"#!/bin/sh
            sleep 10
            "#,
        );

        let executor = InWorkerExecutor {
            python_command: command.display().to_string(),
        };

        let output = executor
            .run(&test_job(1), &workspace, CancellationToken::new())
            .await
            .unwrap();

        assert!(matches!(output.status, ExecutionStatus::TimedOut));
        assert!(output.duration_ms >= 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_cancellation_kills_and_reaps() {
        let temp = TempDir::new().unwrap();
        let workspace = test_workspace(&temp);
        let command = script_path(&temp, "fake-python-cancel.sh");

        write_executable_script(
            &command,
            r#"#!/bin/sh
            sleep 10
            "#,
        );

        let executor = InWorkerExecutor {
            python_command: command.display().to_string(),
        };

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel_for_task.cancel();
        });

        let output = executor
            .run(&test_job(60), &workspace, cancel)
            .await
            .unwrap();

        cancel_task.await.unwrap();

        assert!(matches!(output.status, ExecutionStatus::Cancelled));
        assert!(output.duration_ms >= 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_exit_captures_stdout_and_stderr() {
        let temp = TempDir::new().unwrap();
        let workspace = test_workspace(&temp);
        let command = script_path(&temp, "fake-python-output.sh");

        write_executable_script(
            &command,
            r#"#!/bin/sh
            printf 'hello stdout'
            printf 'hello stderr' >&2
            exit 7
            "#,
        );

        let executor = InWorkerExecutor {
            python_command: command.display().to_string(),
        };

        let output = executor
            .run(&test_job(60), &workspace, CancellationToken::new())
            .await
            .unwrap();

        assert!(matches!(
            output.status,
            ExecutionStatus::Exited { code: Some(7) }
        ));
        assert_eq!(output.stdout_summary.as_deref(), Some("hello stdout"));
        assert_eq!(output.stderr_summary.as_deref(), Some("hello stderr"));
    }
}
