use std::{process::Stdio, time::Instant};

use anyhow::{Context, Result};
use serde::Deserialize;
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
    executor::{ExecutionOutput, ExecutionStatus},
    workspace::JobWorkspace,
};

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const MAX_TIMEOUT_SECONDS: u64 = 60 * 60;
pub const SUMMARY_MAX_BYTES: usize = 64 * 1024;
pub const TRUNCATION_MARKER: &str = "\n...[truncated]";

#[derive(Clone)]
pub struct InWorkerExecutor {
    python_command: String,
}

#[derive(Debug, Deserialize)]
struct ExecutionConfig {
    timeout_seconds: Option<u64>,
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

pub fn parse_timeout_seconds(value: &serde_json::Value) -> u64 {
    let Ok(config) = serde_json::from_value::<ExecutionConfig>(value.clone()) else {
        return DEFAULT_TIMEOUT_SECONDS;
    };

    match config.timeout_seconds {
        None | Some(0) => DEFAULT_TIMEOUT_SECONDS,
        Some(value) => value.min(MAX_TIMEOUT_SECONDS),
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

pub fn bytes_to_bounded_summary(mut bytes: Vec<u8>, truncated: bool) -> String {
    if truncated {
        let marker = TRUNCATION_MARKER.as_bytes();
        let reserved = marker.len().min(SUMMARY_MAX_BYTES);

        if bytes.len() > SUMMARY_MAX_BYTES.saturating_sub(reserved) {
            bytes.truncate(SUMMARY_MAX_BYTES.saturating_sub(reserved));
        }

        bytes.extend_from_slice(marker);
    }

    let summary = String::from_utf8_lossy(&bytes).into_owned();
    cap_summary_string(summary, truncated)
}

pub fn cap_summary_string(mut value: String, truncated: bool) -> String {
    if value.len() <= SUMMARY_MAX_BYTES {
        return value;
    }

    let marker = if truncated { TRUNCATION_MARKER } else { "" };
    let marker_len = marker.len();
    let max_prefix = SUMMARY_MAX_BYTES.saturating_sub(marker_len);

    let mut end = max_prefix;
    while !value.is_char_boundary(end) {
        end -= 1;
    }

    value.truncate(end);
    value.push_str(marker);
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timeout_missing_defaults() {
        assert_eq!(parse_timeout_seconds(&json!({})), DEFAULT_TIMEOUT_SECONDS);
    }

    #[test]
    fn timeout_zero_defaults() {
        assert_eq!(
            parse_timeout_seconds(&json!({"timeout_seconds": 0})),
            DEFAULT_TIMEOUT_SECONDS
        );
    }

    #[test]
    fn timeout_valid_used() {
        assert_eq!(parse_timeout_seconds(&json!({"timeout_seconds": 12})), 12);
    }

    #[test]
    fn timeout_huge_clamped() {
        assert_eq!(
            parse_timeout_seconds(&json!({"timeout_seconds": 999999})),
            MAX_TIMEOUT_SECONDS
        );
    }

    #[test]
    fn summary_adds_marker_within_cap() {
        let bytes = vec![b'a'; SUMMARY_MAX_BYTES + 100];
        let summary = bytes_to_bounded_summary(bytes, true);

        assert!(summary.ends_with(TRUNCATION_MARKER));
        assert!(summary.len() <= SUMMARY_MAX_BYTES);
    }

    #[test]
    fn summary_caps_after_lossy_utf8() {
        let bytes = vec![0xff; SUMMARY_MAX_BYTES];
        let summary = bytes_to_bounded_summary(bytes, false);

        assert!(summary.len() <= SUMMARY_MAX_BYTES);
    }

    #[test]
    fn summary_truncates_on_char_boundary() {
        let value = "é".repeat(SUMMARY_MAX_BYTES);
        let capped = cap_summary_string(value, true);

        assert!(capped.len() <= SUMMARY_MAX_BYTES);
        assert!(capped.ends_with(TRUNCATION_MARKER));
    }
}
