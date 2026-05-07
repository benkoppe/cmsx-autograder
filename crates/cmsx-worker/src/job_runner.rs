use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use reqwest::StatusCode;
use serde_json::json;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use cmsx_core::{
    ClaimedJob, GradingResult, JobEventBatchRequest, JobEventPayload, JobFailureRequest,
    JobResultRequest, ResultStatus,
};

use crate::{
    client::{ClientError, ControlPlaneClient, cap_message_bytes},
    config::WorkerConfig,
    executor::{ExecutionOutput, ExecutionStatus, Executor},
    worker::{CancellationReason, apply_cancellation_reason},
    workspace::{
        JobWorkspace, MAX_INPUT_FILE_BYTES, MaterializeInputError, ResultReadError, WorkspaceError,
        cleanup_attempt_workspace, install_grader_bundle, materialize_input_file_from_async_read,
        prepare_attempt_workspace, read_bounded_result_json,
    },
};

pub const FAILURE_INPUT_DOWNLOAD: &str = "input_download_failed";
pub const FAILURE_INPUT_HASH: &str = "input_hash_mismatch";
pub const FAILURE_GRADER_MISSING: &str = "grader_missing";
pub const FAILURE_EXECUTOR_ERROR: &str = "executor_error";
pub const FAILURE_RESULT_MISSING: &str = "result_missing";
pub const FAILURE_RESULT_INVALID: &str = "result_invalid";
pub const FAILURE_TIMEOUT: &str = "timeout";
pub const FAILURE_LEASE_LOST: &str = "lease_lost";
pub const FAILURE_WORKSPACE_ERROR: &str = "workspace_error";

pub const FAILURE_MESSAGE_MAX_BYTES: usize = 64 * 1024;

struct JobLifecycle {
    client: ControlPlaneClient,
    job: ClaimedJob,
    cancel: CancellationToken,
    reason: Arc<RwLock<CancellationReason>>,
    events_enabled: bool,
    events: JobEventSequencer,
}

struct JobEventSequencer {
    next: i64,
}

impl JobEventSequencer {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> i64 {
        let sequence = self.next;
        self.next += 1;
        sequence
    }
}

pub async fn run_job(
    config: WorkerConfig,
    client: ControlPlaneClient,
    executor: Executor,
    job: ClaimedJob,
    cancel: CancellationToken,
    reason: Arc<RwLock<CancellationReason>>,
) -> Result<()> {
    let mut lifecycle = JobLifecycle {
        client,
        job,
        cancel,
        reason,
        events_enabled: true,
        events: JobEventSequencer::new(),
    };

    let workspace_result =
        prepare_attempt_workspace(config.executor.workspace_root(), &lifecycle.job).await;

    let workspace = match workspace_result {
        Ok(workspace) => workspace,
        Err(error) => {
            if lifecycle.handle_pre_start_cancellation().await {
                return Ok(());
            }

            let failure_reason = match error {
                WorkspaceError::InvalidAttempt(_) => FAILURE_WORKSPACE_ERROR,
                _ => FAILURE_WORKSPACE_ERROR,
            };

            lifecycle
                .post_failed(
                    failure_reason,
                    format_failure_message("workspace preparation failed", &error),
                    false,
                )
                .await;

            return Ok(());
        }
    };

    let lifecycle_result = lifecycle
        .run_prepared_workspace(&config, executor, &workspace)
        .await;

    if let Err(error) = lifecycle_result {
        tracing::error!(
            job_id = %lifecycle.job.id,
            ?error,
            "job lifecycle returned unexpected error"
        );
    }

    if !should_keep_workspaces(&config)
        && let Err(error) = cleanup_attempt_workspace(&workspace).await
    {
        tracing::warn!(
            job_id = %lifecycle.job.id,
            path = %workspace.root.display(),
            ?error,
            "failed to cleanup job workspace"
        );
    }

    Ok(())
}

impl JobLifecycle {
    async fn run_prepared_workspace(
        &mut self,
        config: &WorkerConfig,
        executor: Executor,
        workspace: &JobWorkspace,
    ) -> Result<()> {
        if self.handle_pre_start_cancellation().await {
            return Ok(());
        }

        for file in self.job.files.clone() {
            if self.handle_pre_start_cancellation().await {
                return Ok(());
            }

            let download = match self.client.get_job_file_stream(self.job.id, file.id).await {
                Ok(download) => download,
                Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                    self.mark_cancellation(CancellationReason::LeaseLost).await;
                    self.cancel.cancel();
                    tracing::info!(
                        job_id = %self.job.id,
                        file_id = %file.id,
                        "job file download returned 404; treating job ownership as lost"
                    );
                    return Ok(());
                }
                Err(error) => {
                    self.post_failed(
                        FAILURE_INPUT_DOWNLOAD,
                        format_failure_message("failed to download input file", &error),
                        false,
                    )
                    .await;
                    return Ok(());
                }
            };

            let result = materialize_input_file_from_async_read(
                download.reader,
                &workspace.files_dir,
                file.id,
                &file.safe_filename,
                file.size_bytes,
                &file.content_sha256,
                MAX_INPUT_FILE_BYTES,
                self.cancel.clone(),
            )
            .await;

            if let Err(error) = result {
                self.handle_materialization_error(error).await;
                return Ok(());
            }
        }

        self.post_event(
            "job.input.prepared",
            "worker",
            "staff",
            "Input files prepared",
            json!({}),
        )
        .await;

        if self.handle_pre_start_cancellation().await {
            return Ok(());
        }

        match install_grader_bundle(
            config.executor.grader_root(),
            &self.job.assignment_slug,
            workspace,
        ) {
            Ok(()) => {}
            Err(error) => {
                let reason = match error {
                    WorkspaceError::GraderMissing(_) | WorkspaceError::GradePyMissing(_) => {
                        FAILURE_GRADER_MISSING
                    }
                    _ => FAILURE_WORKSPACE_ERROR,
                };
                self.post_failed(
                    reason,
                    format_failure_message("failed to install grader bundle", &error),
                    false,
                )
                .await;
                return Ok(());
            }
        }

        if self.handle_pre_start_cancellation().await {
            return Ok(());
        }

        match self.client.post_started(self.job.id).await {
            Ok(()) => {}
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                self.handle_post_started_404().await;
                return Ok(());
            }
            Err(error) => {
                self.post_failed(
                    FAILURE_WORKSPACE_ERROR,
                    format_failure_message("control plane rejected job start", &error),
                    false,
                )
                .await;
                return Ok(());
            }
        }

        self.post_event(
            "executor.started",
            "worker",
            "staff",
            "Executor started",
            json!({ "backend": "in-worker" }),
        )
        .await;

        if self.current_reason().await == CancellationReason::LeaseLost {
            return Ok(());
        }

        if self.cancel.is_cancelled() {
            self.handle_cancelled_before_executor().await;
            return Ok(());
        }

        let output = match executor
            .run(&self.job, workspace, self.cancel.clone())
            .await
        {
            Ok(output) => output,
            Err(error) => {
                self.post_failed(
                    FAILURE_EXECUTOR_ERROR,
                    format_failure_message("executor failed", &error),
                    false,
                )
                .await;
                return Ok(());
            }
        };

        self.post_output_events(&output).await;
        self.handle_execution_output(workspace, output).await;

        Ok(())
    }

    async fn handle_execution_output(&mut self, workspace: &JobWorkspace, output: ExecutionOutput) {
        match output.status {
            ExecutionStatus::Exited { code } => {
                let result = read_bounded_result_json(&workspace.result_path).await;
                self.post_event(
                    "result.read",
                    "worker",
                    "staff",
                    "Result file read",
                    json!({ "status": result_read_event_status(&result) }),
                )
                .await;

                match result {
                    Ok(result) => {
                        self.post_result(result, Some(output.duration_ms), &output)
                            .await;
                    }
                    Err(ResultReadError::Missing) => {
                        let reason = if code.is_none() {
                            FAILURE_EXECUTOR_ERROR
                        } else {
                            FAILURE_RESULT_MISSING
                        };

                        let message = match code {
                            Some(code) => format!("result.json missing after grader exit code {code}"),
                            None => {
                                "result.json missing after process exited without status code; likely signal"
                                    .to_string()
                            }
                        };

                        self.post_failed(reason, message, false).await;
                    }
                    Err(error) => {
                        let message = match code {
                            Some(code) => format_failure_message(
                                &format!("result.json invalid after grader exit code {code}"),
                                &error,
                            ),
                            None => format_failure_message(
                                "result.json invalid after process exited without status code",
                                &error,
                            ),
                        };

                        self.post_failed(FAILURE_RESULT_INVALID, message, false)
                            .await;
                    }
                }
            }
            ExecutionStatus::TimedOut => {
                if self.current_reason().await != CancellationReason::LeaseLost {
                    self.post_event("job.timeout", "worker", "staff", "Job timed out", json!({}))
                        .await;
                    self.post_failed(FAILURE_TIMEOUT, "Job timed out".to_string(), false)
                        .await;
                }
            }
            ExecutionStatus::Cancelled => match self.current_reason().await {
                CancellationReason::ControlPlaneCancelled => {
                    self.post_event(
                        "job.cancelled",
                        "worker",
                        "staff",
                        "Job cancelled",
                        json!({}),
                    )
                    .await;

                    match read_bounded_result_json(&workspace.result_path).await {
                        Ok(result) => {
                            self.post_result(result, Some(output.duration_ms), &output)
                                .await;
                        }
                        Err(_) => {
                            self.post_result(
                                explicit_cancelled_result(),
                                Some(output.duration_ms),
                                &output,
                            )
                            .await;
                        }
                    }
                }
                CancellationReason::LeaseLost => {
                    tracing::info!(
                        job_id = %self.job.id,
                        "executor cancelled after lease loss; skipping terminal post"
                    );
                }
                CancellationReason::None => {
                    tracing::warn!(
                        job_id = %self.job.id,
                        "executor returned cancelled without cancellation reason"
                    );
                }
            },
        }
    }

    async fn handle_materialization_error(&mut self, error: MaterializeInputError) {
        match error {
            MaterializeInputError::Cancelled => match self.current_reason().await {
                CancellationReason::ControlPlaneCancelled => {
                    self.post_result(explicit_cancelled_result(), None, &empty_output())
                        .await;
                }
                CancellationReason::LeaseLost => {
                    tracing::info!(
                        job_id = %self.job.id,
                        "input materialization cancelled after lease loss"
                    );
                }
                CancellationReason::None => {
                    tracing::warn!(
                        job_id = %self.job.id,
                        "input materialization cancelled without cancellation reason"
                    );
                }
            },
            MaterializeInputError::HashMismatch { .. } => {
                self.post_failed(
                    FAILURE_INPUT_HASH,
                    format_failure_message("input file hash mismatch", &error),
                    false,
                )
                .await;
            }
            MaterializeInputError::InvalidExpectedHash(_)
            | MaterializeInputError::SizeMismatch { .. }
            | MaterializeInputError::TooLarge { .. }
            | MaterializeInputError::InvalidFilename(_)
            | MaterializeInputError::FinalPathExists(_)
            | MaterializeInputError::InvalidExpectedSize(_)
            | MaterializeInputError::Io(_)
            | MaterializeInputError::Other(_) => {
                self.post_failed(
                    FAILURE_INPUT_DOWNLOAD,
                    format_failure_message("input file materialization failed", &error),
                    false,
                )
                .await;
            }
        }
    }

    async fn handle_pre_start_cancellation(&mut self) -> bool {
        if !self.cancel.is_cancelled() {
            return false;
        }

        match self.current_reason().await {
            CancellationReason::ControlPlaneCancelled => {
                self.post_result(explicit_cancelled_result(), None, &empty_output())
                    .await;
                true
            }
            CancellationReason::LeaseLost => {
                tracing::info!(
                    job_id = %self.job.id,
                    "job cancelled before start after lease loss"
                );
                true
            }
            CancellationReason::None => {
                tracing::warn!(
                    job_id = %self.job.id,
                    "job cancellation observed before start without reason"
                );
                true
            }
        }
    }

    async fn handle_cancelled_before_executor(&mut self) {
        match self.current_reason().await {
            CancellationReason::ControlPlaneCancelled => {
                self.post_result(explicit_cancelled_result(), None, &empty_output())
                    .await;
            }
            CancellationReason::LeaseLost => {
                tracing::info!(
                    job_id = %self.job.id,
                    "job cancelled before executor after lease loss"
                );
            }
            CancellationReason::None => {
                tracing::warn!(
                    job_id = %self.job.id,
                    "job cancelled before executor without cancellation reason"
                );
            }
        }
    }

    async fn handle_post_started_404(&mut self) {
        match self.current_reason().await {
            CancellationReason::ControlPlaneCancelled => {
                self.post_result(explicit_cancelled_result(), None, &empty_output())
                    .await;
            }
            CancellationReason::LeaseLost => {
                tracing::info!(
                    job_id = %self.job.id,
                    "post_started returned 404 after lease loss"
                );
            }
            CancellationReason::None => {
                self.mark_cancellation(CancellationReason::LeaseLost).await;
                self.cancel.cancel();

                tracing::info!(
                    job_id = %self.job.id,
                    "post_started returned 404; marking lease lost"
                );

                let failure = JobFailureRequest {
                    reason: FAILURE_LEASE_LOST.to_string(),
                    message: "job ownership was lost before start".to_string(),
                    retryable: false,
                };

                if let Err(error) = self.client.post_failed(self.job.id, &failure).await {
                    tracing::debug!(
                        job_id = %self.job.id,
                        ?error,
                        "best-effort lease_lost post_failed failed"
                    );
                }
            }
        }
    }

    async fn post_result(
        &mut self,
        result: GradingResult,
        duration_ms: Option<i64>,
        output: &ExecutionOutput,
    ) {
        if self.current_reason().await == CancellationReason::LeaseLost {
            tracing::info!(
                job_id = %self.job.id,
                "skipping result post after lease loss"
            );
            return;
        }

        let request = JobResultRequest {
            result,
            duration_ms,
            stdout_summary: output.stdout_summary.clone(),
            stderr_summary: output.stderr_summary.clone(),
        };

        match self.client.post_result(self.job.id, &request).await {
            Ok(()) => {
                self.events_enabled = false;
            }
            Err(error) if error.is_status(StatusCode::BAD_REQUEST) => {
                let message = rejected_result_message(&error);
                self.post_failed(FAILURE_RESULT_INVALID, message, false)
                    .await;
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "post_result returned 404; ownership lost"
                );
                self.events_enabled = false;
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "post_result failed"
                );
            }
        }
    }

    async fn post_failed(&mut self, reason: &str, message: String, retryable: bool) {
        if self.current_reason().await == CancellationReason::LeaseLost
            && reason != FAILURE_LEASE_LOST
        {
            tracing::info!(
                job_id = %self.job.id,
                reason,
                "skipping failure post after lease loss"
            );
            return;
        }

        let request = JobFailureRequest {
            reason: reason.to_string(),
            message: cap_failure_message(&message),
            retryable,
        };

        match self.client.post_failed(self.job.id, &request).await {
            Ok(()) => {
                self.events_enabled = false;
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "post_failed returned 404; ownership lost"
                );
                self.events_enabled = false;
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "post_failed failed"
                );
            }
        }
    }

    async fn post_event(
        &mut self,
        event_type: &str,
        stream: &str,
        visibility: &str,
        message: &str,
        data: serde_json::Value,
    ) {
        if !self.events_enabled {
            return;
        }

        if self.current_reason().await == CancellationReason::LeaseLost {
            self.events_enabled = false;
            return;
        }

        let request = JobEventBatchRequest {
            events: vec![JobEventPayload {
                sequence: self.events.next(),
                timestamp: Utc::now(),
                event_type: event_type.to_string(),
                stream: stream.to_string(),
                visibility: visibility.to_string(),
                message: message.to_string(),
                data,
            }],
        };

        match self.client.post_events(self.job.id, &request).await {
            Ok(()) => {}
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                self.events_enabled = false;
                self.mark_cancellation(CancellationReason::LeaseLost).await;
                self.cancel.cancel();
                tracing::info!(
                    job_id = %self.job.id,
                    "event post returned 404; marking lease lost"
                );
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    event_type,
                    "event post failed"
                );
            }
        }
    }

    async fn post_output_events(&mut self, output: &ExecutionOutput) {
        if let Some(stdout) = &output.stdout_summary {
            self.post_event(
                "stdout",
                "stdout",
                "staff",
                stdout,
                json!({ "summary": true }),
            )
            .await;
        }

        if let Some(stderr) = &output.stderr_summary {
            self.post_event(
                "stderr",
                "stderr",
                "staff",
                stderr,
                json!({ "summary": true }),
            )
            .await;
        }
    }

    async fn current_reason(&self) -> CancellationReason {
        *self.reason.read().await
    }

    async fn mark_cancellation(&self, next: CancellationReason) {
        let mut current = self.reason.write().await;
        apply_cancellation_reason(&mut current, next);
    }
}

fn should_keep_workspaces(config: &WorkerConfig) -> bool {
    config.executor.keep_workspaces()
}

fn explicit_cancelled_result() -> GradingResult {
    GradingResult {
        schema_version: "1".to_string(),
        status: ResultStatus::Cancelled,
        score: 0.0,
        max_score: 0.0,
        feedback: Some("Job cancelled".to_string()),
        tests: Vec::new(),
        artifacts: Vec::new(),
    }
}

fn empty_output() -> ExecutionOutput {
    ExecutionOutput {
        status: ExecutionStatus::Cancelled,
        duration_ms: 0,
        stdout_summary: None,
        stderr_summary: None,
    }
}

fn result_read_event_status(result: &Result<GradingResult, ResultReadError>) -> &'static str {
    match result {
        Ok(_) => "valid",
        Err(ResultReadError::Missing) => "missing",
        Err(ResultReadError::TooLarge { .. }) => "too_large",
        Err(ResultReadError::Io(_)) => "io_error",
        Err(ResultReadError::InvalidJson(_)) => "invalid_json",
    }
}

fn format_failure_message(prefix: &str, error: &dyn std::fmt::Display) -> String {
    cap_failure_message(&format!("{prefix}: {error}"))
}

fn cap_failure_message(message: &str) -> String {
    cap_message_bytes(message, FAILURE_MESSAGE_MAX_BYTES)
}

fn rejected_result_message(error: &ClientError) -> String {
    let body = error.bounded_body().unwrap_or("");
    cap_failure_message(&format!("control plane rejected result: {body}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_message_caps_after_prefix() {
        let body = "a".repeat(FAILURE_MESSAGE_MAX_BYTES * 2);
        let error = ClientError::Status {
            status: StatusCode::BAD_REQUEST,
            body,
        };

        let message = rejected_result_message(&error);

        assert!(message.len() <= FAILURE_MESSAGE_MAX_BYTES);
        assert!(message.starts_with("control plane rejected result: "));
    }

    #[test]
    fn explicit_cancelled_result_shape() {
        let result = explicit_cancelled_result();

        assert_eq!(result.schema_version, "1");
        assert!(matches!(result.status, ResultStatus::Cancelled));
        assert_eq!(result.score, 0.0);
        assert_eq!(result.max_score, 0.0);
        assert_eq!(result.feedback.as_deref(), Some("Job cancelled"));
        assert!(result.tests.is_empty());
        assert!(result.artifacts.is_empty());
    }
}
