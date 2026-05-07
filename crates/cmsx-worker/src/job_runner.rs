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
        build_workspace_paths, cleanup_attempt_workspace, install_grader_bundle,
        materialize_input_file_from_async_read, prepare_attempt_workspace,
        read_bounded_result_json,
    },
};

pub const FAILURE_INPUT_DOWNLOAD: &str = "input_download_failed";
pub const FAILURE_INPUT_HASH: &str = "input_hash_mismatch";
pub const FAILURE_GRADER_MISSING: &str = "grader_missing";
pub const FAILURE_EXECUTOR_ERROR: &str = "executor_error";
pub const FAILURE_RESULT_MISSING: &str = "result_missing";
pub const FAILURE_RESULT_INVALID: &str = "result_invalid";
pub const FAILURE_CANCELLED_BEFORE_START: &str = "cancelled_before_start";
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
    terminal_posted_or_lost: bool,
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
        terminal_posted_or_lost: false,
        events: JobEventSequencer::new(),
    };

    if lifecycle.handle_pre_start_cancellation().await {
        return Ok(());
    }

    let workspace_result =
        prepare_attempt_workspace(config.executor.workspace_root(), &lifecycle.job).await;

    let workspace = match workspace_result {
        Ok(workspace) => workspace,
        Err(error) => {
            if !should_keep_workspaces(&config) {
                cleanup_after_preparation_failure(&config, &lifecycle.job).await;
            }

            if lifecycle.handle_pre_start_cancellation().await {
                return Ok(());
            }

            let failure = classify_workspace_preparation_error(&error);

            lifecycle
                .post_failed(
                    failure.reason,
                    format_failure_message(failure.message_prefix, &error),
                    failure.retryable,
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
                let failure = classify_grader_install_error(&error);

                self.post_failed(
                    failure.reason,
                    format_failure_message(failure.message_prefix, &error),
                    failure.retryable,
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
                    Err(error) => {
                        let failure = classify_result_read_error(&error, code);
                        let message = match (&error, code) {
                            (ResultReadError::Missing, Some(code)) => {
                                format!("result.json missing after grader exit code {code}")
                            }
                            (ResultReadError::Missing, None) => {
                                "result.json missing after process exited without status code; likely signal"
                                    .to_string()
                            }
                            (_, Some(code)) => format_failure_message(
                                &format!("result.json invalid after grader exit code {code}"),
                                &error,
                            ),
                            (_, None) => format_failure_message(
                                "result.json invalid after process exited without status code",
                                &error,
                            ),
                        };

                        self.post_failed(failure.reason, message, failure.retryable)
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
                let failure = classify_materialize_error(&error);
                self.post_failed(
                    failure.reason,
                    format_failure_message(failure.message_prefix, &error),
                    failure.retryable,
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
                let failure = classify_materialize_error(&error);
                self.post_failed(
                    failure.reason,
                    format_failure_message(failure.message_prefix, &error),
                    failure.retryable,
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
        if !should_post_terminal_for_reason(self.current_reason().await) {
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
                self.stop_network_lifecycle_after_terminal();
            }
            Err(error) if error.is_status(StatusCode::BAD_REQUEST) => {
                let message = rejected_result_message(&error);
                self.post_failed_after_rejected_result(message).await;
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "post_result returned 404; ownership lost"
                );
                self.stop_network_lifecycle_after_terminal();
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

    async fn post_failed_after_rejected_result(&mut self, message: String) {
        let request = JobFailureRequest {
            reason: FAILURE_RESULT_INVALID.to_string(),
            message: cap_failure_message(&message),
            retryable: false,
        };

        match self.client.post_failed(self.job.id, &request).await {
            Ok(()) => {
                self.stop_network_lifecycle_after_terminal();
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "post_failed result_invalid after rejected result returned 404; ownership lost"
                );
                self.stop_network_lifecycle_after_terminal();
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "post_failed result_invalid after rejected result failed"
                );
            }
        }
    }

    async fn post_failed(&mut self, reason: &str, message: String, retryable: bool) {
        if !should_post_terminal_for_reason(self.current_reason().await)
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
                self.stop_network_lifecycle_after_terminal();
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "post_failed returned 404; ownership lost"
                );
                self.stop_network_lifecycle_after_terminal();
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
        if self.terminal_posted_or_lost || !self.events_enabled {
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
            let message = cap_failure_message(stdout);
            self.post_event(
                "stdout",
                "stdout",
                "staff",
                &message,
                json!({ "summary": true }),
            )
            .await;
        }

        if let Some(stderr) = &output.stderr_summary {
            let message = cap_failure_message(stderr);
            self.post_event(
                "stderr",
                "stderr",
                "staff",
                &message,
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

    fn stop_network_lifecycle_after_terminal(&mut self) {
        self.events_enabled = false;
        self.terminal_posted_or_lost = true;
    }
}

fn should_keep_workspaces(config: &WorkerConfig) -> bool {
    config.executor.keep_workspaces()
}

async fn cleanup_after_preparation_failure(config: &WorkerConfig, job: &ClaimedJob) {
    let Ok(workspace) = build_workspace_paths(config.executor.workspace_root(), job) else {
        return;
    };

    if let Err(error) = cleanup_attempt_workspace(&workspace).await {
        tracing::warn!(
            job_id = %job.id,
            path = %workspace.root.display(),
            ?error,
            "failed to cleanup workspace after preparation failure"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureClassification {
    pub reason: &'static str,
    pub message_prefix: &'static str,
    pub retryable: bool,
}

pub fn classify_workspace_preparation_error(error: &WorkspaceError) -> FailureClassification {
    match error {
        WorkspaceError::InvalidAttempt(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "invalid claimed job metadata",
            retryable: false,
        },
        WorkspaceError::Io(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "workspace preparation failed",
            retryable: false,
        },
        WorkspaceError::Json(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "workspace metadata writing failed",
            retryable: false,
        },
        WorkspaceError::InvalidSafeComponent(_)
        | WorkspaceError::GraderMissing(_)
        | WorkspaceError::GradePyMissing(_)
        | WorkspaceError::GraderDestinationNotEmpty(_)
        | WorkspaceError::GraderSymlink(_)
        | WorkspaceError::GraderUnsupportedFileType(_)
        | WorkspaceError::InvalidTrustedPath(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "workspace preparation failed",
            retryable: false,
        },
    }
}

pub fn classify_grader_install_error(error: &WorkspaceError) -> FailureClassification {
    match error {
        WorkspaceError::GraderMissing(_)
        | WorkspaceError::GradePyMissing(_)
        | WorkspaceError::GraderSymlink(_)
        | WorkspaceError::GraderUnsupportedFileType(_) => FailureClassification {
            reason: FAILURE_GRADER_MISSING,
            message_prefix: "grader bundle is invalid or missing",
            retryable: false,
        },
        WorkspaceError::GraderDestinationNotEmpty(_)
        | WorkspaceError::InvalidTrustedPath(_)
        | WorkspaceError::InvalidSafeComponent(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "grader workspace setup failed",
            retryable: false,
        },
        WorkspaceError::Io(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "grader bundle copy failed",
            retryable: false,
        },
        WorkspaceError::Json(_) | WorkspaceError::InvalidAttempt(_) => FailureClassification {
            reason: FAILURE_WORKSPACE_ERROR,
            message_prefix: "unexpected grader workspace error",
            retryable: false,
        },
    }
}

pub fn classify_materialize_error(error: &MaterializeInputError) -> FailureClassification {
    match error {
        MaterializeInputError::HashMismatch { .. } => FailureClassification {
            reason: FAILURE_INPUT_HASH,
            message_prefix: "input file hash mismatch",
            retryable: false,
        },
        MaterializeInputError::InvalidExpectedHash(_) => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "bad input hash metadata from control plane",
            retryable: false,
        },
        MaterializeInputError::SizeMismatch { .. } => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "input file size mismatch",
            retryable: false,
        },
        MaterializeInputError::TooLarge { .. } => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "input file exceeds worker size limit",
            retryable: false,
        },
        MaterializeInputError::InvalidFilename(_) => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "invalid input filename",
            retryable: false,
        },
        MaterializeInputError::FinalPathExists(_) => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "input final path already exists",
            retryable: false,
        },
        MaterializeInputError::InvalidExpectedSize(_) => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "bad input size metadata from control plane",
            retryable: false,
        },
        MaterializeInputError::Io(_) | MaterializeInputError::Other(_) => FailureClassification {
            reason: FAILURE_INPUT_DOWNLOAD,
            message_prefix: "input file materialization failed",
            retryable: false,
        },
        MaterializeInputError::Cancelled => FailureClassification {
            reason: FAILURE_CANCELLED_BEFORE_START,
            message_prefix: "input materialization cancelled",
            retryable: false,
        },
    }
}

pub fn classify_result_read_error(
    error: &ResultReadError,
    process_code: Option<i32>,
) -> FailureClassification {
    match error {
        ResultReadError::Missing if process_code.is_none() => FailureClassification {
            reason: FAILURE_EXECUTOR_ERROR,
            message_prefix: "process exited without status code and result.json is missing",
            retryable: false,
        },
        ResultReadError::Missing => FailureClassification {
            reason: FAILURE_RESULT_MISSING,
            message_prefix: "result.json is missing",
            retryable: false,
        },
        ResultReadError::TooLarge { .. }
        | ResultReadError::Io(_)
        | ResultReadError::InvalidJson(_) => FailureClassification {
            reason: FAILURE_RESULT_INVALID,
            message_prefix: "result.json is invalid",
            retryable: false,
        },
    }
}

pub fn classify_execution_status(status: &ExecutionStatus) -> FailureClassification {
    match status {
        ExecutionStatus::TimedOut => FailureClassification {
            reason: FAILURE_TIMEOUT,
            message_prefix: "job timed out",
            retryable: false,
        },
        ExecutionStatus::Cancelled => FailureClassification {
            reason: FAILURE_CANCELLED_BEFORE_START,
            message_prefix: "job cancelled",
            retryable: false,
        },
        ExecutionStatus::Exited { code: None } => FailureClassification {
            reason: FAILURE_EXECUTOR_ERROR,
            message_prefix: "process exited without status code",
            retryable: false,
        },
        ExecutionStatus::Exited { code: Some(_) } => FailureClassification {
            reason: FAILURE_RESULT_MISSING,
            message_prefix: "grader exited without a valid result",
            retryable: false,
        },
    }
}

pub fn should_read_result_after_execution(status: &ExecutionStatus) -> bool {
    matches!(status, ExecutionStatus::Exited { .. })
}

pub fn should_post_terminal_for_reason(reason: CancellationReason) -> bool {
    !matches!(reason, CancellationReason::LeaseLost)
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

    #[test]
    fn workspace_preparation_invalid_attempt_is_workspace_error() {
        let error = WorkspaceError::InvalidAttempt(0);
        let classification = classify_workspace_preparation_error(&error);

        assert_eq!(classification.reason, FAILURE_WORKSPACE_ERROR);
        assert!(!classification.retryable);
    }

    #[test]
    fn grader_missing_maps_to_grader_missing() {
        let error = WorkspaceError::GraderMissing("missing".to_string());
        let classification = classify_grader_install_error(&error);

        assert_eq!(classification.reason, FAILURE_GRADER_MISSING);
        assert!(!classification.retryable);
    }

    #[test]
    fn grader_symlink_maps_to_grader_missing() {
        let error = WorkspaceError::GraderSymlink("linked.py".to_string());
        let classification = classify_grader_install_error(&error);

        assert_eq!(classification.reason, FAILURE_GRADER_MISSING);
        assert!(!classification.retryable);
    }

    #[test]
    fn materialize_hash_mismatch_maps_to_input_hash() {
        let error = MaterializeInputError::HashMismatch {
            expected: "expected".to_string(),
            actual: "actual".to_string(),
        };
        let classification = classify_materialize_error(&error);

        assert_eq!(classification.reason, FAILURE_INPUT_HASH);
        assert!(!classification.retryable);
    }

    #[test]
    fn materialize_invalid_expected_hash_maps_to_download_failure() {
        let error = MaterializeInputError::InvalidExpectedHash("bad".to_string());
        let classification = classify_materialize_error(&error);

        assert_eq!(classification.reason, FAILURE_INPUT_DOWNLOAD);
        assert!(classification.message_prefix.contains("metadata"));
        assert!(!classification.retryable);
    }

    #[test]
    fn result_missing_after_signal_maps_to_executor_error() {
        let error = ResultReadError::Missing;
        let classification = classify_result_read_error(&error, None);

        assert_eq!(classification.reason, FAILURE_EXECUTOR_ERROR);
        assert!(!classification.retryable);
    }

    #[test]
    fn result_missing_after_exit_code_maps_to_result_missing() {
        let error = ResultReadError::Missing;
        let classification = classify_result_read_error(&error, Some(1));

        assert_eq!(classification.reason, FAILURE_RESULT_MISSING);
        assert!(!classification.retryable);
    }

    #[test]
    fn invalid_result_maps_to_result_invalid() {
        let error = serde_json::from_str::<GradingResult>("not-json").unwrap_err();
        let classification =
            classify_result_read_error(&ResultReadError::InvalidJson(error), Some(0));

        assert_eq!(classification.reason, FAILURE_RESULT_INVALID);
        assert!(!classification.retryable);
    }

    #[test]
    fn timeout_status_maps_to_timeout() {
        let classification = classify_execution_status(&ExecutionStatus::TimedOut);

        assert_eq!(classification.reason, FAILURE_TIMEOUT);
        assert!(!classification.retryable);
    }

    #[test]
    fn signal_exit_status_maps_to_executor_error() {
        let classification = classify_execution_status(&ExecutionStatus::Exited { code: None });

        assert_eq!(classification.reason, FAILURE_EXECUTOR_ERROR);
        assert!(!classification.retryable);
    }

    #[test]
    fn exited_execution_should_read_result() {
        assert!(should_read_result_after_execution(
            &ExecutionStatus::Exited { code: Some(0) }
        ));
        assert!(should_read_result_after_execution(
            &ExecutionStatus::Exited { code: None }
        ));
    }

    #[test]
    fn timeout_and_cancelled_should_not_read_result_by_default() {
        assert!(!should_read_result_after_execution(
            &ExecutionStatus::TimedOut
        ));
        assert!(!should_read_result_after_execution(
            &ExecutionStatus::Cancelled
        ));
    }

    #[test]
    fn terminal_posts_are_not_allowed_after_lease_loss() {
        assert!(should_post_terminal_for_reason(CancellationReason::None));
        assert!(should_post_terminal_for_reason(
            CancellationReason::ControlPlaneCancelled
        ));
        assert!(!should_post_terminal_for_reason(
            CancellationReason::LeaseLost
        ));
    }

    #[test]
    fn cap_failure_message_preserves_char_boundary() {
        let value = "é".repeat(FAILURE_MESSAGE_MAX_BYTES);
        let capped = cap_failure_message(&value);

        assert!(capped.len() <= FAILURE_MESSAGE_MAX_BYTES);
        assert!(capped.is_char_boundary(capped.len()));
    }

    #[test]
    fn rejected_result_message_handles_empty_body() {
        let error = ClientError::Status {
            status: StatusCode::BAD_REQUEST,
            body: String::new(),
        };

        let message = rejected_result_message(&error);

        assert_eq!(message, "control plane rejected result: ");
    }
}
