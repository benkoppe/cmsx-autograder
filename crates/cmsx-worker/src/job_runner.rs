use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use reqwest::StatusCode;
use serde_json::json;
use tokio::{
    fs as tokio_fs,
    sync::{RwLock, mpsc, oneshot},
    time::{Duration, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use cmsx_core::{
    ClaimedJob, GradingResult, JobEventBatchRequest, JobEventPayload, JobFailureRequest,
    JobResultRequest, ResultStatus,
    protocol::{
        ARTIFACT_MAX_COUNT, JOB_EVENT_MESSAGE_MAX_BYTES, cap_text, failure_reason, job_event_type,
        validate_artifact_label, validate_artifact_relative_path,
    },
};

use crate::{
    client::{ArtifactUpload, ArtifactUploadOutcome, ClientError, ControlPlaneClient},
    config::WorkerConfig,
    events::{ExecutorEvent, ExecutorEventSink, JobEventWriterCommand},
    executor::{ExecutionOutput, ExecutionStatus, Executor},
    worker::{CancellationReason, apply_cancellation_reason},
    workspace::{
        DiscoveredArtifact, JobWorkspace, MAX_INPUT_FILE_BYTES, MaterializeInputError,
        MaterializeInputFileRequest, ResultReadError, WorkspaceError, build_workspace_paths,
        cleanup_attempt_workspace, discover_artifacts, hash_artifact_file_with_revalidation,
        install_grader_bundle, materialize_input_file_from_async_read, prepare_attempt_workspace,
        read_bounded_result_json,
    },
};

pub const FAILURE_INPUT_DOWNLOAD: &str = failure_reason::INPUT_DOWNLOAD_FAILED;
pub const FAILURE_INPUT_HASH: &str = failure_reason::INPUT_HASH_MISMATCH;
pub const FAILURE_GRADER_MISSING: &str = failure_reason::GRADER_MISSING;
pub const FAILURE_EXECUTOR_ERROR: &str = failure_reason::EXECUTOR_ERROR;
pub const FAILURE_RESULT_MISSING: &str = failure_reason::RESULT_MISSING;
pub const FAILURE_RESULT_INVALID: &str = failure_reason::RESULT_INVALID;
pub const FAILURE_CANCELLED_BEFORE_START: &str = failure_reason::CANCELLED_BEFORE_START;
pub const FAILURE_TIMEOUT: &str = failure_reason::TIMEOUT;
pub const FAILURE_LEASE_LOST: &str = failure_reason::LEASE_LOST;
pub const FAILURE_WORKSPACE_ERROR: &str = failure_reason::WORKSPACE_ERROR;
pub const FAILURE_ARTIFACT_INVALID: &str = failure_reason::ARTIFACT_INVALID;
pub const FAILURE_ARTIFACT_UPLOAD_FAILED: &str = failure_reason::ARTIFACT_UPLOAD_FAILED;

pub const FAILURE_MESSAGE_MAX_BYTES: usize = JOB_EVENT_MESSAGE_MAX_BYTES;

struct JobLifecycle {
    client: ControlPlaneClient,
    job: ClaimedJob,
    cancel: CancellationToken,
    reason: Arc<RwLock<CancellationReason>>,
    event_writer: JobEventWriterHandle,
}

const EVENT_BATCH_TARGET_EVENTS: usize = 50;
const EVENT_BATCH_FLUSH_APPROX_BYTES: usize = JOB_EVENT_MESSAGE_MAX_BYTES;
const EVENT_FLUSH_INTERVAL_MS: u64 = 250;

const ARTIFACT_UPLOAD_MAX_ATTEMPTS: usize = 3;
const ARTIFACT_UPLOAD_RETRY_BACKOFF_MS: u64 = 500;

#[derive(Clone)]
struct JobEventWriterHandle {
    sender: mpsc::UnboundedSender<JobEventWriterCommand>,
}

struct JobEventWriter {
    client: ControlPlaneClient,
    job_id: Uuid,
    receiver: mpsc::UnboundedReceiver<JobEventWriterCommand>,
    sequence: i64,
    reason: Arc<RwLock<CancellationReason>>,
    cancel: CancellationToken,
    events_enabled: bool,
    buffer: Vec<JobEventPayload>,
    buffered_bytes: usize,
}

impl JobEventWriterHandle {
    fn spawn(
        client: ControlPlaneClient,
        job_id: Uuid,
        reason: Arc<RwLock<CancellationReason>>,
        cancel: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        let writer = JobEventWriter {
            client,
            job_id,
            receiver,
            sequence: 0,
            reason,
            cancel,
            events_enabled: true,
            buffer: Vec::new(),
            buffered_bytes: 0,
        };

        tokio::spawn(writer.run());

        Self { sender }
    }

    fn sink(&self) -> ExecutorEventSink {
        ExecutorEventSink::new(self.sender.clone())
    }

    fn emit(&self, event: ExecutorEvent) {
        if self
            .sender
            .send(JobEventWriterCommand::Event(event))
            .is_err()
        {
            tracing::debug!("job event writer is closed; dropping event");
        }
    }

    async fn flush(&self) {
        let (sender, receiver) = oneshot::channel();

        if self
            .sender
            .send(JobEventWriterCommand::Flush(sender))
            .is_err()
        {
            return;
        }

        let _ = receiver.await;
    }

    fn disable(&self) {
        let _ = self.sender.send(JobEventWriterCommand::Disable);
    }

    async fn shutdown(&self) {
        let (sender, receiver) = oneshot::channel();

        if self
            .sender
            .send(JobEventWriterCommand::Shutdown(sender))
            .is_err()
        {
            return;
        }

        let _ = receiver.await;
    }
}

impl JobEventWriter {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(EVENT_FLUSH_INTERVAL_MS));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.flush_buffer().await;
                }
                command = self.receiver.recv() => {
                    match command {
                        Some(JobEventWriterCommand::Event(event)) => {
                            self.push_event(event).await;
                        }
                        Some(JobEventWriterCommand::Flush(reply)) => {
                            self.flush_buffer().await;
                            let _ = reply.send(());
                        }
                        Some(JobEventWriterCommand::Disable) => {
                            self.events_enabled = false;
                            self.buffer.clear();
                            self.buffered_bytes = 0;
                        }
                        Some(JobEventWriterCommand::Shutdown(reply)) => {
                            self.flush_buffer().await;
                            self.events_enabled = false;
                            let _ = reply.send(());
                            break;
                        }
                        None => {
                            self.flush_buffer().await;
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn push_event(&mut self, event: ExecutorEvent) {
        if !self.events_enabled {
            return;
        }

        if *self.reason.read().await == CancellationReason::LeaseLost {
            self.events_enabled = false;
            self.buffer.clear();
            self.buffered_bytes = 0;
            return;
        }

        let message_len = event.message.len();
        let payload = event.into_payload(self.sequence);
        self.sequence += 1;

        self.buffered_bytes = self.buffered_bytes.saturating_add(message_len);
        self.buffer.push(payload);

        if self.buffer.len() >= EVENT_BATCH_TARGET_EVENTS
            || self.buffered_bytes >= EVENT_BATCH_FLUSH_APPROX_BYTES
        {
            self.flush_buffer().await;
        }
    }

    async fn flush_buffer(&mut self) {
        if self.buffer.is_empty() || !self.events_enabled {
            return;
        }

        let events = std::mem::take(&mut self.buffer);
        self.buffered_bytes = 0;

        let request = JobEventBatchRequest { events };

        match self.client.post_events(self.job_id, &request).await {
            Ok(()) => {}
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                self.events_enabled = false;
                self.mark_lease_lost().await;
                self.cancel.cancel();
                tracing::info!(
                    job_id = %self.job_id,
                    "event batch post returned 404; marking lease lost"
                );
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job_id,
                    ?error,
                    "event batch post failed"
                );
            }
        }
    }

    async fn mark_lease_lost(&self) {
        let mut current = self.reason.write().await;
        apply_cancellation_reason(&mut current, CancellationReason::LeaseLost);
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
    let event_writer =
        JobEventWriterHandle::spawn(client.clone(), job.id, reason.clone(), cancel.clone());

    let mut lifecycle = JobLifecycle {
        client,
        job,
        cancel,
        reason,
        event_writer,
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

    lifecycle.event_writer.shutdown().await;

    Ok(())
}

#[derive(Debug)]
enum ResultReportOutcome {
    Accepted,
    CancellationRequested,
    OwnershipLost,
    Rejected(String),
    Failed,
}

enum ArtifactPreparationOutcome {
    Uploaded,
    Failed {
        reason: &'static str,
        message: String,
        retryable: bool,
    },
    OwnershipLost,
    CancellationRequested,
}

enum ArtifactUploadLifecycleOutcome {
    Uploaded,
    OwnershipLost,
    CancellationRequested,
    Failed { message: String, retryable: bool },
    Invalid { message: String },
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
                MaterializeInputFileRequest {
                    files_dir: &workspace.files_dir,
                    file_id: file.id,
                    safe_filename: &file.safe_filename,
                    expected_size_bytes: file.size_bytes,
                    expected_sha256: &file.content_sha256,
                    max_bytes: MAX_INPUT_FILE_BYTES,
                    cancel: self.cancel.clone(),
                },
            )
            .await;

            if let Err(error) = result {
                self.handle_materialization_error(error).await;
                return Ok(());
            }
        }

        self.post_event(
            job_event_type::JOB_INPUT_PREPARED,
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
            Err(error) if error.is_status(StatusCode::CONFLICT) => {
                self.mark_cancellation(CancellationReason::ControlPlaneCancelled)
                    .await;
                self.cancel.cancel();
                self.handle_cancelled_before_executor().await;
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

        let executor_backend = executor.backend_name();

        self.post_event(
            job_event_type::EXECUTOR_STARTED,
            "Executor started",
            json!({ "backend": executor_backend }),
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
            .run(
                &self.job,
                workspace,
                self.cancel.clone(),
                self.event_writer.sink(),
            )
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

        self.handle_execution_output(workspace, output).await;

        Ok(())
    }

    async fn handle_execution_output(&mut self, workspace: &JobWorkspace, output: ExecutionOutput) {
        match self.current_reason().await {
            CancellationReason::ControlPlaneCancelled => {
                self.post_control_plane_cancelled_result(&output).await;
                return;
            }
            CancellationReason::LeaseLost
                if matches!(output.status, ExecutionStatus::Cancelled) =>
            {
                tracing::info!(
                    job_id = %self.job.id,
                    "executor cancelled after lease loss; skipping terminal post"
                );
                return;
            }
            _ => {}
        }

        match output.status {
            ExecutionStatus::Exited { code } => {
                let result = read_bounded_result_json(&workspace.result_path).await;
                self.post_event(
                    job_event_type::RESULT_READ,
                    "Result file read",
                    json!({ "status": result_read_event_status(&result) }),
                )
                .await;

                match result {
                    Ok(result) => {
                        match self.prepare_and_upload_artifacts(workspace, &result).await {
                            ArtifactPreparationOutcome::Uploaded => {
                                self.report_result(result, Some(output.duration_ms), &output)
                                    .await;
                            }
                            ArtifactPreparationOutcome::Failed {
                                reason,
                                message,
                                retryable,
                            } => {
                                self.event_writer.flush().await;
                                self.post_failed(reason, message, retryable).await;
                            }
                            ArtifactPreparationOutcome::OwnershipLost => {
                                tracing::info!(
                                    job_id = %self.job.id,
                                    "artifact upload stopped after job ownership was lost"
                                );
                            }
                            ArtifactPreparationOutcome::CancellationRequested => {
                                self.report_cancelled_result(&output).await;
                            }
                        }
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
                    self.post_event(job_event_type::JOB_TIMEOUT, "Job timed out", json!({}))
                        .await;
                    self.post_failed(FAILURE_TIMEOUT, "Job timed out".to_string(), false)
                        .await;
                }
            }
            ExecutionStatus::Cancelled => match self.current_reason().await {
                CancellationReason::LeaseLost => {
                    tracing::info!(
                        job_id = %self.job.id,
                        "executor cancelled after lease loss; skipping terminal post"
                    );
                }
                CancellationReason::ControlPlaneCancelled => {
                    self.post_control_plane_cancelled_result(&output).await;
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
                    self.report_result(GradingResult::cancelled(), None, &empty_output())
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
                self.report_result(GradingResult::cancelled(), None, &empty_output())
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
                self.report_result(GradingResult::cancelled(), None, &empty_output())
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
                self.report_result(GradingResult::cancelled(), None, &empty_output())
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

                self.event_writer.flush().await;

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

    async fn prepare_and_upload_artifacts(
        &mut self,
        workspace: &JobWorkspace,
        result: &GradingResult,
    ) -> ArtifactPreparationOutcome {
        let artifacts = match discover_artifacts(workspace).await {
            Ok(artifacts) => artifacts,
            Err(error) => {
                self.post_event(
                    job_event_type::ARTIFACT_REJECTED,
                    "Artifact discovery failed",
                    json!({ "error": error.to_string(), "attempt": self.job.attempt }),
                )
                .await;

                return ArtifactPreparationOutcome::Failed {
                    reason: FAILURE_ARTIFACT_INVALID,
                    message: format_failure_message("artifact discovery failed", &error),
                    retryable: false,
                };
            }
        };

        if let Err(message) = validate_result_artifact_refs_for_worker(result, &artifacts) {
            self.post_event(
                job_event_type::ARTIFACT_REJECTED,
                "Artifact references are invalid",
                json!({ "error": message, "attempt": self.job.attempt }),
            )
            .await;

            return ArtifactPreparationOutcome::Failed {
                reason: FAILURE_ARTIFACT_INVALID,
                message,
                retryable: false,
            };
        }

        self.post_event(
            job_event_type::ARTIFACT_DISCOVERED,
            "Artifacts discovered",
            json!({
                "count": artifacts.len(),
                "attempt": self.job.attempt,
            }),
        )
        .await;

        for artifact in artifacts {
            match self
                .upload_artifact_with_retries(workspace, &artifact)
                .await
            {
                ArtifactUploadLifecycleOutcome::Uploaded => {}
                ArtifactUploadLifecycleOutcome::OwnershipLost => {
                    return ArtifactPreparationOutcome::OwnershipLost;
                }
                ArtifactUploadLifecycleOutcome::CancellationRequested => {
                    return ArtifactPreparationOutcome::CancellationRequested;
                }
                ArtifactUploadLifecycleOutcome::Failed { message, retryable } => {
                    self.post_event(
                        job_event_type::ARTIFACT_UPLOAD_FAILED,
                        "Artifact upload failed",
                        json!({
                            "path": artifact.relative_path,
                            "attempt": self.job.attempt,
                            "retryable": retryable,
                            "message": message,
                        }),
                    )
                    .await;

                    return ArtifactPreparationOutcome::Failed {
                        reason: FAILURE_ARTIFACT_UPLOAD_FAILED,
                        message,
                        retryable,
                    };
                }
                ArtifactUploadLifecycleOutcome::Invalid { message } => {
                    self.post_event(
                        job_event_type::ARTIFACT_REJECTED,
                        "Artifact is invalid",
                        json!({
                            "path": artifact.relative_path,
                            "attempt": self.job.attempt,
                            "message": message,
                        }),
                    )
                    .await;

                    return ArtifactPreparationOutcome::Failed {
                        reason: FAILURE_ARTIFACT_INVALID,
                        message,
                        retryable: false,
                    };
                }
            }
        }

        ArtifactPreparationOutcome::Uploaded
    }

    async fn upload_artifact_with_retries(
        &mut self,
        workspace: &JobWorkspace,
        artifact: &DiscoveredArtifact,
    ) -> ArtifactUploadLifecycleOutcome {
        let mut last_message = None;

        for attempt in 1..=ARTIFACT_UPLOAD_MAX_ATTEMPTS {
            if self.cancel.is_cancelled() {
                return match self.current_reason().await {
                    CancellationReason::ControlPlaneCancelled => {
                        ArtifactUploadLifecycleOutcome::CancellationRequested
                    }
                    CancellationReason::LeaseLost => ArtifactUploadLifecycleOutcome::OwnershipLost,
                    CancellationReason::None => ArtifactUploadLifecycleOutcome::Failed {
                        message: "artifact upload cancelled without reason".to_string(),
                        retryable: true,
                    },
                };
            }

            let bytes = match read_artifact_bytes_for_upload(workspace, artifact).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    return ArtifactUploadLifecycleOutcome::Invalid {
                        message: format_failure_message("artifact changed before upload", &error),
                    };
                }
            };

            let request = ArtifactUpload {
                artifact_id: artifact.id,
                relative_path: &artifact.relative_path,
                sha256: &artifact.sha256,
                size_bytes: artifact.size_bytes,
                bytes,
            };

            let upload = self.client.put_artifact(self.job.id, request).await;

            match upload {
                Ok(ArtifactUploadOutcome::Uploaded) => {
                    self.post_event(
                        job_event_type::ARTIFACT_UPLOADED,
                        "Artifact uploaded",
                        json!({
                            "path": artifact.relative_path,
                            "artifact_id": artifact.id,
                            "attempt": self.job.attempt,
                        }),
                    )
                    .await;

                    return ArtifactUploadLifecycleOutcome::Uploaded;
                }
                Ok(ArtifactUploadOutcome::JobNotActive) => {
                    self.mark_cancellation(CancellationReason::LeaseLost).await;
                    self.cancel.cancel();
                    return ArtifactUploadLifecycleOutcome::OwnershipLost;
                }
                Ok(ArtifactUploadOutcome::CancellationRequested) => {
                    self.mark_cancellation(CancellationReason::ControlPlaneCancelled)
                        .await;
                    self.cancel.cancel();
                    return ArtifactUploadLifecycleOutcome::CancellationRequested;
                }
                Ok(ArtifactUploadOutcome::NonRetryableFailure { body, .. }) => {
                    return ArtifactUploadLifecycleOutcome::Failed {
                        message: cap_failure_message(&body),
                        retryable: false,
                    };
                }
                Ok(ArtifactUploadOutcome::RetryableFailure { body, .. }) => {
                    last_message = Some(cap_failure_message(&body));
                }
                Err(error) => {
                    last_message = Some(format_failure_message(
                        "artifact upload request failed",
                        &error,
                    ));
                }
            }

            if attempt < ARTIFACT_UPLOAD_MAX_ATTEMPTS {
                let sleep =
                    tokio::time::sleep(Duration::from_millis(ARTIFACT_UPLOAD_RETRY_BACKOFF_MS));
                tokio::pin!(sleep);

                tokio::select! {
                    _ = &mut sleep => {}
                    _ = self.cancel.cancelled() => {}
                }
            }
        }

        ArtifactUploadLifecycleOutcome::Failed {
            message: last_message.unwrap_or_else(|| "artifact upload failed".to_string()),
            retryable: true,
        }
    }

    async fn report_result(
        &mut self,
        result: GradingResult,
        duration_ms: Option<i64>,
        output: &ExecutionOutput,
    ) {
        let is_cancelled_result = matches!(result.status, ResultStatus::Cancelled);

        match self.report_result_once(result, duration_ms, output).await {
            ResultReportOutcome::Accepted
            | ResultReportOutcome::OwnershipLost
            | ResultReportOutcome::Failed => {}
            ResultReportOutcome::Rejected(message) => {
                self.post_failed_after_rejected_result(message).await;
            }
            ResultReportOutcome::CancellationRequested if is_cancelled_result => {
                tracing::warn!(
                    job_id = %self.job.id,
                    "cancelled result conflicted after cancellation was already requested"
                );
            }
            ResultReportOutcome::CancellationRequested => {
                self.report_cancelled_result_after_conflict(output).await;
            }
        }
    }

    async fn report_result_once(
        &mut self,
        result: GradingResult,
        duration_ms: Option<i64>,
        output: &ExecutionOutput,
    ) -> ResultReportOutcome {
        if !should_post_terminal_for_reason(self.current_reason().await) {
            tracing::info!(
                job_id = %self.job.id,
                "skipping result report after lease loss"
            );
            return ResultReportOutcome::OwnershipLost;
        }

        self.event_writer.flush().await;

        let request = JobResultRequest {
            result,
            duration_ms,
            stdout_summary: output.stdout_summary.clone(),
            stderr_summary: output.stderr_summary.clone(),
        };

        match self.client.post_result(self.job.id, &request).await {
            Ok(()) => {
                self.stop_network_lifecycle_after_terminal().await;
                ResultReportOutcome::Accepted
            }
            Err(error) if error.is_status(StatusCode::BAD_REQUEST) => {
                ResultReportOutcome::Rejected(rejected_result_message(&error))
            }
            Err(error) if error.is_status(StatusCode::CONFLICT) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "result report conflicted with control-plane cancellation"
                );
                ResultReportOutcome::CancellationRequested
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "result report returned 404; ownership lost"
                );
                self.stop_network_lifecycle_after_terminal().await;
                ResultReportOutcome::OwnershipLost
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "result report failed"
                );
                ResultReportOutcome::Failed
            }
        }
    }

    async fn post_failed_after_rejected_result(&mut self, message: String) {
        self.event_writer.flush().await;

        let request = JobFailureRequest {
            reason: FAILURE_RESULT_INVALID.to_string(),
            message: cap_failure_message(&message),
            retryable: false,
        };

        match self.client.post_failed(self.job.id, &request).await {
            Ok(()) => {
                self.stop_network_lifecycle_after_terminal().await;
            }
            Err(error) if error.is_status(StatusCode::CONFLICT) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "result-invalid failure report conflicted with control-plane cancellation"
                );

                let output = empty_output();
                self.report_cancelled_result_after_conflict(&output).await;
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "result-invalid failure report returned 404; ownership lost"
                );
                self.stop_network_lifecycle_after_terminal().await;
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "result-invalid failure report failed"
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
                "skipping failure report after lease loss"
            );
            return;
        }

        self.event_writer.flush().await;

        let request = JobFailureRequest {
            reason: reason.to_string(),
            message: cap_failure_message(&message),
            retryable,
        };

        match self.client.post_failed(self.job.id, &request).await {
            Ok(()) => {
                self.stop_network_lifecycle_after_terminal().await;
            }
            Err(error) if error.is_status(StatusCode::CONFLICT) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "failure report conflicted with control-plane cancellation"
                );
                let output = empty_output();
                self.report_cancelled_result_after_conflict(&output).await;
            }
            Err(error) if error.is_status(StatusCode::NOT_FOUND) => {
                tracing::info!(
                    job_id = %self.job.id,
                    ?error,
                    "failure report returned 404; ownership lost"
                );
                self.stop_network_lifecycle_after_terminal().await;
            }
            Err(error) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    ?error,
                    "failure report failed"
                );
            }
        }
    }

    async fn report_cancelled_result_after_conflict(&mut self, output: &ExecutionOutput) {
        self.mark_cancellation(CancellationReason::ControlPlaneCancelled)
            .await;
        self.cancel.cancel();

        tracing::info!(
            job_id = %self.job.id,
            "terminal report conflicted with cancellation; reporting cancelled result"
        );

        self.report_cancelled_result(output).await;
    }

    async fn report_cancelled_result(&mut self, output: &ExecutionOutput) {
        self.post_event(job_event_type::JOB_CANCELLED, "Job cancelled", json!({}))
            .await;

        match self
            .report_result_once(GradingResult::cancelled(), Some(output.duration_ms), output)
            .await
        {
            ResultReportOutcome::Accepted
            | ResultReportOutcome::OwnershipLost
            | ResultReportOutcome::Failed => {}
            ResultReportOutcome::CancellationRequested => {
                tracing::warn!(
                    job_id = %self.job.id,
                    "cancelled result conflicted after cancellation was already requested"
                );
            }
            ResultReportOutcome::Rejected(message) => {
                tracing::warn!(
                    job_id = %self.job.id,
                    message,
                    "cancelled result was rejected by control plane"
                );
            }
        }
    }

    async fn post_control_plane_cancelled_result(&mut self, output: &ExecutionOutput) {
        self.report_cancelled_result(output).await;
    }

    async fn post_event(&mut self, event_type: &str, message: &str, data: serde_json::Value) {
        self.event_writer
            .emit(ExecutorEvent::worker(event_type, message, data));
    }

    async fn current_reason(&self) -> CancellationReason {
        *self.reason.read().await
    }

    async fn mark_cancellation(&self, next: CancellationReason) {
        let mut current = self.reason.write().await;
        apply_cancellation_reason(&mut current, next);
    }

    async fn stop_network_lifecycle_after_terminal(&mut self) {
        self.event_writer.disable();
        self.event_writer.shutdown().await;
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

fn validate_result_artifact_refs_for_worker(
    result: &GradingResult,
    artifacts: &[DiscoveredArtifact],
) -> Result<(), String> {
    if result.schema_version != cmsx_core::protocol::GRADING_RESULT_SCHEMA_VERSION {
        return Err("unsupported result schema_version".to_string());
    }

    if result.artifacts.len() > ARTIFACT_MAX_COUNT {
        return Err("too many artifact references".to_string());
    }

    let uploaded: HashSet<&str> = artifacts
        .iter()
        .map(|artifact| artifact.relative_path.as_str())
        .collect();

    let mut seen = HashSet::new();

    for artifact in &result.artifacts {
        validate_artifact_relative_path(&artifact.path).map_err(|error| error.to_string())?;

        if !seen.insert(artifact.path.as_str()) {
            return Err("duplicate artifact reference".to_string());
        }

        if !uploaded.contains(artifact.path.as_str()) {
            return Err(format!(
                "artifact reference was not produced: {}",
                artifact.path
            ));
        }

        if let Some(label) = &artifact.label {
            validate_artifact_label(label).map_err(|error| error.to_string())?;
        }
    }

    Ok(())
}

async fn read_artifact_bytes_for_upload(
    workspace: &JobWorkspace,
    artifact: &DiscoveredArtifact,
) -> Result<Bytes, crate::workspace::ArtifactDiscoveryError> {
    let (size, sha256) = hash_artifact_file_with_revalidation(
        &workspace.artifacts_dir,
        &artifact.path,
        &artifact.relative_path,
    )
    .await?;

    if size != artifact.size_bytes || sha256 != artifact.sha256 {
        return Err(crate::workspace::ArtifactDiscoveryError::Changed(
            artifact.relative_path.clone(),
        ));
    }

    let bytes = tokio_fs::read(&artifact.path).await?;

    if bytes.len() as u64 != artifact.size_bytes {
        return Err(crate::workspace::ArtifactDiscoveryError::Changed(
            artifact.relative_path.clone(),
        ));
    }

    Ok(Bytes::from(bytes))
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

#[allow(dead_code)] // Kept as a pure lifecycle policy helper for focused tests/future wiring.
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

#[allow(dead_code)] // Kept as a pure lifecycle policy helper for focused tests/future wiring.
pub fn should_read_result_after_execution(status: &ExecutionStatus) -> bool {
    matches!(status, ExecutionStatus::Exited { .. })
}

pub fn should_post_terminal_for_reason(reason: CancellationReason) -> bool {
    !matches!(reason, CancellationReason::LeaseLost)
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
    cap_text(message, FAILURE_MESSAGE_MAX_BYTES)
}

fn rejected_result_message(error: &ClientError) -> String {
    let body = error.bounded_body().unwrap_or("");
    cap_failure_message(&format!("control plane rejected result: {body}"))
}

#[cfg(test)]
mod tests {
    use cmsx_core::ResultStatus;

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
    fn cancelled_result_shape() {
        let result = GradingResult::cancelled();

        assert_eq!(
            result.schema_version,
            cmsx_core::protocol::GRADING_RESULT_SCHEMA_VERSION
        );
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
    fn rejected_result_message_handles_empty_body() {
        let error = ClientError::Status {
            status: StatusCode::BAD_REQUEST,
            body: String::new(),
        };

        let message = rejected_result_message(&error);

        assert_eq!(message, "control plane rejected result: ");
    }
}
