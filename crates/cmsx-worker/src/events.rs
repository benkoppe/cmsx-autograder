use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use cmsx_core::{
    JobEventPayload,
    protocol::{
        JOB_EVENT_MESSAGE_MAX_BYTES, JobEventStream, JobEventVisibility, cap_text_with_marker,
        job_event_type,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorEvent {
    pub event_type: String,
    pub stream: JobEventStream,
    pub visibility: JobEventVisibility,
    pub message: String,
    pub data: Value,
}

#[derive(Clone)]
pub struct ExecutorEventSink {
    sender: Option<mpsc::UnboundedSender<JobEventWriterCommand>>,
}

pub(crate) enum JobEventWriterCommand {
    Event(ExecutorEvent),
    Flush(oneshot::Sender<()>),
    Disable,
    Shutdown(oneshot::Sender<()>),
}

impl ExecutorEvent {
    pub fn new(
        event_type: impl Into<String>,
        stream: JobEventStream,
        visibility: JobEventVisibility,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            stream,
            visibility,
            message: cap_text_with_marker(message.into(), JOB_EVENT_MESSAGE_MAX_BYTES),
            data,
        }
    }

    pub fn stdout(message: impl Into<String>) -> Self {
        Self::new(
            job_event_type::STDOUT,
            JobEventStream::Stdout,
            JobEventVisibility::Staff,
            message,
            json!({}),
        )
    }

    pub fn stderr(message: impl Into<String>) -> Self {
        Self::new(
            job_event_type::STDERR,
            JobEventStream::Stderr,
            JobEventVisibility::Staff,
            message,
            json!({}),
        )
    }

    pub fn worker(event_type: impl Into<String>, message: impl Into<String>, data: Value) -> Self {
        Self::new(
            event_type,
            JobEventStream::Worker,
            JobEventVisibility::Staff,
            message,
            data,
        )
    }

    pub(crate) fn into_payload(self, sequence: i64) -> JobEventPayload {
        JobEventPayload {
            sequence,
            timestamp: chrono::Utc::now(),
            event_type: self.event_type,
            stream: self.stream.as_str().to_string(),
            visibility: self.visibility.as_str().to_string(),
            message: self.message,
            data: self.data,
        }
    }
}

impl ExecutorEventSink {
    pub fn noop() -> Self {
        Self { sender: None }
    }

    pub fn emit(&self, event: ExecutorEvent) {
        let Some(sender) = &self.sender else {
            return;
        };

        if sender.send(JobEventWriterCommand::Event(event)).is_err() {
            tracing::debug!("job event sink is closed; dropping executor event");
        }
    }

    pub(crate) fn new(sender: mpsc::UnboundedSender<JobEventWriterCommand>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}

#[cfg(test)]
mod tests {
    use cmsx_core::protocol::TEXT_TRUNCATION_MARKER;

    use super::*;

    #[test]
    fn stdout_event_uses_expected_shape() {
        let event = ExecutorEvent::stdout("hello");

        assert_eq!(event.event_type, job_event_type::STDOUT);
        assert_eq!(event.stream, JobEventStream::Stdout);
        assert_eq!(event.visibility, JobEventVisibility::Staff);
        assert_eq!(event.message, "hello");
        assert_eq!(event.data, json!({}));
    }

    #[test]
    fn stderr_event_uses_expected_shape() {
        let event = ExecutorEvent::stderr("oops");

        assert_eq!(event.event_type, job_event_type::STDERR);
        assert_eq!(event.stream, JobEventStream::Stderr);
        assert_eq!(event.visibility, JobEventVisibility::Staff);
        assert_eq!(event.message, "oops");
        assert_eq!(event.data, json!({}));
    }

    #[test]
    fn worker_event_uses_expected_shape() {
        let event = ExecutorEvent::worker(
            job_event_type::EXECUTOR_STARTED,
            "Executor started",
            json!({"backend": "in-worker"}),
        );

        assert_eq!(event.event_type, job_event_type::EXECUTOR_STARTED);
        assert_eq!(event.stream, JobEventStream::Worker);
        assert_eq!(event.visibility, JobEventVisibility::Staff);
        assert_eq!(event.message, "Executor started");
        assert_eq!(event.data, json!({"backend": "in-worker"}));
    }

    #[test]
    fn event_message_is_capped_on_char_boundary() {
        let event = ExecutorEvent::stdout("é".repeat(JOB_EVENT_MESSAGE_MAX_BYTES));

        assert!(event.message.len() <= JOB_EVENT_MESSAGE_MAX_BYTES);
        assert!(event.message.ends_with(TEXT_TRUNCATION_MARKER));
        assert!(event.message.is_char_boundary(event.message.len()));
    }
}
