use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use cmsx_core::JobEventPayload;

pub const EVENT_MESSAGE_MAX_BYTES: usize = 64 * 1024;

const EVENT_TRUNCATION_MARKER: &str = "\n...[truncated]";

pub mod event_type {
    pub const STDOUT: &str = "stdout";
    pub const STDERR: &str = "stderr";
    pub const JOB_INPUT_PREPARED: &str = "job.input.prepared";
    pub const EXECUTOR_STARTED: &str = "executor.started";
    pub const EXECUTOR_CONTAINER_CREATED: &str = "executor.container.created";
    pub const EXECUTOR_CONTAINER_STARTED: &str = "executor.container.started";
    pub const RESULT_READ: &str = "result.read";
    pub const JOB_TIMEOUT: &str = "job.timeout";
    pub const JOB_CANCELLED: &str = "job.cancelled";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStream {
    Stdout,
    Stderr,
    Worker,
    Resource,
}

impl EventStream {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Worker => "worker",
            Self::Resource => "resource",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventVisibility {
    Student,
    Staff,
    Internal,
}

impl EventVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Student => "student",
            Self::Staff => "staff",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorEvent {
    pub event_type: String,
    pub stream: EventStream,
    pub visibility: EventVisibility,
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
        stream: EventStream,
        visibility: EventVisibility,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            stream,
            visibility,
            message: cap_event_message(message.into()),
            data,
        }
    }

    pub fn stdout(message: impl Into<String>) -> Self {
        Self::new(
            event_type::STDOUT,
            EventStream::Stdout,
            EventVisibility::Staff,
            message,
            json!({}),
        )
    }

    pub fn stderr(message: impl Into<String>) -> Self {
        Self::new(
            event_type::STDERR,
            EventStream::Stderr,
            EventVisibility::Staff,
            message,
            json!({}),
        )
    }

    pub fn worker(event_type: impl Into<String>, message: impl Into<String>, data: Value) -> Self {
        Self::new(
            event_type,
            EventStream::Worker,
            EventVisibility::Staff,
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

fn cap_event_message(mut message: String) -> String {
    if message.len() <= EVENT_MESSAGE_MAX_BYTES {
        return message;
    }

    let marker_len = EVENT_TRUNCATION_MARKER.len();
    let max_prefix = EVENT_MESSAGE_MAX_BYTES.saturating_sub(marker_len);

    let mut end = max_prefix;
    while !message.is_char_boundary(end) {
        end -= 1;
    }

    message.truncate(end);
    message.push_str(EVENT_TRUNCATION_MARKER);
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdout_event_uses_expected_shape() {
        let event = ExecutorEvent::stdout("hello");

        assert_eq!(event.event_type, event_type::STDOUT);
        assert_eq!(event.stream, EventStream::Stdout);
        assert_eq!(event.visibility, EventVisibility::Staff);
        assert_eq!(event.message, "hello");
        assert_eq!(event.data, json!({}));
    }

    #[test]
    fn stderr_event_uses_expected_shape() {
        let event = ExecutorEvent::stderr("oops");

        assert_eq!(event.event_type, event_type::STDERR);
        assert_eq!(event.stream, EventStream::Stderr);
        assert_eq!(event.visibility, EventVisibility::Staff);
        assert_eq!(event.message, "oops");
        assert_eq!(event.data, json!({}));
    }

    #[test]
    fn worker_event_uses_expected_shape() {
        let event = ExecutorEvent::worker(
            event_type::EXECUTOR_STARTED,
            "Executor started",
            json!({"backend": "in-worker"}),
        );

        assert_eq!(event.event_type, event_type::EXECUTOR_STARTED);
        assert_eq!(event.stream, EventStream::Worker);
        assert_eq!(event.visibility, EventVisibility::Staff);
        assert_eq!(event.message, "Executor started");
        assert_eq!(event.data, json!({"backend": "in-worker"}));
    }

    #[test]
    fn event_message_is_capped_on_char_boundary() {
        let event = ExecutorEvent::stdout("é".repeat(EVENT_MESSAGE_MAX_BYTES));

        assert!(event.message.len() <= EVENT_MESSAGE_MAX_BYTES);
        assert!(event.message.ends_with(EVENT_TRUNCATION_MARKER));
        assert!(event.message.is_char_boundary(event.message.len()));
    }
}
