pub const TEXT_TRUNCATION_MARKER: &str = "\n...[truncated]";

pub const JOB_EVENT_MESSAGE_MAX_BYTES: usize = 64 * 1024;
pub const JOB_EVENT_BATCH_MAX_EVENTS: usize = 512;

pub const GRADING_RESULT_SCHEMA_VERSION: &str = "1";

pub const WORKER_JWT_AUDIENCE: &str = "cmsx-control-plane";
pub const WORKER_AUTH_SCHEME: &str = "WorkerJWT";

pub mod job_event_type {
    pub const STDOUT: &str = "stdout";
    pub const STDERR: &str = "stderr";
    pub const JOB_INPUT_PREPARED: &str = "job.input.prepared";
    pub const JOB_STARTED: &str = "job.started";
    pub const EXECUTOR_STARTED: &str = "executor.started";
    pub const EXECUTOR_CONTAINER_CREATED: &str = "executor.container.created";
    pub const EXECUTOR_CONTAINER_STARTED: &str = "executor.container.started";
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
