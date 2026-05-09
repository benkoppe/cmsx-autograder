use serde::Deserialize;

use cmsx_core::protocol::{
    JOB_EVENT_MESSAGE_MAX_BYTES, TEXT_TRUNCATION_MARKER, cap_text, cap_text_with_marker,
};

pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
pub const MAX_TIMEOUT_SECONDS: u64 = 60 * 60;
pub const SUMMARY_MAX_BYTES: usize = JOB_EVENT_MESSAGE_MAX_BYTES;

#[derive(Debug, Deserialize)]
struct TimeoutConfig {
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputSummaries {
    pub stdout: BoundedSummary,
    pub stderr: BoundedSummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundedSummary {
    bytes: Vec<u8>,
    truncated: bool,
}

impl BoundedSummary {
    pub fn push(&mut self, bytes: &[u8]) {
        let remaining = SUMMARY_MAX_BYTES.saturating_sub(self.bytes.len());

        if bytes.len() <= remaining {
            self.bytes.extend_from_slice(bytes);
        } else {
            if remaining > 0 {
                self.bytes.extend_from_slice(&bytes[..remaining]);
            }
            self.truncated = true;
        }
    }

    pub fn into_summary_string(mut self) -> Option<String> {
        if self.bytes.is_empty() && !self.truncated {
            return None;
        }

        if self.truncated {
            let marker = TEXT_TRUNCATION_MARKER.as_bytes();
            let reserved = marker.len().min(SUMMARY_MAX_BYTES);

            if self.bytes.len() > SUMMARY_MAX_BYTES.saturating_sub(reserved) {
                self.bytes
                    .truncate(SUMMARY_MAX_BYTES.saturating_sub(reserved));
            }

            self.bytes.extend_from_slice(marker);
        }

        Some(cap_summary_string(
            String::from_utf8_lossy(&self.bytes).into_owned(),
            self.truncated,
        ))
    }
}

pub fn parse_timeout_seconds(value: &serde_json::Value) -> u64 {
    let Ok(config) = serde_json::from_value::<TimeoutConfig>(value.clone()) else {
        return DEFAULT_TIMEOUT_SECONDS;
    };

    normalize_timeout_seconds(config.timeout_seconds)
}

pub fn normalize_timeout_seconds(value: Option<u64>) -> u64 {
    match value {
        None | Some(0) => DEFAULT_TIMEOUT_SECONDS,
        Some(value) => value.min(MAX_TIMEOUT_SECONDS),
    }
}

pub fn bytes_to_bounded_summary(bytes: Vec<u8>, truncated: bool) -> String {
    let summary = BoundedSummary { bytes, truncated };
    summary.into_summary_string().unwrap_or_default()
}

pub fn cap_summary_string(value: String, truncated: bool) -> String {
    if truncated {
        cap_text_with_marker(value, SUMMARY_MAX_BYTES)
    } else {
        cap_text(&value, SUMMARY_MAX_BYTES)
    }
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
    fn bounded_summary_adds_marker_within_cap() {
        let mut summary = BoundedSummary::default();
        summary.push(&vec![b'a'; SUMMARY_MAX_BYTES + 100]);

        let value = summary.into_summary_string().unwrap();

        assert!(value.len() <= SUMMARY_MAX_BYTES);
        assert!(value.ends_with(TEXT_TRUNCATION_MARKER));
    }

    #[test]
    fn bounded_summary_preserves_char_boundary_after_lossy_utf8() {
        let mut summary = BoundedSummary::default();
        summary.push(&vec![0xff; SUMMARY_MAX_BYTES]);

        let value = summary.into_summary_string().unwrap();

        assert!(value.len() <= SUMMARY_MAX_BYTES);
        assert!(value.is_char_boundary(value.len()));
    }

    #[test]
    fn bytes_to_bounded_summary_adds_marker_within_cap() {
        let value = bytes_to_bounded_summary(vec![b'a'; SUMMARY_MAX_BYTES + 100], true);

        assert!(value.len() <= SUMMARY_MAX_BYTES);
        assert!(value.ends_with(TEXT_TRUNCATION_MARKER));
    }
}
