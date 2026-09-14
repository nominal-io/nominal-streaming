//! Bounded streaming of timestamped log messages.

mod journal;
mod stream;
mod transport;

use std::collections::HashMap;
use std::time::Duration;

pub use stream::LogWriter;
pub use stream::NominalLogStream;
pub use stream::NominalLogStreamBuilder;

/// One log event. Integer timestamps are signed nanoseconds since the Unix epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub timestamp_ns: i64,
    pub message: String,
    /// Arguments belong to this event, not to a telemetry series.
    pub args: HashMap<String, String>,
}

impl LogRecord {
    pub fn new(
        timestamp_ns: i64,
        message: impl Into<String>,
        args: HashMap<String, String>,
    ) -> Self {
        Self {
            timestamp_ns,
            message: message.into(),
            args,
        }
    }

    // Conservatively charge framing, string allocations, and map entries as well as content.
    // This is an admission budget, not a measurement of process RSS or allocator bookkeeping.
    fn accounted_bytes(&self, channel: &str) -> usize {
        256usize
            .saturating_add(channel.len())
            .saturating_add(self.message.capacity())
            .saturating_add(self.args.capacity().saturating_mul(96))
            .saturating_add(
                self.args
                    .iter()
                    .map(|(k, v)| k.capacity().saturating_add(v.capacity()))
                    .sum::<usize>(),
            )
    }
}

/// Limits include ready and in-flight batches, so a stalled backend applies backpressure.
#[derive(Clone, Debug)]
pub struct LogStreamOptions {
    pub max_batch_bytes: usize,
    pub max_buffered_bytes: usize,
    pub max_records_per_batch: usize,
    pub max_request_delay: Duration,
    pub num_upload_workers: usize,
    pub base_api_url: String,
    pub request_timeout: Duration,
    /// Additional attempts after the initial request. There is no nested HTTP retry loop.
    pub max_retries: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// If Retry-After exceeds this bound, back up instead of retrying earlier than requested.
    pub max_retry_after: Duration,
}

impl Default for LogStreamOptions {
    fn default() -> Self {
        Self {
            max_batch_bytes: 16 * 1024 * 1024,
            max_buffered_bytes: 64 * 1024 * 1024,
            max_records_per_batch: 50_000,
            max_request_delay: Duration::from_millis(250),
            num_upload_workers: 4,
            base_api_url: crate::client::PRODUCTION_API_URL.into(),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            max_retry_after: Duration::from_secs(30),
        }
    }
}

impl LogStreamOptions {
    fn validate(&self) -> Result<(), LogStreamError> {
        if self.max_batch_bytes < 512
            || self.max_buffered_bytes < self.max_batch_bytes
            || self.max_records_per_batch == 0
            || self.num_upload_workers == 0
            || self.max_request_delay.is_zero()
            || self.request_timeout.is_zero()
            || self.initial_backoff > self.max_backoff
        {
            return Err(LogStreamError::Invalid(
                "invalid batch, buffer, worker, timeout or backoff limits".into(),
            ));
        }
        Ok(())
    }
}

/// Cumulative delivery outcomes. Backup is preservation on disk, not backend acknowledgement.
#[derive(Clone, Debug, Default)]
pub struct LogStreamStats {
    pub accepted_records: u64,
    pub acknowledged_records: u64,
    pub backed_up_records: u64,
    pub failed_records: u64,
    pub requests: u64,
    pub retries: u64,
    pub buffered_bytes: usize,
    pub last_error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum LogStreamError {
    #[error("log stream is closed")]
    Closed,
    #[error("invalid log stream input: {0}")]
    Invalid(String),
    #[error("log delivery failed: {0}")]
    Delivery(String),
    #[error("log stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests;
