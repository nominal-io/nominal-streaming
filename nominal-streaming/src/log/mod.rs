//! Streaming timestamped log messages to Core or journal files.
//!
//! Like [`crate::stream::NominalDatasetStream`], a log stream batches writes, applies
//! backpressure and delivers complete requests through a consumer. Logs use a separate
//! columnar wire representation and journal JSONL fallback. Their admission limits count
//! serialized bytes and memory as well as records, since messages and arguments vary in size.
//!
//! # Example: recording to a journal
//!
//! ```no_run
//! use std::collections::HashMap;
//! use nominal_streaming::log::NominalLogStream;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let stream = NominalLogStream::builder()
//!     .stream_to_file("logs")
//!     .build()?;
//! let writer = stream.log_writer("application", HashMap::new());
//! writer.push(1_789_392_441_123_456_789, "Started")?;
//! let stats = stream.close()?;
//! assert_eq!(stats.backed_up_records, 1);
//! # Ok(())
//! # }
//! ```
//!
//! For Core delivery, configure [`NominalLogStreamBuilder::stream_to_core`] and
//! [`NominalLogStreamBuilder::with_file_fallback`]. Keep the supplied multi-thread Tokio
//! runtime alive until close completes. These APIs block; use a blocking context in async code.
//!
//! Enqueue accepts records into memory. [`NominalLogStream::flush`] and
//! [`NominalLogStream::close`] wait for delivery or preservation and report failures.
//! Acknowledged batches never go to fallback; an unconfirmed request may already exist in
//! Core, so retries or journal recovery can duplicate it. Buffered records are not crash-durable.

mod batch;
mod consumer;
mod journal;
mod stream;
mod transport;

use std::collections::HashMap;
use std::time::Duration;

pub use stream::NominalLogStream;
pub use stream::NominalLogStreamBuilder;
pub use stream::NominalLogWriter;

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
/// Construct with `Default` and customize its public fields before passing to the builder.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct NominalLogStreamOpts {
    /// Maximum uncompressed protobuf request size, including all framing.
    pub max_request_bytes: usize,
    /// Charged memory per batch, including raw/compressed encoding reservations.
    pub max_batch_bytes: usize,
    /// Total charged memory for pending, queued, in-flight and retained failed records.
    /// Excludes caller-owned input, allocator bookkeeping and per-worker codec/runtime overhead.
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

impl Default for NominalLogStreamOpts {
    fn default() -> Self {
        Self {
            max_request_bytes: 8 * 1024 * 1024,
            max_batch_bytes: 16 * 1024 * 1024,
            max_buffered_bytes: 64 * 1024 * 1024,
            max_records_per_batch: 10_000,
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

impl NominalLogStreamOpts {
    fn validate(&self) -> Result<(), LogStreamError> {
        if self.max_request_bytes < 512
            || self.max_batch_bytes < 512
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
#[non_exhaustive]
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
