//! Log streaming support for Nominal datasets.
//!
//! This module provides [`NominalLogStream`], which buffers timestamped log entries and
//! sends them to the Nominal channel-writer `writeLogs` endpoint
//! (`POST /storage/writer/v1/logs/{dataSourceRid}`).  The API mirrors the Python client's
//! `Dataset.get_log_stream()` / `process_log_batch()` approach: log points are grouped by
//! channel and sent as a [`WriteLogsRequest`] conjure JSON payload.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use nominal_streaming::log_stream::{NominalLogStreamBuilder, NominalLogStreamOpts};
//! use std::time::UNIX_EPOCH;
//!
//! let mut log_stream = NominalLogStreamBuilder::new()
//!     .stream_to_core(token, dataset_rid, handle)
//!     .with_channel("my_logs")
//!     .build();
//!
//! log_stream.push(UNIX_EPOCH.elapsed().unwrap(), "hello from nominal");
//! // Remaining buffered points are flushed automatically on drop.
//! ```
//!
//! # Async safety
//!
//! [`NominalLogStream::flush`] (called by [`NominalLogStream::push`] when the buffer is full
//! or the request delay has elapsed, and also on [`Drop`]) uses
//! [`tokio::runtime::Handle::block_on`] internally.  **Do not call `push` or `flush` from
//! within an async context** (i.e. from inside a Tokio task) — doing so will panic.  If you
//! need to stream logs from an async context, use
//! [`tokio::task::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).

use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;

use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use conjure_object::SafeLong;
use nominal_api::api::Channel;
use nominal_api::api::Timestamp;
use nominal_api::api::rids::NominalDataSourceOrDatasetRid;
use nominal_api::storage::writer::api::LogPoint as ApiLogPoint;
use nominal_api::storage::writer::api::LogValue;
use nominal_api::storage::writer::api::WriteLogsRequest;
use tracing::error;
use tracing::info;

use crate::client::NominalApiClients;
use crate::client::PRODUCTION_API_URL;
use crate::types::IntoTimestamp;

// ── defaults ─────────────────────────────────────────────────────────────────

const DEFAULT_MAX_BATCH_SIZE: usize = 1_000;
const DEFAULT_MAX_REQUEST_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL: &str = "logs";

// ── options ───────────────────────────────────────────────────────────────────

/// Configuration for a [`NominalLogStream`].
#[derive(Debug, Clone)]
pub struct NominalLogStreamOpts {
    /// Maximum number of log points to buffer before flushing to Nominal.
    ///
    /// Default: `1_000`.
    pub max_batch_size: usize,

    /// Maximum time to wait before flushing buffered points, even if the batch is not full.
    ///
    /// Default: `1` second.
    pub max_request_delay: Duration,

    /// Base URL for the Nominal API.
    ///
    /// Default: `"https://api.gov.nominal.io/api"`.
    pub base_api_url: String,
}

impl Default for NominalLogStreamOpts {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_request_delay: DEFAULT_MAX_REQUEST_DELAY,
            base_api_url: PRODUCTION_API_URL.to_string(),
        }
    }
}

// ── builder ───────────────────────────────────────────────────────────────────

struct NominalLogSendConfig {
    clients: NominalApiClients,
    handle: tokio::runtime::Handle,
    token: BearerToken,
    data_source_rid: ResourceIdentifier,
}

/// Builder for [`NominalLogStream`].
///
/// Must call [`stream_to_core`](Self::stream_to_core) before [`build`](Self::build); otherwise
/// the stream will silently discard all log points on flush (useful for testing).
#[derive(Default)]
pub struct NominalLogStreamBuilder {
    stream_to_core: Option<(BearerToken, ResourceIdentifier, tokio::runtime::Handle)>,
    channel: Option<String>,
    opts: NominalLogStreamOpts,
}

impl NominalLogStreamBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the stream to send logs to Nominal Core.
    ///
    /// `dataset` is the RID of the Nominal dataset to write logs into.
    /// `handle` must be a handle to an already-running Tokio runtime.
    pub fn stream_to_core(
        mut self,
        token: BearerToken,
        dataset: ResourceIdentifier,
        handle: tokio::runtime::Handle,
    ) -> Self {
        self.stream_to_core = Some((token, dataset, handle));
        self
    }

    /// Set the channel name used when writing logs.
    ///
    /// Default: `"logs"`.
    pub fn with_channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }

    /// Override stream options.
    pub fn with_options(mut self, opts: NominalLogStreamOpts) -> Self {
        self.opts = opts;
        self
    }

    /// Build the [`NominalLogStream`].
    pub fn build(self) -> NominalLogStream {
        let send_config = self.stream_to_core.map(|(token, rid, handle)| {
            NominalLogSendConfig {
                clients: NominalApiClients::from_uri(&self.opts.base_api_url),
                handle,
                token,
                data_source_rid: rid,
            }
        });

        NominalLogStream {
            channel: self.channel.unwrap_or_else(|| DEFAULT_CHANNEL.to_string()),
            buffer: Vec::new(),
            last_flushed_at: Instant::now(),
            opts: self.opts,
            send_config,
        }
    }
}

// ── public log point type ─────────────────────────────────────────────────────

/// A single log entry buffered in a [`NominalLogStream`].
#[derive(Debug, Clone)]
pub struct LogPoint {
    /// Nanoseconds since the Unix epoch.
    pub timestamp_nanos: i64,
    /// The log message.
    pub message: String,
    /// Optional key-value metadata (sent as `LogValue.args`).
    pub args: BTreeMap<String, String>,
}

// ── stream ────────────────────────────────────────────────────────────────────

/// A stream for writing timestamped log entries to a Nominal dataset.
///
/// Log points are buffered locally and sent in batches to the Nominal
/// `writeLogs` endpoint (`POST /storage/writer/v1/logs/{dataSourceRid}`).
/// This is the same wire format used by the Nominal Python client's
/// `Dataset.get_log_stream()` / `process_log_batch()` path.
///
/// A flush is triggered automatically when:
/// - the buffer reaches [`NominalLogStreamOpts::max_batch_size`], or
/// - [`NominalLogStreamOpts::max_request_delay`] has elapsed since the last flush.
///
/// Any remaining buffered points are flushed when the stream is dropped.
///
/// # Example
///
/// ```rust,ignore
/// let mut log_stream = NominalLogStream::builder()
///     .stream_to_core(token, dataset_rid, handle)
///     .with_channel("flight_logs")
///     .build();
///
/// log_stream.push(timestamp, "engine started");
/// log_stream.push_with_args(timestamp, "sensor fault", [("sensor_id", "imu_1")].into());
/// ```
pub struct NominalLogStream {
    channel: String,
    buffer: Vec<LogPoint>,
    last_flushed_at: Instant,
    opts: NominalLogStreamOpts,
    send_config: Option<NominalLogSendConfig>,
}

impl NominalLogStream {
    /// Create a [`NominalLogStreamBuilder`].
    pub fn builder() -> NominalLogStreamBuilder {
        NominalLogStreamBuilder::new()
    }

    /// Push a log message with the given timestamp.
    ///
    /// If the buffer has reached `max_batch_size` or `max_request_delay` has elapsed,
    /// this will flush synchronously before returning.
    pub fn push(&mut self, timestamp: impl IntoTimestamp, message: impl Into<String>) {
        self.push_with_args(timestamp, message, BTreeMap::new());
    }

    /// Push a log message with key-value arguments.
    ///
    /// `args` maps to the `LogValue.args` field in the Nominal wire format.
    pub fn push_with_args(
        &mut self,
        timestamp: impl IntoTimestamp,
        message: impl Into<String>,
        args: BTreeMap<String, String>,
    ) {
        let ts = timestamp.into_timestamp();
        let timestamp_nanos = ts.seconds * 1_000_000_000 + ts.nanos as i64;
        self.buffer.push(LogPoint {
            timestamp_nanos,
            message: message.into(),
            args,
        });
        if self.buffer.len() >= self.opts.max_batch_size
            || self.last_flushed_at.elapsed() > self.opts.max_request_delay
        {
            self.flush();
        }
    }

    /// Flush all buffered log points to Nominal.
    ///
    /// This is a no-op if the buffer is empty.  Called automatically on drop and
    /// whenever the batch size / request delay thresholds are crossed during [`push`](Self::push).
    pub fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let Some(config) = &self.send_config else {
            // No send config (e.g. test / dry-run) — just discard.
            self.buffer.clear();
            self.last_flushed_at = Instant::now();
            return;
        };

        let logs: Vec<ApiLogPoint> = self
            .buffer
            .iter()
            .map(|lp| {
                let secs = lp.timestamp_nanos / 1_000_000_000;
                let nanos = lp.timestamp_nanos % 1_000_000_000;
                ApiLogPoint::builder()
                    .timestamp(
                        Timestamp::builder()
                            .seconds(SafeLong::try_from(secs).unwrap_or(SafeLong::MAX))
                            .nanos(SafeLong::try_from(nanos).unwrap_or(SafeLong::ZERO))
                            .build(),
                    )
                    .value(
                        LogValue::builder()
                            .message(lp.message.clone())
                            .args(lp.args.clone())
                            .build(),
                    )
                    .build()
            })
            .collect();

        let request = WriteLogsRequest::builder()
            .logs(logs)
            .channel(Channel::new(self.channel.clone()))
            .build();

        info!(
            "flushing {} log points to channel '{}'",
            self.buffer.len(),
            self.channel,
        );

        let rid = NominalDataSourceOrDatasetRid(config.data_source_rid.clone());
        if let Err(e) = config.handle.block_on(
            config
                .clients
                .channel_writer
                .write_logs(&config.token, &rid, &request),
        ) {
            error!("failed to send log batch: {e:?}");
        }

        self.buffer.clear();
        self.last_flushed_at = Instant::now();
    }
}

impl Drop for NominalLogStream {
    fn drop(&mut self) {
        info!(
            "flushing then dropping NominalLogStream for channel '{}'",
            self.channel
        );
        self.flush();
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    /// Build a log stream with no send config (dry-run mode) and verify that push / flush work
    /// without panicking and that the buffer is cleared after a flush.
    #[test]
    fn test_log_stream_no_send_config() {
        let mut stream = NominalLogStreamBuilder::new()
            .with_channel("test_channel")
            .with_options(NominalLogStreamOpts {
                max_batch_size: 3,
                max_request_delay: Duration::from_secs(60),
                base_api_url: PRODUCTION_API_URL.to_string(),
            })
            .build();

        let ts = UNIX_EPOCH.elapsed().unwrap();
        stream.push(ts, "first log");
        stream.push(ts, "second log");

        // Buffer should hold 2 points — not yet at max_batch_size=3.
        assert_eq!(stream.buffer.len(), 2);

        // Third push crosses the threshold → auto-flush (discards because no send config).
        stream.push(ts, "third log");
        assert_eq!(stream.buffer.len(), 0, "buffer should be empty after auto-flush");
    }

    #[test]
    fn test_log_stream_with_args() {
        let mut stream = NominalLogStreamBuilder::new()
            .with_channel("tagged_logs")
            .build();

        let ts = UNIX_EPOCH.elapsed().unwrap();
        let mut args = BTreeMap::new();
        args.insert("severity".to_string(), "ERROR".to_string());
        args.insert("component".to_string(), "engine".to_string());
        stream.push_with_args(ts, "engine fault detected", args.clone());

        assert_eq!(stream.buffer.len(), 1);
        let point = &stream.buffer[0];
        assert_eq!(point.message, "engine fault detected");
        assert_eq!(point.args, args);
    }

    #[test]
    fn test_log_stream_timestamp_split() {
        // Verify that nanosecond timestamps are split correctly into seconds and nanos.
        let mut stream = NominalLogStreamBuilder::new()
            .with_channel("serial_test")
            .build();

        // timestamp: 1_000_000_001 ns → seconds=1, nanos=1
        stream.push(1_000_000_001_i64, "hello");

        let lp = &stream.buffer[0];
        assert_eq!(lp.timestamp_nanos, 1_000_000_001);
        assert_eq!(lp.timestamp_nanos / 1_000_000_000, 1);
        assert_eq!(lp.timestamp_nanos % 1_000_000_000, 1);
        assert_eq!(lp.message, "hello");
    }
}
