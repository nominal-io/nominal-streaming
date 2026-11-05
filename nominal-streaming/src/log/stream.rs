use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self};

use conjure_object::ResourceIdentifier;
use parking_lot::Condvar;
use parking_lot::Mutex;

use super::batch::LogBatch;
use super::batch::LogRecordSize;
use super::consumer::DeliveryOutcome;
use super::consumer::HttpTransport;
use super::consumer::LogConsumer;
use super::consumer::LogTransport;
use super::consumer::TokenProvider;
use super::journal::Journal;
use super::journal::{self};
use super::LogRecord;
use super::LogStreamError;
use super::LogStreamStats;
use super::NominalLogStreamOpts;
use crate::types::AuthProvider;
use crate::types::IntoTimestamp;

struct CoreTarget {
    auth: TokenProvider,
    dataset_rid: ResourceIdentifier,
    handle: tokio::runtime::Handle,
}

/// Configure a Core or file target and the log stream resource limits.
#[derive(Default)]
pub struct NominalLogStreamBuilder {
    opts: NominalLogStreamOpts,
    core: Option<CoreTarget>,
    backup: Option<PathBuf>,
    file: Option<PathBuf>,
}

impl NominalLogStreamBuilder {
    /// Create a builder with default log stream options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set batching and buffering limits.
    pub fn with_options(mut self, opts: NominalLogStreamOpts) -> Self {
        self.opts = opts;
        self
    }
    /// Upload to an existing dataset using the supplied authentication provider and runtime.
    pub fn stream_to_core(
        mut self,
        auth: impl AuthProvider + 'static,
        dataset_rid: ResourceIdentifier,
        handle: tokio::runtime::Handle,
    ) -> Self {
        self.core = Some(CoreTarget {
            auth: Arc::new(move || auth.token()),
            dataset_rid,
            handle,
        });
        self
    }
    /// Directory for per-channel journal files and their import manifests.
    pub fn with_file_fallback(mut self, directory: impl Into<PathBuf>) -> Self {
        self.backup = Some(directory.into());
        self
    }
    /// Write only journal files. A Core target or fallback directory cannot also be set.
    pub fn stream_to_file(mut self, directory: impl Into<PathBuf>) -> Self {
        self.file = Some(directory.into());
        self
    }
    /// Enable tracing with the default filter or RUST_LOG environment setting.
    #[cfg(feature = "logging")]
    pub fn enable_logging(self) -> Self {
        crate::logging::init(None);
        self
    }

    /// Enable tracing with an explicit filter directive.
    #[cfg(feature = "logging")]
    pub fn enable_logging_with_directive(self, directive: &str) -> Self {
        crate::logging::init(Some(directive));
        self
    }

    /// Validate the configuration and start upload workers.
    pub fn build(self) -> Result<NominalLogStream, LogStreamError> {
        if self.file.is_some() == self.core.is_some() {
            return Err(LogStreamError::Invalid(
                "choose a Core target or file-only target".into(),
            ));
        }
        if self.file.is_some() && self.backup.is_some() {
            return Err(LogStreamError::Invalid(
                "file-only streams cannot also configure a fallback directory".into(),
            ));
        }
        let (target, rid) = match self.core {
            Some(core) => (
                Some(
                    Arc::new(HttpTransport::new(core.auth, core.handle, &self.opts)?)
                        as Arc<dyn LogTransport>,
                ),
                core.dataset_rid.to_string(),
            ),
            None => (None, String::new()),
        };
        NominalLogStream::start(self.opts, target, rid, self.file.or(self.backup))
    }
}

#[derive(Default)]
struct LogStreamState {
    pending: LogBatch,
    ready: VecDeque<LogBatch>,
    closed: bool,
    stats: LogStreamStats,
    // The first failure that flush has not reported yet. The stream keeps running.
    unreported_error: Option<String>,
}

struct LogStreamShared {
    state: Mutex<LogStreamState>,
    changed: Condvar,
    opts: NominalLogStreamOpts,
    consumer: LogConsumer,
    dataset_rid: String,
}

/// A bounded log stream. Enqueue acknowledges memory acceptance, not remote durability.
///
/// Call `flush` or `close` to observe delivery failures. Ambiguous network failures may
/// result in duplicates after retry or journal recovery. No ordering across batches is promised.
pub struct NominalLogStream {
    shared: Arc<LogStreamShared>,
    admission: Mutex<()>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl NominalLogStream {
    pub fn builder() -> NominalLogStreamBuilder {
        NominalLogStreamBuilder::default()
    }

    pub(super) fn start(
        opts: NominalLogStreamOpts,
        target: Option<Arc<dyn LogTransport>>,
        dataset_rid: String,
        backup: Option<PathBuf>,
    ) -> Result<Self, LogStreamError> {
        opts.validate()?;
        let shared = Arc::new(LogStreamShared {
            state: Mutex::new(LogStreamState::default()),
            changed: Condvar::new(),
            consumer: LogConsumer::new(target, backup.map(Journal::new)),
            opts,
            dataset_rid,
        });
        let stream = Self {
            shared,
            admission: Mutex::new(()),
            workers: Mutex::new(Vec::new()),
        };
        for index in 0..stream.shared.opts.num_upload_workers {
            let shared = stream.shared.clone();
            // On error, dropping the stream closes it and joins the workers already started.
            let join = thread::Builder::new()
                .name(format!("nominal-log-{index}"))
                .spawn(move || worker(shared))?;
            stream.workers.lock().push(join);
        }
        Ok(stream)
    }

    pub fn enqueue(&self, channel: &str, record: LogRecord) -> Result<(), LogStreamError> {
        self.enqueue_batch(channel, vec![record])
    }

    /// Accept a complete batch, or reject it before accepting any records. The input batch
    /// must fit the memory budget; it is split into request-sized batches internally.
    pub fn enqueue_batch(
        &self,
        channel: &str,
        records: Vec<LogRecord>,
    ) -> Result<(), LogStreamError> {
        if channel.is_empty() {
            return Err(LogStreamError::Invalid("channel must not be empty".into()));
        }
        let mut total_bytes = 0usize;
        let mut sizes = Vec::with_capacity(records.len());
        for record in &records {
            if self.shared.consumer.has_journal()
                && (record.args.contains_key(journal::MESSAGE_KEY)
                    || record.args.contains_key(journal::TIMESTAMP_KEY))
            {
                return Err(LogStreamError::Invalid(
                    "journal backups reserve MESSAGE and __REALTIME_TIMESTAMP argument keys".into(),
                ));
            }
            let size = LogRecordSize::new(record);
            if size.singleton_len(channel, &self.shared.dataset_rid)
                > self.shared.opts.max_request_bytes
            {
                return Err(LogStreamError::Invalid(
                    "log record exceeds max_request_bytes".into(),
                ));
            }
            let bytes = size.record_len();
            sizes.push((size, bytes));
            total_bytes += bytes;
        }
        if total_bytes > self.shared.opts.max_buffered_bytes {
            return Err(LogStreamError::Invalid(
                "input batch exceeds max_buffered_bytes; submit smaller batches".into(),
            ));
        }
        let _admission = self.admission.lock();
        let mut state = self.shared.state.lock();
        loop {
            if state.closed {
                return Err(LogStreamError::Closed);
            }
            if state.stats.buffered_bytes <= self.shared.opts.max_buffered_bytes - total_bytes {
                break;
            }
            // Only force a partial batch out if no queued/in-flight work can free capacity.
            // Otherwise let the pending batch fill after an upload completes. Its normal
            // flush timer still applies, and close still wakes admission.
            if state.stats.buffered_bytes == state.pending.bytes {
                queue_pending(&mut state);
            }
            self.shared.changed.notify_all();
            self.shared.changed.wait(&mut state);
        }
        let pending_was_empty = state.pending.count == 0;
        let ready_before = state.ready.len();
        state.stats.accepted_records += records.len() as u64;
        state.stats.buffered_bytes += total_bytes;
        for (record, (size, bytes)) in records.into_iter().zip(sizes) {
            // A full batch by count was already queued after the previous push.
            if state
                .pending
                .encoded_len_after(channel, size, &self.shared.dataset_rid)
                > self.shared.opts.max_request_bytes
            {
                queue_pending(&mut state);
            }
            state.pending.push(channel, record, size, bytes);
            if state.pending.count >= self.shared.opts.max_records_per_batch
                || state.pending.encoded_len(&self.shared.dataset_rid)
                    >= self.shared.opts.max_request_bytes
            {
                queue_pending(&mut state);
            }
        }
        // Existing pending work already has a timer. Wake uploaders only when its
        // first record sets that timer or a complete batch becomes ready.
        if pending_was_empty || state.ready.len() > ready_before {
            self.shared.changed.notify_all();
        }
        Ok(())
    }

    /// Cache a channel name and common arguments for convenient repeated writes.
    pub fn log_writer(
        &self,
        channel: impl Into<String>,
        args: HashMap<String, String>,
    ) -> NominalLogWriter<'_> {
        NominalLogWriter {
            stream: self,
            channel: channel.into(),
            args,
        }
    }

    pub fn stats(&self) -> LogStreamStats {
        self.shared.state.lock().stats.clone()
    }

    /// Block new enqueue calls while all accepted records finish delivery or backup.
    pub fn flush(&self) -> Result<LogStreamStats, LogStreamError> {
        let _admission = self.admission.lock();
        let mut state = self.shared.state.lock();
        // Holding admission, nothing can refill the pending batch while this waits.
        queue_pending(&mut state);
        self.shared.changed.notify_all();
        while state.stats.buffered_bytes > 0 {
            self.shared.changed.wait(&mut state);
        }
        match state.unreported_error.take() {
            Some(error) => Err(LogStreamError::Delivery(error)),
            None => Ok(state.stats.clone()),
        }
    }

    pub fn stop_accepting_writes(&self) {
        self.shared.state.lock().closed = true;
        self.shared.changed.notify_all();
    }

    /// Stop admission, drain accepted records, and join every worker. Repeated calls are safe
    /// and return the same error if a batch was dropped after the last successful flush.
    pub fn close(&self) -> Result<LogStreamStats, LogStreamError> {
        self.stop_accepting_writes();
        // Delivery panics are caught in the worker, so joining cannot fail.
        for join in self.workers.lock().drain(..) {
            let _ = join.join();
        }
        let state = self.shared.state.lock();
        match &state.unreported_error {
            Some(error) => Err(LogStreamError::Delivery(error.clone())),
            None => Ok(state.stats.clone()),
        }
    }
}

impl Drop for NominalLogStream {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            tracing::error!("Closing log stream failed: {error}");
        }
    }
}

/// A channel writer with common per-record arguments.
pub struct NominalLogWriter<'a> {
    stream: &'a NominalLogStream,
    channel: String,
    args: HashMap<String, String>,
}

impl NominalLogWriter<'_> {
    /// Enqueue a message with the writer's common arguments.
    pub fn push(
        &self,
        timestamp: impl IntoTimestamp,
        message: impl Into<String>,
    ) -> Result<(), LogStreamError> {
        let timestamp_ns = timestamp
            .try_into_timestamp()
            .ok()
            .and_then(|t| {
                i64::try_from(i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos)).ok()
            })
            .ok_or_else(|| {
                LogStreamError::Invalid("timestamp exceeds signed nanosecond range".into())
            })?;
        self.stream.enqueue(
            &self.channel,
            LogRecord::new(timestamp_ns, message, self.args.clone()),
        )
    }
    /// Enqueue records, preserving record-specific overrides of common arguments.
    pub fn enqueue_batch(&self, records: Vec<LogRecord>) -> Result<(), LogStreamError> {
        let records = records
            .into_iter()
            .map(|mut record| {
                for (key, value) in &self.args {
                    record
                        .args
                        .entry(key.clone())
                        .or_insert_with(|| value.clone());
                }
                record
            })
            .collect();
        self.stream.enqueue_batch(&self.channel, records)
    }
}

fn queue_pending(state: &mut LogStreamState) {
    if state.pending.count > 0 {
        state.ready.push_back(std::mem::take(&mut state.pending));
    }
}

fn next_batch(shared: &LogStreamShared) -> Option<LogBatch> {
    let mut state = shared.state.lock();
    loop {
        if let Some(batch) = state.ready.pop_front() {
            return Some(batch);
        }
        if let Some(first) = state.pending.first_record {
            let remaining = shared
                .opts
                .max_request_delay
                .saturating_sub(first.elapsed());
            if state.closed || remaining.is_zero() {
                return Some(std::mem::take(&mut state.pending));
            }
            shared.changed.wait_for(&mut state, remaining);
        } else if state.closed {
            return None;
        } else {
            shared.changed.wait(&mut state);
        }
    }
}

fn worker(shared: Arc<LogStreamShared>) {
    while let Some(batch) = next_batch(&shared) {
        let count = batch.count;
        let bytes = batch.bytes;
        let request = batch.into_request(&shared.dataset_rid);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shared.consumer.consume(&request, || {
                let mut state = shared.state.lock();
                state.stats.requests += 1;
            })
        }))
        .unwrap_or_else(|_| Err("log delivery worker panicked".into()));
        let mut state = shared.state.lock();
        state.stats.buffered_bytes -= bytes;
        match result {
            Ok(delivery) => match delivery {
                DeliveryOutcome::Acknowledged => state.stats.acknowledged_records += count as u64,
                DeliveryOutcome::BackedUp { delivery_error } => {
                    state.stats.backed_up_records += count as u64;
                    if let Some(error) = delivery_error {
                        state.stats.last_error = Some(error);
                    }
                }
            },
            Err(error) => {
                tracing::error!(records = count, "Dropped log batch: {error}");
                state.stats.failed_records += count as u64;
                state.stats.last_error = Some(error.clone());
                state.unreported_error.get_or_insert(error);
            }
        }
        shared.changed.notify_all();
    }
}
