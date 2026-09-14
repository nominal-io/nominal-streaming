use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self};

use conjure_object::ResourceIdentifier;
use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use parking_lot::Condvar;
use parking_lot::Mutex;

use super::batch::Batch;
use super::batch::RecordSize;
use super::consumer::DeliveryOutcome;
use super::consumer::LogConsumer;
use super::journal;
use super::transport::CoreTarget;
use super::transport::HttpTransport;
use super::transport::LogTransport;
use super::LogRecord;
use super::LogStreamError;
use super::LogStreamOptions;
use super::LogStreamStats;
use crate::types::AuthProvider;

/// Configure a Core or file target and the log stream resource limits.
#[derive(Default)]
pub struct NominalLogStreamBuilder {
    opts: LogStreamOptions,
    core: Option<CoreTarget>,
    backup: Option<PathBuf>,
    file: Option<PathBuf>,
}

impl NominalLogStreamBuilder {
    /// Set batching, buffering and retry limits.
    pub fn with_options(mut self, opts: LogStreamOptions) -> Self {
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
    /// Directory for per-channel journal segments and their import manifests.
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
        self.opts.validate()?;
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
struct State {
    pending: Batch,
    ready: VecDeque<Batch>,
    closed: bool,
    flushing: bool,
    unfinished: usize,
    stats: LogStreamStats,
    fatal_error: Option<String>,
    // Keep unpreserved records owned until explicitly rescued or the stream is dropped.
    failed: Vec<FailedBatch>,
}

struct FailedBatch {
    request: Arc<wire::WriteBatchesRequest>,
    bytes: usize,
    count: usize,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    opts: LogStreamOptions,
    consumer: LogConsumer,
    dataset_rid: String,
    backup: Option<PathBuf>,
}

/// A bounded log stream. Enqueue acknowledges memory acceptance, not remote durability.
///
/// Call `flush` or `close` to observe delivery failures. Ambiguous network failures may
/// result in duplicates after retry or journal recovery. No ordering across batches is promised.
pub struct NominalLogStream {
    shared: Arc<Shared>,
    admission: Mutex<()>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl NominalLogStream {
    pub fn builder() -> NominalLogStreamBuilder {
        NominalLogStreamBuilder::default()
    }

    pub(super) fn start(
        opts: LogStreamOptions,
        target: Option<Arc<dyn LogTransport>>,
        dataset_rid: String,
        backup: Option<PathBuf>,
    ) -> Result<Self, LogStreamError> {
        opts.validate()?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            consumer: LogConsumer::new(opts.clone(), target, backup.clone()),
            opts,
            dataset_rid,
            backup,
        });
        let mut workers = Vec::new();
        for index in 0..shared.opts.num_upload_workers {
            let worker_shared = shared.clone();
            match thread::Builder::new()
                .name(format!("nominal-log-{index}"))
                .spawn(move || worker(worker_shared))
            {
                Ok(join) => workers.push(join),
                Err(error) => {
                    shared.state.lock().closed = true;
                    shared.changed.notify_all();
                    for join in workers {
                        let _ = join.join();
                    }
                    return Err(error.into());
                }
            }
        }
        Ok(Self {
            shared,
            admission: Mutex::new(()),
            workers: Mutex::new(workers),
        })
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
            if self.shared.backup.is_some()
                && (record.args.contains_key("MESSAGE")
                    || record.args.contains_key("__REALTIME_TIMESTAMP"))
            {
                return Err(LogStreamError::Invalid(
                    "journal backups reserve MESSAGE and __REALTIME_TIMESTAMP argument keys".into(),
                ));
            }
            let size = RecordSize::new(record);
            if size.singleton_len(channel, &self.shared.dataset_rid)
                > self.shared.opts.max_request_bytes
            {
                return Err(LogStreamError::Invalid(
                    "log record exceeds max_request_bytes".into(),
                ));
            }
            let bytes = record
                .accounted_bytes(channel)
                .saturating_add(size.encoding_reservation(channel, &self.shared.dataset_rid));
            sizes.push((size, bytes));
            if bytes > self.shared.opts.max_batch_bytes {
                return Err(LogStreamError::Invalid(
                    "log record exceeds max_batch_bytes".into(),
                ));
            }
            total_bytes = total_bytes
                .checked_add(bytes)
                .ok_or_else(|| LogStreamError::Invalid("batch size overflow".into()))?;
        }
        if total_bytes > self.shared.opts.max_buffered_bytes {
            return Err(LogStreamError::Invalid(
                "input batch exceeds max_buffered_bytes; submit smaller batches".into(),
            ));
        }
        let _admission = self.admission.lock();
        let mut state = self.shared.state.lock();
        loop {
            if let Some(error) = &state.fatal_error {
                return Err(LogStreamError::Delivery(error.clone()));
            }
            if state.closed {
                return Err(LogStreamError::Closed);
            }
            if state.stats.buffered_bytes <= self.shared.opts.max_buffered_bytes - total_bytes {
                break;
            }
            // Only force a partial batch out if no queued/in-flight work can free capacity.
            // Otherwise let the pending batch fill after an upload completes. Its normal
            // flush timer still applies, and close/fatal outcomes still wake admission.
            if state.unfinished == state.pending.count {
                queue_pending(&mut state);
            }
            self.shared.changed.notify_all();
            self.shared.changed.wait(&mut state);
        }
        let pending_was_empty = state.pending.count == 0;
        let ready_before = state.ready.len();
        state.stats.accepted_records += records.len() as u64;
        state.unfinished += records.len();
        state.stats.buffered_bytes += total_bytes;
        for (record, (size, bytes)) in records.into_iter().zip(sizes) {
            if state.pending.count > 0
                && (state.pending.bytes.saturating_add(bytes) > self.shared.opts.max_batch_bytes
                    || state
                        .pending
                        .encoded_len_after(channel, size, &self.shared.dataset_rid)
                        > self.shared.opts.max_request_bytes
                    || state.pending.count >= self.shared.opts.max_records_per_batch)
            {
                queue_pending(&mut state);
            }
            state.pending.push(channel, record, size, bytes);
            if state.pending.count >= self.shared.opts.max_records_per_batch
                || state.pending.bytes >= self.shared.opts.max_batch_bytes
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
    pub fn writer(
        &self,
        channel: impl Into<String>,
        args: HashMap<String, String>,
    ) -> LogWriter<'_> {
        LogWriter {
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
        state.flushing = true;
        self.shared.changed.notify_all();
        while state.unfinished > 0 {
            self.shared.changed.wait(&mut state);
        }
        state.flushing = false;
        outcome(&state)
    }

    pub fn stop_accepting_writes(&self) {
        self.shared.state.lock().closed = true;
        self.shared.changed.notify_all();
    }

    /// Stop admission, drain accepted records, and join every worker. Repeated calls are safe.
    pub fn close(&self) -> Result<LogStreamStats, LogStreamError> {
        self.stop_accepting_writes();
        let mut workers = self.workers.lock();
        let mut panicked = false;
        for join in workers.drain(..) {
            panicked |= join.join().is_err();
        }
        if panicked {
            let mut state = self.shared.state.lock();
            state.fatal_error = Some("log worker panicked".into());
        }
        outcome(&self.shared.state.lock())
    }

    /// After a failed close, rescue retained batches to a usable directory. The stream stays
    /// closed. A partial rescue can duplicate a channel segment; inspect manifests when recovering.
    pub fn save_failed(
        &self,
        directory: impl AsRef<Path>,
    ) -> Result<LogStreamStats, LogStreamError> {
        self.save_failed_with(directory.as_ref(), journal::save)
    }

    fn save_failed_with(
        &self,
        directory: &Path,
        save: impl Fn(&Path, &wire::WriteBatchesRequest) -> std::io::Result<()>,
    ) -> Result<LogStreamStats, LogStreamError> {
        let _ = self.close();
        // Admission is closed. Reuse its lock to serialize rescue calls without blocking stats.
        let _rescue = self.admission.lock();
        loop {
            let request = {
                let mut state = self.shared.state.lock();
                let Some(batch) = state.failed.last() else {
                    state.fatal_error = None;
                    return Ok(state.stats.clone());
                };
                Arc::clone(&batch.request)
            };
            // State retains ownership until the write succeeds, including if the saver panics.
            save(directory, &request)?;
            let mut state = self.shared.state.lock();
            let saved = state
                .failed
                .pop()
                .expect("rescue owns the last failed batch");
            state.stats.backed_up_records += saved.count as u64;
            state.stats.failed_records -= saved.count as u64;
            state.stats.buffered_bytes -= saved.bytes;
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
pub struct LogWriter<'a> {
    stream: &'a NominalLogStream,
    channel: String,
    args: HashMap<String, String>,
}

impl LogWriter<'_> {
    /// Enqueue a message with the writer's common arguments.
    pub fn push(
        &self,
        timestamp_ns: i64,
        message: impl Into<String>,
    ) -> Result<(), LogStreamError> {
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

fn outcome(state: &State) -> Result<LogStreamStats, LogStreamError> {
    match &state.fatal_error {
        Some(error) => Err(LogStreamError::Delivery(error.clone())),
        None => Ok(state.stats.clone()),
    }
}

fn queue_pending(state: &mut State) {
    if state.pending.count > 0 {
        state.ready.push_back(std::mem::take(&mut state.pending));
    }
}

fn next_batch(shared: &Shared) -> Option<Batch> {
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
            if state.closed || state.flushing || remaining.is_zero() {
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

fn worker(shared: Arc<Shared>) {
    while let Some(batch) = next_batch(&shared) {
        let count = batch.count;
        let bytes = batch.bytes;
        let request = batch.into_request(&shared.dataset_rid);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shared.consumer.consume(&request, |retry| {
                let mut state = shared.state.lock();
                state.stats.requests += 1;
                state.stats.retries += u64::from(retry);
            })
        }))
        .unwrap_or_else(|_| Err("log delivery worker panicked".into()));
        let mut state = shared.state.lock();
        state.unfinished -= count;
        match result {
            Ok(delivery) => {
                match delivery {
                    DeliveryOutcome::Acknowledged => {
                        state.stats.acknowledged_records += count as u64
                    }
                    DeliveryOutcome::BackedUp { delivery_error } => {
                        state.stats.backed_up_records += count as u64;
                        if let Some(error) = delivery_error {
                            state.stats.last_error = Some(error);
                        }
                    }
                }
                state.stats.buffered_bytes -= bytes;
            }
            Err(error) => {
                tracing::error!("Log batch could not be preserved: {error}");
                state.stats.failed_records += count as u64;
                state.stats.last_error = Some(error.clone());
                state.fatal_error = Some(error);
                state.failed.push(FailedBatch {
                    request: Arc::new(request),
                    count,
                    bytes,
                });
                state.closed = true;
            }
        }
        shared.changed.notify_all();
    }
}

#[cfg(test)]
mod pressure_tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;
    use std::time::Instant;

    use prost::Message;

    use super::*;
    use crate::log::transport;

    #[test]
    fn recovery_keeps_stats_available_during_journal_io() {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join("occupied");
        std::fs::write(&occupied, "not a directory").unwrap();
        let stream = Arc::new(
            NominalLogStream::builder()
                .stream_to_file(occupied)
                .build()
                .unwrap(),
        );
        stream
            .enqueue("app", LogRecord::new(1, "retained", HashMap::new()))
            .unwrap();
        assert!(stream.close().is_err());
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let recovering = stream.clone();
        let output = directory.path().join("rescued");
        let rescue = thread::spawn(move || {
            recovering.save_failed_with(&output, |dir, request| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                journal::save(dir, request)
            })
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let observing = stream.clone();
        let (stats_tx, stats_rx) = mpsc::channel();
        let observer = thread::spawn(move || stats_tx.send(observing.stats()).unwrap());
        let stats = stats_rx.recv_timeout(Duration::from_secs(2));
        // Always release disk I/O before asserting, so regressions don't strand the test threads.
        release_tx.send(()).unwrap();
        let saved = rescue.join().unwrap().unwrap();
        observer.join().unwrap();
        let stats = stats.expect("stats must not wait for journal I/O");
        assert_eq!(stats.failed_records, 1);
        assert!(stats.buffered_bytes > 0);
        assert_eq!(saved.failed_records, 0);
        assert_eq!(saved.buffered_bytes, 0);
        assert_eq!(saved.backed_up_records, 1);
    }

    struct HeldFirstUpload {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        calls: AtomicUsize,
        sizes: Mutex<Vec<usize>>,
    }
    impl LogTransport for HeldFirstUpload {
        fn send(&self, body: &bytes::Bytes) -> Result<(), transport::AttemptError> {
            let request = wire::WriteBatchesRequest::decode(
                zstd::decode_all(body.as_ref()).unwrap().as_slice(),
            )
            .unwrap();
            self.sizes.lock().push(
                request
                    .batches
                    .iter()
                    .map(|b| b.points.as_ref().unwrap().timestamps.len())
                    .sum(),
            );
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.started.send(()).unwrap();
                self.release.lock().recv().unwrap();
            }
            Ok(())
        }
    }

    #[test]
    fn backpressure_waits_for_inflight_capacity_before_splitting_partial_batch() {
        let input = LogRecord::new(0, "one", HashMap::new());
        let charge = input.accounted_bytes("a")
            + RecordSize::new(&input).encoding_reservation("a", "fixture");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let target = Arc::new(HeldFirstUpload {
            started: started_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
            sizes: Mutex::new(Vec::new()),
        });
        let opts = LogStreamOptions {
            max_batch_bytes: charge * 2,
            max_buffered_bytes: charge * 3,
            max_records_per_batch: 2,
            max_request_delay: Duration::from_secs(60),
            num_upload_workers: 1,
            ..Default::default()
        };
        let stream = Arc::new(
            NominalLogStream::start(opts, Some(target.clone()), "fixture".into(), None).unwrap(),
        );
        stream
            .enqueue_batch("a", vec![input.clone(), input.clone()])
            .unwrap();
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        stream.enqueue("a", input.clone()).unwrap();
        let writer = stream.clone();
        let pending = thread::spawn(move || writer.enqueue("a", input));
        let deadline = Instant::now() + Duration::from_secs(2);
        // The upload is held in the mock, so only the blocked producer can wait here.
        let mut producer_waited = false;
        while Instant::now() < deadline {
            if stream.shared.changed.notify_one() {
                producer_waited = true;
                break;
            }
            thread::yield_now();
        }
        release_tx.send(()).unwrap();
        pending.join().unwrap().unwrap();
        let stats = stream.close().unwrap();
        assert!(producer_waited);
        assert_eq!(stats.acknowledged_records, 4);
        assert_eq!(target.sizes.lock().as_slice(), &[2, 2]);
    }
}
