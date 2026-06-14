use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use tracing::debug;
use tracing::error;
use tracing::info;

use super::error::AvroWriterError;
use super::helpers::ensure_parent_and_truncate;
use super::helpers::open_error_latching_consumer;
use super::helpers::open_stream;
use super::helpers::path_for_index;
use super::opts::AvroWriterOpts;
use super::state::AvroWriterInner;
use super::stats::PipelineStats;
use crate::stream::points_len;
use crate::types::ChannelDescriptor;
use crate::types::IntoPoints;

/// Purpose-built avro file writer for the Nominal schema.
///
/// Thin wrapper over [`crate::stream::NominalDatasetStream`] that drives the
/// same battle-tested pipeline (primary/secondary buffer + batch_processor +
/// request_dispatcher) with an [`crate::consumer::AvroFileConsumer`] as the
/// sink. Producers call [`Self::write`] / [`Self::write_batch`] from any
/// thread (it's `Send + Sync`); ordering across threads is unspecified.
/// Errors latch on first failure and surface from every subsequent operation.
///
/// # Concurrency
///
/// - Multi-producer: any number of threads may call `write` / `write_batch`
///   concurrently.
/// - Ordering: not guaranteed across threads. Within a single producer,
///   order is preserved.
/// - Backpressure: writes block when the stream's internal buffers are full.
///
/// # Errors
///
/// Background encoder errors (I/O, avro encoding, fsync) are latched on first
/// occurrence. Every subsequent `write` / `flush` / `sync` / `close` returns
/// the same latched error. The distinct `SendAfterClose` error is returned
/// only when a write is attempted after `close()` completed.
///
/// # Note
///
/// - [`Self::points_accepted`] counts successful enqueues, not on-disk bytes.
///   Call [`Self::close`] to ensure all data is on disk.
/// - Latched errors are **permanent**: there is no reset mechanism. Once an
///   error is latched, every subsequent method call on every clone of this
///   writer returns the same `Arc<AvroWriterError>`.
/// - `Drop` performs a best-effort close; errors are logged via
///   `tracing::error!` but cannot propagate. For error visibility,
///   call [`Self::close`] explicitly.
/// - `flush()` and `sync()` are best-effort: the underlying stream does not
///   expose a force-drain hook, so `flush()` is a no-op aside from the latched
///   error check, and `sync()` fsyncs whatever data has already dispatched.
///   For strong durability, call `close()`.
#[derive(Clone)]
pub struct AvroWriter {
    pub(super) inner: Arc<AvroWriterInner>,
}

impl AvroWriter {
    /// Construct a writer that will write to `path`.
    ///
    /// If a file already exists at `path`, it is **truncated** before writing
    /// begins. This differs from
    /// [`crate::consumer::AvroFileConsumer::new_with_full_path`], which opens
    /// without truncation. The `AvroWriter` contract is "write a fresh avro
    /// file"; leaving stale tail bytes from a prior run would corrupt the
    /// stream.
    ///
    /// The parent directory is created if it does not exist.
    ///
    /// Opens the avro file and starts the underlying stream immediately.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the parent directory cannot be created, the
    /// file cannot be truncated, or the underlying consumer cannot open the
    /// file for writing.
    pub fn new(path: impl Into<PathBuf>, opts: AvroWriterOpts) -> std::io::Result<Self> {
        let base_path: PathBuf = path.into();
        // Decide the initial file path based on rotation mode.
        let current_path = if opts.max_points_per_file == 0 {
            base_path.clone()
        } else {
            path_for_index(&base_path, 0)
        };

        // Ensure parent dir exists and truncate any pre-existing file.
        // AvroWriter writes a fresh avro stream; leaving stale tail bytes
        // from a prior run would corrupt the file for readers.
        ensure_parent_and_truncate(&current_path)?;

        // Build the avro consumer, wrap it in an error-latching layer.
        let first_error: Arc<OnceLock<Arc<AvroWriterError>>> = Arc::new(OnceLock::new());
        let stats: Arc<PipelineStats> = Arc::new(PipelineStats::default());
        let wrapped =
            open_error_latching_consumer(&current_path, first_error.clone(), stats.clone())?;
        let stream = open_stream(wrapped, opts.max_points_per_batch, opts.max_batch_delay);

        let inner = Arc::new(AvroWriterInner {
            base_path: base_path.clone(),
            current_path: parking_lot::Mutex::new(current_path.clone()),
            file_index: AtomicUsize::new(0),
            finalized_paths: parking_lot::Mutex::new(Vec::new()),
            stream: parking_lot::Mutex::new(Some(stream)),
            first_error,
            points_in_current: AtomicU64::new(0),
            total_points_accepted: AtomicU64::new(0),
            close_result: OnceLock::new(),
            close_lock: parking_lot::RwLock::new(()),
            drop_mutex: parking_lot::Mutex::new(()),
            opts: opts.clone(),
            stats,
        });

        info!(
            "AvroWriter opened: path={} opts={:?}",
            current_path.display(),
            opts
        );

        Ok(Self { inner })
    }

    /// Test-only constructor that injects a custom `WriteRequestConsumer`
    /// instead of the default `AvroFileConsumer`. The consumer is wrapped
    /// in the standard `ErrorLatchingConsumer` so error-latching semantics
    /// are identical. Used to exercise end-to-end error propagation
    /// without depending on filesystem failures.
    #[cfg(test)]
    pub(crate) fn new_with_consumer_for_testing<C>(
        consumer: C,
        path: PathBuf,
        opts: AvroWriterOpts,
    ) -> Self
    where
        C: crate::consumer::WriteRequestConsumer + 'static,
    {
        use super::consumer::ErrorLatchingConsumer;

        let first_error: Arc<OnceLock<Arc<AvroWriterError>>> = Arc::new(OnceLock::new());
        let stats: Arc<PipelineStats> = Arc::new(PipelineStats::default());
        let wrapped = ErrorLatchingConsumer {
            inner: consumer,
            first_error: first_error.clone(),
            stats: stats.clone(),
        };
        let stream = open_stream(wrapped, opts.max_points_per_batch, opts.max_batch_delay);

        let current_path = path.clone();
        let inner = Arc::new(AvroWriterInner {
            base_path: path.clone(),
            current_path: parking_lot::Mutex::new(current_path),
            file_index: AtomicUsize::new(0),
            finalized_paths: parking_lot::Mutex::new(Vec::new()),
            stream: parking_lot::Mutex::new(Some(stream)),
            first_error,
            points_in_current: AtomicU64::new(0),
            total_points_accepted: AtomicU64::new(0),
            close_result: OnceLock::new(),
            close_lock: parking_lot::RwLock::new(()),
            drop_mutex: parking_lot::Mutex::new(()),
            // Force rotation off for the test consumer so we don't try to
            // reopen a new `AvroFileConsumer` after rotation — the test's
            // injected consumer is single-shot.
            opts: AvroWriterOpts {
                max_points_per_file: 0,
                ..opts.clone()
            },
            stats,
        });

        Self { inner }
    }

    /// The file path currently being written to.
    ///
    /// When `max_points_per_file == 0`, this is the path given to the constructor.
    /// When rotating, this is the path of the currently-open file.
    pub fn path(&self) -> PathBuf {
        self.inner.current_path.lock().clone()
    }

    /// All finalized (fully-closed) file paths, in order.
    ///
    /// Empty until at least one rotation has occurred (or `close()` is called).
    pub fn finalized_paths(&self) -> Vec<PathBuf> {
        self.inner.finalized_paths.lock().clone()
    }

    /// Every file path this writer has opened, in order.
    ///
    /// Includes the currently-open file while the writer is live; after
    /// `close()`, identical to `finalized_paths()`.
    pub fn written_files(&self) -> Vec<PathBuf> {
        let mut v = self.inner.finalized_paths.lock().clone();
        if self.inner.close_result.get().is_none() {
            v.push(self.inner.current_path.lock().clone());
        }
        v
    }

    /// Total points successfully accepted by the writer so far, across all files.
    ///
    /// Does not include points that failed (latched error, `SendAfterClose`).
    /// Does not reflect on-disk state — use [`Self::close`] to ensure durability.
    pub fn points_accepted(&self) -> u64 {
        self.inner.total_points_accepted.load(Ordering::Relaxed)
    }

    /// Typed reference to the pipeline timing counters.
    /// Read fields via `.load(Ordering::Relaxed)` on each `AtomicU64`.
    pub fn stats(&self) -> &PipelineStats {
        &self.inner.stats
    }

    /// Drop the current stream (which drains + closes its avro file via its
    /// own `Drop`), then optionally fsync the just-finished file path per
    /// `opts.fsync_on_close`. Returns the path of the just-finished file on
    /// success; on fsync failure, latches the `Io` error and returns it.
    ///
    /// If an error was already latched during the drain (the consumer
    /// propagated it from a failed `consume`), fsync is skipped — the file
    /// may be in a half-written state and fsyncing wouldn't produce a
    /// meaningful durability guarantee. The caller still sees the latched
    /// error via [`AvroWriterInner::current_error`].
    ///
    /// Shared by `rotate`, `close`, and `Drop` — each of which follows it up
    /// with their own "what next" (open a new shard / record finalized path
    /// / log & exit).
    ///
    /// Must be called without holding `stream.lock()`.
    fn take_and_fsync_stream(&self) -> Result<PathBuf, Arc<AvroWriterError>> {
        // `drop` on the taken stream drains pending batches and joins the
        // dispatcher threads — after this line the consumer has processed
        // every request the writer has dispatched.
        let _ = self.inner.stream.lock().take();
        let current = self.inner.current_path.lock().clone();
        if self.inner.opts.fsync_on_close && self.inner.current_error().is_none() {
            if let Err(e) = std::fs::File::open(&current).and_then(|f| f.sync_all()) {
                let wrapped = Arc::new(AvroWriterError::Io(std::io::Error::new(
                    e.kind(),
                    e.to_string(),
                )));
                let _ = self.inner.first_error.set(wrapped.clone());
                return Err(wrapped);
            }
        }
        Ok(current)
    }

    /// Rotate to a new file: finalize the current shard via
    /// [`Self::take_and_fsync_stream`], record it in `finalized_paths`, then
    /// open a fresh stream writing to the next numbered path.
    ///
    /// Must be called without holding `stream.lock()`.
    fn rotate(&self) -> Result<(), Arc<AvroWriterError>> {
        let finalized = self.take_and_fsync_stream()?;
        self.inner.finalized_paths.lock().push(finalized);

        // Derive and open the next file.
        let new_index = self.inner.file_index.fetch_add(1, Ordering::Relaxed) + 1;
        let new_path = path_for_index(&self.inner.base_path, new_index);
        ensure_parent_and_truncate(&new_path).map_err(|e| {
            let wrapped = Arc::new(AvroWriterError::from(e));
            let _ = self.inner.first_error.set(wrapped.clone());
            wrapped
        })?;
        *self.inner.current_path.lock() = new_path.clone();

        let wrapped = open_error_latching_consumer(
            &new_path,
            self.inner.first_error.clone(),
            self.inner.stats.clone(),
        )
        .map_err(|e| {
            let wrapped = Arc::new(AvroWriterError::from(e));
            let _ = self.inner.first_error.set(wrapped.clone());
            wrapped
        })?;
        let new_stream = open_stream(
            wrapped,
            self.inner.opts.max_points_per_batch,
            self.inner.opts.max_batch_delay,
        );
        *self.inner.stream.lock() = Some(new_stream);
        self.inner.points_in_current.store(0, Ordering::Relaxed);

        debug!("AvroWriter: rotated to {}", new_path.display());
        Ok(())
    }

    /// Write points for a single channel. Generic over any `Vec<T>` that
    /// can be converted into [`PointsType`] via
    /// [`crate::types::IntoPoints`] (e.g. `Vec<DoublePoint>`,
    /// `Vec<IntegerPoint>`, ...).
    ///
    /// Rotation-aware: if `max_points_per_file > 0` and the batch would
    /// overshoot the remaining capacity of the current file, the batch is
    /// split along the point axis so each shard stays at or under the
    /// threshold.
    ///
    /// # Errors
    ///
    /// - Returns any latched encoder error if one was recorded before or
    ///   during this call.
    /// - Returns [`AvroWriterError::SendAfterClose`] if called after close.
    pub fn write<T>(
        &self,
        channel: &ChannelDescriptor,
        points: Vec<T>,
    ) -> Result<(), Arc<AvroWriterError>>
    where
        Vec<T>: IntoPoints,
        T: Clone,
    {
        self.write_chunked(channel.clone(), points)
    }

    /// Rotation-aware chunking for single-channel batches, implemented as a
    /// loop over [`Self::write_batch`]. A large single-channel batch that
    /// would overshoot the current file's remaining capacity gets split
    /// along the point axis into chunks that each fit. Without this helper
    /// the file would overshoot `max_points_per_file` by up to one batch's
    /// worth.
    pub(super) fn write_chunked<T: Clone>(
        &self,
        descriptor: ChannelDescriptor,
        points: Vec<T>,
    ) -> Result<(), Arc<AvroWriterError>>
    where
        Vec<T>: IntoPoints,
    {
        if self.inner.opts.max_points_per_file == 0 {
            return self.write_batch(vec![(descriptor, points.into_points())]);
        }

        let max = self.inner.opts.max_points_per_file as u64;
        let n = points.len();
        let mut i = 0;
        while i < n {
            let current = self.inner.points_in_current.load(Ordering::Relaxed);
            let remaining = max.saturating_sub(current);
            let chunk_size = if remaining == 0 {
                max as usize
            } else {
                remaining as usize
            };
            let chunk_end = (i + chunk_size).min(n);
            let chunk = points[i..chunk_end].to_vec();
            self.write_batch(vec![(descriptor.clone(), chunk.into_points())])?;
            i = chunk_end;
        }
        Ok(())
    }

    /// Multi-channel batch write. Each entry lands in the same critical
    /// section, amortizing the buffer-lock across all channels.
    ///
    /// Takes the outer `stream` lock and the inner buffer lock each once for
    /// the whole batch, amortizing lock-acquisition cost across all channels.
    /// Use this when pushing a wide frame (many channels sharing a timestamp
    /// span) — it is the path `write_dataframe` uses internally.
    ///
    /// Rotation is handled rotate-before: if the current file is already at
    /// or beyond the limit, a rotation happens before the batch is enqueued.
    /// A very large batch may still push the file slightly past
    /// `max_points_per_file` — consistent with single-call `write` behavior.
    ///
    /// # Errors
    ///
    /// - Returns any latched encoder error if one was recorded before or
    ///   during this call.
    /// - Returns [`AvroWriterError::SendAfterClose`] if called after close.
    pub fn write_batch(
        &self,
        batch: Vec<(ChannelDescriptor, PointsType)>,
    ) -> Result<(), Arc<AvroWriterError>> {
        if let Some(err) = self.inner.current_error() {
            return Err(err);
        }
        let _close_guard = self.inner.close_lock.read();
        if self.inner.close_result.get().is_some() {
            return Err(Arc::new(AvroWriterError::SendAfterClose));
        }

        let total: u64 = batch.iter().map(|(_, p)| points_len(p) as u64).sum();
        if total == 0 {
            return Ok(());
        }

        if self.inner.opts.max_points_per_file > 0
            && self.inner.points_in_current.load(Ordering::Relaxed)
                >= self.inner.opts.max_points_per_file as u64
        {
            self.rotate()?;
        }

        // Wall-time instrumentation: time the actual stream interaction
        // (lock + enqueue_batch). Any blocking in `when_capacity` shows up here.
        let t0 = std::time::Instant::now();
        let stream_guard = self.inner.stream.lock();
        let stream = stream_guard
            .as_ref()
            .ok_or_else(|| Arc::new(AvroWriterError::SendAfterClose))?;

        stream.enqueue_batch(batch);
        drop(stream_guard);
        self.inner
            .stats
            .enqueue_batch_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        self.inner
            .points_in_current
            .fetch_add(total, Ordering::Relaxed);
        self.inner
            .total_points_accepted
            .fetch_add(total, Ordering::Relaxed);
        Ok(())
    }

    /// Best-effort: check for latched errors. Points that the stream has already
    /// dispatched to the consumer will be on disk; points still in the stream's
    /// internal buffer may not be. For strong durability, call `close()`.
    ///
    /// # Errors
    ///
    /// Returns any latched encoder error ([`AvroWriterError::Io`],
    /// [`AvroWriterError::Avro`], or [`AvroWriterError::Consumer`]) if one was
    /// recorded before or during this call. Once an error is latched it is
    /// permanent; all subsequent operations return the same error.
    pub fn flush(&self) -> Result<(), Arc<AvroWriterError>> {
        // Best-effort: points that the stream has already dispatched to the
        // consumer will be on disk; points still in the stream's internal
        // buffer may not be. For strong durability, call `close()`.
        if let Some(err) = self.inner.current_error() {
            return Err(err);
        }
        Ok(())
    }

    /// [`Self::flush`] plus a best-effort `File::sync_all()` on data that has
    /// already been dispatched.
    ///
    /// # Errors
    ///
    /// Returns any latched encoder error. The `fsync` failure itself is reported
    /// as [`AvroWriterError::Io`] and is also latched, so subsequent calls will
    /// return the same error.
    pub fn sync(&self) -> Result<(), Arc<AvroWriterError>> {
        // See `flush()`. On a best-effort basis, fsync the file path —
        // anything already dispatched is now durable.
        if let Some(err) = self.inner.current_error() {
            return Err(err);
        }
        let current = self.inner.current_path.lock().clone();
        if let Err(e) = std::fs::File::open(&current).and_then(|f| f.sync_all()) {
            let wrapped = Arc::new(AvroWriterError::from(e));
            let _ = self.inner.first_error.set(wrapped.clone());
            return Err(wrapped);
        }
        debug!("AvroWriter sync: fsync complete path={}", current.display());
        Ok(())
    }

    /// Gracefully shut down the encoder and return the list of all written files.
    ///
    /// Idempotent: the first call performs the shutdown and caches the result;
    /// subsequent calls return the cached result (same `Arc` on error). Once
    /// this returns `Ok`, any further write calls return
    /// [`AvroWriterError::SendAfterClose`].
    ///
    /// When `max_points_per_file == 0` (no rotation), returns a 1-element
    /// `Vec` containing the single output path. When rotating, returns all
    /// files written, in order.
    ///
    /// # Errors
    ///
    /// Returns any latched encoder error, or an error from the shutdown path
    /// (final flush, fsync), whichever came first.
    pub fn close(&self) -> Result<Vec<PathBuf>, Arc<AvroWriterError>> {
        if let Some(cached) = self.inner.close_result.get() {
            return cached.clone();
        }

        let _close_guard = self.inner.close_lock.write();
        // Re-check after acquiring the lock — another thread may have closed
        // while we were waiting.
        if let Some(cached) = self.inner.close_result.get() {
            return cached.clone();
        }

        info!(
            "AvroWriter close: path={} total_points_accepted={}",
            self.inner.current_path.lock().display(),
            self.inner.total_points_accepted.load(Ordering::Relaxed)
        );

        // Finalize the current shard (drain + fsync). If the consumer also
        // latched an error during the drain, `current_error()` reflects it —
        // a latched error takes precedence over a successful fsync result,
        // matching the pre-refactor behavior.
        let result: Result<Vec<PathBuf>, Arc<AvroWriterError>> = match self.take_and_fsync_stream()
        {
            Err(e) => Err(e),
            Ok(current) => match self.inner.current_error() {
                Some(err) => Err(err),
                None => {
                    self.inner.finalized_paths.lock().push(current);
                    Ok(self.inner.finalized_paths.lock().clone())
                }
            },
        };

        let _ = self.inner.close_result.set(result.clone());
        self.inner.close_result.get().cloned().unwrap_or(result)
    }
}

impl Drop for AvroWriter {
    fn drop(&mut self) {
        if self.inner.close_result.get().is_some() {
            return;
        }
        let _drop_guard = self.inner.drop_mutex.lock();
        if self.inner.close_result.get().is_some() {
            return;
        }
        // Only the last surviving clone should trigger shutdown. `drop_mutex`
        // above makes the count check + the decision to proceed atomic w.r.t.
        // other concurrent drops. The `Inner` Arc is held only by external
        // `AvroWriter` clones — no background-thread Arcs — so
        // `strong_count == 1` reliably means "I am the last."
        if Arc::strong_count(&self.inner) > 1 {
            return;
        }

        let _close_guard = self.inner.close_lock.write();

        // Best-effort finalize: drain the stream and fsync. Errors go to
        // tracing (drop can't propagate Result) but are still latched into
        // `first_error` by `take_and_fsync_stream` so any subsequent
        // `close()` / `sync()` / `flush()` call on a surviving clone would
        // surface them — except there are none, we're the last.
        if let Err(e) = self.take_and_fsync_stream() {
            error!("AvroWriter drop: finalize failed: {e}");
        }
        if let Some(err) = self.inner.current_error() {
            error!("AvroWriter drop: latched error: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use apache_avro::Reader;
    use nominal_api::tonic::google::protobuf::Timestamp;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
    use tempfile::TempDir;

    use super::super::helpers::path_for_index;
    use super::*;
    use crate::consumer::ConsumerError;
    use crate::consumer::ConsumerResult;
    use crate::consumer::WriteRequestConsumer;
    use crate::types::ChannelDescriptor;
    use crate::types::IntoPoints;

    /// A `WriteRequestConsumer` that always fails with a configured
    /// `ConsumerError::IoError`, for testing end-to-end error propagation.
    #[derive(Debug)]
    struct FailingConsumer {
        kind: std::io::ErrorKind,
        message: String,
    }

    impl FailingConsumer {
        fn new(kind: std::io::ErrorKind, message: &str) -> Self {
            Self {
                kind,
                message: message.to_string(),
            }
        }
    }

    impl WriteRequestConsumer for FailingConsumer {
        fn consume(&self, _request: &WriteRequestNominal) -> ConsumerResult<()> {
            Err(ConsumerError::IoError(std::io::Error::new(
                self.kind,
                self.message.clone(),
            )))
        }
    }

    fn ts(secs: i64) -> Timestamp {
        Timestamp {
            seconds: secs,
            nanos: 0,
        }
    }

    fn cd(name: &str) -> ChannelDescriptor {
        ChannelDescriptor::new(name)
    }

    /// Test-only shim: routes through `write_chunked` (the same code path
    /// the public `write` method uses) so tests cover the chunking-across-
    /// rotation behavior.
    fn enq<T: Clone>(
        writer: &AvroWriter,
        descriptor: ChannelDescriptor,
        points: Vec<T>,
    ) -> Result<(), Arc<AvroWriterError>>
    where
        Vec<T>: IntoPoints,
    {
        writer.write_chunked(descriptor, points)
    }

    #[test_log::test]
    fn roundtrip_all_value_types() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("all_types.avro");
        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();

        enq(
            &writer,
            cd("d"),
            vec![DoublePoint {
                timestamp: Some(ts(1)),
                value: 1.5,
            }],
        )
        .unwrap();
        enq(
            &writer,
            cd("i"),
            vec![IntegerPoint {
                timestamp: Some(ts(2)),
                value: 42,
            }],
        )
        .unwrap();
        enq(
            &writer,
            cd("s"),
            vec![StringPoint {
                timestamp: Some(ts(3)),
                value: "hello".into(),
            }],
        )
        .unwrap();
        enq(
            &writer,
            cd("struct"),
            vec![StructPoint {
                timestamp: Some(ts(4)),
                json_string: r#"{"a":1}"#.into(),
            }],
        )
        .unwrap();
        enq(
            &writer,
            cd("fa"),
            vec![DoubleArrayPoint {
                timestamp: Some(ts(5)),
                value: vec![1.0, 2.0, 3.0],
            }],
        )
        .unwrap();
        enq(
            &writer,
            cd("sa"),
            vec![StringArrayPoint {
                timestamp: Some(ts(6)),
                value: vec!["a".into(), "b".into()],
            }],
        )
        .unwrap();

        writer.close().unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();
        let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 6, "one record per distinct channel");
    }

    #[test_log::test]
    fn batch_boundaries_emit_multiple_records() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("batches.avro");

        // Set batch size to 10; write 35 points via one write call.
        // With NominalDatasetStream, record count depends on internal flush
        // timing. Assert all 35 points are durably on disk after close.
        let writer = AvroWriter::new(
            path.clone(),
            AvroWriterOpts {
                max_points_per_batch: 10,
                ..Default::default()
            },
        )
        .unwrap();

        let pts: Vec<IntegerPoint> = (0..35)
            .map(|i| IntegerPoint {
                timestamp: Some(ts(i)),
                value: i,
            })
            .collect();
        enq(&writer, cd("x"), pts).unwrap();
        writer.close().unwrap();

        // All points must still be present on disk.
        assert_eq!(count_points_in_file(&path), 35);
    }

    #[test_log::test]
    fn multi_producer_all_points_land() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("mp.avro");
        let writer = Arc::new(AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap());

        let threads: Vec<_> = (0..8)
            .map(|t| {
                let w = writer.clone();
                std::thread::spawn(move || {
                    for i in 0..10_000i64 {
                        enq(
                            &w,
                            cd(&format!("ch{t}")),
                            vec![IntegerPoint {
                                timestamp: Some(ts(i)),
                                value: i,
                            }],
                        )
                        .unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(writer.points_accepted(), 80_000);
        writer.close().unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();
        let record_count = reader.count();
        // 8 channels, at most ceil(10_000 / 250_000) = 1 record per channel
        // (since default max_points_per_batch is 250k). Actual count depends
        // on delay-based emission interleaving; assert lower bound.
        assert!(record_count >= 8, "record_count = {record_count}");
    }

    #[test_log::test]
    fn error_latches_and_surfaces() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("latch.avro");
        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();

        // Write one valid point first so we know the writer is functional.
        enq(
            &writer,
            cd("x"),
            vec![IntegerPoint {
                timestamp: Some(ts(1)),
                value: 1,
            }],
        )
        .unwrap();

        // Inject a synthetic latched error.
        writer
            .inner
            .__test_latch(AvroWriterError::Consumer("boom".into()));

        // Subsequent write should return the latched error.
        let write_err = enq(
            &writer,
            cd("x"),
            vec![IntegerPoint {
                timestamp: Some(ts(2)),
                value: 2,
            }],
        );
        assert!(
            matches!(
                write_err.as_ref().err().map(|e| &**e),
                Some(AvroWriterError::Consumer(_))
            ),
            "expected latched Consumer error, got {write_err:?}"
        );

        // flush should also surface.
        let flush_err = writer.flush();
        assert!(
            flush_err.is_err(),
            "expected flush to surface latched error"
        );

        // sync too.
        let sync_err = writer.sync();
        assert!(sync_err.is_err(), "expected sync to surface latched error");

        // close returns the latched error.
        let close_err = writer.close();
        assert!(
            close_err.is_err(),
            "expected close to surface latched error"
        );

        // Repeated close returns the same cached error (same Arc).
        let close_err2 = writer.close();
        match (close_err.as_ref(), close_err2.as_ref()) {
            (Err(a), Err(b)) => assert!(Arc::ptr_eq(a, b), "close() should return same cached Arc"),
            _ => panic!("both closes should be Err"),
        }
    }

    #[test_log::test]
    fn drop_without_close_flushes_data() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("drop.avro");

        {
            let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();
            for i in 0..50i64 {
                enq(
                    &writer,
                    cd("x"),
                    vec![IntegerPoint {
                        timestamp: Some(ts(i)),
                        value: i,
                    }],
                )
                .unwrap();
            }
            let start = std::time::Instant::now();
            drop(writer);
            let elapsed = start.elapsed();
            println!("drop elapsed: {elapsed:?}");
            assert!(
                elapsed < Duration::from_secs(10),
                "drop too slow: {elapsed:?}"
            );
        }

        // Contract: drop-without-close must preserve all pushed points.
        // Do not assert on record count — the batch_processor may split the
        // 50-push loop into multiple records if `max_batch_delay` (100ms)
        // elapses mid-loop under CPU contention. That's a timing-dependent
        // batching detail, not a data-loss bug.
        assert_eq!(count_points_in_file(&path), 50);
    }

    #[test_log::test]
    fn close_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("idem.avro");
        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();
        enq(
            &writer,
            cd("x"),
            vec![IntegerPoint {
                timestamp: Some(ts(1)),
                value: 1,
            }],
        )
        .unwrap();

        let first = writer.close().unwrap();
        let second = writer.close().unwrap();
        assert_eq!(first, second);
        assert_eq!(first, vec![path.clone()]);

        // Post-close write returns SendAfterClose.
        let after = enq(
            &writer,
            cd("x"),
            vec![IntegerPoint {
                timestamp: Some(ts(2)),
                value: 2,
            }],
        );
        match after {
            Err(e) => match &*e {
                AvroWriterError::SendAfterClose => {}
                other => panic!("expected SendAfterClose, got {other:?}"),
            },
            Ok(()) => panic!("write after close should fail"),
        }
    }

    #[test_log::test]
    fn flush_and_sync_both_succeed() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fs.avro");
        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();
        enq(
            &writer,
            cd("x"),
            vec![DoublePoint {
                timestamp: Some(ts(1)),
                value: 1.0,
            }],
        )
        .unwrap();

        // flush() is best-effort with NominalDatasetStream; just verify it returns Ok.
        writer.flush().unwrap();

        // sync() is also best-effort; verify it returns Ok.
        writer.sync().unwrap();

        // close() guarantees all data is durably on disk.
        writer.close().unwrap();

        // Verify the file exists and has content after close.
        assert!(
            std::fs::metadata(&path).unwrap().len() > 0,
            "file should have content after close"
        );
    }

    #[test_log::test]
    fn roundtrip_single_double() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("out.avro");
        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();

        enq(
            &writer,
            cd("speed"),
            vec![DoublePoint {
                timestamp: Some(ts(42)),
                value: 3.14,
            }],
        )
        .unwrap();

        let closed = writer.close().unwrap();
        assert_eq!(closed, vec![path.clone()]);

        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();
        let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1, "one record per series");
    }

    fn count_points_in_file(path: &std::path::Path) -> usize {
        // A 0-byte file is possible if the writer was closed before any
        // data was written (e.g., in the concurrent-race test where the
        // writer thread hadn't scheduled yet). Treat as "0 points".
        let Ok(file) = std::fs::File::open(path) else {
            return 0;
        };
        let Ok(reader) = Reader::new(file) else {
            return 0;
        };
        reader
            .filter_map(|r| r.ok())
            .map(|v| match v {
                apache_avro::types::Value::Record(fields) => fields
                    .iter()
                    .find_map(|(name, val)| match (name.as_str(), val) {
                        ("timestamps", apache_avro::types::Value::Array(arr)) => Some(arr.len()),
                        _ => None,
                    })
                    .unwrap_or(0),
                _ => 0,
            })
            .sum()
    }

    #[test_log::test]
    fn concurrent_write_and_close_no_silent_drops() {
        // Repeat to reliably trigger the race.
        for iteration in 0..10 {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join(format!("race_{iteration}.avro"));
            let writer =
                Arc::new(AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap());

            let accepted = Arc::new(AtomicU64::new(0));
            let writer_c = writer.clone();
            let accepted_c = accepted.clone();
            let writer_thread = std::thread::spawn(move || {
                for i in 0..10_000i64 {
                    let res = enq(
                        &writer_c,
                        cd("x"),
                        vec![IntegerPoint {
                            timestamp: Some(ts(i)),
                            value: i,
                        }],
                    );
                    if res.is_ok() {
                        accepted_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

            // Give the writer thread a moment to start producing.
            std::thread::sleep(Duration::from_micros(100));
            writer.close().unwrap();
            writer_thread.join().unwrap();

            let accepted_final = accepted.load(Ordering::Relaxed);
            let file_points = count_points_in_file(&path);

            // Every write that returned Ok must be durably in the file.
            assert_eq!(
                accepted_final as usize, file_points,
                "iter {iteration}: accepted {accepted_final} but file has {file_points}"
            );
        }
    }

    #[test_log::test]
    fn simultaneous_drop_triggers_shutdown() {
        // Repeat to reliably trigger the race.
        for iteration in 0..20 {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join(format!("drop_race_{iteration}.avro"));
            let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();
            enq(
                &writer,
                cd("x"),
                vec![IntegerPoint {
                    timestamp: Some(ts(1)),
                    value: 1,
                }],
            )
            .unwrap();

            let c1 = writer.clone();
            let c2 = writer.clone();
            drop(writer);

            let barrier = Arc::new(std::sync::Barrier::new(2));
            let b1 = barrier.clone();
            let b2 = barrier.clone();
            let h1 = std::thread::spawn(move || {
                b1.wait();
                drop(c1);
            });
            let h2 = std::thread::spawn(move || {
                b2.wait();
                drop(c2);
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // If the encoder hung, we would never reach this point or the file
            // would be empty/unreadable. Verify the single point we wrote is
            // present.
            assert_eq!(
                count_points_in_file(&path),
                1,
                "iter {iteration}: expected 1 point"
            );
        }
    }

    #[test_log::test]
    fn new_truncates_preexisting_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("preexisting.avro");

        // Plant garbage bytes at the target path — larger than any avro file
        // the test will write, so the "tail leaks past new content" failure
        // mode would manifest.
        std::fs::write(&path, vec![0xffu8; 10_000]).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 10_000);

        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();
        enq(
            &writer,
            cd("x"),
            vec![IntegerPoint {
                timestamp: Some(ts(1)),
                value: 42,
            }],
        )
        .unwrap();
        writer.close().unwrap();

        // The file must be a clean avro stream with exactly one record of one
        // point — no garbage suffix.
        assert_eq!(count_points_in_file(&path), 1);
        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();
        let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test_log::test]
    fn end_to_end_error_latches_through_dispatcher() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("ignored.avro");
        let writer = AvroWriter::new_with_consumer_for_testing(
            FailingConsumer::new(std::io::ErrorKind::PermissionDenied, "denied"),
            path,
            AvroWriterOpts::default(),
        );

        // Enqueue some points. They travel through the full pipeline:
        //   write() -> stream.enqueue() -> batch_processor -> request_dispatcher
        //   -> ErrorLatchingConsumer::consume() -> FailingConsumer::consume() (fails)
        //   -> ErrorLatchingConsumer latches AvroWriterError::Io(PermissionDenied)
        enq(
            &writer,
            cd("x"),
            vec![IntegerPoint {
                timestamp: Some(ts(1)),
                value: 1,
            }],
        )
        .unwrap();

        // close() drops the stream, which drains any pending commands. After
        // drop, the consumer has processed (and failed on) the enqueued batch,
        // so first_error is populated and close() returns it.
        let err = writer.close().unwrap_err();

        // Variant should be Io (not Consumer)
        match &*err {
            AvroWriterError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::PermissionDenied);
            }
            other => panic!("expected AvroWriterError::Io, got {other:?}"),
        }

        // close() is idempotent — second call returns the same Arc'd error.
        let err2 = writer.close().unwrap_err();
        assert!(
            Arc::ptr_eq(&err, &err2),
            "close() should cache the error Arc"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Rotation-specific tests (max_points_per_file > 0)
    // ─────────────────────────────────────────────────────────────────────────

    #[test_log::test]
    fn single_file_when_max_is_zero() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("no_rotation.avro");
        let writer = AvroWriter::new(path.clone(), AvroWriterOpts::default()).unwrap();

        let pts: Vec<IntegerPoint> = (0..1000i64)
            .map(|i| IntegerPoint {
                timestamp: Some(ts(i)),
                value: i,
            })
            .collect();
        enq(&writer, cd("x"), pts).unwrap();

        let paths = writer.close().unwrap();
        assert_eq!(paths.len(), 1, "no rotation: exactly one file");
        assert_eq!(paths[0], path);
        assert_eq!(count_points_in_file(&paths[0]), 1000);
    }

    #[test_log::test]
    fn rotates_at_threshold() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("rot.avro");
        let opts = AvroWriterOpts {
            max_points_per_file: 100,
            ..Default::default()
        };
        let writer = AvroWriter::new(base.clone(), opts).unwrap();

        // Write 250 points one-at-a-time so single-point enqueue rotation fires.
        for i in 0..250i64 {
            enq(
                &writer,
                cd("x"),
                vec![IntegerPoint {
                    timestamp: Some(ts(i)),
                    value: i,
                }],
            )
            .unwrap();
        }

        let paths = writer.close().unwrap();
        assert_eq!(paths.len(), 3, "expected 3 files for 250 pts at max=100");
        assert_eq!(paths[0], path_for_index(&base, 0));
        assert_eq!(paths[1], path_for_index(&base, 1));
        assert_eq!(paths[2], path_for_index(&base, 2));

        let counts: Vec<usize> = paths.iter().map(|p| count_points_in_file(p)).collect();
        assert_eq!(counts, vec![100, 100, 50]);
    }

    #[test_log::test]
    fn write_batch_splits_across_rotation() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("batch_split.avro");
        let opts = AvroWriterOpts {
            max_points_per_file: 100,
            ..Default::default()
        };
        let writer = AvroWriter::new(base.clone(), opts).unwrap();

        // Pre-fill 80 points into the first file.
        let pre: Vec<IntegerPoint> = (0..80i64)
            .map(|i| IntegerPoint {
                timestamp: Some(ts(i)),
                value: i,
            })
            .collect();
        enq(&writer, cd("x"), pre).unwrap();
        assert_eq!(writer.inner.points_in_current.load(Ordering::Relaxed), 80);
        assert_eq!(writer.finalized_paths().len(), 0);

        // Write a batch of 50 via write_chunked — should split 20 + 30.
        let batch: Vec<IntegerPoint> = (80..130i64)
            .map(|i| IntegerPoint {
                timestamp: Some(ts(i)),
                value: i,
            })
            .collect();
        enq(&writer, cd("x"), batch).unwrap();

        assert_eq!(
            writer.finalized_paths().len(),
            1,
            "first file should be finalized"
        );
        assert_eq!(writer.inner.points_in_current.load(Ordering::Relaxed), 30);

        let paths = writer.close().unwrap();
        assert_eq!(paths.len(), 2);
        let counts: Vec<usize> = paths.iter().map(|p| count_points_in_file(p)).collect();
        assert_eq!(counts, vec![100, 30]);
    }
}
