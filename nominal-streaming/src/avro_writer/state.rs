use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::OnceLock;

use super::error::AvroWriterError;
use super::opts::AvroWriterOpts;
use super::stats::PipelineStats;
use crate::stream::NominalDatasetStream;

/// Shared state behind [`super::AvroWriter`]. Held as `Arc<AvroWriterInner>`
/// so clones are cheap and `Drop` runs exactly when the last reference goes.
pub(super) struct AvroWriterInner {
    /// Template path. When `max_points_per_file == 0`, this IS the output path.
    /// When rotating, derived paths follow `<stem>_<index:03d><suffix>`.
    pub(super) base_path: PathBuf,
    /// The file path currently being written to. Updated on each rotation.
    pub(super) current_path: parking_lot::Mutex<PathBuf>,
    /// Index of the current file (0-based). Incremented on each rotation.
    pub(super) file_index: AtomicUsize,
    /// Paths of all fully-closed (finalized) files, in order.
    pub(super) finalized_paths: parking_lot::Mutex<Vec<PathBuf>>,
    /// The live stream. `Option` so `close()` / `rotate()` can `.take()` it
    /// exclusively and drop it (drop drains + joins the stream's internal
    /// threads). The outer Mutex guards the take; reads via `as_ref()` from
    /// `write` also hold it.
    pub(super) stream: parking_lot::Mutex<Option<NominalDatasetStream>>,
    /// Shared with `ErrorLatchingConsumer` via `Arc` so the consumer can latch
    /// from the stream's dispatcher thread without a backchannel to the writer.
    /// Layering: outer `Arc` shares the cell across writer + consumer;
    /// [`OnceLock`] gives first-wins set semantics with lock-free reads; inner
    /// `Arc` lets [`AvroWriter`] callers clone the latched error cheaply out of
    /// the cell for return from every subsequent method call.
    ///
    /// [`AvroWriter`]: super::AvroWriter
    pub(super) first_error: Arc<OnceLock<Arc<AvroWriterError>>>,
    /// Points accepted into the current file since the last rotation (or since
    /// construction for the first file). Resets to 0 on each rotation.
    pub(super) points_in_current: AtomicU64,
    /// Cumulative points accepted across all files (never resets). Returned by
    /// [`super::AvroWriter::points_accepted`].
    pub(super) total_points_accepted: AtomicU64,
    /// Caches the result of the first `close()` call so subsequent calls return
    /// the same `Arc`'d error or `Vec<PathBuf>` without repeating the shutdown sequence.
    pub(super) close_result: OnceLock<Result<Vec<PathBuf>, Arc<AvroWriterError>>>,
    /// Serializes `close()` (write lock) against in-flight `write` calls
    /// (read lock). Prevents a write that has already checked `close_result`
    /// from enqueuing into a stream that a concurrent `close()` is about to drop.
    pub(super) close_lock: parking_lot::RwLock<()>,
    /// Serializes concurrent `Drop` calls across `AvroWriter` clones.
    /// Makes the `Arc::strong_count` check + "last clone decides" path atomic
    /// so two simultaneous drops don't both believe they are last.
    pub(super) drop_mutex: parking_lot::Mutex<()>,
    /// The opts the writer was constructed with. Kept so `rotate()` can spin
    /// up a fresh stream with the same batching / fsync settings; surfaces
    /// `fsync_on_close` / `max_points_per_file` to the hot paths.
    pub(super) opts: AvroWriterOpts,
    /// Pipeline timing stats. Shared with `ErrorLatchingConsumer` via `Arc`.
    pub(super) stats: Arc<PipelineStats>,
}

impl AvroWriterInner {
    /// Snapshot the latched error, if any. Returns a cheap clone of the `Arc`.
    pub(super) fn current_error(&self) -> Option<Arc<AvroWriterError>> {
        self.first_error.get().cloned()
    }

    /// Test-only: forcibly latch an error for testing error-surfacing paths.
    #[cfg(test)]
    pub(crate) fn __test_latch(&self, err: AvroWriterError) {
        let _ = self.first_error.set(Arc::new(err));
    }
}
