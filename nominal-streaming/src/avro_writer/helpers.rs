use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use super::consumer::ErrorLatchingConsumer;
use super::consumer::ParallelAvroFileConsumer;
use super::error::AvroWriterError;
use super::stats::PipelineStats;
use crate::consumer::WriteRequestConsumer;
use crate::stream::NominalDatasetStream;
use crate::stream::NominalStreamOpts;

/// Derive the numbered path for rotation index `index`.
/// `out.avro` at index 0 → `out_000.avro`.
pub(super) fn path_for_index(base: &Path, index: usize) -> PathBuf {
    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    let suffix = base
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    base.with_file_name(format!("{stem}_{index:03}{suffix}"))
}

/// Ensure the parent directory exists and truncate the file at `path`.
///
/// Returns `io::Error` on failure so callers can surface the error to
/// their own error type. (Historically this panicked; library-grade APIs
/// should not.)
pub(super) fn ensure_parent_and_truncate(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::File::create(path)?;
    Ok(())
}

/// Build a fresh `NominalDatasetStream` with the given consumer and writer opts.
pub(super) fn open_stream<C: WriteRequestConsumer + 'static>(
    consumer: C,
    max_points_per_batch: usize,
    max_batch_delay: Duration,
) -> NominalDatasetStream {
    let stream_opts = NominalStreamOpts {
        max_points_per_record: max_points_per_batch,
        max_request_delay: max_batch_delay,
        max_buffered_requests: 4,
        request_dispatcher_tasks: 1,
        base_api_url: String::new(),
    };
    NominalDatasetStream::new_with_consumer(consumer, stream_opts)
}

/// Open a new `ParallelAvroFileConsumer` + `ErrorLatchingConsumer` for the
/// given path, sharing the `first_error` latch and `stats` counters.
pub(super) fn open_error_latching_consumer(
    path: &Path,
    first_error: Arc<OnceLock<Arc<AvroWriterError>>>,
    stats: Arc<PipelineStats>,
) -> io::Result<impl WriteRequestConsumer> {
    let avro_consumer = ParallelAvroFileConsumer::new_with_full_path(path)?;
    Ok(ErrorLatchingConsumer {
        inner: avro_consumer,
        first_error,
        stats,
    })
}
