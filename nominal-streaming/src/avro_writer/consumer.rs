use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

use nominal_api::tonic::io::nominal::scout::api::proto;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use tracing::error;

use super::error::avro_error_from_consumer_ref;
use super::error::AvroWriterError;
use super::stats::PipelineStats;
use crate::consumer::ConsumerError;
use crate::consumer::ConsumerResult;
use crate::consumer::WriteRequestConsumer;

/// Avro file consumer that parallelizes per-series `Record` building across
/// scoped threads, then feeds the resulting records to a single
/// `apache_avro::Writer` under a mutex (identical on-disk output to
/// [`crate::consumer::AvroFileConsumer`]). Reuses
/// [`crate::consumer::points_to_avro`] for the per-dtype `Value`-tree
/// construction — only the parallel wrapper is new.
///
/// Lives alongside [`super::AvroWriter`] because the parallelism only
/// benefits the single-file write path — the shared `AvroFileConsumer`
/// used by network-streaming callers stays untouched.
pub(super) struct ParallelAvroFileConsumer {
    writer: Arc<parking_lot::Mutex<apache_avro::Writer<'static, std::fs::File>>>,
}

impl std::fmt::Debug for ParallelAvroFileConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelAvroFileConsumer")
            .finish_non_exhaustive()
    }
}

impl ParallelAvroFileConsumer {
    pub(super) fn new_with_full_path(path: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(path.parent().unwrap_or(path))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        let writer = apache_avro::Writer::builder()
            .schema(&crate::consumer::CORE_AVRO_SCHEMA)
            .writer(file)
            .codec(apache_avro::Codec::Snappy)
            .build();
        Ok(Self {
            writer: Arc::new(parking_lot::Mutex::new(writer)),
        })
    }
}

impl WriteRequestConsumer for ParallelAvroFileConsumer {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let records = build_records_parallel(&request.series);
        self.writer
            .lock()
            .extend(records)
            .map_err(|e| ConsumerError::AvroError(Box::new(e)))?;
        Ok(())
    }
}

/// Build avro `Record`s from a slice of `Series` in parallel using scoped
/// threads. Each series is independent (its own timestamp/value vectors and
/// tags); the expensive part — `Value::Union(0, Box::new(Value::Double(x)))`
/// allocation per point — is embarrassingly parallel.
///
/// We use `std::thread::scope` rather than a persistent worker pool so the
/// dependency graph stays minimal; one OS-thread spawn per chunk is
/// ~20-50µs, well under 1% of wall at observed call rates.
fn build_records_parallel(series: &[proto::Series]) -> Vec<apache_avro::types::Record<'static>> {
    if series.is_empty() {
        return Vec::new();
    }

    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(series.len())
        .max(1);
    let chunk_size = series.len().div_ceil(num_threads);

    std::thread::scope(|s| {
        let handles: Vec<_> = series
            .chunks(chunk_size)
            .map(|chunk| s.spawn(move || build_records_for_chunk(chunk)))
            .collect();

        let mut out: Vec<apache_avro::types::Record<'static>> = Vec::with_capacity(series.len());
        for h in handles {
            out.extend(h.join().expect("worker panicked"));
        }
        out
    })
}

fn build_records_for_chunk(chunk: &[proto::Series]) -> Vec<apache_avro::types::Record<'static>> {
    use apache_avro::types::Record;
    use apache_avro::types::Value;
    chunk
        .iter()
        .map(|series| {
            let (timestamps, values) = crate::consumer::points_to_avro(series.points.as_ref());

            let mut record = Record::new(&crate::consumer::CORE_AVRO_SCHEMA)
                .expect("Failed to create Avro record");
            record.put(
                "channel",
                series
                    .channel
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| "values".to_string()),
            );
            record.put("timestamps", Value::Array(timestamps));
            record.put("values", Value::Array(values));
            record.put("tags", series.tags.clone());
            record
        })
        .collect()
}

/// Wraps another [`WriteRequestConsumer`] and latches the first error seen,
/// so [`super::AvroWriter`] can surface disk / avro failures from subsequent
/// `write` / `flush` / `sync` / `close` calls.
///
/// Also records per-call wall time into `stats.consumer_consume_ns` /
/// `consumer_consume_calls` — the consumer runs on the stream's dispatcher
/// thread, so its accumulated wall time attributes downstream work to the
/// pipeline.
pub(super) struct ErrorLatchingConsumer<C: WriteRequestConsumer> {
    pub(super) inner: C,
    pub(super) first_error: Arc<OnceLock<Arc<AvroWriterError>>>,
    pub(super) stats: Arc<PipelineStats>,
}

impl<C: WriteRequestConsumer> std::fmt::Debug for ErrorLatchingConsumer<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorLatchingConsumer")
            .finish_non_exhaustive()
    }
}

impl<C: WriteRequestConsumer> WriteRequestConsumer for ErrorLatchingConsumer<C> {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let t0 = std::time::Instant::now();
        let result = self.inner.consume(request);
        self.stats
            .consumer_consume_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats
            .consumer_consume_calls
            .fetch_add(1, Ordering::Relaxed);
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Latch on first occurrence (first-wins via OnceLock::set).
                // We call `avro_error_from_consumer_ref` rather than consuming
                // `e` because we must also return `e` to the stream's
                // dispatcher — `ConsumerError` isn't `Clone`. The helper
                // reconstructs `Io` from `io::Error::new(kind, msg)` so the
                // `Io` variant is preserved; avro errors can't be cloned so
                // they flatten to `Consumer` in the latched copy.
                if self.first_error.get().is_none() {
                    let latched = Arc::new(avro_error_from_consumer_ref(&e));
                    error!("AvroWriter: encoder error latched: {:?}", latched);
                    let _ = self.first_error.set(latched);
                }
                // Return the error to the stream's dispatcher so it logs / backs off.
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A consumer that always fails with `PermissionDenied` IoError.
    /// Used by `error_latch_preserves_io_variant` (unit-struct, fixed kind).
    #[derive(Debug)]
    struct PermissionDeniedConsumer;

    impl WriteRequestConsumer for PermissionDeniedConsumer {
        fn consume(&self, _request: &WriteRequestNominal) -> ConsumerResult<()> {
            Err(ConsumerError::IoError(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "denied",
            )))
        }
    }

    #[test_log::test]
    fn error_latch_preserves_io_variant() {
        let first_error: Arc<OnceLock<Arc<AvroWriterError>>> = Arc::new(OnceLock::new());
        let consumer = ErrorLatchingConsumer {
            inner: PermissionDeniedConsumer,
            first_error: first_error.clone(),
            stats: Arc::new(PipelineStats::default()),
        };

        // Construct a minimal WriteRequestNominal to satisfy the trait bound.
        let req = WriteRequestNominal {
            series: vec![],
            session_name: None,
        };

        // The consume call should fail (pass the error through) and latch.
        let result = consumer.consume(&req);
        assert!(result.is_err(), "consumer should have propagated the error");

        // Inspect the latched error.
        let latched = first_error.get().expect("error should have been latched");
        match latched.as_ref() {
            AvroWriterError::Io(io_err) => {
                assert_eq!(
                    io_err.kind(),
                    std::io::ErrorKind::PermissionDenied,
                    "expected PermissionDenied, got {:?}",
                    io_err.kind()
                );
            }
            other => panic!("expected AvroWriterError::Io, got {other:?}"),
        }
    }
}
