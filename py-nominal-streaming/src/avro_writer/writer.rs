use nominal_streaming::avro_writer::AvroWriter;
use pyo3::prelude::*;

/// Purpose-built avro file writer for the Nominal schema.
///
/// Thin PyO3 facade over [`nominal_streaming::avro_writer::AvroWriter`] —
/// all pipeline logic (multi-producer enqueue, error latching, rotation,
/// fsync-on-close) lives in the `nominal-streaming` crate. This struct only
/// exists to carry `#[pyclass]` + host the `#[pymethods]` impl in
/// [`super::pymethods`].
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
/// the same latched error as a Python `RuntimeError`.
#[pyclass(
    name = "NominalAvroWriter",
    module = "nominal_streaming._nominal_streaming"
)]
#[derive(Clone)]
pub struct NominalAvroWriter {
    pub(super) inner: AvroWriter,
}
