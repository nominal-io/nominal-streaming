use std::sync::Arc;

use nominal_streaming::avro_writer::AvroWriterError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;

/// Convert a latched `Arc<AvroWriterError>` into a Python `RuntimeError`.
///
/// The `Arc` here is the same sticky error shared across all callers of a
/// closed-or-failed writer — we format it to a message; Python code sees
/// a `RuntimeError`.
pub(super) fn map_err(e: Arc<AvroWriterError>) -> PyErr {
    PyRuntimeError::new_err(format!("{}", e))
}
