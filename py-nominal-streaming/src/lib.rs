//! Top-level entrypoint for exposing Rust streaming code to Python.
//!
//! The pure-Rust pipeline lives in the `nominal-streaming` crate. Each
//! `#[pyclass]` here is a thin facade that wraps a Rust type from the core
//! and delegates to it.
//!
//! Exposes:
//!   - `PyNominalStreamOpts`     settings builder for the streaming manager
//!   - `PyNominalDatasetStream`  wrapper around the streaming manager with Pythonic tweaks
//!   - `NominalAvroWriter`       pyclass facade over `nominal_streaming::avro_writer::AvroWriter`
//!   - `NominalAvroWriterOpts`   pyclass facade over `nominal_streaming::avro_writer::AvroWriterOpts`

mod avro_writer;
mod lazy_dataset_stream_builder;
mod nominal_dataset_stream;
mod nominal_stream_opts;
#[cfg(Py_3_11)]
mod numeric_buffer;
mod point;
mod runtime;

use pyo3::prelude::*;

#[pymodule(name = "_nominal_streaming")]
fn nominal_streaming(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("_BUFFER_FAST_PATH", cfg!(Py_3_11))?;
    m.add(
        "_TimestampTypeError",
        m.py().get_type::<point::_TimestampTypeError>(),
    )?;
    m.add_class::<nominal_stream_opts::PyNominalStreamOpts>()?;
    m.add_class::<nominal_dataset_stream::PyNominalDatasetStream>()?;
    m.add_class::<avro_writer::NominalAvroWriterOpts>()?;
    m.add_class::<avro_writer::NominalAvroWriter>()?;
    Ok(())
}
