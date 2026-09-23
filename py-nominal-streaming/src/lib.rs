//! Top-level entrypoint for exposing Rust streaming code into python
//! Exposes:
//!   - PyNominalStreamOpts     Settings builder object to pass configuration to rust
//!   - PyNominalDatasetStream  Wrapper around rust streaming manager, with tweaks to enable pythonic usage
//!   - PyNominalLogStreamOpts  Log batching, delivery and runtime configuration
//!   - PyNominalLogStream      Log stream lifecycle and record conversion
//!   - LogStreamStats          Delivery and preservation counters

mod lazy_dataset_stream_builder;
mod log_runtime;
mod nominal_dataset_stream;
mod nominal_log_stream;
mod nominal_log_stream_opts;
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
    m.add_class::<nominal_log_stream_opts::PyNominalLogStreamOpts>()?;
    m.add_class::<nominal_log_stream::PyNominalLogStream>()?;
    m.add_class::<nominal_log_stream::PyLogStreamStats>()?;
    Ok(())
}
