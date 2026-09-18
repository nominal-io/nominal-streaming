//! Top-level entrypoint for exposing Rust streaming code into python
//! Exposes:
//!   - PyNominalStreamOpts     Settings builder object to pass configuration to rust
//!   - PyNominalDatasetStream  Wrapper around rust streaming manager, with tweaks to enable pythonic usage

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
    Ok(())
}
