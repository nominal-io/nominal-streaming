//! Top-level entrypoint for exposing Rust streaming code into python
//! Exposes:
//!   - PyNominalStreamOpts     Settings builder object to pass configuration to rust
//!   - PyNominalDatasetStream  Wrapper around rust streaming manager, with tweaks to enable pythonic usage

mod lazy_dataset_stream_builder;
mod nominal_dataset_stream;
mod nominal_stream_opts;
mod point;
mod runtime;

use pyo3::prelude::*;

#[pymodule(name = "_nominal_streaming")]
fn nominal_streaming(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<nominal_stream_opts::PyNominalStreamOpts>()?;
    m.add_class::<nominal_dataset_stream::PyNominalDatasetStream>()?;
    Ok(())
}
