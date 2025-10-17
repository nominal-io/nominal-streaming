//! Python extension entrypoint for `_nominal_streaming`.
//! Exposes:
//!   - NominalStreamOptsWrapper   (your existing options/builder wrapper)
//!   - _NominalDatasetStream      (the Rust-backed stream; the Python layer provides
//!                                 a friendlier `NominalDatasetStream` name and SIGINT wiring)

mod builder_state;
mod nominal_dataset_stream;
mod nominal_stream_opts;
mod point;
mod runtime;

use pyo3::prelude::*;

#[pymodule(name = "_nominal_streaming")]
fn nominal_streaming(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<nominal_stream_opts::NominalStreamOptsWrapper>()?;
    m.add_class::<nominal_dataset_stream::_NominalDatasetStream>()?;
    Ok(())
}
