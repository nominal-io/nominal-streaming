//! Python extension entrypoint for `_nominal_streaming`.
//! Exposes:
//!   - PyNominalStreamOpts   (your existing options/builder wrapper)
//!   - PyNominalDatasetStream      (the Rust-backed stream; the Python layer provides
//!                                 a friendlier `NominalDatasetStream` name and SIGINT wiring)

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
