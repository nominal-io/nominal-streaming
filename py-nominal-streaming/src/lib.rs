mod nominal_dataset_stream;
mod nominal_stream_opts;

use pyo3::prelude::*;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub use nominal_dataset_stream::NominalDatasetStreamWrapper;
pub use nominal_stream_opts::NominalStreamOptsWrapper;

#[pymodule]
fn nominal_streaming(m: &Bound<'_, PyModule>) -> PyResult<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_line_number(true),
        )
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    m.add_class::<NominalStreamOptsWrapper>()?;
    m.add_class::<NominalDatasetStreamWrapper>()?;
    Ok(())
}
