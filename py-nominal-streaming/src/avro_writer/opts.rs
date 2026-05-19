use std::time::Duration;

use nominal_streaming::avro_writer::AvroWriterOpts;
use pyo3::prelude::*;

/// Configuration for [`super::NominalAvroWriter`].
///
/// Thin wrapper around [`nominal_streaming::avro_writer::AvroWriterOpts`] —
/// all fields have sensible defaults. Exposed directly as a PyO3 class.
#[pyclass(
    name = "NominalAvroWriterOpts",
    module = "nominal_streaming._nominal_streaming"
)]
#[derive(Debug, Clone)]
pub struct NominalAvroWriterOpts {
    pub(super) inner: AvroWriterOpts,
}

impl Default for NominalAvroWriterOpts {
    fn default() -> Self {
        Self {
            inner: AvroWriterOpts::default(),
        }
    }
}

#[pymethods]
impl NominalAvroWriterOpts {
    #[new]
    #[pyo3(signature = (*, max_points_per_batch = 250_000, max_batch_delay_secs = 0.1, fsync_on_close = true, max_points_per_file = 0))]
    pub fn py_new(
        max_points_per_batch: usize,
        max_batch_delay_secs: f64,
        fsync_on_close: bool,
        max_points_per_file: usize,
    ) -> Self {
        Self {
            inner: AvroWriterOpts {
                max_points_per_batch,
                max_batch_delay: Duration::from_secs_f64(max_batch_delay_secs),
                fsync_on_close,
                max_points_per_file,
            },
        }
    }

    #[getter]
    fn max_points_per_batch(&self) -> usize {
        self.inner.max_points_per_batch
    }

    #[getter]
    fn max_batch_delay_secs(&self) -> f64 {
        self.inner.max_batch_delay.as_secs_f64()
    }

    #[getter]
    fn fsync_on_close(&self) -> bool {
        self.inner.fsync_on_close
    }

    #[getter]
    fn max_points_per_file(&self) -> usize {
        self.inner.max_points_per_file
    }

    fn with_max_points_per_batch(&self, n: usize) -> Self {
        Self {
            inner: self.inner.clone().with_max_points_per_batch(n),
        }
    }

    fn with_max_batch_delay_secs(&self, secs: f64) -> Self {
        Self {
            inner: self
                .inner
                .clone()
                .with_max_batch_delay(Duration::from_secs_f64(secs)),
        }
    }

    fn with_fsync_on_close(&self, b: bool) -> Self {
        Self {
            inner: self.inner.clone().with_fsync_on_close(b),
        }
    }

    fn with_max_points_per_file(&self, n: usize) -> Self {
        Self {
            inner: self.inner.clone().with_max_points_per_file(n),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "NominalAvroWriterOpts(max_points_per_batch={}, max_batch_delay_secs={:.3}, fsync_on_close={}, max_points_per_file={})",
            self.inner.max_points_per_batch,
            self.inner.max_batch_delay.as_secs_f64(),
            self.inner.fsync_on_close,
            self.inner.max_points_per_file,
        )
    }
}
