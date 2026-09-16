//! The Python-exposed log stream configuration class (Rust side).
use std::time::Duration;

use nominal_streaming::log::NominalLogStreamOpts;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn duration(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|_| PyValueError::new_err("duration must be finite and nonnegative"))
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyNominalLogStreamOpts {
    pub inner: NominalLogStreamOpts,
    #[pyo3(get)]
    pub num_runtime_workers: usize,
}

#[pymethods]
impl PyNominalLogStreamOpts {
    #[new]
    #[pyo3(signature = (*, max_request_bytes=8*1024*1024, max_batch_bytes=16*1024*1024, max_buffered_bytes=64*1024*1024,
        max_points_per_batch=10_000, max_request_delay_secs=0.25, num_upload_workers=4, num_runtime_workers=2,
        base_api_url="https://api.gov.nominal.io/api", request_timeout_secs=30.0,
        max_retries=3, initial_backoff_secs=0.1, max_backoff_secs=5.0, max_retry_after_secs=30.0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_request_bytes: usize,
        max_batch_bytes: usize,
        max_buffered_bytes: usize,
        max_points_per_batch: usize,
        max_request_delay_secs: f64,
        num_upload_workers: usize,
        num_runtime_workers: usize,
        base_api_url: &str,
        request_timeout_secs: f64,
        max_retries: usize,
        initial_backoff_secs: f64,
        max_backoff_secs: f64,
        max_retry_after_secs: f64,
    ) -> PyResult<Self> {
        if num_runtime_workers == 0 {
            return Err(PyValueError::new_err(
                "num_runtime_workers must be positive",
            ));
        }
        Ok(Self {
            num_runtime_workers,
            inner: NominalLogStreamOpts {
                max_request_bytes,
                max_batch_bytes,
                max_buffered_bytes,
                max_records_per_batch: max_points_per_batch,
                max_request_delay: duration(max_request_delay_secs)?,
                num_upload_workers,
                base_api_url: base_api_url.into(),
                request_timeout: duration(request_timeout_secs)?,
                max_retries,
                initial_backoff: duration(initial_backoff_secs)?,
                max_backoff: duration(max_backoff_secs)?,
                max_retry_after: duration(max_retry_after_secs)?,
            },
        })
    }
    #[getter]
    fn max_request_bytes(&self) -> usize {
        self.inner.max_request_bytes
    }
    fn with_max_request_bytes(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_request_bytes = value;
        Ok(slf)
    }
    #[getter]
    fn max_batch_bytes(&self) -> usize {
        self.inner.max_batch_bytes
    }
    fn with_max_batch_bytes(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_batch_bytes = value;
        Ok(slf)
    }
    #[getter]
    fn max_buffered_bytes(&self) -> usize {
        self.inner.max_buffered_bytes
    }
    fn with_max_buffered_bytes(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_buffered_bytes = value;
        Ok(slf)
    }
    #[getter]
    fn max_points_per_batch(&self) -> usize {
        self.inner.max_records_per_batch
    }
    fn with_max_points_per_batch(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_records_per_batch = value;
        Ok(slf)
    }
    #[getter]
    fn max_request_delay_secs(&self) -> f64 {
        self.inner.max_request_delay.as_secs_f64()
    }
    fn with_max_request_delay_secs(
        mut slf: PyRefMut<'_, Self>,
        value: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_request_delay = duration(value)?;
        Ok(slf)
    }
    #[getter]
    fn num_upload_workers(&self) -> usize {
        self.inner.num_upload_workers
    }
    fn with_num_upload_workers(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.num_upload_workers = value;
        Ok(slf)
    }
    #[getter]
    fn base_api_url(&self) -> String {
        self.inner.base_api_url.clone()
    }
    fn with_api_base_url(
        mut slf: PyRefMut<'_, Self>,
        value: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.base_api_url = value;
        Ok(slf)
    }
    #[getter]
    fn request_timeout_secs(&self) -> f64 {
        self.inner.request_timeout.as_secs_f64()
    }
    fn with_request_timeout_secs(
        mut slf: PyRefMut<'_, Self>,
        value: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.request_timeout = duration(value)?;
        Ok(slf)
    }
    #[getter]
    fn max_retries(&self) -> usize {
        self.inner.max_retries
    }
    fn with_max_retries(mut slf: PyRefMut<'_, Self>, value: usize) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_retries = value;
        Ok(slf)
    }
    #[getter]
    fn initial_backoff_secs(&self) -> f64 {
        self.inner.initial_backoff.as_secs_f64()
    }
    fn with_initial_backoff_secs(
        mut slf: PyRefMut<'_, Self>,
        value: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.initial_backoff = duration(value)?;
        Ok(slf)
    }
    #[getter]
    fn max_backoff_secs(&self) -> f64 {
        self.inner.max_backoff.as_secs_f64()
    }
    fn with_max_backoff_secs(
        mut slf: PyRefMut<'_, Self>,
        value: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_backoff = duration(value)?;
        Ok(slf)
    }
    #[getter]
    fn max_retry_after_secs(&self) -> f64 {
        self.inner.max_retry_after.as_secs_f64()
    }
    fn with_max_retry_after_secs(
        mut slf: PyRefMut<'_, Self>,
        value: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_retry_after = duration(value)?;
        Ok(slf)
    }
    fn with_num_runtime_workers(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if value == 0 {
            return Err(PyValueError::new_err(
                "num_runtime_workers must be positive",
            ));
        }
        slf.num_runtime_workers = value;
        Ok(slf)
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }
    fn __str__(&self) -> String {
        self.to_string()
    }
}

impl std::fmt::Display for PyNominalLogStreamOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyNominalLogStreamOpts(max_request_bytes={}, max_batch_bytes={}, max_buffered_bytes={}, max_points_per_batch={}, max_request_delay_secs={}, num_upload_workers={}, base_api_url={:?}, request_timeout_secs={}, max_retries={}, initial_backoff_secs={}, max_backoff_secs={}, max_retry_after_secs={}, num_runtime_workers={})", self.inner.max_request_bytes, self.inner.max_batch_bytes, self.inner.max_buffered_bytes, self.inner.max_records_per_batch, self.inner.max_request_delay.as_secs_f64(), self.inner.num_upload_workers, self.inner.base_api_url.clone(), self.inner.request_timeout.as_secs_f64(), self.inner.max_retries, self.inner.initial_backoff.as_secs_f64(), self.inner.max_backoff.as_secs_f64(), self.inner.max_retry_after.as_secs_f64(), self.num_runtime_workers)
    }
}
