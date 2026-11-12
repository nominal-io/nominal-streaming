//! The Python-exposed log stream configuration class (Rust side).
use std::time::Duration;

use nominal_streaming::client::ProductAgent;
use nominal_streaming::log::NominalLogStreamOpts;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn duration(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|_| PyValueError::new_err("duration must be finite and nonnegative"))
}

fn runtime_workers(value: usize) -> PyResult<usize> {
    if value == 0 {
        return Err(PyValueError::new_err(
            "num_runtime_workers must be positive",
        ));
    }
    Ok(value)
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
    #[pyo3(signature = (*, max_request_bytes=8*1024*1024, max_buffered_bytes=64*1024*1024,
        max_points_per_batch=10_000, max_request_delay_secs=0.25, num_upload_workers=4, num_runtime_workers=2,
        base_api_url="https://api.gov.nominal.io/api"))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_request_bytes: usize,
        max_buffered_bytes: usize,
        max_points_per_batch: usize,
        max_request_delay_secs: f64,
        num_upload_workers: usize,
        num_runtime_workers: usize,
        base_api_url: &str,
    ) -> PyResult<Self> {
        let num_runtime_workers = runtime_workers(num_runtime_workers)?;
        let mut inner = NominalLogStreamOpts::default();
        inner.max_request_bytes = max_request_bytes;
        inner.max_buffered_bytes = max_buffered_bytes;
        inner.max_records_per_batch = max_points_per_batch;
        inner.max_request_delay = duration(max_request_delay_secs)?;
        inner.num_upload_workers = num_upload_workers;
        inner.base_api_url = base_api_url.into();
        Ok(Self {
            num_runtime_workers,
            inner,
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
    fn with_num_runtime_workers(
        mut slf: PyRefMut<'_, Self>,
        value: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.num_runtime_workers = runtime_workers(value)?;
        Ok(slf)
    }
    #[getter]
    fn user_agent(&self) -> Option<String> {
        self.inner.user_agent.as_ref().map(ToString::to_string)
    }
    fn with_user_agent<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: &str,
        version: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let agent =
            ProductAgent::new(name, version).map_err(|e| PyValueError::new_err(e.to_string()))?;
        slf.inner.user_agent = Some(agent);
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
        let opts = &self.inner;
        write!(
            f,
            "PyNominalLogStreamOpts(max_request_bytes={}, max_buffered_bytes={}, \
             max_points_per_batch={}, max_request_delay_secs={}, num_upload_workers={}, \
             base_api_url={:?}, num_runtime_workers={}, user_agent={})",
            opts.max_request_bytes,
            opts.max_buffered_bytes,
            opts.max_records_per_batch,
            opts.max_request_delay.as_secs_f64(),
            opts.num_upload_workers,
            opts.base_api_url,
            self.num_runtime_workers,
            opts.user_agent.as_ref().map_or_else(
                || "None".to_string(),
                |agent| format!("{:?}", agent.to_string())
            ),
        )
    }
}
