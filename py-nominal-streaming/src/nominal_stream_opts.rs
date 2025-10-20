use std::fmt;
use std::time::Duration;

use nominal_streaming::stream::NominalStreamOpts;
use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass(name = "NominalStreamOpts")]
#[derive(Debug, Clone)]
pub struct NominalStreamOptsWrapper {
    pub inner: NominalStreamOpts,

    #[pyo3(get)]
    pub num_runtime_workers: usize,
}

impl fmt::Display for NominalStreamOptsWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NominalStreamOpts(max_points_per_record={}, max_request_delay_secs={}, max_buffered_requests={}, num_upload_workers={}, num_runtime_workers={}, base_api_url={})",
            self.inner.max_points_per_record,
            self.inner.max_request_delay.as_secs_f64(),
            self.inner.max_buffered_requests,
            self.inner.request_dispatcher_tasks,
            self.num_runtime_workers,
            self.inner.base_api_url,
        )
    }
}

#[pymethods]
impl NominalStreamOptsWrapper {
    #[new]
    fn new(
        max_points_per_record: usize,
        max_request_delay: Duration,
        max_buffered_requests: usize,
        num_upload_workers: usize,
        base_api_url: String,
        num_runtime_workers: usize,
    ) -> Self {
        NominalStreamOptsWrapper {
            inner: NominalStreamOpts {
                max_points_per_record,
                max_request_delay,
                max_buffered_requests,
                request_dispatcher_tasks: num_upload_workers,
                base_api_url,
            },
            num_runtime_workers: num_runtime_workers,
        }
    }

    #[classmethod]
    #[pyo3(text_signature = "()")]
    fn default(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        Ok(Self {
            inner: NominalStreamOpts::default(),
            num_runtime_workers: 8,
        })
    }

    #[getter]
    fn max_points_per_record(&self) -> PyResult<usize> {
        Ok(self.inner.max_points_per_record)
    }

    #[getter]
    fn max_request_delay(&self) -> PyResult<Duration> {
        Ok(self.inner.max_request_delay)
    }

    #[getter]
    fn max_buffered_requests(&self) -> PyResult<usize> {
        Ok(self.inner.max_buffered_requests)
    }

    #[getter]
    fn num_upload_workers(&self) -> PyResult<usize> {
        Ok(self.inner.request_dispatcher_tasks)
    }

    #[getter]
    fn base_api_url(&self) -> PyResult<String> {
        Ok(self.inner.base_api_url.clone())
    }

    fn with_max_points_per_record(
        mut slf: PyRefMut<'_, Self>,
        n: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_points_per_record = n;
        Ok(slf)
    }

    fn with_max_request_delay(
        mut slf: PyRefMut<'_, Self>,
        delay: Duration,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_request_delay = delay;
        Ok(slf)
    }

    fn with_max_buffered_requests(
        mut slf: PyRefMut<'_, Self>,
        n: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_buffered_requests = n;
        Ok(slf)
    }

    fn with_num_upload_workers(
        mut slf: PyRefMut<'_, Self>,
        n: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.request_dispatcher_tasks = n;
        Ok(slf)
    }

    fn with_num_runtime_workers(
        mut slf: PyRefMut<'_, Self>,
        n: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.num_runtime_workers = n;
        Ok(slf)
    }

    fn with_api_base_url(mut slf: PyRefMut<'_, Self>, url: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.base_api_url = url;
        Ok(slf)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }

    fn __str__(&self) -> String {
        self.to_string()
    }
}
