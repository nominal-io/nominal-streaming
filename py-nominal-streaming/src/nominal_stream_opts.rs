use std::fmt;
use std::time::Duration;

use nominal_streaming::stream::NominalStreamOpts;
use pyo3::prelude::*;

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
            "NominalStreamOpts(max_points_per_batch={}, max_request_delay_secs{}, max_buffered_requests={}, num_upload_workers={}, num_runtime_workers={}, base_api_url='{}')",
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
    #[pyo3(signature = (
        *,
        max_points_per_batch=250_000,
        max_request_delay_secs=0.1,
        max_buffered_requests=4,
        num_upload_workers=8,
        num_runtime_workers=8,
        base_api_url="https://api.gov.nominal.io/api",
    ))]
    fn new(
        max_points_per_batch: usize,
        max_request_delay_secs: f64,
        max_buffered_requests: usize,
        num_upload_workers: usize,
        num_runtime_workers: usize,
        base_api_url: &str,
    ) -> Self {
        NominalStreamOptsWrapper {
            inner: NominalStreamOpts {
                max_points_per_record: max_points_per_batch,
                max_request_delay: Duration::from_secs_f64(max_request_delay_secs),
                max_buffered_requests,
                request_dispatcher_tasks: num_upload_workers,
                base_api_url: base_api_url.to_string(),
            },
            num_runtime_workers: num_runtime_workers,
        }
    }

    #[getter]
    fn max_points_per_batch(&self) -> PyResult<usize> {
        Ok(self.inner.max_points_per_record)
    }

    #[getter]
    fn max_request_delay_secs(&self) -> PyResult<f64> {
        Ok(self.inner.max_request_delay.as_secs_f64())
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

    fn with_max_points_per_batch(
        mut slf: PyRefMut<'_, Self>,
        n: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_points_per_record = n;
        Ok(slf)
    }

    fn with_max_request_delay_secs(
        mut slf: PyRefMut<'_, Self>,
        delay_secs: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.max_request_delay = Duration::from_secs_f64(delay_secs);
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
