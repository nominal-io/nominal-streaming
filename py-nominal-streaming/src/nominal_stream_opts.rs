//! The Python-exposed stream configuration class (Rust side).

use std::fmt;
use std::time::Duration;

use nominal_streaming::client::TransportOptions;
use nominal_streaming::stream::NominalStreamOpts;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::nominal_dataset_stream::DICT_METRIC_CHANNELS;

// `from_py_object` opts in to the derived `FromPyObject`, which pyo3 0.29 deprecates as an
// implicit behaviour for `Clone` pyclasses. It is required here: `NominalDatasetStream` takes
// this class by value (see `nominal_dataset_stream.rs`).
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyNominalStreamOpts {
    pub inner: NominalStreamOpts,

    #[pyo3(get)]
    pub num_runtime_workers: usize,
}

impl fmt::Display for PyNominalStreamOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NominalStreamOpts(max_points_per_batch={}, max_request_delay_secs{}, max_buffered_requests={}, num_upload_workers={}, num_runtime_workers={}, base_api_url='{}', track_metrics={})",
            self.inner.max_points_per_record,
            self.inner.max_request_delay.as_secs_f64(),
            self.inner.max_buffered_requests,
            self.inner.request_dispatcher_tasks,
            self.num_runtime_workers,
            self.inner.base_api_url,
            self.inner.track_metrics,
        )
    }
}

#[pymethods]
impl PyNominalStreamOpts {
    #[new]
    // Preserve the existing keyword-only Python constructor while exposing transport settings.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        max_points_per_batch=250_000,
        max_request_delay_secs=0.1,
        max_buffered_requests=4,
        num_upload_workers=8,
        num_runtime_workers=8,
        base_api_url="https://api.gov.nominal.io/api",
        track_metrics=false,
        max_retries=5,
        retry_backoff_slot_secs=0.25,
        connect_timeout_secs=5.0,
        read_timeout_secs=15.0,
        write_timeout_secs=15.0,
        delivery_timeout_secs=60.0,

    ))]
    fn new(
        max_points_per_batch: usize,
        max_request_delay_secs: f64,
        max_buffered_requests: usize,
        num_upload_workers: usize,
        num_runtime_workers: usize,
        base_api_url: &str,
        track_metrics: bool,
        max_retries: u32,
        retry_backoff_slot_secs: f64,
        connect_timeout_secs: f64,
        read_timeout_secs: f64,
        write_timeout_secs: f64,
        delivery_timeout_secs: f64,
    ) -> PyResult<Self> {
        let mut transport = TransportOptions::default();
        transport.max_retries = max_retries;
        transport.backoff_slot =
            Duration::try_from_secs_f64(retry_backoff_slot_secs).map_err(|_| {
                PyValueError::new_err("retry_backoff_slot_secs must be finite and nonnegative")
            })?;
        transport.connect_timeout =
            Duration::try_from_secs_f64(connect_timeout_secs).map_err(|_| {
                PyValueError::new_err("connect_timeout_secs must be finite and nonnegative")
            })?;
        transport.read_timeout = Duration::try_from_secs_f64(read_timeout_secs).map_err(|_| {
            PyValueError::new_err("read_timeout_secs must be finite and nonnegative")
        })?;
        transport.write_timeout =
            Duration::try_from_secs_f64(write_timeout_secs).map_err(|_| {
                PyValueError::new_err("write_timeout_secs must be finite and nonnegative")
            })?;
        transport.delivery_timeout =
            Duration::try_from_secs_f64(delivery_timeout_secs).map_err(|_| {
                PyValueError::new_err("delivery_timeout_secs must be finite and nonnegative")
            })?;
        transport.validate().map_err(PyValueError::new_err)?;
        Ok(PyNominalStreamOpts {
            inner: NominalStreamOpts::default()
                .with_max_points_per_record(max_points_per_batch)
                .with_max_request_delay(Duration::from_secs_f64(max_request_delay_secs))
                .with_max_buffered_requests(max_buffered_requests)
                .with_request_dispatcher_tasks(num_upload_workers)
                .with_base_api_url(base_api_url)
                .with_track_metrics(track_metrics)
                .with_transport_options(transport)
                .with_additional_metric_channels(DICT_METRIC_CHANNELS),
            num_runtime_workers,
        })
    }

    #[getter]
    fn max_retries(&self) -> u32 {
        self.inner.transport.max_retries
    }

    #[getter]
    fn retry_backoff_slot_secs(&self) -> f64 {
        self.inner.transport.backoff_slot.as_secs_f64()
    }

    #[getter]
    fn connect_timeout_secs(&self) -> f64 {
        self.inner.transport.connect_timeout.as_secs_f64()
    }

    #[getter]
    fn read_timeout_secs(&self) -> f64 {
        self.inner.transport.read_timeout.as_secs_f64()
    }

    #[getter]
    fn write_timeout_secs(&self) -> f64 {
        self.inner.transport.write_timeout.as_secs_f64()
    }

    #[getter]
    fn delivery_timeout_secs(&self) -> f64 {
        self.inner.transport.delivery_timeout.as_secs_f64()
    }

    #[getter]
    fn track_metrics(&self) -> bool {
        self.inner.track_metrics
    }

    fn with_track_metrics(mut slf: PyRefMut<'_, Self>, enabled: bool) -> PyRefMut<'_, Self> {
        slf.inner.track_metrics = enabled;
        slf
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
