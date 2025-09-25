use nominal_streaming::stream::NominalStreamOpts;
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::fmt;
use std::time::Duration;

#[pyclass(name = "NominalStreamOpts")]
#[derive(Debug, Clone)]
pub struct NominalStreamOptsWrapper {
    pub inner: NominalStreamOpts,
}

impl fmt::Display for NominalStreamOptsWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NominalStreamOpts(max_points_per_record={}, max_request_delay_secs={}, max_buffered_requests={}, request_dispatcher_tasks={})",
            self.inner.max_points_per_record,
            self.inner.max_request_delay.as_secs_f64(),
            self.inner.max_buffered_requests,
            self.inner.request_dispatcher_tasks,
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
        request_dispatcher_tasks: usize,
    ) -> Self {
        NominalStreamOptsWrapper {
            inner: NominalStreamOpts {
                max_points_per_record,
                max_request_delay,
                max_buffered_requests,
                request_dispatcher_tasks,
            },
        }
    }

    #[classmethod]
    #[pyo3(text_signature = "()")]
    fn default(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        Ok(Self {
            inner: NominalStreamOpts::default(),
        })
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

    fn with_request_dispatcher_tasks(
        mut slf: PyRefMut<'_, Self>,
        n: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.inner.request_dispatcher_tasks = n;
        Ok(slf)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }

    fn __str__(&self) -> String {
        self.to_string()
    }
}
