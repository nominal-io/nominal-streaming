use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::collections::HashMap;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use ::nominal_streaming::prelude::*;
use ::nominal_streaming::stream::NominalDatasetStream;

use crate::NominalStreamOptsWrapper; // for .builder()

#[pyclass(name = "NominalDatasetStream")]
pub struct NominalDatasetStreamWrapper {
    stream_builder: Option<NominalDatasetStreamBuilder>,
    stream: Option<NominalDatasetStream>,
    runtime: Option<Runtime>,
}

impl fmt::Display for NominalDatasetStreamWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NominalDatasetStream()",)
    }
}

#[pymethods]
impl NominalDatasetStreamWrapper {
    #[new]
    #[pyo3(signature=(num_runtime_threads=None))]
    fn new(num_runtime_threads: Option<usize>) -> PyResult<Self> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all().thread_name("tokio");

        if num_runtime_threads.is_some() {
            builder.worker_threads(num_runtime_threads.unwrap());
        }

        let runtime = builder.build();
        match runtime {
            Ok(rt) => Ok(Self {
                stream_builder: Option::None,
                stream: Option::None,
                runtime: Some(rt),
            }),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create tokio runtime: {e}"
            ))),
        }
    }

    fn with_core_consumer(
        mut slf: PyRefMut<'_, Self>,
        api_token: String,
        dataset_rid: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if slf.stream.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Nominal dataset stream already open! Cannot add consumer...",
            ));
        } else if slf.runtime.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Nominal dataset has no runtime... Cannot add consumer...",
            ));
        }

        let builder = slf
            .stream_builder
            .take()
            .unwrap_or_else(|| NominalDatasetStreamBuilder::new());

        let bearer_token = BearerToken::new(api_token.as_str()).expect("Invalid token");
        let dataset_rid = ResourceIdentifier::new(dataset_rid.as_str()).unwrap();

        slf.stream_builder = Some(builder.stream_to_core(
            bearer_token,
            dataset_rid,
            slf.runtime.as_ref().unwrap().handle().clone(),
        ));
        Ok(slf)
    }

    fn with_file_consumer(
        mut slf: PyRefMut<'_, Self>,
        file_path: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if slf.stream.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Nominal dataset stream already open! Cannot add consumer...",
            ));
        }

        let builder = slf
            .stream_builder
            .take()
            .unwrap_or_else(|| NominalDatasetStreamBuilder::new());

        slf.stream_builder = Some(builder.stream_to_file(file_path));
        Ok(slf)
    }

    fn with_file_fallback(
        mut slf: PyRefMut<'_, Self>,
        file_path: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if slf.stream.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Nominal dataset stream already open! Cannot add consumer...",
            ));
        }

        let builder = slf
            .stream_builder
            .take()
            .unwrap_or_else(|| NominalDatasetStreamBuilder::new());

        slf.stream_builder = Some(builder.with_file_fallback(file_path));
        Ok(slf)
    }

    fn with_options(
        mut slf: PyRefMut<'_, Self>,
        opts: NominalStreamOptsWrapper,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if slf.stream.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Nominal dataset stream already open! Cannot add consumer...",
            ));
        }

        let builder = slf
            .stream_builder
            .take()
            .unwrap_or_else(|| NominalDatasetStreamBuilder::new());

        slf.stream_builder = Some(builder.with_options(opts.inner));
        Ok(slf)
    }

    fn open(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        if slf.stream.is_some() {
            return Ok(slf);
        } else if slf.stream_builder.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Nominal dataset stream has not been configured with consumers! Cannot open...",
            ));
        }

        slf.stream = Some(slf.stream_builder.take().unwrap().build());
        return Ok(slf);
    }

    fn close(mut slf: PyRefMut<'_, Self>) -> PyResult<()> {
        // drops the stream while runtime is alive
        slf.stream.take();

        // drop the runtime, cancelling all remaining tasks
        slf.runtime.take();

        Ok(())
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        Self::open(slf)
    }

    fn __exit__(
        slf: PyRefMut<'_, Self>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _exc_traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        match Self::close(slf) {
            Ok(_) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn enqueue(
        &self,
        channel: String,
        timestamp: i64,
        value: f64,
        tags: HashMap<String, String>,
    ) -> PyResult<()> {
        let mut points = Vec::new();
        points.push(DoublePoint {
            timestamp: Some(Timestamp {
                seconds: timestamp / 1_000_000_000,
                nanos: (timestamp % 1_000_000_000) as i32,
            }),
            value: value,
        });
        self.stream
            .as_ref()
            .unwrap()
            .enqueue(&ChannelDescriptor::with_tags(channel, tags), points);
        Ok(())
    }

    fn enqueue_batch(
        &self,
        channel: String,
        timestamps: Vec<i64>,
        values: Vec<f64>,
        tags: HashMap<String, String>,
    ) -> PyResult<()> {
        if timestamps.len() != timestamps.len() {
            return Err(PyRuntimeError::new_err(format!(
                "Cannot enqueue batch of points for channel {}-- mismatching count of timestamps ({}) and values({})",
                channel,
                timestamps.len(),
                values.len()
            )));
        }

        let mut points = Vec::new();
        for idx in 0..timestamps.len() {
            let timestamp = timestamps[idx];
            let value = values[idx];
            points.push(DoublePoint {
                timestamp: Some(Timestamp {
                    seconds: timestamp / 1_000_000_000,
                    nanos: (timestamp % 1_000_000_000) as i32,
                }),
                value: value,
            });
        }

        self.stream
            .as_ref()
            .unwrap()
            .enqueue(&ChannelDescriptor::with_tags(channel, tags), points);
        Ok(())
    }

    fn enqueue_from_dict(&self, timestamp: i64, values: HashMap<String, f64>) -> PyResult<()> {
        Ok(())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }

    fn __str__(&self) -> String {
        self.to_string()
    }
}
