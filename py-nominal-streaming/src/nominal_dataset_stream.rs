//! The Python-exposed stream class (Rust side).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;

use ::nominal_streaming::prelude::*;
use nominal_streaming::prelude::BearerToken;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use tracing::info;
use tracing::warn;

use crate::lazy_dataset_stream_builder::CoreTarget;
use crate::lazy_dataset_stream_builder::FileTarget;
use crate::lazy_dataset_stream_builder::LazyDatasetStreamBuilder;
use crate::lazy_dataset_stream_builder::StreamTargets;
use crate::nominal_stream_opts::PyNominalStreamOpts;
use crate::point::*;
use crate::runtime::spawn_runtime_worker;
use crate::runtime::StreamRuntime;

static JSON_DUMPS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn json_dumps<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let cached = JSON_DUMPS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("json")?.getattr("dumps")?.unbind())
    })?;
    Ok(cached.bind(py).clone())
}

fn extract_single_points(timestamp: Timestamp, value: &Bound<'_, PyAny>) -> PyResult<PointsType> {
    // Try extractions in order: float → int → string
    if let Ok(v) = value.extract::<f64>() {
        Ok(single_double(timestamp, v))
    } else if let Ok(v) = value.extract::<i64>() {
        Ok(single_int(timestamp, v))
    } else if let Ok(v) = value.extract::<String>() {
        Ok(single_string(timestamp, v))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "value must be float, int, or str",
        ))
    }
}

fn extract_series_points(
    timestamps: Vec<Timestamp>,
    values: &Bound<'_, PyAny>,
) -> PyResult<PointsType> {
    match classify_values(values)? {
        ValueKind::Floats => series_doubles(timestamps, extract_vec_f64(values)?),
        ValueKind::Ints => series_ints(timestamps, extract_vec_i64(values)?),
        ValueKind::Strings => series_strings(timestamps, extract_vec_string(values)?),
    }
}

/// The PyNominalDatasetStream is a thin layer bound to python that handles two main concerns:
/// - Configuring and managing a tokio runtime for running streaming code
/// - Passing data from python, converting it to standard rust types, and pushing into streaming code.
///
/// Enqueued points go straight into the underlying stream from the calling python thread.
/// `NominalDatasetStream::enqueue` is synchronous and `Sync`, so there is nothing to hand off to the
/// runtime -- it exists only for the uploader. The GIL is released around each call, both so that
/// other python threads run while the uploader is caught up with and so that several python threads
/// can enqueue concurrently, contending only on the stream's own buffer lock.
#[pyclass]
pub struct PyNominalDatasetStream {
    builder: LazyDatasetStreamBuilder,
    runtime_task: Option<JoinHandle<()>>,
    runtime: Option<StreamRuntime>,
    is_open: Arc<AtomicBool>,
}

impl PyNominalDatasetStream {
    /// Borrow the underlying stream or raise a python error if it hasn't started
    #[inline]
    fn stream(&self) -> PyResult<&Arc<NominalDatasetStream>> {
        self.runtime
            .as_ref()
            .map(|rt| &rt.stream)
            .ok_or_else(|| PyRuntimeError::new_err("stream not open"))
    }

    /// Extract nominal api token from env or overridden by argument
    #[inline]
    fn token_from_env_or_arg(token: Option<&str>) -> PyResult<String> {
        token
            .map(str::to_owned)
            .or_else(|| std::env::var("NOMINAL_TOKEN").ok())
            .ok_or_else(|| PyRuntimeError::new_err("NOMINAL_TOKEN not set and no token provided"))
    }

    /// Push one channel's points into the stream, releasing the GIL for the call.
    fn push_one(&self, py: Python<'_>, ch: ChannelDescriptor, points: PointsType) -> PyResult<()> {
        let stream = self.stream()?;
        py.detach(|| stream.try_enqueue(&ch, points))
            .map_err(|_| PyRuntimeError::new_err("cancelled or closed"))
    }

    /// Push many channels' points into the stream as one unit, releasing the GIL for the call.
    fn push_many(
        &self,
        py: Python<'_>,
        entries: Vec<(ChannelDescriptor, PointsType)>,
    ) -> PyResult<()> {
        let stream = self.stream()?;
        py.detach(|| stream.try_enqueue_many(entries))
            .map_err(|_| PyRuntimeError::new_err("cancelled or closed"))
    }
}

#[pymethods]
impl PyNominalDatasetStream {
    #[new]
    #[pyo3(text_signature = "(/, opts=None)")]
    pub fn new(opts: Option<PyNominalStreamOpts>) -> PyResult<Self> {
        Ok(Self {
            builder: LazyDatasetStreamBuilder {
                log_level: None,
                opts,
                targets: StreamTargets::default(),
            },
            runtime_task: None,
            runtime: None,
            is_open: Arc::new(AtomicBool::new(false)),
        })
    }

    #[pyo3(signature = (log_level=None), text_signature = "(self, log_level=None)")]
    pub fn enable_logging<'py>(
        mut slf: PyRefMut<'py, Self>,
        log_level: Option<&str>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.builder.log_level = Some(log_level.unwrap_or("debug").to_string());
        Ok(slf)
    }

    #[pyo3(text_signature = "(self, opts)")]
    pub fn with_options<'py>(
        mut slf: PyRefMut<'py, Self>,
        opts: PyNominalStreamOpts,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.builder.opts = Some(opts);
        Ok(slf)
    }

    #[pyo3(signature = (dataset_rid, token=None), text_signature = "(self, dataset_rid, token=None)")]
    pub fn with_core_consumer<'py>(
        mut slf: PyRefMut<'py, Self>,
        dataset_rid: &str,
        token: Option<&str>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let tok = Self::token_from_env_or_arg(token)?;
        let bearer = BearerToken::new(&tok).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let rid = ResourceIdentifier::new(dataset_rid)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        slf.builder.targets.core_target = Some(CoreTarget { token: bearer, rid });
        Ok(slf)
    }

    #[pyo3(text_signature = "(self, path)")]
    pub fn to_file<'py>(
        mut slf: PyRefMut<'py, Self>,
        path: PathBuf,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.builder.targets.file_target = Some(FileTarget { path });
        Ok(slf)
    }

    #[pyo3(text_signature = "(self, path)")]
    pub fn with_file_fallback<'py>(
        mut slf: PyRefMut<'py, Self>,
        path: PathBuf,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.builder.targets.file_fallback = Some(path);
        Ok(slf)
    }

    #[pyo3(text_signature = "(self)")]
    pub fn open(&mut self) -> PyResult<()> {
        if self.is_open.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.builder
            .validate()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let (runtime_task, runtime) = match spawn_runtime_worker(self.builder.clone()) {
            Ok(parts) => parts,
            Err(e) => {
                // leave the stream closed so that a failed open can be retried
                self.is_open.store(false, Ordering::SeqCst);
                return Err(PyRuntimeError::new_err(e.to_string()));
            }
        };

        self.runtime_task = Some(runtime_task);
        self.runtime = Some(runtime);
        Ok(())
    }

    /// Graceful drain and teardown (releases GIL while draining and joining)
    #[pyo3(text_signature = "(self)")]
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(StreamRuntime {
            stream,
            shutdown_tx,
        }) = self.runtime.take()
        {
            // Drop the stream first: its own `Drop` waits for every buffered point to be uploaded,
            // and those uploads run on the runtime we are about to shut down.
            info!("Dropping stream to drain buffered points");
            py.detach(|| drop(stream));

            info!("Signalling runtime thread to shut down");
            let _ = shutdown_tx.send(());
        }

        // Join the runtime thread (releases GIL)
        if let Some(j) = self.runtime_task.take() {
            info!("Joining runtime thread");
            py.detach(|| {
                let _ = j.join();
            });
        }

        // Mark closed (idempotent)
        self.is_open.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Fast teardown (used by the SIGINT handler).
    ///
    /// Abandons buffered points rather than waiting for them to upload, and releases any thread
    /// blocked in `enqueue` waiting on backpressure.
    #[pyo3(text_signature = "(self)")]
    pub fn cancel(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(rt) = &self.runtime {
            info!("Cancel requested; abandoning buffered points");
            rt.stream.cancel();
        } else {
            warn!("Cancel requested, but stream not open...");
        }

        self.close(py)
    }

    #[pyo3(signature = (channel_name, timestamp, value, tags=None), text_signature = "(self, channel_name, timestamp, value, tags=None)")]
    pub fn enqueue(
        &self,
        py: Python<'_>,
        channel_name: &str,
        timestamp: u64,
        value: &Bound<'_, PyAny>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let ts = parse_timestamp(timestamp);
        let ch = description_with_tags(channel_name, tags);
        self.push_one(py, ch, extract_single_points(ts, value)?)
    }

    #[pyo3(signature = (channel_name, timestamps, values, tags=None), text_signature = "(self, channel_name, timestamps, values, tags=None)")]
    pub fn enqueue_batch(
        &self,
        py: Python<'_>,
        channel_name: &str,
        timestamps: Vec<u64>,
        values: &Bound<'_, PyAny>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let tss = extract_vec_ts(timestamps);
        let ch = description_with_tags(channel_name, tags);
        self.push_one(py, ch, extract_series_points(tss, values)?)
    }

    #[pyo3(signature = (timestamp, channel_values, tags=None), text_signature = "(self, timestamp, channel_values, tags=None)")]
    pub fn enqueue_from_dict(
        &self,
        py: Python<'_>,
        timestamp: u64,
        channel_values: &Bound<'_, PyDict>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let ts = parse_timestamp(timestamp);
        // built once for the whole record rather than per channel
        let tags = into_tag_map(tags);
        let mut entries: Vec<(ChannelDescriptor, PointsType)> =
            Vec::with_capacity(channel_values.len());

        for (k, v) in channel_values {
            let ch = ChannelDescriptor {
                name: k.extract()?,
                tags: tags.clone(),
            };
            entries.push((ch, extract_single_points(ts, &v)?));
        }

        self.push_many(py, entries)
    }

    #[pyo3(
        signature = (channel_name, timestamp, value, tags=None),
        text_signature = "(self, channel_name, timestamp, value, tags=None)"
    )]
    pub fn enqueue_struct(
        &self,
        py: Python<'_>,
        channel_name: &str,
        timestamp: u64,
        value: &Bound<'_, PyAny>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("allow_nan", false)?;
        let json_string: String = json_dumps(py)?.call((value,), Some(&kwargs))?.extract()?;

        let ts = parse_timestamp(timestamp);
        let ch = description_with_tags(channel_name, tags);
        self.push_one(py, ch, single_struct(ts, json_string))
    }

    #[pyo3(
        signature = (channel_name, timestamp, value, tags=None),
        text_signature = "(self, channel_name, timestamp, value, tags=None)"
    )]
    pub fn enqueue_float_array(
        &self,
        py: Python<'_>,
        channel_name: &str,
        timestamp: u64,
        value: Vec<f64>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let ts = parse_timestamp(timestamp);
        let ch = description_with_tags(channel_name, tags);
        self.push_one(py, ch, single_double_array(ts, value))
    }

    #[pyo3(
        signature = (channel_name, timestamp, value, tags=None),
        text_signature = "(self, channel_name, timestamp, value, tags=None)"
    )]
    pub fn enqueue_string_array(
        &self,
        py: Python<'_>,
        channel_name: &str,
        timestamp: u64,
        value: Vec<String>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let ts = parse_timestamp(timestamp);
        let ch = description_with_tags(channel_name, tags);
        self.push_one(py, ch, single_string_array(ts, value))
    }

    fn __enter__<'py>(mut slf: PyRefMut<'py, Self>) -> PyResult<PyRefMut<'py, Self>> {
        slf.open()?;
        Ok(slf)
    }
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _t: Py<PyAny>,
        _e: Py<PyAny>,
        _tb: Py<PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }
}
