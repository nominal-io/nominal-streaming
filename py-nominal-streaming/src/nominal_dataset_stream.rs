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

fn extract_single_enqueue_item(
    channel_descriptor: ChannelDescriptor,
    timestamp: Timestamp,
    value: &Bound<'_, PyAny>,
) -> Result<EnqueueItem, PyErr> {
    // Try extractions in order: float → int → string
    if let Ok(v) = value.extract::<f64>() {
        Ok(single_double(channel_descriptor, timestamp, v))
    } else if let Ok(v) = value.extract::<i64>() {
        Ok(single_int(channel_descriptor, timestamp, v))
    } else if let Ok(v) = value.extract::<String>() {
        Ok(single_string(channel_descriptor, timestamp, v))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "value must be float, int, or str",
        ))
    }
}

fn extract_series_enqueue_item(
    channel_descriptor: ChannelDescriptor,
    timestamps: Vec<Timestamp>,
    values: &Bound<'_, PyAny>,
) -> Result<EnqueueItem, PyErr> {
    match classify_values(values)? {
        ValueKind::Floats => {
            series_doubles(channel_descriptor, timestamps, extract_vec_f64(values)?)
        }
        ValueKind::Ints => series_ints(channel_descriptor, timestamps, extract_vec_i64(values)?),
        ValueKind::Strings => {
            series_strings(channel_descriptor, timestamps, extract_vec_string(values)?)
        }
    }
}

/// The PyNominalDatasetStream is a thin layer bound to python that handles two main concerns:
/// - Configuring and managing a tokio runtime for running streaming code
/// - Passing data from python, converting it to standard rust types, and pushing into streaming code.
#[pyclass]
pub struct PyNominalDatasetStream {
    builder: LazyDatasetStreamBuilder,
    runtime_task: Option<JoinHandle<()>>,
    runtime: Option<StreamRuntime>,
    is_open: Arc<AtomicBool>,
}

impl PyNominalDatasetStream {
    /// Borrow the active runtime or raise a python error if it hasn't started
    #[inline]
    fn runtime(&self) -> PyResult<&StreamRuntime> {
        self.runtime
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("runtime not started"))
    }

    /// Extract nominal api token from env or overridden by argument
    #[inline]
    fn token_from_env_or_arg(token: Option<&str>) -> PyResult<String> {
        token
            .map(str::to_owned)
            .or_else(|| std::env::var("NOMINAL_TOKEN").ok())
            .ok_or_else(|| PyRuntimeError::new_err("NOMINAL_TOKEN not set and no token provided"))
    }

    fn send_one(&self, py: Python<'_>, item: EnqueueItem) -> PyResult<()> {
        let runtime = self.runtime()?;
        py.detach(|| {
            runtime.runtime_handle.block_on(async move {
                tokio::select! {
                    _ = runtime.cancel_token.cancelled() => Err(()),        // cancelled
                    r = runtime.ingest_tx.send(item) => r.map_err(|_| ()),  // sent or cancelled
                }
            })
        })
        .map_err(|_| PyRuntimeError::new_err("cancelled or closed"))
    }

    fn send_many(&self, py: Python<'_>, items: Vec<EnqueueItem>) -> PyResult<()> {
        let runtime = self.runtime()?;
        py.detach(|| {
            runtime.runtime_handle.block_on(async move {
                for item in items {
                    tokio::select! {
                        _ = runtime.cancel_token.cancelled() => return Err(()), // cancelled
                        r = runtime.ingest_tx.send(item) => r.map_err(|_| ())?, // sent or cancelled
                    }
                }
                Ok::<(), ()>(())
            })
        })
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
        slf.builder.targets.file_target = Some(FileTarget { path: path });
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

        let (runtime_task, runtime) = spawn_runtime_worker(self.builder.clone())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        self.runtime_task = Some(runtime_task);
        self.runtime = Some(runtime);
        Ok(())
    }

    /// Graceful drain and teardown (releases GIL while joining)
    #[pyo3(text_signature = "(self)")]
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        // Take ownership of the runtime parts so we can drop the sender
        if let Some(rt) = self.runtime.take() {
            let StreamRuntime {
                runtime_handle: _,
                cancel_token: _,
                ingest_tx,
                runtime_exited_rx,
            } = rt;

            // Close the data path so the worker's recv() returns None and exits
            info!("Signalling shutdown: dropping ingest sender to initiate drain");
            drop(ingest_tx);

            // Wait for the async worker to finish draining (releases GIL)
            info!("Awaiting async worker exit");
            py.detach(|| {
                let _ = runtime_exited_rx.blocking_recv();
            });
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

    /// Fast cancellation (used by SIGINT handler)
    #[pyo3(text_signature = "(self)")]
    pub fn cancel(&mut self, py: Python<'_>) -> PyResult<()> {
        // Tell async worker to quickly cancel
        if let Some(rt) = &self.runtime {
            info!("Cancel requested; signalling cancellation token");
            rt.cancel_token.cancel();
        } else {
            warn!("Cancel requested, but runtime  not open...");
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
        self.send_one(py, extract_single_enqueue_item(ch, ts, value)?)
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
        self.send_one(py, extract_series_enqueue_item(ch, tss, values)?)
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
        let mut items: Vec<EnqueueItem> = Vec::with_capacity(channel_values.len());

        for (k, v) in channel_values {
            let ch_name: String = k.extract()?;
            let ch = description_with_tags(ch_name.as_str(), tags.clone()); // same tags for all entries
            items.push(extract_single_enqueue_item(ch, ts, &v)?);
        }

        self.send_many(py, items)
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
