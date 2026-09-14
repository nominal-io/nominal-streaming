//! Log bindings keep the runtime alive until graceful drain finishes.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use nominal_streaming::log::LogRecord;
use nominal_streaming::log::LogStreamError;
use nominal_streaming::log::LogStreamOptions;
use nominal_streaming::log::LogStreamStats;
use nominal_streaming::log::NominalLogStream;
use nominal_streaming::prelude::BearerToken;
use nominal_streaming::prelude::ResourceIdentifier;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
fn duration(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|_| PyValueError::new_err("duration must be finite and nonnegative"))
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyNominalLogStreamOpts {
    inner: LogStreamOptions,
}

#[pymethods]
impl PyNominalLogStreamOpts {
    #[new]
    #[pyo3(signature = (*, max_request_bytes=8*1024*1024, max_batch_bytes=16*1024*1024, max_buffered_bytes=64*1024*1024,
        max_records_per_batch=10_000, max_request_delay_secs=0.25, num_upload_workers=4,
        base_api_url="https://api.gov.nominal.io/api", request_timeout_secs=30.0,
        max_retries=3, initial_backoff_secs=0.1, max_backoff_secs=5.0, max_retry_after_secs=30.0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_request_bytes: usize,
        max_batch_bytes: usize,
        max_buffered_bytes: usize,
        max_records_per_batch: usize,
        max_request_delay_secs: f64,
        num_upload_workers: usize,
        base_api_url: &str,
        request_timeout_secs: f64,
        max_retries: usize,
        initial_backoff_secs: f64,
        max_backoff_secs: f64,
        max_retry_after_secs: f64,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: LogStreamOptions {
                max_request_bytes,
                max_batch_bytes,
                max_buffered_bytes,
                max_records_per_batch,
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
}

#[pyclass(name = "LogStreamStats", get_all)]
pub struct PyLogStreamStats {
    accepted_records: u64,
    acknowledged_records: u64,
    backed_up_records: u64,
    failed_records: u64,
    requests: u64,
    retries: u64,
    buffered_bytes: usize,
    last_error: Option<String>,
}
impl From<LogStreamStats> for PyLogStreamStats {
    fn from(s: LogStreamStats) -> Self {
        Self {
            accepted_records: s.accepted_records,
            acknowledged_records: s.acknowledged_records,
            backed_up_records: s.backed_up_records,
            failed_records: s.failed_records,
            requests: s.requests,
            retries: s.retries,
            buffered_bytes: s.buffered_bytes,
            last_error: s.last_error,
        }
    }
}

struct OwnedStream {
    // Field order ensures stream drop precedes runtime drop even without explicit close.
    stream: NominalLogStream,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
}
impl OwnedStream {
    fn close(&self) -> Result<LogStreamStats, LogStreamError> {
        let result = self.stream.close();
        if result.is_ok() {
            if let Some(runtime) = self.runtime.lock().unwrap().take() {
                runtime.shutdown_background();
            }
        }
        result
    }
}

#[pyclass]
pub struct PyNominalLogStream {
    opts: LogStreamOptions,
    core: Option<(BearerToken, ResourceIdentifier)>,
    file: Option<PathBuf>,
    fallback: Option<PathBuf>,
    owned: Option<Arc<OwnedStream>>,
}
impl PyNominalLogStream {
    fn stream(&self) -> PyResult<&Arc<OwnedStream>> {
        self.owned.as_ref().ok_or_else(|| error("stream not open"))
    }
    fn configuring(&self) -> PyResult<()> {
        if self.owned.is_some() {
            Err(error("stream already opened"))
        } else {
            Ok(())
        }
    }
}
#[pymethods]
impl PyNominalLogStream {
    #[new]
    #[pyo3(signature = (opts=None))]
    fn new(opts: Option<PyNominalLogStreamOpts>) -> Self {
        Self {
            opts: opts.map(|o| o.inner).unwrap_or_default(),
            core: None,
            file: None,
            fallback: None,
            owned: None,
        }
    }
    #[pyo3(signature = (dataset_rid, token=None))]
    fn with_core_consumer(&mut self, dataset_rid: &str, token: Option<&str>) -> PyResult<()> {
        self.configuring()?;
        let token = token
            .map(str::to_owned)
            .or_else(|| std::env::var("NOMINAL_TOKEN").ok())
            .ok_or_else(|| error("NOMINAL_TOKEN not set and no token provided"))?;
        self.core = Some((
            BearerToken::new(&token).map_err(error)?,
            ResourceIdentifier::new(dataset_rid).map_err(error)?,
        ));
        Ok(())
    }
    #[pyo3(name = "to_file")]
    fn set_file_target(&mut self, directory: PathBuf) -> PyResult<()> {
        self.configuring()?;
        self.file = Some(directory);
        Ok(())
    }
    fn with_file_fallback(&mut self, directory: PathBuf) -> PyResult<()> {
        self.configuring()?;
        self.fallback = Some(directory);
        Ok(())
    }
    fn open(&mut self, py: Python<'_>) -> PyResult<()> {
        self.configuring()?;
        let owned = py.detach(|| -> PyResult<OwnedStream> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(error)?;
            let mut builder = NominalLogStream::builder().with_options(self.opts.clone());
            if let Some((token, rid)) = &self.core {
                builder =
                    builder.stream_to_core(token.clone(), rid.clone(), runtime.handle().clone());
            }
            if let Some(dir) = &self.file {
                builder = builder.stream_to_file(dir);
            }
            if let Some(dir) = &self.fallback {
                builder = builder.with_file_fallback(dir);
            }
            Ok(OwnedStream {
                stream: builder.build().map_err(error)?,
                runtime: Mutex::new(Some(runtime)),
            })
        })?;
        self.owned = Some(Arc::new(owned));
        Ok(())
    }
    #[pyo3(signature = (channel, timestamp, message, args=None))]
    fn enqueue(
        &self,
        py: Python<'_>,
        channel: &str,
        timestamp: i64,
        message: String,
        args: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let owned = self.stream()?;
        py.detach(|| {
            owned.stream.enqueue(
                channel,
                LogRecord::new(timestamp, message, args.unwrap_or_default()),
            )
        })
        .map_err(error)
    }
    #[pyo3(signature = (channel, timestamps, messages, args=None, per_record_args=None))]
    fn enqueue_batch(
        &self,
        py: Python<'_>,
        channel: &str,
        timestamps: Vec<i64>,
        messages: Vec<String>,
        args: Option<HashMap<String, String>>,
        per_record_args: Option<Vec<HashMap<String, String>>>,
    ) -> PyResult<()> {
        if timestamps.len() != messages.len()
            || per_record_args
                .as_ref()
                .is_some_and(|a| a.len() != messages.len())
        {
            return Err(PyValueError::new_err(
                "timestamps, messages, and per_record_args must have equal lengths",
            ));
        }
        let owned = self.stream()?;
        py.detach(|| {
            let common = args.unwrap_or_default();
            let mut per_record = per_record_args.unwrap_or_default().into_iter();
            let records = timestamps
                .into_iter()
                .zip(messages)
                .map(|(ts, msg)| {
                    let mut args = common.clone();
                    if let Some(extra) = per_record.next() {
                        args.extend(extra);
                    }
                    LogRecord::new(ts, msg, args)
                })
                .collect();
            owned.stream.enqueue_batch(channel, records)
        })
        .map_err(error)
    }
    fn stop_accepting_writes(&self) -> PyResult<()> {
        self.stream()?.stream.stop_accepting_writes();
        Ok(())
    }
    fn stats(&self) -> PyResult<PyLogStreamStats> {
        Ok(self.stream()?.stream.stats().into())
    }
    fn save_failed(&self, py: Python<'_>, directory: PathBuf) -> PyResult<PyLogStreamStats> {
        let owned = self.stream()?;
        py.detach(|| owned.stream.save_failed(directory))
            .map(Into::into)
            .map_err(error)
    }
    fn flush(&self, py: Python<'_>) -> PyResult<PyLogStreamStats> {
        let owned = self.stream()?;
        py.detach(|| owned.stream.flush())
            .map(Into::into)
            .map_err(error)
    }
    #[pyo3(signature = (wait=true))]
    fn close(&self, py: Python<'_>, wait: bool) -> PyResult<Option<PyLogStreamStats>> {
        let Some(owned) = &self.owned else {
            return Ok(None);
        };
        owned.stream.stop_accepting_writes();
        if wait {
            py.detach(|| owned.close())
                .map(|s| Some(s.into()))
                .map_err(error)
        } else {
            let owned = Arc::clone(owned);
            std::thread::Builder::new()
                .name("nominal-log-drain".into())
                .spawn(move || {
                    let _ = owned.close();
                })
                .map_err(error)?;
            Ok(None)
        }
    }
}
impl Drop for PyNominalLogStream {
    fn drop(&mut self) {
        if let Some(owned) = self.owned.take() {
            // Destructors run with the GIL held; explicit close reports errors and waits.
            std::thread::spawn(move || {
                let _ = owned.close();
            });
        }
    }
}
