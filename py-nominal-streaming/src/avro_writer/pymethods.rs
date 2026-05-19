use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
use nominal_streaming::avro_writer::AvroWriter;
use nominal_streaming::types::ChannelDescriptor;
use nominal_streaming::types::IntoPoints;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::error_map::map_err;
use super::opts::NominalAvroWriterOpts;
use super::polars_ffi::py_df_to_rust;
use super::writer::NominalAvroWriter;
use crate::point::classify_values;
use crate::point::description_with_tags;
use crate::point::extract_vec_f64;
use crate::point::extract_vec_i64;
use crate::point::extract_vec_string;
use crate::point::extract_vec_ts;
use crate::point::parse_timestamp;
use crate::point::ValueKind;

#[pymethods]
impl NominalAvroWriter {
    #[new]
    #[pyo3(signature = (path, opts = None))]
    fn py_new(path: PathBuf, opts: Option<NominalAvroWriterOpts>) -> PyResult<Self> {
        let opts = opts.unwrap_or_default().inner.clone();
        let inner = AvroWriter::new(path, opts)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to open avro writer: {e}")))?;
        Ok(Self { inner })
    }

    #[pyo3(name = "close")]
    fn py_close(&self, py: Python<'_>) -> PyResult<Vec<PathBuf>> {
        py.detach(|| self.inner.close().map_err(map_err))
    }

    #[pyo3(name = "points_accepted")]
    fn py_points_accepted(&self) -> u64 {
        self.inner.points_accepted()
    }

    #[pyo3(name = "path")]
    fn py_path(&self) -> PathBuf {
        self.inner.path()
    }

    #[pyo3(name = "finalized_paths")]
    fn py_finalized_paths(&self) -> Vec<PathBuf> {
        self.inner.finalized_paths()
    }

    #[pyo3(name = "written_files")]
    fn py_written_files(&self) -> Vec<PathBuf> {
        self.inner.written_files()
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> PyResult<()> {
        py.detach(|| self.inner.close().map(|_paths| ()).map_err(map_err))
    }

    // All per-point Python writers funnel into the single internal
    // `write_batch` entry point: extract values from the Python layer,
    // build a `(ChannelDescriptor, PointsType)` pair (or a batch of them
    // for wide writes), then one GIL-released `write_batch` call handles
    // error-latching, close-guard, rotate-before, stream hand-off, and
    // counter updates. No other path reaches the underlying stream.
    #[pyo3(signature = (channel, ts_ns, value, tags = None))]
    fn write(
        &self,
        py: Python<'_>,
        channel: &str,
        ts_ns: u64,
        value: Bound<'_, PyAny>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let descriptor = description_with_tags(channel, tags);
        let ts = parse_timestamp(ts_ns);
        // Check int first so ints don't get routed to Double via f64 extraction.
        let points = if let Ok(i) = value.extract::<i64>() {
            vec![IntegerPoint {
                timestamp: Some(ts),
                value: i,
            }]
            .into_points()
        } else if let Ok(f) = value.extract::<f64>() {
            vec![DoublePoint {
                timestamp: Some(ts),
                value: f,
            }]
            .into_points()
        } else if let Ok(s) = value.extract::<String>() {
            vec![StringPoint {
                timestamp: Some(ts),
                value: s,
            }]
            .into_points()
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "value must be int, float, or str",
            ));
        };
        py.detach(|| {
            self.inner
                .write_batch(vec![(descriptor, points)])
                .map_err(map_err)
        })
    }

    #[pyo3(signature = (channel, ts_ns, values, tags = None))]
    fn write_batch(
        &self,
        py: Python<'_>,
        channel: &str,
        ts_ns: Vec<u64>,
        values: Bound<'_, PyAny>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let descriptor = description_with_tags(channel, tags);
        let timestamps = extract_vec_ts(ts_ns);
        let kind = classify_values(&values)?;

        // Build the typed point vec, then hand it to `AvroWriter::write`
        // which internally loops over `write_batch` splitting on rotation
        // boundaries. Keeps files tight when callers pass large batches
        // with a small `max_points_per_file`.
        match kind {
            ValueKind::Floats => {
                let vs = extract_vec_f64(&values)?;
                if vs.len() != timestamps.len() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "timestamps and values must have same length",
                    ));
                }
                let points: Vec<DoublePoint> = timestamps
                    .into_iter()
                    .zip(vs)
                    .map(|(t, v)| DoublePoint {
                        timestamp: Some(t),
                        value: v,
                    })
                    .collect();
                py.detach(|| self.inner.write(&descriptor, points).map_err(map_err))
            }
            ValueKind::Ints => {
                let vs = extract_vec_i64(&values)?;
                if vs.len() != timestamps.len() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "timestamps and values must have same length",
                    ));
                }
                let points: Vec<IntegerPoint> = timestamps
                    .into_iter()
                    .zip(vs)
                    .map(|(t, v)| IntegerPoint {
                        timestamp: Some(t),
                        value: v,
                    })
                    .collect();
                py.detach(|| self.inner.write(&descriptor, points).map_err(map_err))
            }
            ValueKind::Strings => {
                let vs = extract_vec_string(&values)?;
                if vs.len() != timestamps.len() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "timestamps and values must have same length",
                    ));
                }
                let points: Vec<StringPoint> = timestamps
                    .into_iter()
                    .zip(vs)
                    .map(|(t, v)| StringPoint {
                        timestamp: Some(t),
                        value: v,
                    })
                    .collect();
                py.detach(|| self.inner.write(&descriptor, points).map_err(map_err))
            }
        }
    }

    #[pyo3(signature = (ts_ns, channel_values, tags = None))]
    fn write_from_dict(
        &self,
        py: Python<'_>,
        ts_ns: u64,
        channel_values: HashMap<String, Bound<'_, PyAny>>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        // Collect every channel's single point into one batch, then dispatch
        // in a single `write_batch` call — one outer-lock acquisition total.
        let ts = parse_timestamp(ts_ns);
        let mut batch: Vec<(ChannelDescriptor, PointsType)> =
            Vec::with_capacity(channel_values.len());
        for (name, value) in channel_values {
            let descriptor = description_with_tags(&name, tags.clone());
            let points = if let Ok(i) = value.extract::<i64>() {
                vec![IntegerPoint {
                    timestamp: Some(ts),
                    value: i,
                }]
                .into_points()
            } else if let Ok(f) = value.extract::<f64>() {
                vec![DoublePoint {
                    timestamp: Some(ts),
                    value: f,
                }]
                .into_points()
            } else if let Ok(s) = value.extract::<String>() {
                vec![StringPoint {
                    timestamp: Some(ts),
                    value: s,
                }]
                .into_points()
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "values must be int, float, or str",
                ));
            };
            batch.push((descriptor, points));
        }
        py.detach(|| self.inner.write_batch(batch).map_err(map_err))
    }

    #[pyo3(signature = (channel, ts_ns, value, tags = None))]
    fn write_struct(
        &self,
        py: Python<'_>,
        channel: &str,
        ts_ns: u64,
        value: Bound<'_, PyAny>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        // Serialize the Python mapping to JSON using json.dumps(allow_nan=False).
        let json_module = py.import("json")?;
        let dumps = json_module.getattr("dumps")?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("allow_nan", false)?;
        let json_str: String = dumps.call((value,), Some(&kwargs))?.extract()?;
        let descriptor = description_with_tags(channel, tags);
        let points = vec![StructPoint {
            timestamp: Some(parse_timestamp(ts_ns)),
            json_string: json_str,
        }]
        .into_points();
        py.detach(|| {
            self.inner
                .write_batch(vec![(descriptor, points)])
                .map_err(map_err)
        })
    }

    #[pyo3(signature = (channel, ts_ns, value, tags = None))]
    fn write_float_array(
        &self,
        py: Python<'_>,
        channel: &str,
        ts_ns: u64,
        value: Vec<f64>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let descriptor = description_with_tags(channel, tags);
        let points = vec![DoubleArrayPoint {
            timestamp: Some(parse_timestamp(ts_ns)),
            value,
        }]
        .into_points();
        py.detach(|| {
            self.inner
                .write_batch(vec![(descriptor, points)])
                .map_err(map_err)
        })
    }

    #[pyo3(signature = (channel, ts_ns, value, tags = None))]
    fn write_string_array(
        &self,
        py: Python<'_>,
        channel: &str,
        ts_ns: u64,
        value: Vec<String>,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        let descriptor = description_with_tags(channel, tags);
        let points = vec![StringArrayPoint {
            timestamp: Some(parse_timestamp(ts_ns)),
            value,
        }]
        .into_points();
        py.detach(|| {
            self.inner
                .write_batch(vec![(descriptor, points)])
                .map_err(map_err)
        })
    }

    #[pyo3(name = "flush")]
    fn py_flush(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.flush().map_err(map_err))
    }

    #[pyo3(name = "sync")]
    fn py_sync(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.sync().map_err(map_err))
    }

    /// Snapshot of pipeline timing counters (nanoseconds, except `*_calls`).
    /// Values accumulate across the lifetime of this writer. See
    /// [`nominal_streaming::avro_writer::PipelineStats`] for field
    /// semantics.
    ///
    /// Used for throughput diagnosis — compare `consumer_consume_ns` (work on
    /// the single dispatcher thread, in parallel with the producer) against
    /// `write_dataframe_total_ns` to see whether the consumer is saturated
    /// (→ encoder bottleneck) or idle (→ producer bottleneck).
    fn stats(&self) -> HashMap<&'static str, u64> {
        let s = self.inner.stats();
        HashMap::from([
            ("df_handoff_ns", s.df_handoff_ns.load(Ordering::Relaxed)),
            ("extract_ts_ns", s.extract_ts_ns.load(Ordering::Relaxed)),
            ("column_build_ns", s.column_build_ns.load(Ordering::Relaxed)),
            (
                "enqueue_batch_ns",
                s.enqueue_batch_ns.load(Ordering::Relaxed),
            ),
            (
                "consumer_consume_ns",
                s.consumer_consume_ns.load(Ordering::Relaxed),
            ),
            (
                "consumer_consume_calls",
                s.consumer_consume_calls.load(Ordering::Relaxed),
            ),
            (
                "write_dataframe_calls",
                s.write_dataframe_calls.load(Ordering::Relaxed),
            ),
            (
                "write_dataframe_total_ns",
                s.write_dataframe_total_ns.load(Ordering::Relaxed),
            ),
        ])
    }

    /// Write each non-timestamp column of a polars DataFrame as a separate channel.
    ///
    /// See [`nominal_streaming::avro_writer::AvroWriter::write_dataframe`] for
    /// the full contract, including null / NaN / Infinity handling and the
    /// coupling with the Nominal Core ingest backend.
    #[pyo3(signature = (df, timestamp_column, tags = None))]
    fn write_dataframe(
        &self,
        py: Python<'_>,
        df: Bound<'_, PyAny>,
        timestamp_column: &str,
        tags: Option<HashMap<String, String>>,
    ) -> PyResult<()> {
        use pyo3::exceptions::PyTypeError;
        use pyo3::exceptions::PyValueError;

        let stats = self.inner.stats();

        // Convert Python polars DataFrame → Rust DataFrame using Arrow C Data Interface.
        // This avoids the pyarrow dependency that pyo3-polars' to_arrow() path requires.
        let t0 = std::time::Instant::now();
        let df = py_df_to_rust(py, &df)?;
        stats
            .df_handoff_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Pre-validate the timestamp column on the Python side so callers see
        // `ValueError` / `TypeError` (not `RuntimeError`) for malformed input.
        // The underlying `AvroWriter::write_dataframe` also validates, but its
        // `AvroWriterError::Consumer` variant maps to Python `RuntimeError` —
        // the public Python contract (established by these tests) is that a
        // missing / wrong-dtype / null timestamp raises the Python-standard
        // lookup/type/value errors.
        {
            let ts_col = df
                .column(timestamp_column)
                .map_err(|e| PyValueError::new_err(format!("timestamp column: {e}")))?;
            let ts_i64 = ts_col.i64().map_err(|_| {
                PyTypeError::new_err(format!(
                    "timestamp column {:?} must be Int64, got {:?}",
                    timestamp_column,
                    ts_col.dtype()
                ))
            })?;
            if ts_i64.null_count() > 0 {
                return Err(PyValueError::new_err("null in timestamp column"));
            }
        }

        // Release GIL for the column iteration + enqueue (all pure-Rust in
        // the core crate).
        let inner = self.inner.clone();
        py.detach(move || inner.write_dataframe(&df, timestamp_column, tags))
            .map_err(map_err)
    }
}
