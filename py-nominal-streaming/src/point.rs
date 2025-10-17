//! Helpers for translating Python arguments into nominal_streaming types.
//!
//! Exposed surface (used by the pyo3 class):
//!   - parse_timestamp(): int ns or datetime.datetime → google.protobuf.Timestamp
//!   - description_with_tags(): &str + {k: v} → ChannelDescriptor
//!   - EnqueueItem: typed payload enum crossing the sync→async boundary
//!   - constructors for single/batch points for double/int/string series

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyAnyMethods, PyDateTime, PySequence};
use std::collections::HashMap;

use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_streaming::prelude::*;

/// Convert a Python timestamp into google.protobuf.Timestamp.
///
/// Accepts:
/// - int nanoseconds since Unix epoch (UTC)
/// - datetime.datetime (aware → converted to UTC; naive → treated as UTC)
///
/// NOTE: this utilizes the python runtime, and will result in the GIL being held while
///       running. Future optimizations may involve doing preprocessing in python and
///       only passing integral timestamps across the language barrier.
pub fn parse_timestamp(ts_obj: &Bound<'_, PyAny>) -> PyResult<Timestamp> {
    if let Ok(ns_total) = ts_obj.extract::<i128>() {
        // Absolute integer nanoseconds since UTC unix epoch
        // This is the fastest path
        let seconds = ns_total.div_euclid(1_000_000_000) as i64;
        let nanos = ns_total.rem_euclid(1_000_000_000) as i32;
        return Ok(Timestamp { seconds, nanos });
    } else if let Ok(dt) = ts_obj.downcast::<PyDateTime>() {
        // Slower path-- process datetime.datetime
        let py = dt.py();
        let datetime_mod = py.import("datetime")?;
        let timezone = datetime_mod.getattr("timezone")?;
        let utc = timezone.getattr("utc")?;

        // If aware → convert to UTC; if naive → treat as UTC
        let tzinfo = dt.getattr("tzinfo")?;
        let dt_utc: Bound<'_, PyAny> = if tzinfo.is_none() {
            dt.clone().into_any() // keep as-is; treated as UTC
        } else {
            dt.call_method1("astimezone", (utc,))?
        };

        // seconds (float) in UTC → integer nanoseconds
        let secs_f: f64 = dt_utc.call_method0("timestamp")?.extract()?;
        let ns_total = (secs_f * 1_000_000_000.0).round() as i128;
        let seconds = ns_total.div_euclid(1_000_000_000) as i64;
        let nanos = ns_total.rem_euclid(1_000_000_000) as i32;
        return Ok(Timestamp { seconds, nanos });
    }

    Err(pyo3::exceptions::PyTypeError::new_err(
        "timestamp must be integral UTC nanoseconds or a UTC datetime.datetime",
    ))
}

/// Build a ChannelDescriptor from channel name and optional tags.
pub fn description_with_tags(
    name: &str,
    tags: Option<HashMap<String, String>>,
) -> ChannelDescriptor {
    ChannelDescriptor::with_tags(
        name.to_string(),
        tags.map_or_else(Vec::new, |t| t.into_iter().collect()),
    )
}

/// The typed payload that crosses the sync→async boundary.
#[derive(Clone, Debug)]
pub enum EnqueueItem {
    Doubles {
        ch: ChannelDescriptor,
        points: Vec<DoublePoint>,
    },
    Ints {
        ch: ChannelDescriptor,
        points: Vec<IntegerPoint>,
    },
    Strings {
        ch: ChannelDescriptor,
        points: Vec<StringPoint>,
    },
}

// ---- Single-point constructors ----------------------------------------------

pub fn single_double(ch: ChannelDescriptor, ts: Timestamp, v: f64) -> EnqueueItem {
    EnqueueItem::Doubles {
        ch,
        points: vec![DoublePoint {
            timestamp: Some(ts),
            value: v,
        }],
    }
}
pub fn single_int(ch: ChannelDescriptor, ts: Timestamp, v: i64) -> EnqueueItem {
    EnqueueItem::Ints {
        ch,
        points: vec![IntegerPoint {
            timestamp: Some(ts),
            value: v,
        }],
    }
}
pub fn single_string(ch: ChannelDescriptor, ts: Timestamp, v: String) -> EnqueueItem {
    EnqueueItem::Strings {
        ch,
        points: vec![StringPoint {
            timestamp: Some(ts),
            value: v,
        }],
    }
}

// ---- Series (timestamps + values) constructors ------------------------------

pub fn series_doubles(
    ch: ChannelDescriptor,
    tss: Vec<Timestamp>,
    vals: Vec<f64>,
) -> PyResult<EnqueueItem> {
    if tss.len() != vals.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "timestamps and values must have same length",
        ));
    }
    Ok(EnqueueItem::Doubles {
        ch,
        points: tss
            .into_iter()
            .zip(vals.into_iter())
            .map(|(ts, v)| DoublePoint {
                timestamp: Some(ts),
                value: v,
            })
            .collect(),
    })
}
pub fn series_ints(
    ch: ChannelDescriptor,
    tss: Vec<Timestamp>,
    vals: Vec<i64>,
) -> PyResult<EnqueueItem> {
    if tss.len() != vals.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "timestamps and values must have same length",
        ));
    }
    Ok(EnqueueItem::Ints {
        ch,
        points: tss
            .into_iter()
            .zip(vals.into_iter())
            .map(|(ts, v)| IntegerPoint {
                timestamp: Some(ts),
                value: v,
            })
            .collect(),
    })
}
pub fn series_strings(
    ch: ChannelDescriptor,
    tss: Vec<Timestamp>,
    vals: Vec<String>,
) -> PyResult<EnqueueItem> {
    if tss.len() != vals.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "timestamps and values must have same length",
        ));
    }
    Ok(EnqueueItem::Strings {
        ch,
        points: tss
            .into_iter()
            .zip(vals.into_iter())
            .map(|(ts, v)| StringPoint {
                timestamp: Some(ts),
                value: v,
            })
            .collect(),
    })
}

// ---- Python collection helpers ----------------------------------------------

pub enum ValueKind {
    Floats,
    Ints,
    Strings,
}

/// Peek the first element to decide the homogeneous value kind.
/// (Full extraction to Vec<T> will still enforce homogeneity.)
pub fn classify_values(values: &Bound<'_, PyAny>) -> PyResult<ValueKind> {
    let seq = values.downcast::<PySequence>()?;
    let len = seq.len()?;
    if len == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "values cannot be empty",
        ));
    }
    let first = seq.get_item(0)?;
    if first.extract::<f64>().is_ok() {
        Ok(ValueKind::Floats)
    } else if first.extract::<i64>().is_ok() {
        Ok(ValueKind::Ints)
    } else if first.extract::<String>().is_ok() {
        Ok(ValueKind::Strings)
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "values must be all floats, ints, or strings",
        ))
    }
}

pub fn extract_vec_f64(values: &Bound<'_, PyAny>) -> PyResult<Vec<f64>> {
    let seq = values.downcast::<PySequence>()?;
    let len = seq.len()? as usize;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let item = seq.get_item(i)?;
        let v: f64 = item
            .extract()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("values must be floats"))?;
        out.push(v);
    }
    Ok(out)
}

pub fn extract_vec_i64(values: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    let seq = values.downcast::<PySequence>()?;
    let len = seq.len()? as usize;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let item = seq.get_item(i)?;
        let v: i64 = item
            .extract()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("values must be ints"))?;
        out.push(v);
    }
    Ok(out)
}

pub fn extract_vec_string(values: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let seq = values.downcast::<PySequence>()?;
    let len = seq.len()? as usize;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let item = seq.get_item(i)?;
        let v: String = item
            .extract()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("values must be strings"))?;
        out.push(v);
    }
    Ok(out)
}

pub fn extract_vec_ts(timestamps: &Bound<'_, PyAny>) -> PyResult<Vec<Timestamp>> {
    let seq = timestamps.downcast::<PySequence>()?;
    let len = seq.len()? as usize;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let o = seq.get_item(i)?;
        out.push(parse_timestamp(&o)?);
    }
    Ok(out)
}
