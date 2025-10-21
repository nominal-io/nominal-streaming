//! Helpers for translating Python arguments into nominal_streaming types.

use std::collections::HashMap;

use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_streaming::prelude::*;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyAnyMethods;
use pyo3::types::PySequence;

/// Convert a integral nanosecond timestamp into google.protobuf.Timestamp.
pub fn parse_timestamp(timestamp: u64) -> Timestamp {
    let seconds = timestamp.div_euclid(1_000_000_000) as i64;
    let nanos = timestamp.rem_euclid(1_000_000_000) as i32;
    Timestamp { seconds, nanos }
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

/// The typed payload that crosses the sync => async boundary.
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

/// Ensure the given lists of timestamps and values have the same length
fn ensure_same_len<T, U>(a: &[T], b: &[U]) -> PyResult<()> {
    if a.len() != b.len() {
        Err(PyValueError::new_err(
            "timestamps and values must have same length",
        ))
    } else {
        Ok(())
    }
}

/// Make a vector of protobuf points from the given python sequences and the given lambda for constructing points
fn make_points<P, V>(
    timestamps: Vec<Timestamp>,
    values: Vec<V>,
    mut f: impl FnMut(Timestamp, V) -> P,
) -> PyResult<Vec<P>> {
    ensure_same_len(&timestamps, &values)?;
    Ok(timestamps
        .into_iter()
        .zip(values)
        .map(|(ts, v)| f(ts, v))
        .collect())
}

/// Generic method to convert a pysequence into a homogenous vector of rust data
fn extract_vec_generic<'py, T>(
    values: &Bound<'py, PyAny>,
    typename_for_error: &'static str,
) -> PyResult<Vec<T>>
where
    T: FromPyObject<'py>,
{
    let seq = values.downcast::<PySequence>()?;
    let len = seq.len()? as usize;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let item = seq.get_item(i)?;
        let value: T = item
            .extract()
            .map_err(|_| PyTypeError::new_err(format!("Values must be {}", typename_for_error)))?;
        out.push(value);
    }
    Ok(out)
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
    let points = make_points(tss, vals, |ts, v| DoublePoint {
        timestamp: Some(ts),
        value: v,
    })?;
    Ok(EnqueueItem::Doubles { ch, points })
}
pub fn series_ints(
    ch: ChannelDescriptor,
    tss: Vec<Timestamp>,
    vals: Vec<i64>,
) -> PyResult<EnqueueItem> {
    let points = make_points(tss, vals, |ts, v| IntegerPoint {
        timestamp: Some(ts),
        value: v,
    })?;
    Ok(EnqueueItem::Ints { ch, points })
}
pub fn series_strings(
    ch: ChannelDescriptor,
    tss: Vec<Timestamp>,
    vals: Vec<String>,
) -> PyResult<EnqueueItem> {
    let points = make_points(tss, vals, |ts, v| StringPoint {
        timestamp: Some(ts),
        value: v,
    })?;
    Ok(EnqueueItem::Strings { ch, points })
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
    extract_vec_generic(values, "floats")
}

pub fn extract_vec_i64(values: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    extract_vec_generic(values, "ints")
}

pub fn extract_vec_string(values: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    extract_vec_generic(values, "strings")
}

pub fn extract_vec_ts(timestamps: Vec<u64>) -> Vec<Timestamp> {
    timestamps.into_iter().map(parse_timestamp).collect()
}
