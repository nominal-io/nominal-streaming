//! Helpers for translating Python arguments into nominal_streaming types.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_streaming::prelude::*;
use nominal_streaming::types::IntoPoints;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyList;
use pyo3::types::PySequence;
use pyo3::types::PyTuple;

pyo3::create_exception!(_nominal_streaming, _TimestampTypeError, PyTypeError);

pub fn extract_timestamp(timestamp: &Bound<'_, PyAny>) -> PyResult<Timestamp> {
    timestamp
        .extract::<i64>()
        .map(IntoTimestamp::into_timestamp)
        .map_err(|error| timestamp_extraction_error(timestamp.py(), error))
}

fn timestamp_extraction_error(py: Python<'_>, error: PyErr) -> PyErr {
    if error.is_instance_of::<pyo3::exceptions::PyOverflowError>(py) {
        PyValueError::new_err("timestamp exceeds the signed 64-bit nanosecond range")
    } else {
        error
    }
}

/// Convert python tags into the descriptor's tag representation.
///
/// Absent and empty tags both become `None` so that the same channel written with `tags=None` and
/// with `tags={}` lands in one series rather than splitting into two. Both encode to an empty tag
/// map on the wire, so this is purely about keying the buffer consistently.
///
/// Callers writing many channels at once should call this once and clone the result: the clone is
/// a refcount bump, so every channel in a wide record can share one map.
pub fn into_tag_map(
    tags: Option<HashMap<String, String>>,
) -> Option<Arc<BTreeMap<String, String>>> {
    tags.filter(|tags| !tags.is_empty())
        .map(|tags| Arc::new(tags.into_iter().collect()))
}

/// Build a ChannelDescriptor from channel name and optional tags.
pub fn description_with_tags(
    name: &str,
    tags: Option<HashMap<String, String>>,
) -> ChannelDescriptor {
    ChannelDescriptor {
        name: name.to_string(),
        tags: into_tag_map(tags),
    }
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

/// Length of a values argument, rejecting anything we cannot index element by element.
///
/// Deliberately not `cast::<PySequence>()`: that requires registration as
/// `collections.abc.Sequence`, which a numpy array is not, so it rejected the most common way of
/// holding a column of values. `__len__` plus `__getitem__` is the property actually needed, and
/// requiring `__len__` still rejects one-shot iterables such as generators, which cannot be
/// classified and then re-read.
fn indexable_len(values: &Bound<'_, PyAny>) -> PyResult<usize> {
    values.len().map_err(|_| {
        PyTypeError::new_err(
            "values must be a sized, indexable sequence (list, tuple, or numpy array)",
        )
    })
}

/// Generic method to convert an indexable python object into a homogenous vector of rust data
fn extract_vec_generic<'py, T>(
    values: &Bound<'py, PyAny>,
    typename_for_error: &'static str,
) -> PyResult<Vec<T>>
where
    T: FromPyObjectOwned<'py>,
{
    let len = indexable_len(values)?;
    let mut out = Vec::with_capacity(len);

    // Keep the faster sequence access for registered sequences, but use one
    // extraction loop. ABC membership controls access, never validation.
    let sequence = values.cast::<PySequence>().ok();
    for i in 0..len {
        let item = match sequence {
            Some(seq) => seq.get_item(i)?,
            None => values.get_item(i)?,
        };
        out.push(
            item.extract().map_err(|_| {
                PyTypeError::new_err(format!("Values must be {}", typename_for_error))
            })?,
        );
    }
    Ok(out)
}

// ---- Single-point constructors ----------------------------------------------

pub fn single_double(ts: Timestamp, v: f64) -> PointsType {
    vec![DoublePoint {
        timestamp: Some(ts),
        value: v,
    }]
    .into_points()
}

pub fn single_int(ts: Timestamp, v: i64) -> PointsType {
    vec![IntegerPoint {
        timestamp: Some(ts),
        value: v,
    }]
    .into_points()
}

pub fn single_string(ts: Timestamp, v: String) -> PointsType {
    vec![StringPoint {
        timestamp: Some(ts),
        value: v,
    }]
    .into_points()
}

pub fn single_struct(ts: Timestamp, json_string: String) -> PointsType {
    vec![StructPoint {
        timestamp: Some(ts),
        json_string,
    }]
    .into_points()
}

pub fn single_double_array(ts: Timestamp, value: Vec<f64>) -> PointsType {
    vec![DoubleArrayPoint {
        timestamp: Some(ts),
        value,
    }]
    .into_points()
}

pub fn single_string_array(ts: Timestamp, value: Vec<String>) -> PointsType {
    vec![StringArrayPoint {
        timestamp: Some(ts),
        value,
    }]
    .into_points()
}

// ---- Series (timestamps + values) constructors ------------------------------

fn series_doubles(tss: Vec<Timestamp>, vals: Vec<f64>) -> PyResult<PointsType> {
    Ok(make_points(tss, vals, |ts, v| DoublePoint {
        timestamp: Some(ts),
        value: v,
    })?
    .into_points())
}

fn series_ints(tss: Vec<Timestamp>, vals: Vec<i64>) -> PyResult<PointsType> {
    Ok(make_points(tss, vals, |ts, v| IntegerPoint {
        timestamp: Some(ts),
        value: v,
    })?
    .into_points())
}

fn series_strings(tss: Vec<Timestamp>, vals: Vec<String>) -> PyResult<PointsType> {
    Ok(make_points(tss, vals, |ts, v| StringPoint {
        timestamp: Some(ts),
        value: v,
    })?
    .into_points())
}

// ---- Python collection helpers ----------------------------------------------

/// Reject array-likes whose elements would be silently reinterpreted rather than written.
///
/// Only reachable for objects carrying numpy's attributes; a list or tuple has neither, and the
/// caller skips this for them. Both cases below otherwise succeed and write plausible, wrong data,
/// which is worse than refusing.
fn reject_lossy_arrays(values: &Bound<'_, PyAny>) -> PyResult<()> {
    if let Ok(dtype) = values.getattr(intern!(values.py(), "dtype")) {
        if let Ok(kind) = dtype.getattr(intern!(values.py(), "kind")) {
            let kind: String = kind.extract().unwrap_or_default();
            // 'M' is datetime64, 'm' is timedelta64. Both convert to a number, so passing one as
            // values stores an epoch count and looks like it worked.
            if kind == "M" || kind == "m" {
                return Err(PyTypeError::new_err(
                    "values has a datetime64/timedelta64 dtype; these would be written as raw \
                     epoch counts. Pass the timestamps as the `timestamps` argument, or convert \
                     explicitly with `.astype('int64')` if the count is what you want.",
                ));
            }
        }
    }

    // A masked array converts masked elements to NaN on read, so a gap becomes a real data point.
    if let Ok(mask) = values.getattr(intern!(values.py(), "mask")) {
        let any_masked = mask
            .call_method0(intern!(values.py(), "any"))
            .and_then(|m| m.extract::<bool>())
            .unwrap_or(false);
        if any_masked {
            return Err(PyTypeError::new_err(
                "values is a masked array with masked elements, which would be written as NaN. \
                 Choose explicitly: `.filled(float('nan'))` to write NaN, or `.compressed()` with \
                 matching timestamps to drop them.",
            ));
        }
    }

    Ok(())
}

/// Validate and convert one batch, preserving float-first scalar classification.
pub fn extract_series_points(
    timestamps: Vec<Timestamp>,
    values: &Bound<'_, PyAny>,
) -> PyResult<PointsType> {
    if indexable_len(values)? == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "values cannot be empty",
        ));
    }
    // Only exact built-ins are known to have no dtype or mask. Subclasses and
    // registered sequences still need validation, regardless of their access path.
    if !values.is_exact_instance_of::<PyList>() && !values.is_exact_instance_of::<PyTuple>() {
        reject_lossy_arrays(values)?;
    }
    let first = values.get_item(0)?;
    if first.extract::<f64>().is_ok() {
        #[cfg(Py_3_11)]
        if let Some(values) = crate::numeric_buffer::floats(values)? {
            return series_doubles(timestamps, values);
        }
        series_doubles(timestamps, extract_vec_generic(values, "floats")?)
    } else if first.extract::<i64>().is_ok() {
        series_ints(timestamps, extract_vec_generic(values, "ints")?)
    } else if first.extract::<String>().is_ok() {
        series_strings(timestamps, extract_vec_generic(values, "strings")?)
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "values must be all floats, ints, or strings",
        ))
    }
}

pub fn extract_vec_ts(timestamps: Vec<i64>) -> Vec<Timestamp> {
    timestamps
        .into_iter()
        .map(IntoTimestamp::into_timestamp)
        .collect()
}

/// Distinguish timestamp extraction failures from value errors in the Python wrapper.
pub fn extract_timestamp_input(values: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    #[cfg(Py_3_11)]
    if let Some(timestamps) = crate::numeric_buffer::timestamps(values)? {
        return Ok(timestamps);
    }
    values.extract().map_err(|error: PyErr| {
        if error.is_instance_of::<PyTypeError>(values.py()) {
            let timestamp_error = _TimestampTypeError::new_err(error.to_string());
            timestamp_error.set_cause(values.py(), Some(error));
            timestamp_error
        } else {
            timestamp_extraction_error(values.py(), error)
        }
    })
}
