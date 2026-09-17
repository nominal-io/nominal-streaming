//! Optional NumPy buffer copies. All borrows end before enqueue releases the GIL.

use pyo3::buffer::Element;
use pyo3::buffer::ElementType;
use pyo3::buffer::PyUntypedBuffer;
use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

static NDARRAY: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Only exact ndarrays: subclasses may override scalar access or attach masks.
/// NumPy is imported lazily, only when an ndarray is actually passed to us.
fn array_buffer(values: &Bound<'_, PyAny>) -> PyResult<Option<PyUntypedBuffer>> {
    let py = values.py();
    let ty = values.get_type();
    if NDARRAY.get(py).is_none()
        && (ty.name()?.to_str()? != "ndarray" || ty.module()?.to_str()? != "numpy")
    {
        return Ok(None);
    }
    let ndarray = NDARRAY.get_or_try_init(py, || -> PyResult<Py<PyType>> {
        Ok(py
            .import("numpy")?
            .getattr("ndarray")?
            .cast_into()?
            .unbind())
    })?;
    if !ty.is(ndarray.bind(py)) {
        return Ok(None);
    }
    // Unsupported dtypes (e.g. object or datetime) keep their scalar semantics.
    let Ok(buffer) = PyUntypedBuffer::get(values) else {
        return Ok(None);
    };
    // Use only native, single-character PEP 3118 formats. In particular, do not
    // trust PyO3 0.29's endian compatibility check for explicitly ordered buffers.
    if buffer.dimensions() != 1 || buffer.format().to_bytes().len() != 1 {
        return Ok(None);
    }
    Ok(Some(buffer))
}

fn copy_as<T: Element>(buffer: &PyUntypedBuffer, py: Python<'_>) -> PyResult<Option<Vec<T>>> {
    // Type/size/alignment mismatch is ineligibility; a failed copy is an error.
    // to_vec handles strided/reversed inputs without assuming contiguous storage.
    buffer
        .as_typed::<T>()
        .ok()
        .map(|b| b.to_vec(py))
        .transpose()
}

pub fn timestamps(values: &Bound<'_, PyAny>) -> PyResult<Option<Vec<u64>>> {
    let Some(buffer) = array_buffer(values)? else {
        return Ok(None);
    };
    match ElementType::from_format(buffer.format()) {
        ElementType::UnsignedInteger { bytes: 8 } => copy_as::<u64>(&buffer, values.py()),
        ElementType::SignedInteger { bytes: 8 } => {
            let Some(timestamps) = copy_as::<i64>(&buffer, values.py())? else {
                return Ok(None);
            };
            timestamps
                .into_iter()
                .map(|v| {
                    u64::try_from(v).map_err(|_| {
                        PyOverflowError::new_err("can't convert negative int to unsigned")
                    })
                })
                .collect::<PyResult<Vec<_>>>()
                .map(Some)
        }
        _ => Ok(None),
    }
}

/// Preserve the existing float-first classification, including integer rounding.
pub fn floats(values: &Bound<'_, PyAny>) -> PyResult<Option<Vec<f64>>> {
    let Some(buffer) = array_buffer(values)? else {
        return Ok(None);
    };
    match ElementType::from_format(buffer.format()) {
        ElementType::Float { bytes: 8 } => copy_as::<f64>(&buffer, values.py()),
        ElementType::Float { bytes: 4 } => {
            Ok(copy_as::<f32>(&buffer, values.py())?
                .map(|v| v.into_iter().map(f64::from).collect()))
        }
        ElementType::SignedInteger { bytes: 8 } => Ok(copy_as::<i64>(&buffer, values.py())?
            .map(|v| v.into_iter().map(|n| n as f64).collect())),
        ElementType::UnsignedInteger { bytes: 8 } => Ok(copy_as::<u64>(&buffer, values.py())?
            .map(|v| v.into_iter().map(|n| n as f64).collect())),
        _ => Ok(None),
    }
}
