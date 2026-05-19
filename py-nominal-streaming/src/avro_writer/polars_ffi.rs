use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Convert a Python polars Series → Rust polars Series via Arrow C Data Interface.
///
/// This uses `_s._export_arrow_to_c(array_ptr, schema_ptr)` on the Python
/// side — the native Rust object embedded in the Python polars Series.  This
/// path does NOT require `pyarrow`; it is a direct memory transfer via the
/// Arrow C Data Interface (capsules / raw pointers).
///
/// Works with any Python polars version that exposes `_export_arrow_to_c`
/// on its inner `_s` object (i.e., all Python polars 1.x releases).
pub(super) fn py_series_to_rust(
    _py: Python<'_>,
    py_series: &Bound<'_, PyAny>,
) -> PyResult<polars::prelude::Series> {
    use polars::prelude::*;
    use polars_arrow::ffi;

    // Rechunk to ensure a single contiguous Arrow buffer before export.
    let py_series = py_series.call_method0("rechunk")?;
    // Get the series name for the resulting Rust Series.
    let name: String = py_series.getattr("name")?.extract()?;

    // Access the native Rust PySeries object embedded in the Python Series.
    let ps = py_series.getattr("_s")?;

    // Allocate empty Arrow C Data Interface structs on the Rust heap.
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray as usize;
    let schema_ptr = &*schema as *const ffi::ArrowSchema as usize;

    // Ask polars' native object to fill those structs.
    // `_export_arrow_to_c(array_uintptr, schema_uintptr)` is the same C Data
    // Interface export that polars uses internally.
    ps.call_method1("_export_arrow_to_c", (array_ptr, schema_ptr))?;

    // Now import the filled structs back into Rust types.
    let field = unsafe { ffi::import_field_from_c(schema.as_ref()) }
        .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema import failed: {e}")))?;
    let array = unsafe { ffi::import_array_from_c(*array, field.dtype.clone()) }
        .map_err(|e| PyRuntimeError::new_err(format!("Arrow array import failed: {e}")))?;

    // Build a Rust Series from the imported Arrow array + field.
    let series = Series::try_from((&field, array))
        .map_err(|e| PyRuntimeError::new_err(format!("Series construction failed: {e}")))?;

    // Rename to match original series name (field name from Arrow may differ).
    Ok(series.with_name(PlSmallStr::from(name.as_str())))
}

/// Convert a Python polars DataFrame → Rust polars DataFrame column-by-column
/// via `py_series_to_rust`.
pub(super) fn py_df_to_rust(
    py: Python<'_>,
    py_df: &Bound<'_, PyAny>,
) -> PyResult<polars::prelude::DataFrame> {
    use polars::prelude::*;

    let py_columns = py_df.call_method0("get_columns")?;
    let n: usize = py_df.getattr("width")?.extract()?;
    let mut columns: Vec<Column> = Vec::with_capacity(n);
    for py_col in py_columns.try_iter()? {
        let py_col = py_col?;
        let series = py_series_to_rust(py, &py_col)?;
        columns.push(series.into_column());
    }
    // SAFETY: we trust the Python polars invariants (same length across columns).
    Ok(unsafe { DataFrame::new_no_checks_height_from_first(columns) })
}

// `anyvalue_to_json_value` / `anyvalue_to_json` now live in the
// nominal-streaming crate under `avro_writer::polars` (feature-gated) —
// they're pure Rust and don't need to be here.
