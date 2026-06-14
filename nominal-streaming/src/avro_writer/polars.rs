//! Polars DataFrame → [`AvroWriter`] integration.
//!
//! Feature-gated behind the `polars` Cargo feature. Provides the pure-Rust
//! [`AvroWriter::write_dataframe`] method — column-loop, dtype dispatch,
//! row-axis rotation splitting, and null / NaN / Inf handling.
//!
//! Consumes a Rust-native `polars::DataFrame`. If you're binding from Python
//! you'll want to hand the frame across via the Arrow C Data Interface first
//! (the `polars_ffi` module in the `py-nominal-streaming` crate does that,
//! then delegates here).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
use polars::prelude::*;

use super::error::AvroWriterError;
use super::writer::AvroWriter;
use crate::types::ChannelDescriptor;
use crate::types::IntoPoints;

/// Recursively convert a polars `AnyValue` into a `serde_json::Value`.
///
/// Returns `None` **only** when the input is a true polars null
/// (`AnyValue::Null`). Callers use this `None` as a signal to skip the
/// enclosing row (at the top level) or the enclosing field (inside a
/// struct / list).
///
/// # Non-finite float handling — read this before editing the Float arms
///
/// Spec JSON (RFC 8259) has no representation for `NaN`, `+Infinity`, or
/// `-Infinity`. We map all three to JSON `null` rather than to the
/// non-standard literal tokens that `serde_json` refuses to emit anyway.
/// This is **intentional and load-bearing**:
///
/// - The Nominal Core ingest backend stores the payload in a ClickHouse
///   `JSON(max_dynamic_paths=0)` column. ClickHouse's JSON parser
///   *silently accepts* non-RFC-8259 tokens at insert time but later
///   fails or returns NULL from JSON-extraction functions at query time.
///   Emitting a literal `NaN` token here would therefore produce a
///   silent-data-loss time-bomb: the avro file would ingest cleanly and
///   the row would land in the table, but the downstream chart / query
///   path would break on read with no indication of what went wrong.
/// - Dropping the field entirely (what the previous implementation did
///   via `filter_map`) changes the shape of the JSON object the reader
///   sees and is equally bad.
///
/// Mapping to JSON `null` is lossy — on read you can't tell a polars
/// null field from a NaN/Inf field — but it is correct end-to-end, the
/// container's shape is preserved, and it round-trips through strict
/// JSON parsers. Do not change this without re-verifying the scout
/// backend's JSON-column behavior in `DirectClickHouseFileWriterV2.java`.
pub fn anyvalue_to_json_value(av: AnyValue<'_>) -> Option<serde_json::Value> {
    use serde_json::Value;

    // Normalize the `Struct(idx, arr, fields)` / `String(&str)` / etc. borrowed
    // variants to their owned equivalents so one set of match arms covers both.
    // `into_static()` is cheap for scalars (just rebinds the variant) and
    // materializes owned copies only for structs/strings/binaries where it's
    // genuinely needed.
    let av = av.into_static();

    match av {
        AnyValue::Null => None,
        AnyValue::Boolean(b) => Some(Value::Bool(b)),
        AnyValue::Int8(v) => Some(Value::Number(v.into())),
        AnyValue::Int16(v) => Some(Value::Number(v.into())),
        AnyValue::Int32(v) => Some(Value::Number(v.into())),
        AnyValue::Int64(v) => Some(Value::Number(v.into())),
        AnyValue::UInt8(v) => Some(Value::Number(v.into())),
        AnyValue::UInt16(v) => Some(Value::Number(v.into())),
        AnyValue::UInt32(v) => Some(Value::Number(v.into())),
        AnyValue::UInt64(v) => Some(Value::Number(v.into())),
        // `serde_json::Number::from_f64` returns `None` for NaN / ±Inf.
        // We funnel those to JSON `null` to dodge the backend time-bomb —
        // see the function-level doc comment above.
        AnyValue::Float32(v) => Some(
            serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        ),
        AnyValue::Float64(v) => Some(
            serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        ),
        AnyValue::StringOwned(s) => Some(Value::String(s.as_str().to_string())),
        AnyValue::List(series) => {
            // Preserve array length: polars-null inner elements become JSON null.
            let arr: Vec<Value> = series
                .iter()
                .map(|v| anyvalue_to_json_value(v).unwrap_or(Value::Null))
                .collect();
            Some(Value::Array(arr))
        }
        AnyValue::StructOwned(boxed) => {
            // StructOwned holds (Vec<AnyValue>, Vec<Field>). We keep every
            // field in the output — polars-null field values become JSON null,
            // matching the nullability contract for scalar columns.
            let (values, fields) = *boxed;
            let obj: serde_json::Map<String, Value> = fields
                .iter()
                .zip(values.into_iter())
                .map(|(field, val)| {
                    (
                        field.name().to_string(),
                        anyvalue_to_json_value(val).unwrap_or(Value::Null),
                    )
                })
                .collect();
            Some(Value::Object(obj))
        }
        // Catch-all: convert to string representation for unknown variants
        // (dates, durations, binary, etc.)
        other => Some(Value::String(format!("{other}"))),
    }
}

/// Convert a top-level `AnyValue` row (typically from a Struct column) to
/// a JSON string. Returns `None` if the value is a polars null — callers
/// treat that as "skip the row entirely". See [`anyvalue_to_json_value`]
/// for the field-level recursion and the non-finite-float encoding
/// contract.
pub fn anyvalue_to_json(av: AnyValue<'_>) -> Option<String> {
    anyvalue_to_json_value(av).map(|v| v.to_string())
}

/// Build a ChannelDescriptor from a name + optional tags.
fn descriptor_with_tags(name: &str, tags: Option<&HashMap<String, String>>) -> ChannelDescriptor {
    ChannelDescriptor::with_tags(
        name.to_string(),
        tags.map_or_else(Vec::new, |t| t.clone().into_iter().collect()),
    )
}

/// Convert an i64 nanoseconds-since-epoch value to a `Timestamp`.
fn timestamp_from_nanos(nanos: i64) -> Timestamp {
    let seconds = nanos.div_euclid(1_000_000_000);
    let n = nanos.rem_euclid(1_000_000_000) as i32;
    Timestamp { seconds, nanos: n }
}

impl AvroWriter {
    /// Write each non-timestamp column of a polars DataFrame as a separate channel.
    ///
    /// Per-column dispatch:
    ///   * `Float64` → `Vec<DoublePoint>` per column
    ///   * `Int64`   → `Vec<IntegerPoint>` per column
    ///   * `String`  → `Vec<StringPoint>` per column
    ///   * `List(*)` / `Array(*, _)` with String inner → `Vec<StringArrayPoint>` per row
    ///   * `List(*)` / `Array(*, _)` with numeric inner → `Vec<DoubleArrayPoint>` per row (cast to f64)
    ///   * `Struct(…)` → `Vec<StructPoint>` per row (JSON-serialized via serde_json)
    ///
    /// # Null / NaN / Infinity handling
    ///
    /// These semantics are deliberate — they match how the Nominal Core
    /// ingest backend (`DirectClickHouseFileWriterV2.java`) processes the
    /// Avro file it receives. Do not change them without re-checking the
    /// backend: the client-side and server-side behaviors are coupled.
    ///
    /// **Polars null → row skipped** (no avro record emitted, timestamp
    /// not paired with a sentinel). The backend drops null-Tuple rows at
    /// insert time via a `WHERE Tuple IS NOT NULL` filter, so emitting a
    /// null-valued record would produce nothing downstream anyway — we
    /// skip client-side to save the wire round-trip and to keep scalar
    /// columns free of sentinel confusion (a real 0.0 vs a "was null").
    ///
    /// **Float64 NaN / ±Infinity → preserved**. These are real IEEE-754
    /// values, not nulls. They round-trip unchanged through the avro
    /// `double` branch and ClickHouse's `Variant(Float64, ...)` column,
    /// which stores the IEEE-754 bit patterns as-is.
    ///
    /// **Complex columns (`Struct`, `List`, `Array`):** a polars-null
    /// *row* is skipped (matching the backend's row-level Tuple filter).
    /// Inside a struct or list, polars-null *elements* become JSON `null`
    /// so the containing object/array structure survives to the reader.
    /// Non-finite floats inside a struct field also encode as JSON `null`
    /// — **not** as literal `NaN` / `Infinity` tokens. See
    /// [`anyvalue_to_json_value`] for the rationale.
    ///
    /// The timestamp column itself must be `Int64` (nanoseconds since
    /// epoch) and must not contain nulls; a null timestamp is a hard
    /// error.
    pub fn write_dataframe(
        &self,
        df: &DataFrame,
        timestamp_column: &str,
        tags: Option<HashMap<String, String>>,
    ) -> Result<(), Arc<AvroWriterError>> {
        let stats = self.inner.stats.clone();
        let t_total = std::time::Instant::now();

        // --- Extract timestamp column (must be Int64 nanoseconds) ---
        let t0 = std::time::Instant::now();
        let ts_col = df
            .column(timestamp_column)
            .map_err(|e| Arc::new(AvroWriterError::Consumer(format!("timestamp column: {e}"))))?;
        let ts_i64 = ts_col.i64().map_err(|_| {
            Arc::new(AvroWriterError::Consumer(format!(
                "timestamp column {:?} must be Int64, got {:?}",
                timestamp_column,
                ts_col.dtype()
            )))
        })?;
        let timestamps_raw: Vec<i64> = ts_i64
            .into_iter()
            .map(|v| {
                v.ok_or_else(|| {
                    Arc::new(AvroWriterError::Consumer(
                        "null in timestamp column".to_string(),
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let timestamps: Vec<Timestamp> = timestamps_raw
            .iter()
            .map(|&ns| timestamp_from_nanos(ns))
            .collect();
        stats
            .extract_ts_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // --- Column loop: accumulate every (descriptor, points) pair and
        // hand the whole frame off to `write_batch` in one shot. This
        // amortizes the stream + buffer-lock across all channels.
        let t_cols = std::time::Instant::now();
        let mut batch: Vec<(ChannelDescriptor, PointsType)> =
            Vec::with_capacity(df.get_columns().len());

        for col in df.get_columns() {
            let name = col.name().as_str();
            if name == timestamp_column {
                continue;
            }
            let desc = descriptor_with_tags(name, tags.as_ref());

            match col.dtype() {
                // Null handling (applies to every scalar arm below):
                //   - Polars null  → row **skipped** (no record emitted; the
                //     timestamp is dropped for this channel, not paired with
                //     a sentinel value). This aligns with the backend's
                //     row-level `WHERE Tuple IS NOT NULL` filter — a
                //     null-valued record we might emit would be dropped at
                //     insert anyway, so we skip client-side to save the
                //     round-trip and keep scalar columns free of sentinel
                //     ambiguity (a real 0.0 vs "was null").
                //   - Float64 NaN / ±Inf → preserved as-is (IEEE-754
                //     round-trips through avro's double-branch encoding and
                //     ClickHouse's Variant(Float64, ...) column).
                //
                // See the `write_dataframe` docstring for the full contract
                // and the backend coupling — do not change without checking
                // `DirectClickHouseFileWriterV2.java` in the scout repo.
                DataType::Float64 => {
                    let ch = col.f64().expect("dtype checked above");
                    let pts: Vec<DoublePoint> = timestamps
                        .iter()
                        .zip(ch.into_iter())
                        .filter_map(|(&t, v)| {
                            v.map(|value| DoublePoint {
                                timestamp: Some(t),
                                value,
                            })
                        })
                        .collect();
                    if !pts.is_empty() {
                        batch.push((desc, pts.into_points()));
                    }
                }
                DataType::Int64 => {
                    let ch = col.i64().expect("dtype checked above");
                    let pts: Vec<IntegerPoint> = timestamps
                        .iter()
                        .zip(ch.into_iter())
                        .filter_map(|(&t, v)| {
                            v.map(|value| IntegerPoint {
                                timestamp: Some(t),
                                value,
                            })
                        })
                        .collect();
                    if !pts.is_empty() {
                        batch.push((desc, pts.into_points()));
                    }
                }
                DataType::String => {
                    let ch = col.str().expect("dtype checked above");
                    let pts: Vec<StringPoint> = timestamps
                        .iter()
                        .zip(ch.into_iter())
                        .filter_map(|(&t, v)| {
                            v.map(|value| StringPoint {
                                timestamp: Some(t),
                                value: value.to_string(),
                            })
                        })
                        .collect();
                    if !pts.is_empty() {
                        batch.push((desc, pts.into_points()));
                    }
                }
                DataType::Struct(_) => {
                    // Iterate via as_materialized_series().iter() → AnyValue per row.
                    //
                    // `anyvalue_to_json` returns `None` exactly when the row
                    // is a polars null. We drop it — no record emitted for
                    // this timestamp. Symmetric with the scalar-null case
                    // above and with the backend's row-level Tuple filter.
                    // The non-finite-float → JSON-null mapping happens
                    // inside `anyvalue_to_json_value`; see its doc comment
                    // for why that (rather than a literal NaN token) is
                    // the correct encoding.
                    let series = col.as_materialized_series();
                    let mut pts: Vec<StructPoint> = Vec::with_capacity(series.len());
                    for (i, row_value) in series.iter().enumerate() {
                        if let Some(json_str) = anyvalue_to_json(row_value) {
                            pts.push(StructPoint {
                                timestamp: Some(timestamps[i]),
                                json_string: json_str,
                            });
                        }
                    }
                    if !pts.is_empty() {
                        batch.push((desc, pts.into_points()));
                    }
                }
                DataType::List(inner) => {
                    if matches!(**inner, DataType::String) {
                        // List(String) → StringArrayPoint per row
                        let ch = col.list().expect("dtype checked above");
                        let mut pts: Vec<StringArrayPoint> = Vec::with_capacity(ch.len());
                        for (i, row) in ch.into_iter().enumerate() {
                            if let Some(inner_series) = row {
                                let str_ch = inner_series.str().expect("inner is utf8/string");
                                let v: Vec<String> = str_ch
                                    .into_iter()
                                    .map(|s| s.unwrap_or("").to_string())
                                    .collect();
                                pts.push(StringArrayPoint {
                                    timestamp: Some(timestamps[i]),
                                    value: v,
                                });
                            }
                            // Polars-null row → no record emitted for this
                            // timestamp. Symmetric with the scalar-null
                            // case and the backend's row-level Tuple
                            // filter; see `write_dataframe` docstring.
                        }
                        if !pts.is_empty() {
                            batch.push((desc, pts.into_points()));
                        }
                    } else {
                        // List(numeric) → DoubleArrayPoint per row, cast inner to f64
                        let ch = col.list().expect("dtype checked above");
                        let mut pts: Vec<DoubleArrayPoint> = Vec::with_capacity(ch.len());
                        for (i, row) in ch.into_iter().enumerate() {
                            if let Some(inner_series) = row {
                                let f_series =
                                    inner_series.cast(&DataType::Float64).map_err(|e| {
                                        Arc::new(AvroWriterError::Consumer(format!(
                                            "cast inner list to Float64 failed: {e}"
                                        )))
                                    })?;
                                let f_ch = f_series.f64().expect("cast succeeded");
                                let v: Vec<f64> =
                                    f_ch.into_iter().map(|x| x.unwrap_or(f64::NAN)).collect();
                                pts.push(DoubleArrayPoint {
                                    timestamp: Some(timestamps[i]),
                                    value: v,
                                });
                            }
                            // Polars-null row → no record emitted for this
                            // timestamp. Symmetric with the scalar-null
                            // case and the backend's row-level Tuple
                            // filter; see `write_dataframe` docstring.
                        }
                        if !pts.is_empty() {
                            batch.push((desc, pts.into_points()));
                        }
                    }
                }
                DataType::Array(inner, _) => {
                    if matches!(**inner, DataType::String) {
                        // Array(String, _) → StringArrayPoint per row
                        let ch = col.array().expect("dtype checked above");
                        let mut pts: Vec<StringArrayPoint> = Vec::with_capacity(ch.len());
                        for (i, row) in ch.into_iter().enumerate() {
                            if let Some(inner_series) = row {
                                let str_ch = inner_series.str().expect("inner is utf8/string");
                                let v: Vec<String> = str_ch
                                    .into_iter()
                                    .map(|s| s.unwrap_or("").to_string())
                                    .collect();
                                pts.push(StringArrayPoint {
                                    timestamp: Some(timestamps[i]),
                                    value: v,
                                });
                            }
                        }
                        if !pts.is_empty() {
                            batch.push((desc, pts.into_points()));
                        }
                    } else {
                        // Array(numeric, _) → DoubleArrayPoint per row
                        let ch = col.array().expect("dtype checked above");
                        let mut pts: Vec<DoubleArrayPoint> = Vec::with_capacity(ch.len());
                        for (i, row) in ch.into_iter().enumerate() {
                            if let Some(inner_series) = row {
                                let f_series =
                                    inner_series.cast(&DataType::Float64).map_err(|e| {
                                        Arc::new(AvroWriterError::Consumer(format!(
                                            "cast inner array to Float64 failed: {e}"
                                        )))
                                    })?;
                                let f_ch = f_series.f64().expect("cast succeeded");
                                let v: Vec<f64> =
                                    f_ch.into_iter().map(|x| x.unwrap_or(f64::NAN)).collect();
                                pts.push(DoubleArrayPoint {
                                    timestamp: Some(timestamps[i]),
                                    value: v,
                                });
                            }
                        }
                        if !pts.is_empty() {
                            batch.push((desc, pts.into_points()));
                        }
                    }
                }
                other => {
                    return Err(Arc::new(AvroWriterError::Consumer(format!(
                        "unsupported dtype for column {:?}: {:?}",
                        name, other
                    ))));
                }
            }
        }
        // Close the column-build phase *before* handing off to the stream;
        // `enqueue_batch_ns` is measured separately inside `write_batch`.
        stats
            .column_build_ns
            .fetch_add(t_cols.elapsed().as_nanos() as u64, Ordering::Relaxed);

        if !batch.is_empty() {
            self.write_batch(batch)?;
        }

        stats.write_dataframe_calls.fetch_add(1, Ordering::Relaxed);
        stats
            .write_dataframe_total_ns
            .fetch_add(t_total.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(())
    }
}
