use std::sync::atomic::AtomicU64;

/// Atomic counters used to attribute wall-clock time across the pipeline.
///
/// The `write_dataframe_*`, `df_handoff_ns`, `extract_ts_ns`, and
/// `column_build_ns` fields are populated only by the
/// `write_dataframe` method (available when the `polars` feature is
/// enabled). The remaining fields (`enqueue_batch_ns`,
/// `consumer_consume_ns`, `consumer_consume_calls`) are populated on every
/// write path regardless of feature flags.
///
/// Shared via `Arc` between [`super::AvroWriter`] and the consumer wrapper so
/// both sides accumulate into the same totals.
///
/// Producer time is wall-clock on the caller thread; consumer time is
/// wall-clock on the single dispatcher thread and runs in parallel with the
/// producer. If `consumer_consume_ns ≈ wall_total`, the consumer is
/// saturated (bottleneck). If producer CPU fields ≈ `wall_total`, the
/// producer is the bottleneck.
///
/// Read each field with `.load(Ordering::Relaxed)`.
#[derive(Default, Debug)]
pub struct PipelineStats {
    // ── Producer CPU phases (populated by write_dataframe only) ──
    /// `py_df_to_rust` via Arrow C Data Interface (per-column rechunk +
    /// import). Written by the Python facade's `write_dataframe`, not by
    /// the pure-Rust core — a pure-Rust caller that passes a `polars::DataFrame`
    /// in-process pays no FFI cost and this counter stays at 0.
    pub df_handoff_ns: AtomicU64,
    /// Timestamp column extraction + `parse_timestamp`.
    pub extract_ts_ns: AtomicU64,
    /// Column-loop wall: polars-chunk extraction + point-struct building +
    /// `.into_points()` type-erasure + batch accumulation, summed across all
    /// non-timestamp columns of every dataframe.
    pub column_build_ns: AtomicU64,

    // ── Producer wall time into the stream (every write path) ──
    /// Wall time producer spends handing a batch off to the underlying
    /// `NominalDatasetStream::enqueue_batch`. Includes buffer-lock
    /// acquisition, extends, *and* time blocked waiting for buffer capacity
    /// (when both primary and secondary buffers are full → the consumer is
    /// too slow). Named after the stream-level method, not the writer's
    /// public `write_batch`, because this measures just the hand-off portion
    /// inside the critical section.
    pub enqueue_batch_ns: AtomicU64,

    // ── Consumer-side (runs on the dispatcher thread, in parallel) ──
    /// Wall time spent inside `AvroFileConsumer::consume` per request (avro
    /// encode + file write). Summed across all calls on the single dispatcher
    /// thread. Compare to wall-clock to see if consumer is saturated.
    pub consumer_consume_ns: AtomicU64,
    /// Number of `consume` calls — i.e., `WriteRequest`s dispatched.
    pub consumer_consume_calls: AtomicU64,

    // ── Meta (write_dataframe only) ──
    /// Number of `write_dataframe` invocations so far.
    pub write_dataframe_calls: AtomicU64,
    /// Total wall time across all `write_dataframe` invocations (sum of
    /// producer CPU + `enqueue_batch_ns` for this method only).
    pub write_dataframe_total_ns: AtomicU64,
}
