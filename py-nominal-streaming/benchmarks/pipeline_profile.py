#!/usr/bin/env python3
r"""Single-run pipeline attribution for NominalAvroWriter.write_dataframe.

Runs one workload end-to-end and reads NominalAvroWriter.stats() to attribute
wall-clock time across the pipeline's stages:

  * Producer CPU phases inside write_dataframe (FFI, extract, column build)
    — run on the caller thread.
  * enqueue_batch wall (producer's handoff into the stream) — includes time
    blocked on buffer capacity when the consumer can't keep up.
  * Consumer wall (avro encode + snappy + file write) — runs in parallel on
    the stream's single dispatcher thread.

Interpretation:
  - If consumer_consume_ns ≈ elapsed → the consumer is saturated (downstream
    bottleneck; enqueue_batch_ns grows because the producer is backpressured).
  - If producer_cpu_ns ≈ elapsed → the producer is the bottleneck (rare at
    reasonable frame sizes; usually signals a Python-side issue).

Example:
    uv run python py-nominal-streaming/benchmarks/pipeline_profile.py \\
        --cols 1000 --frame-points 1_000_000 --total-points 200_000_000
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import tempfile
import time

# Fail loud if run under Python 3.10 / 3.11: PyO3 boundary crossings are
# materially slower there, and silent cross-version comparisons are the
# single most common source of misleading bench numbers on this pipeline.
if sys.version_info < (3, 12):
    sys.exit(
        f"benchmarks require Python 3.12+ (got {sys.version_info.major}.{sys.version_info.minor}). "
        f"Re-run under 3.12+, e.g. `uv run --python 3.12 python {pathlib.Path(__file__).name}`."
    )

import numpy as np
import polars as pl
from nominal_streaming import NominalAvroWriter, NominalAvroWriterOpts


def build_pool(n_cols: int, pool_rows: int) -> pl.DataFrame:
    ts = pl.Series("ts", np.arange(pool_rows, dtype=np.int64))
    cols = {"ts": ts}
    data = np.random.default_rng(0).random((pool_rows, n_cols), dtype=np.float64)
    for c in range(n_cols):
        cols[f"c{c}"] = pl.Series(f"c{c}", data[:, c])
    return pl.DataFrame(cols)


def run(
    pool: pl.DataFrame, rows_per_frame: int, num_frames: int, out_dir: pathlib.Path, max_points_per_batch: int
) -> tuple[float, dict[str, int]]:
    max_offset = max(pool.height - rows_per_frame + 1, 1)
    opts = NominalAvroWriterOpts(
        max_points_per_batch=max_points_per_batch,
        fsync_on_close=True,
        max_points_per_file=0,
    )
    base = out_dir / "profile.avro"
    start = time.monotonic()
    with NominalAvroWriter(base, opts) as w:
        for i in range(num_frames):
            offset = (i * rows_per_frame) % max_offset
            w.write_dataframe(pool.slice(offset, rows_per_frame), timestamp_column="ts")
        stats = w.stats()
    return time.monotonic() - start, stats


def fmt_ns(ns: int) -> str:
    if ns < 1_000:
        return f"{ns}ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.1f}µs"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.1f}ms"
    return f"{ns / 1_000_000_000:.3f}s"


def bar(pct: float, width: int = 40) -> str:
    filled = max(0, min(width, int(round(pct / 100 * width))))
    return "█" * filled + "·" * (width - filled)


def print_report(elapsed: float, stats: dict[str, int], total_points: int) -> None:
    """Print the flame-graph-style attribution and diagnosis for one run."""
    elapsed_ns = int(elapsed * 1e9)
    pts_per_s = total_points / elapsed

    df_handoff = stats["df_handoff_ns"]
    extract_ts = stats["extract_ts_ns"]
    column_build = stats["column_build_ns"]
    enqueue_batch = stats["enqueue_batch_ns"]
    consumer = stats["consumer_consume_ns"]
    consumer_calls = stats["consumer_consume_calls"]
    wdf_calls = stats["write_dataframe_calls"]
    wdf_total = stats["write_dataframe_total_ns"]
    producer_cpu = df_handoff + extract_ts + column_build

    def row(label: str, ns: int) -> None:
        pct = 100 * ns / elapsed_ns
        print(f"  {label:<26}{fmt_ns(ns):>10}{pct:>8.1f}%  {bar(pct)}")

    print("=== Pipeline attribution ===")
    print(f"wall_clock:              {fmt_ns(elapsed_ns):>10}  100.0%   ({pts_per_s:,.0f} pts/s)")
    print(
        f"write_dataframe total:   {fmt_ns(wdf_total):>10}  {100 * wdf_total / elapsed_ns:>5.1f}%   "
        f"({wdf_calls} calls, avg {fmt_ns(wdf_total // max(wdf_calls, 1))})"
    )
    print()
    print("Producer CPU phases (inside write_dataframe, caller thread):")
    row("df_handoff (FFI)", df_handoff)
    row("extract timestamps", extract_ts)
    row("column_build loop", column_build)
    row("  → producer CPU total", producer_cpu)
    print()
    print("Producer wall inside stream (includes blocked-on-buffer):")
    row("enqueue_batch (prod wall)", enqueue_batch)
    print()
    print("Consumer (parallel, single dispatcher thread):")
    row("consumer.consume total", consumer)
    print(f"    ({consumer_calls} consume calls; avg {fmt_ns(consumer // max(consumer_calls, 1))} per WriteRequest)")
    print()

    consumer_pct = 100 * consumer / elapsed_ns
    unaccounted = elapsed_ns - (producer_cpu + enqueue_batch)
    print("=== Diagnosis ===")
    print(
        f"producer_cpu + enqueue_batch = {fmt_ns(producer_cpu + enqueue_batch)} "
        f"({100 * (producer_cpu + enqueue_batch) / elapsed_ns:.1f}% of wall)"
    )
    print(f"unaccounted (GIL / Python loop / misc): {fmt_ns(unaccounted)} ({100 * unaccounted / elapsed_ns:.1f}%)")
    saturation = "SATURATED — bottleneck" if consumer_pct > 90 else f"idle {round(100 - consumer_pct, 1)}% of wall"
    print(f"consumer utilization: {consumer_pct:.1f}%  ({saturation})")
    if consumer_pct > 90:
        print("→ Consumer (avro encode + snappy + write) is the bottleneck; producer is backpressured.")
    elif producer_cpu > enqueue_batch * 0.5:
        print("→ Producer CPU is significant; consider bigger frames to amortize FFI.")
    else:
        print("→ Consumer has headroom; producer is the limiter.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cols", type=int, default=1000)
    parser.add_argument("--total-points", type=int, default=200_000_000)
    parser.add_argument("--frame-points", type=int, default=1_000_000)
    parser.add_argument("--pool-rows", type=int, default=50_000)
    parser.add_argument("--max-points-per-batch", type=int, default=1_000_000)
    args = parser.parse_args()

    rows_per_frame = args.frame_points // args.cols
    num_frames = args.total_points // args.frame_points
    print(
        f"Config: {args.cols} cols × {args.total_points:,} points × {args.frame_points:,} frame_points "
        f"= {num_frames} frames × {rows_per_frame} rows/frame"
    )
    print(f"Building pool ({args.pool_rows} rows × {args.cols} cols)...")
    pool = build_pool(args.cols, args.pool_rows)
    print("Pool built.\n")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        elapsed, stats = run(pool, rows_per_frame, num_frames, tmp_path, args.max_points_per_batch)

    print_report(elapsed, stats, args.total_points)
    return 0


if __name__ == "__main__":
    sys.exit(main())
