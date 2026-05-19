#!/usr/bin/env python3
r"""Head-to-head benchmark: NominalAvroWriter vs. PyNominalDatasetStream.to_file.

Compares the two ways a Python caller can write an avro file with this crate:

  1. **Old idiom** — build a PyNominalDatasetStream, point it at a file via
     ``.to_file(path)``, then call ``enqueue_batch(channel, ts_list, values)``
     once per column per frame. This is the pattern you'd write before this
     branch landed — one PyO3 FFI crossing per column per frame, and per-column
     Python list materialization via ``Series.to_list()``.

  2. **New idiom** — ``NominalAvroWriter(path).write_dataframe(df, "ts")``.
     One FFI crossing per frame; the per-column iteration runs Rust-side with
     the GIL released.

Both paths write the same avro schema to the same on-disk format. The output
files should be byte-identical modulo non-determinism from snappy + the
per-file ingest_rid tag (neither of which we generate here).

Run with Python 3.12+ (the guard will fail loud otherwise).

Example::

    uv run --python 3.12 python py-nominal-streaming/benchmarks/head_to_head.py

    # Custom shape:
    uv run --python 3.12 python py-nominal-streaming/benchmarks/head_to_head.py \
        --cols 1000 --total-points 200_000_000 --frame-points 1_000_000
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import tempfile
import time

if sys.version_info < (3, 12):
    sys.exit(
        f"benchmarks require Python 3.12+ (got {sys.version_info.major}.{sys.version_info.minor}). "
        f"Re-run under 3.12+, e.g. `uv run --python 3.12 python {pathlib.Path(__file__).name}`."
    )

import numpy as np
import polars as pl
from nominal_streaming import (
    NominalAvroWriter,
    NominalAvroWriterOpts,
    PyNominalStreamOpts,
)
from nominal_streaming._nominal_streaming import PyNominalDatasetStream


def build_pool(n_cols: int, pool_rows: int) -> pl.DataFrame:
    ts = pl.Series("ts", np.arange(pool_rows, dtype=np.int64))
    cols: dict[str, pl.Series] = {"ts": ts}
    data = np.random.default_rng(0).random((pool_rows, n_cols), dtype=np.float64)
    for c in range(n_cols):
        cols[f"c{c}"] = pl.Series(f"c{c}", data[:, c])
    return pl.DataFrame(cols)


def run_old(
    pool: pl.DataFrame, rows_per_frame: int, num_frames: int, path: pathlib.Path, max_points_per_batch: int
) -> float:
    """Old idiom: PyNominalDatasetStream + per-column enqueue_batch."""
    opts = PyNominalStreamOpts(
        max_points_per_batch=max_points_per_batch,
        max_request_delay_secs=0.1,
    )
    stream = PyNominalDatasetStream(opts).to_file(path)
    stream.open()
    max_offset = max(pool.height - rows_per_frame + 1, 1)
    data_cols = [c for c in pool.columns if c != "ts"]
    start = time.monotonic()
    try:
        for i in range(num_frames):
            offset = (i * rows_per_frame) % max_offset
            frame = pool.slice(offset, rows_per_frame)
            ts_list = frame["ts"].to_list()
            for col in data_cols:
                stream.enqueue_batch(col, ts_list, frame[col].to_list())
    finally:
        stream.close()
    return time.monotonic() - start


def run_new(
    pool: pl.DataFrame, rows_per_frame: int, num_frames: int, path: pathlib.Path, max_points_per_batch: int
) -> float:
    """New idiom: NominalAvroWriter.write_dataframe."""
    opts = NominalAvroWriterOpts(
        max_points_per_batch=max_points_per_batch,
        # Match the old-idiom baseline, which doesn't fsync on close — fair comparison.
        fsync_on_close=False,
        max_points_per_file=0,
    )
    max_offset = max(pool.height - rows_per_frame + 1, 1)
    start = time.monotonic()
    with NominalAvroWriter(path, opts) as w:
        for i in range(num_frames):
            offset = (i * rows_per_frame) % max_offset
            w.write_dataframe(pool.slice(offset, rows_per_frame), timestamp_column="ts")
    return time.monotonic() - start


def run_pair(
    pool: pl.DataFrame, n_cols: int, total_points: int, frame_points: int, max_points_per_batch: int
) -> tuple[float, float]:
    rows_per_frame = frame_points // n_cols
    num_frames = total_points // frame_points
    if rows_per_frame < 1 or num_frames < 1:
        return (float("nan"), float("nan"))

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        t_old = run_old(pool, rows_per_frame, num_frames, tmp_path / "old.avro", max_points_per_batch)
        t_new = run_new(pool, rows_per_frame, num_frames, tmp_path / "new.avro", max_points_per_batch)
    return (t_old, t_new)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cols",
        nargs="+",
        type=int,
        default=[10, 100, 1000],
        help="Column counts to sweep. Default: 10, 100, 1000.",
    )
    parser.add_argument(
        "--frame-points",
        nargs="+",
        type=int,
        default=[100_000, 1_000_000],
        help="Frame sizes (points per write call). Default: 100K, 1M.",
    )
    parser.add_argument("--total-points", type=int, default=50_000_000)
    parser.add_argument("--pool-rows", type=int, default=50_000)
    parser.add_argument("--max-points-per-batch", type=int, default=1_000_000)
    args = parser.parse_args()

    print("Head-to-head: NominalAvroWriter vs PyNominalDatasetStream.to_file")
    print(f"  total_points={args.total_points:,}  pool_rows={args.pool_rows:,}")
    print(f"  cols={args.cols}  frame_points={[f'{f:,}' for f in args.frame_points]}\n")

    header = (
        f"{'cols':>5} {'frame_pts':>11} {'old wall':>10} {'old pts/s':>14} "
        f"{'new wall':>10} {'new pts/s':>14} {'speedup':>9}"
    )
    print(header)
    print("-" * len(header))

    for n_cols in args.cols:
        pool = build_pool(n_cols, args.pool_rows)
        for fp in args.frame_points:
            t_old, t_new = run_pair(pool, n_cols, args.total_points, fp, args.max_points_per_batch)
            old_rate = args.total_points / t_old if t_old > 0 else float("nan")
            new_rate = args.total_points / t_new if t_new > 0 else float("nan")
            speedup = t_old / t_new if t_new > 0 else float("nan")
            print(
                f"{n_cols:>5,} {fp:>11,} {t_old:>9.2f}s {old_rate:>14,.0f} "
                f"{t_new:>9.2f}s {new_rate:>14,.0f} {speedup:>8.2f}×"
            )
        del pool

    print()
    print("Legend:")
    print("  old idiom = PyNominalDatasetStream(opts).to_file(path) + per-column enqueue_batch.")
    print("  new idiom = NominalAvroWriter(path).write_dataframe(df, 'ts').")
    print("  speedup   = old_wall / new_wall.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
