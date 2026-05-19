#!/usr/bin/env python3
r"""Benchmark NominalAvroWriter across (dtype × n_cols × total_points × frame_points).

Reports elapsed write+close time, total bytes on disk, file count, and points/sec.
Writes results incrementally to CSV so partial output survives interrupts.

Anti-compression-cheating: each config pre-builds a pool DataFrame, and each
iteration slices a different offset from it. Reusing a single sample frame
lets snappy compress repetitions across iterations and produces misleading
bytes/point numbers.

Tuning:
    * ``--max-points-per-batch`` default 1,000,000 (higher than the library
      default of 250k — fewer encoder-side emit cycles).
    * ``--rotate-every`` default 1,000,000,000 (effectively never rotate within
      one bench config). Pass a smaller value to measure rotation overhead.
    * fsync on close is ON by default; toggle with ``--no-fsync``.

Python version note:
    Throughput is meaningfully higher on CPython 3.12+ than on 3.10
    (PyO3 boundary crossings + polars calls are both faster in 3.12).
    Pin the Python version when comparing runs.

Example:
    # Quick smoke run
    uv run python py-nominal-streaming/benchmarks/bench_nominal_avro_writer.py \\
        --dtypes float string --cols 10 100 \\
        --total-points 1_000_000 --frame-points 100_000

    # Full sweep to CSV
    uv run python py-nominal-streaming/benchmarks/bench_nominal_avro_writer.py \\
        --out bench_full.csv
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass

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

DTYPES = ("float", "string", "array", "struct")
COL_COUNTS = (10, 100, 1000)
TOTAL_POINTS = (1_000_000, 10_000_000, 100_000_000, 500_000_000)
FRAME_POINTS = (10_000, 50_000, 100_000, 1_000_000)

_ARRAY_LEN = 8
_STRUCT_SCHEMA = pl.Struct([pl.Field("a", pl.Int64), pl.Field("b", pl.Float64)])


def build_pool(dtype: str, pool_rows: int, n_cols: int) -> pl.DataFrame:
    """Build a pool DataFrame; iterations slice distinct windows from it."""
    ts = pl.Series("ts", np.arange(pool_rows, dtype=np.int64))
    cols: dict[str, pl.Series] = {"ts": ts}

    if dtype == "float":
        data = np.random.default_rng(0).random((pool_rows, n_cols), dtype=np.float64)
        for c in range(n_cols):
            cols[f"c{c}"] = pl.Series(f"c{c}", data[:, c])
    elif dtype == "string":
        sample = [f"v{i}" for i in range(pool_rows)]
        for c in range(n_cols):
            cols[f"c{c}"] = pl.Series(f"c{c}", sample, dtype=pl.String)
    elif dtype == "array":
        sample = [[float(i + k) for k in range(_ARRAY_LEN)] for i in range(pool_rows)]
        for c in range(n_cols):
            cols[f"c{c}"] = pl.Series(f"c{c}", sample, dtype=pl.List(pl.Float64))
    elif dtype == "struct":
        sample = [{"a": i, "b": float(i)} for i in range(pool_rows)]
        for c in range(n_cols):
            cols[f"c{c}"] = pl.Series(f"c{c}", sample, dtype=_STRUCT_SCHEMA)
    else:
        raise ValueError(f"unknown dtype: {dtype}")

    return pl.DataFrame(cols)


def _pool_rows_for(dtype: str, n_cols: int, rows_per_frame: int, budget_mib: int) -> int:
    """Pick a pool size under ``budget_mib`` while giving each iteration a distinct slice."""
    per_cell = {"float": 8, "string": 24, "array": 80, "struct": 96}[dtype]
    max_rows = max(1, (budget_mib * 1024 * 1024) // (n_cols * per_cell))
    return max(rows_per_frame, min(rows_per_frame * 10, max_rows))


@dataclass
class Result:
    dtype: str
    n_cols: int
    total_points: int
    frame_points: int
    elapsed_s: float
    total_bytes: int
    n_files: int

    @property
    def mib(self) -> float:
        return self.total_bytes / (1024 * 1024)

    @property
    def points_per_s(self) -> float:
        return self.total_points / self.elapsed_s if self.elapsed_s > 0 else float("inf")


def run_one(
    dtype: str,
    n_cols: int,
    total_points: int,
    frame_points: int,
    out_root: pathlib.Path,
    opts: NominalAvroWriterOpts,
    rotate_every: int,
    pool_budget_mib: int,
) -> Result | None:
    rows_per_frame = frame_points // n_cols
    if rows_per_frame < 1:
        return None
    num_frames = total_points // frame_points
    if num_frames < 1:
        return None

    # Pre-build a pool outside the timed section; each iteration slices a
    # different offset so snappy can't cheat by compressing identical frames.
    pool_rows = _pool_rows_for(dtype, n_cols, rows_per_frame, pool_budget_mib)
    pool = build_pool(dtype, pool_rows, n_cols)
    max_offset = pool_rows - rows_per_frame + 1  # > 0 since pool_rows >= rows_per_frame

    out_dir = out_root / f"{dtype}_c{n_cols}_t{total_points}_f{frame_points}"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "bench.avro"

    run_opts = opts.with_max_points_per_file(rotate_every)
    start = time.monotonic()
    writer = NominalAvroWriter(base, run_opts)
    try:
        for i in range(num_frames):
            offset = (i * rows_per_frame) % max_offset
            frame = pool.slice(offset, rows_per_frame)
            writer.write_dataframe(frame, timestamp_column="ts")
    finally:
        files = list(writer.close())  # returns all paths after shutdown
    elapsed = time.monotonic() - start

    total_bytes = sum(pathlib.Path(p).stat().st_size for p in files)
    n_files = len(files)
    shutil.rmtree(out_dir)

    return Result(dtype, n_cols, total_points, frame_points, elapsed, total_bytes, n_files)


class _ExistingDir:
    """Context-manager shim so args.workdir and tempfile.TemporaryDirectory share a call site."""

    def __init__(self, path: pathlib.Path) -> None:
        self._path = path

    def __enter__(self) -> str:
        self._path.mkdir(parents=True, exist_ok=True)
        return str(self._path)

    def __exit__(self, *_: object) -> None:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("nominal_avro_writer_bench.csv"))
    parser.add_argument("--dtypes", nargs="+", choices=DTYPES, default=DTYPES)
    parser.add_argument("--cols", nargs="+", type=int, default=COL_COUNTS)
    parser.add_argument("--total-points", nargs="+", type=int, default=TOTAL_POINTS)
    parser.add_argument("--frame-points", nargs="+", type=int, default=FRAME_POINTS)
    parser.add_argument(
        "--max-points-per-batch",
        type=int,
        default=1_000_000,
        help="NominalAvroWriterOpts.max_points_per_batch (default: 1M; library default is 250k).",
    )
    parser.add_argument("--max-batch-delay-secs", type=float, default=0.1)
    parser.add_argument(
        "--rotate-every",
        type=int,
        default=1_000_000_000,
        help="Rotate to a new file after this many points. Default 1B = effectively never.",
    )
    parser.add_argument(
        "--pool-budget-mib", type=int, default=256, help="Memory budget per config for the source pool."
    )
    parser.add_argument("--no-fsync", action="store_true")
    parser.add_argument("--workdir", type=pathlib.Path, default=None)
    args = parser.parse_args()

    opts = NominalAvroWriterOpts(
        max_points_per_batch=args.max_points_per_batch,
        max_batch_delay_secs=args.max_batch_delay_secs,
        fsync_on_close=not args.no_fsync,
    )

    print(
        f"Writing results to {args.out}  "
        f"(opts: max_points_per_batch={args.max_points_per_batch:,}, "
        f"max_batch_delay_secs={args.max_batch_delay_secs:g}, "
        f"fsync_on_close={not args.no_fsync}, "
        f"rotate_every={args.rotate_every:,})"
    )

    header = [
        "dtype",
        "n_cols",
        "total_points",
        "frame_points",
        "elapsed_s",
        "total_bytes",
        "total_mib",
        "n_files",
        "points_per_s",
    ]

    tmp_ctx = tempfile.TemporaryDirectory() if args.workdir is None else _ExistingDir(args.workdir)
    with tmp_ctx as tmp, open(args.out, "w", newline="") as f:
        tmp_root = pathlib.Path(tmp)
        writer = csv.writer(f)
        writer.writerow(header)
        f.flush()

        for dtype in args.dtypes:
            for n_cols in args.cols:
                for total in args.total_points:
                    for frame in args.frame_points:
                        label = f"{dtype:6} cols={n_cols:<5} total={total:>12,} frame={frame:>10,}"
                        r = run_one(
                            dtype,
                            n_cols,
                            total,
                            frame,
                            tmp_root,
                            opts=opts,
                            rotate_every=args.rotate_every,
                            pool_budget_mib=args.pool_budget_mib,
                        )
                        if r is None:
                            print(f"SKIP {label}: infeasible shape")
                            continue
                        writer.writerow(
                            [
                                r.dtype,
                                r.n_cols,
                                r.total_points,
                                r.frame_points,
                                f"{r.elapsed_s:.3f}",
                                r.total_bytes,
                                f"{r.mib:.2f}",
                                r.n_files,
                                f"{r.points_per_s:.0f}",
                            ]
                        )
                        f.flush()
                        print(
                            f"{label}  elapsed={r.elapsed_s:8.2f}s "
                            f"size={r.mib:9.1f}MiB files={r.n_files:4} "
                            f"pts/s={r.points_per_s:>13,.0f}"
                        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
