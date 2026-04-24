#!/usr/bin/env python3
r"""1D or 2D sweep over (cols, frame_points) for NominalAvroWriter.

Prints a throughput matrix + consumer-utilization matrix so you can see how
the pipeline shape shifts across workloads. Both axes default to a small
characterization grid; pass `--cols N1 N2 ...` / `--frame-points N1 N2 ...`
to customize.

Pool size is chosen per-cell to cap memory at ~400MB.

Example:
    # Default 2D sweep (cols × frame_points)
    uv run python py-nominal-streaming/benchmarks/sweep.py

    # Just a cols sweep at one frame size
    uv run python py-nominal-streaming/benchmarks/sweep.py \\
        --cols 10 100 1000 --frame-points 1_000_000
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

DEFAULT_COLS = [10, 100, 1000, 5000]
DEFAULT_FRAME_POINTS = [250_000, 1_000_000, 5_000_000]
POOL_MEM_CAP_BYTES = 400 * 1024 * 1024  # 400MB


def build_pool(n_cols: int, pool_rows: int) -> pl.DataFrame:
    ts = pl.Series("ts", np.arange(pool_rows, dtype=np.int64))
    cols = {"ts": ts}
    data = np.random.default_rng(0).random((pool_rows, n_cols), dtype=np.float64)
    for c in range(n_cols):
        cols[f"c{c}"] = pl.Series(f"c{c}", data[:, c])
    return pl.DataFrame(cols)


def pick_pool_rows(n_cols: int, rows_per_frame: int) -> int:
    target = max(rows_per_frame * 10, 10_000)
    mem_cap_rows = POOL_MEM_CAP_BYTES // (n_cols * 8)
    rows = min(target, mem_cap_rows)
    return max(rows, rows_per_frame)


def run_bench(
    pool: pl.DataFrame, n_cols: int, rows_per_frame: int, total_points: int, out_dir: pathlib.Path
) -> tuple[float, dict[str, int]]:
    num_frames = total_points // (rows_per_frame * n_cols)
    max_offset = max(pool.height - rows_per_frame + 1, 1)
    opts = NominalAvroWriterOpts(
        max_points_per_batch=1_000_000,
        fsync_on_close=True,
        max_points_per_file=0,
    )
    base = out_dir / f"sweep_{n_cols}_{rows_per_frame}.avro"
    start = time.monotonic()
    with NominalAvroWriter(base, opts) as w:
        for i in range(num_frames):
            offset = (i * rows_per_frame) % max_offset
            w.write_dataframe(pool.slice(offset, rows_per_frame), timestamp_column="ts")
        stats = w.stats()
    return time.monotonic() - start, stats


def print_table(title: str, grid: dict, n_cols_grid: list[int], frame_grid: list[int], fmt: str) -> None:
    print(f"\n{title}")
    header = "cols \\ frame_pts " + "".join(f"{f:>14,}" for f in frame_grid)
    print(header)
    print("-" * len(header))
    for c in n_cols_grid:
        row = f"{c:>7,}          "
        for f in frame_grid:
            v = grid.get((c, f))
            row += f"{v:>13{fmt}} "
        print(row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cols", nargs="+", type=int, default=DEFAULT_COLS)
    parser.add_argument("--frame-points", nargs="+", type=int, default=DEFAULT_FRAME_POINTS)
    parser.add_argument("--total-points", type=int, default=200_000_000)
    args = parser.parse_args()

    print(f"Sweep: total_points={args.total_points:,}")
    print(f"  cols:     {args.cols}")
    print(f"  frame_pt: {[f'{f:,}' for f in args.frame_points]}\n")

    throughput: dict[tuple[int, int], float] = {}
    cons_util: dict[tuple[int, int], float] = {}
    elapsed_s: dict[tuple[int, int], float] = {}

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        for n_cols in args.cols:
            for fp in args.frame_points:
                rows_per_frame = fp // n_cols
                if rows_per_frame < 1:
                    print(f"  SKIP n_cols={n_cols}, frame_pts={fp:,}: rows/frame < 1")
                    continue
                pool_rows = pick_pool_rows(n_cols, rows_per_frame)
                pool = build_pool(n_cols, pool_rows)
                print(
                    f"  running n_cols={n_cols:>5}, frame_pts={fp:>9,} "
                    f"(rows/frm={rows_per_frame:,}, pool_rows={pool_rows:,})...",
                    flush=True,
                )
                elapsed, stats = run_bench(pool, n_cols, rows_per_frame, args.total_points, tmp_path)
                pts_per_s = args.total_points / elapsed
                consumer = stats["consumer_consume_ns"] / 1e9
                cu = 100 * consumer / elapsed

                throughput[(n_cols, fp)] = pts_per_s
                cons_util[(n_cols, fp)] = cu
                elapsed_s[(n_cols, fp)] = elapsed

                print(f"    elapsed={elapsed:.2f}s  pts/s={pts_per_s / 1e6:.1f}M  cons_util={cu:.1f}%")
                del pool

    throughput_m = {k: v / 1e6 for k, v in throughput.items()}

    print_table("=== Throughput (M pts/s) ===", throughput_m, args.cols, args.frame_points, ".1f")
    print_table("=== Consumer utilization (% of wall) ===", cons_util, args.cols, args.frame_points, ".1f")
    print_table("=== Wall elapsed (seconds) ===", elapsed_s, args.cols, args.frame_points, ".2f")

    if throughput:
        best = max(throughput, key=throughput.get)
        worst = min(throughput, key=throughput.get)
        print()
        print(f"Best:  cols={best[0]:,}  frame_pts={best[1]:,}  →  {throughput[best] / 1e6:.1f} M pts/s")
        print(f"Worst: cols={worst[0]:,}  frame_pts={worst[1]:,}  →  {throughput[worst] / 1e6:.1f} M pts/s")
        print(f"Range: {throughput[best] / throughput[worst]:.2f}×")

    return 0


if __name__ == "__main__":
    sys.exit(main())
