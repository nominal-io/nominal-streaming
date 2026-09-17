"""Measure public enqueue_batch calls without upload backpressure; verify local output."""

import argparse
import json
import platform
import random
import statistics
import tempfile
import time
from pathlib import Path

import numpy as np
from fastavro import reader
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts
from nominal_streaming._nominal_streaming import _BUFFER_FAST_PATH


def measure(size: int, kind: str, total_points: int) -> float:
    """Return microseconds/call, excluding stream startup, drain, and output verification."""
    calls = max(1, total_points // size)
    timestamps = np.arange(size, dtype=np.uint64) + 1_700_000_000_000_000_000
    values = np.arange(size, dtype=np.float64) / 8
    timestamp_list, value_list = timestamps.tolist(), values.tolist()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "points.avro"
        opts = PyNominalStreamOpts(
            max_points_per_batch=(calls + 2) * size,
            max_request_delay_secs=0.5,
            num_upload_workers=1,
            num_runtime_workers=1,
        )
        with NominalDatasetStream(opts=opts).to_file(path) as stream:
            stream.enqueue_batch("channel", timestamps, values)
            if kind == "numpy":

                def enqueue():
                    stream.enqueue_batch("channel", timestamps, values)
            elif kind == "lists":

                def enqueue():
                    stream.enqueue_batch("channel", timestamp_list, value_list)
            else:

                def enqueue():
                    stream.enqueue_batch("channel", timestamps.tolist(), values.tolist())

            start = time.perf_counter_ns()
            for _ in range(calls):
                enqueue()
            elapsed = time.perf_counter_ns() - start
        count = 0
        checksum = 0.0
        with path.open("rb") as file:
            for record in reader(file):
                assert len(record["timestamps"]) == len(record["values"])
                count += len(record["values"])
                checksum += sum(record["values"])
        assert count == (calls + 1) * size
        assert checksum == sum(value_list) * (calls + 1)
    return elapsed / calls / 1_000


def main() -> None:
    """Benchmark the installed wheel, including .tolist() cost in that case."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--sizes", type=int, nargs="+", default=[1_000, 10_000, 100_000])
    parser.add_argument("--points-per-sample", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.rounds < 1 or args.points_per_sample < 1 or any(n < 1 for n in args.sizes):
        parser.error("rounds, sizes, and points-per-sample must be positive")
    samples = {(size, kind): [] for size in args.sizes for kind in ("numpy", "tolist", "lists")}
    for round_index in range(args.rounds):
        jobs = list(samples)
        random.Random(round_index).shuffle(jobs)
        for size, kind in jobs:
            samples[size, kind].append(measure(size, kind, args.points_per_sample))
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "numpy": np.__version__,
                "buffer_fast_path": _BUFFER_FAST_PATH,
                "results": [
                    {"points": size, "input": kind, "median_us": statistics.median(times), "samples_us": times}
                    for (size, kind), times in samples.items()
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
