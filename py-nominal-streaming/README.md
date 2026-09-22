# nominal-streaming Python Bindings

`nominal-streaming` is a thin python wrapper around the existing [nominal-streaming rust crate](https://crates.io/crates/nominal-streaming).
Usage semantics remain largely the same, but with some slight alterations to allow for a more pythonic interface.

The library aims to balance three concerns:

1. Data should exist in-memory only for a limited, configurable amount of time before it's sent to Core.
1. Writes should fall back to disk if there are network failures.
1. Backpressure should be applied to incoming requests when network throughput is saturated.

This library streams data to Nominal Core, to a file, or to Nominal Core with a file as backup (recommended to protect against network failures).
It also provides configuration to manage the tradeoff between above listed concerns.

> [!WARNING]
> This library is still under active development and may make breaking changes.

## Usage example: streaming from memory to Nominal Core with file fallback

```python
import pathlib
import time

from nominal.core import NominalClient
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

if __name__ == "__main__":
    num_points = 100_000
    stream = (
        NominalDatasetStream(
            auth_header="<api key>",
            opts=PyNominalStreamOpts(),
        )
        .enable_logging("info") # can set debug, warn, etc.
        .with_core_consumer("<dataset rid>")
        .with_file_fallback(pathlib.Path("local_fallback.avro"))
    )

    with stream:
        # Stream 100_000 live readings (made up values)
        for idx in range(num_points):
            time_ns = int(time.time() * 1e9)
            value = (idx % 50) + 0.5
            stream.enqueue("channel_name", time_ns, value, tags={"tag_key": "tag_value"})

        # Stream 100_000 points in one batch
        start_time = int(time.time() * 1e9)
        timestamp_offsets = int(1e9 / 1600)
        timestamps = [start_time + timestamp_offsets * idx for idx in range(num_points)]
        values = [(idx % 50) + 0.5 for idx in range(num_points)]
        stream.enqueue_batch(
            "channel_name",
            timestamps,
            values,
            tags={"tag_key": "tag_value"}
        )
```

### Runtime metrics

Enable `PyNominalStreamOpts(track_metrics=True)` (or pass `track_metrics=True` to
`NominalDatasetStream.create`) to emit dictionary enqueue staleness and Core
request latency metrics. Metrics are disabled by default.

## NumPy batches

NumPy is optional: install `nominal-streaming[numpy]`, or use the NumPy installation
already provided by your application. Pass arrays directly to `enqueue_batch`:

```python
import numpy as np

timestamps = np.array([1_700_000_000_000_000_000, 1_700_000_000_000_000_001], dtype="uint64")
values = np.array([1.25, 2.5], dtype="float64")
stream.enqueue_batch("temperature", timestamps, values)
```

Python 3.11+ wheels copy native-endian, aligned, one-dimensional NumPy arrays into
Rust-owned memory without creating a Python object for every element. The fast path
supports `int64`/`uint64` timestamps and `float32`/`float64`/`int64`/`uint64` values,
including strided, reversed, and read-only views. Integer values retain the API's
existing conversion to doubles. Inputs can be changed or freed after the call returns.

Other dtypes, unaligned or non-native-endian arrays, and ndarray subclasses use the
existing element-wise conversion. Masked values and datetime/timedelta values still
require explicit conversion. Lists and tuples continue to work without NumPy.

Python 3.10 wheels retain the element-wise path and emit one `RuntimeWarning` per
process on the first array batch. On that build, `.tolist()` can be faster; on an
accelerated build, pass supported arrays directly. Python 3.11+ installers prefer
the accelerated wheel when both variants are available.

### Building and testing the two wheels

Both variants use the same sources and public API. Build them separately:

```sh
maturin build --release -m py-nominal-streaming/Cargo.toml --no-default-features --features python310
maturin build --release -m py-nominal-streaming/Cargo.toml --no-default-features --features python311
```

The output tags are `cp310-abi3` and `cp311-abi3`, respectively. The default source
build targets Python 3.10. Cargo features are additive: enabling both selects the
older ABI and disables buffer acceleration, so use `--no-default-features` for the
Python 3.11 variant. NumPy is never needed to import the library or use lists.

CI installs each built wheel in a clean environment, checks its compiled
capability, exercises lists without NumPy, and runs the batch regression tests with
NumPy on the minimum and newer Python runtimes. To run tests locally after installing
a wheel and the `test` dependency group:

```sh
python -m unittest discover -s py-nominal-streaming/tests -v
```

### Measuring enqueue performance

With a wheel and the `test` dependencies installed:

```sh
python py-nominal-streaming/benchmarks/enqueue_batch.py
```

This compares direct arrays, pre-existing lists, and `.tolist()` plus the enqueue
call. It reports all samples and medians and reads the local Avro output to verify
point counts and value checksums. It measures the complete public enqueue call with
buffer capacity available; startup and draining are excluded. It does **not** measure
network throughput. Downstream serialization, compression, and upload backpressure
can limit the overall streaming improvement.
## Log streams

`NominalLogStream` sends timestamped string messages and per-record string arguments to
an existing dataset's log channel. It uses bounded native buffering: enqueue blocks
when the byte budget is full, while releasing the Python GIL. Individual writes are
automatically batched into requests by Rust.

```python
import os
from pathlib import Path
from nominal_streaming import NominalLogStream, PyNominalLogStreamOpts

opts = PyNominalLogStreamOpts(max_buffered_bytes=64 * 1024 * 1024)
with (NominalLogStream(os.environ["NOMINAL_TOKEN"], opts)
      .with_core_consumer(os.environ["NOMINAL_DATASET_RID"])
      .with_file_fallback(Path("log-backup"))) as stream:
    stream.enqueue("engine", "2026-09-14T12:00:00.123456789Z", "started",
                   args={"engine": "left"})
    stream.enqueue("engine", 1_800_000_000_000_000_001, "running",
                   args={"engine": "left", "phase": "test"})
    stream.enqueue("engine", 1_800_000_000_000_000_002, "stopped",
                   args={"engine": "left", "phase": "done"})
    print(stream.flush().acknowledged_records)
```

For local-only recording, use `with NominalLogStream().to_file(Path("logs")) as stream:`.
The destination is a directory of per-channel JSONL journals plus manifests with explicit
nanosecond timestamp metadata, not a telemetry Avro file. Journal field names reserved
for timestamp and message cannot also be argument keys. File preservation increments
`backed_up_records`, not `acknowledged_records`.

Integer timestamps are signed Unix nanoseconds. Aware `datetime` values use exact
integer arithmetic. ISO 8601 strings require a timezone and support up to nine fractional
digits; ambiguous dates and naive datetimes are rejected. `tags` is an alias for `args`;
passing both is an error. All messages and argument keys/values must be strings.

`close()` refuses new writes, drains all accepted records, and reports delivery failures.
`close(wait=False)` starts graceful background draining; a later `close()` waits and
reports its result. `stats()` separates acceptance, acknowledgement, backup, and failure.
Use explicit close or a context manager to observe failures before process exit. No global
signal handlers are installed, and streams may be used from worker threads.

Run file-only native integration tests after installing the wheel:

```shell
python -m unittest discover -s py-nominal-streaming/tests -v
```

If a journal write also fails, accepted batches remain in memory while the stream
object is alive. After fixing disk access, call `stream.save_failed(Path("recovered-logs"))`
and then `stream.close()`. Recovery may produce duplicate segments after a partial disk
write; inspect the manifests before importing recovered files.

Log options follow `PyNominalStreamOpts`: keyword configuration, read-only properties,
fluent `with_*` setters, and a readable `repr`. Streams copy their options when configured.

```python
opts = (
    PyNominalLogStreamOpts()
    .with_max_request_bytes(8 * 1024 * 1024)
    .with_num_upload_workers(4)
    .with_num_runtime_workers(2)
)
stream = NominalLogStream().with_options(opts).enable_logging("info")
```

Configure the destination before opening the stream. Runtime workers drive asynchronous
HTTP I/O; upload workers perform compression and dispatch. Logs default to two runtime
workers and four upload workers. Runtime workers must be positive but need not match the
upload count. Configuration is frozen once opened; use a new stream to change it.
