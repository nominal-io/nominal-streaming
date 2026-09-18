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
