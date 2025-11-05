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
