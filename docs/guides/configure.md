# Configure a stream

{.lead}
Stream options set where points go, how they're batched, and when the stream applies backpressure. The stream can also write points to files, and log what goes wrong.

The Rust examples use `token` and `dataset` as created in the [quickstart](../index.md#2-stream-to-a-dataset).

## Stream options

A stream sends a request when it has `max_points_per_record` points (`max_points_per_batch` in Python), or when its oldest point has waited `max_request_delay`, whichever comes first. When `max_buffered_requests` requests are waiting to be sent, writes block until one is sent.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use std::time::Duration;

use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};

let opts = NominalStreamOpts::default()
    .with_base_api_url("https://api.gov.nominal.io/api") // your Nominal REST API URL
    .with_max_points_per_record(100_000)                 // default 250,000
    .with_max_request_delay(Duration::from_millis(250))  // default 100 ms
    .with_max_buffered_requests(4)                       // default 4
    .with_request_dispatcher_tasks(8);                   // requests sent at once; default 8

let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_options(opts)
    .build();
```
:::

:::{tab-item} Python
:sync: python

```python
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

opts = PyNominalStreamOpts(
    base_api_url="https://api.gov.nominal.io/api",  # your Nominal REST API URL
    max_points_per_batch=100_000,                   # default 250,000
    max_request_delay_secs=0.25,                    # default 0.1
    max_buffered_requests=4,                        # default 4
    num_upload_workers=8,                           # requests sent at once; default 8
)
stream = NominalDatasetStream(auth_header="<api key>", opts=opts)  # DO NOT COMMIT THIS
```
:::
::::

To find your REST API URL, click the **Settings** icon (a gear) at the bottom of Nominal's sidebar, then **API keys**, and click **COPY URL** beside **API URL**.

## Write to files

A fallback file gets only the points that couldn't be sent. The file is created when a request first fails, with the stream's start time (UTC) added to its name: `fallback.avro` becomes `fallback_20260101_120000.avro`, so each run gets its own file.

To keep a copy of everything, or to stream with no network at all, write to a file instead:

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
// Send to Nominal and write every point to a file
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .stream_to_file("my_data.avro")
    .build();
```

Leave out `stream_to_core` to write only to the file.
:::

:::{tab-item} Python
:sync: python

```python
import pathlib

# Write only to a file; no API key needed
stream = NominalDatasetStream().to_file(pathlib.Path("my_data.avro"))
```
:::
::::

Avro has no unsigned 64-bit integers, so `u64` values are written as signed 64-bit integers.

## Log errors

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .enable_logging()
    .build();
```
:::

:::{tab-item} Python
:sync: python

```python
stream = (
    NominalDatasetStream(auth_header="<api key>")  # DO NOT COMMIT THIS
    .enable_logging("info")  # debug by default
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
)
```
:::
::::
