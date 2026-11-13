# Configure a stream

{.lead}
Stream options set where points go, how they're batched, and when writes wait for the network. This guide also covers how a stream reports errors, and how to close it.

The Rust examples use `token` and `dataset` as created in the [quickstart](../index.md#4-stream-values).

## Stream options

| Rust (`NominalStreamOpts`) | Python (`PyNominalStreamOpts`) | Default | Sets |
| --- | --- | --- | --- |
| `with_base_api_url` | `base_api_url` | `https://api.gov.nominal.io/api` | Your Nominal REST API URL: the base URL from [API Keys Setup](api-keys-setup.md) |
| `with_max_points_per_record` | `max_points_per_batch` | 250,000 | The most points in one request |
| `with_max_request_delay` | `max_request_delay_secs` | 100 ms | How long points wait for a fuller request before it's sent anyway |
| `with_max_buffered_requests` | `max_buffered_requests` | 4 | How many requests can wait to be sent before writes wait too |
| `with_request_dispatcher_tasks` | `num_upload_workers` | 8 | How many requests are sent at once |
| | `num_runtime_workers` | 8 | Threads the stream runs on; at least `num_upload_workers` |
| `with_track_metrics` | `track_metrics` | off | [Metric channels](#metrics) about the stream's latency |

Smaller requests and a shorter delay get points to Nominal sooner. Larger requests send fewer of them. When the network can't keep up, the buffered requests fill, and writes wait until one is sent, so your program slows down rather than running out of memory.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use std::time::Duration;

use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};

let opts = NominalStreamOpts::default()
    .with_base_api_url("<base url>")
    .with_max_points_per_record(100_000)
    .with_max_request_delay(Duration::from_millis(250));

let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_options(opts)
    .with_file_fallback("fallback.avro")
    .build();
```
:::

:::{tab-item} Python
:sync: python

```python
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

opts = PyNominalStreamOpts(
    base_api_url="<base url>",
    max_points_per_batch=100_000,
    max_request_delay_secs=0.25,
)
stream = NominalDatasetStream(auth_header="<api key>", opts=opts)  # DO NOT COMMIT THIS
```

`NominalDatasetStream.create` takes the API key, the base URL and the same options as arguments, in place of a `PyNominalStreamOpts`.
:::
::::

## Metrics

With metrics on, the stream adds these channels to the dataset, in the requests it already sends. Each point's timestamp is when its request finished, and its value is in seconds.

| Channel | Value |
| --- | --- |
| `__nominal.metric.request_rtt` | How long the request took, including retries |
| `__nominal.metric.largest_latency_before_request` | Time from the request's oldest point to when it was sent |
| `__nominal.metric.smallest_latency_before_request` | Time from the request's newest point to when it was sent |
| `__nominal.metric.largest_latency_after_request` | Time from the request's oldest point to when it finished |
| `__nominal.metric.smallest_latency_after_request` | Time from the request's newest point to when it finished |

In Python, `enqueue_from_dict` also writes `enque_dict_start_staleness` and `enque_dict_end_staleness`: the time from the points' timestamp to when the call started and finished.

## Handle errors

A stream retries failed requests. A request that still fails goes to the fallback file, if there is one, and is otherwise dropped. Your writes don't return the error, so turn on logging to see it:

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

`enable_logging` prints the stream's log messages, from debug level up, unless the `RUST_LOG` environment variable sets another filter. `enable_logging_with_directive` takes the filter, such as `"warn"`.

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_file_fallback("fallback.avro")
    .enable_logging()
    .build();
```

To act on failures in your program, add a listener. It hears about each request after the stream has tried the fallback file, so with a fallback file, a request saved there counts as a success:

```rust
use std::error::Error;
use std::sync::Arc;

use nominal_streaming::listener::NominalStreamListener;

#[derive(Debug)]
struct ReportFailures;

impl NominalStreamListener for ReportFailures {
    fn on_error(&self, error: &dyn Error, _request: &WriteRequestNominal) {
        eprintln!("points were lost: {error}");
    }
}

let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .add_listener(Arc::new(ReportFailures))
    .build();
```

`build` panics if the stream can't start, such as when it has no target or can't create its file. `try_build` returns those as a `StreamBuildError` instead.
:::

:::{tab-item} Python
:sync: python

`enable_logging` prints the stream's messages at the level you give it: `"debug"` by default, or `"info"`, `"warn"` or `"error"`.

```python
stream = (
    NominalDatasetStream(auth_header="<api key>")  # DO NOT COMMIT THIS
    .enable_logging("warn")
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
)
```
:::
::::

## Close the stream

Closing a stream sends what it has buffered, and waits until every point has been sent or saved to the fallback file.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

Drop each writer, then the stream. Dropping the stream blocks its thread until the points are flushed, with no time limit.

```rust
drop(rpm);
drop(stream);
```
:::

:::{tab-item} Python
:sync: python

Leaving the `with` block closes the stream. Without `with`, call `open()` before writing and `close()` when done.

When you press Ctrl+C in a stream opened from the main thread, the stream stops taking writes, sends what it has, and raises `KeyboardInterrupt`.
:::
::::

## Write to a file

A stream can also write to a file, as well as or instead of Nominal. See [Stream files](files.md).
