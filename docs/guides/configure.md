# Stream configuration

{.lead}
Stream options set where points go, how they're batched, and when writes wait for the network. A stream logs the requests that fail, and sends what it has buffered when you close it.

## Stream options

Options go in a {rust:struct}`NominalStreamOpts` in Rust or a {py:class}`~nominal_streaming.PyNominalStreamOpts` in Python, which you pass to the stream when you create it:

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use std::time::Duration;

use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

// Load the API key and base URL that API Keys Setup saved
let config = nominal::Config::load()?;
let profile = config.get_profile("default").ok_or("no profile named default")?;
let token = BearerToken::new(profile.token())?;
let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>")?;

let opts = NominalStreamOpts::default()
    .with_base_api_url(profile.base_url())
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
from nominal.config import NominalConfig
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

# Load the API key and base URL that API Keys Setup saved
profile = NominalConfig.from_yaml().get_profile("default")

opts = PyNominalStreamOpts(
    base_api_url=profile.base_url,
    max_points_per_batch=100_000,
    max_request_delay_secs=0.25,
)
stream = NominalDatasetStream(auth_header=profile.token, opts=opts)

# ALTERNATE:
# Pass the options straight to create, with no PyNominalStreamOpts
stream = NominalDatasetStream.create(
    profile.token,
    profile.base_url,
    max_points_per_batch=100_000,
    max_request_delay_secs=0.25,
)
```
:::
::::

### Available Options

| Rust (`NominalStreamOpts`) | Python (`PyNominalStreamOpts`) | Default | Sets |
| --- | --- | --- | --- |
| {rust:method}`with_base_api_url <NominalStreamOpts::with_base_api_url>` | {py:attr}`~nominal_streaming.PyNominalStreamOpts.base_api_url` | `https://api.gov.nominal.io/api` | The base URL from [API Keys Setup](../api-keys-setup.md) |
| {rust:method}`with_max_points_per_record <NominalStreamOpts::with_max_points_per_record>` | {py:attr}`~nominal_streaming.PyNominalStreamOpts.max_points_per_batch` | 250,000 | The most points in one request |
| {rust:method}`with_max_request_delay <NominalStreamOpts::with_max_request_delay>` | {py:attr}`~nominal_streaming.PyNominalStreamOpts.max_request_delay_secs` | 100 ms | How long points wait for a fuller request before it's sent anyway |
| {rust:method}`with_max_buffered_requests <NominalStreamOpts::with_max_buffered_requests>` | {py:attr}`~nominal_streaming.PyNominalStreamOpts.max_buffered_requests` | 4 | How many requests can wait to be sent before writes wait too |
| {rust:method}`with_request_dispatcher_tasks <NominalStreamOpts::with_request_dispatcher_tasks>` | {py:attr}`~nominal_streaming.PyNominalStreamOpts.num_upload_workers` | 8 | How many requests are sent at once |
| | {py:attr}`~nominal_streaming.PyNominalStreamOpts.num_runtime_workers` | 8 | Threads the stream runs on; at least `num_upload_workers` |
| {rust:method}`with_track_metrics <NominalStreamOpts::with_track_metrics>` | {py:attr}`~nominal_streaming.PyNominalStreamOpts.track_metrics` | off | [Metric channels](#metrics) about the stream's latency |

### Batching and backpressure

The request size (`with_max_points_per_record`, or `max_points_per_batch` in Python) and the delay (`with_max_request_delay`, or `max_request_delay_secs`) set how points are [batched into requests](write.md#batching-and-backpressure).

The option `with_max_buffered_requests` (`max_buffered_requests` in Python) sets how many requests can wait to be sent before backpressure makes your writes wait too.

## Metrics

With metrics on, the stream adds these channels to the dataset, in the requests it already sends. Each point's timestamp is when its request finished, and its value is in seconds.

| Channel | Value |
| --- | --- |
| `__nominal.metric.request_rtt` | How long the request took, including retries |
| `__nominal.metric.largest_latency_before_request` | Time from the request's oldest point to when it was sent |
| `__nominal.metric.smallest_latency_before_request` | Time from the request's newest point to when it was sent |
| `__nominal.metric.largest_latency_after_request` | Time from the request's oldest point to when it finished |
| `__nominal.metric.smallest_latency_after_request` | Time from the request's newest point to when it finished |

## Reporting and handling errors

A stream retries failed requests. A request that still fails goes to the [fallback file](fallback.md), if configured.

:::{warning}
Your writes don't return streaming errors. Turn on logging to see it.
:::

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_file_fallback("fallback.avro")
    .enable_logging_with_directive("warn") // or enable_logging() for debug and up
    .try_build()?; // build() panics instead if the stream can't start
```

To handle failures in your own code, add a [`NominalStreamListener`](../ref/rust/nominal_streaming/listener/trait.NominalStreamListener.md) with {rust:method}`add_listener <NominalDatasetStreamBuilder::add_listener>`.
:::

:::{tab-item} Python
:sync: python

```python
stream = (
    NominalDatasetStream(auth_header=profile.token, opts=PyNominalStreamOpts(base_api_url=profile.base_url))
    .enable_logging("warn")  # or "debug" (the default), "info" or "error"
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
)
```
:::
::::

Log messages print to standard output. A warning that a request failed means its points went to the fallback file, so [upload it](fallback.md#upload-the-file) when the network is back. An error that a request failed means its points were lost.

## Close the stream

Closing a stream sends what it has buffered.

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

Leaving the `with` block closes the stream. Without `with`, call {py:meth}`~nominal_streaming.NominalDatasetStream.open` before writing and {py:meth}`~nominal_streaming.NominalDatasetStream.close` when done.

:::
::::
