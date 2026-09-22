/*!
`nominal-streaming` is a crate for streaming data into [Nominal Core](https://nominal.io/products/core).

The library aims to balance three concerns:

1. Data should exist in-memory only for a limited, configurable amount of time before it's sent to Core.
1. Writes should fall back to disk if there are network failures.
1. Backpressure should be applied to incoming requests when network throughput is saturated.

This library streams data to Nominal Core, to a file, or to Nominal Core with a file as backup (recommended to protect against network failures).
It also provides configuration to manage the tradeoff between above listed concerns.

<div class="warning">
This library is still under active development and may make breaking changes.
</div>

## Conceptual overview

Data is sent to a [Stream](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalDatasetStream.html) via a Writer.
For example:

- A file stream is constructed as:

  ```rust,no_run
  use nominal_streaming::stream::NominalDatasetStreamBuilder;

  let stream = NominalDatasetStreamBuilder::new()
      .stream_to_file("my_data.avro")
      .build();
  ```

- A stream that sends data to Nominal Core, but writes failed requests to a file, is created as follows:

  ```rust,ignore
  let stream = NominalDatasetStreamBuilder::new()
      .stream_to_core(token, dataset_rid, handle)
      .with_file_fallback("fallback.avro")
      .build();
  ```

- Or, you can build a stream that sends data to Nominal Core *and* to a file:

  ```rust,ignore
  let stream = NominalDatasetStreamBuilder::new()
      .stream_to_core(token, dataset_rid, handle)
      .stream_to_file("my_data.avro")
      .build();
  ```

(See below for a [full example](#example-streaming-from-memory-to-nominal-core-with-file-fallback), that also shows how to create the `token`, `dataset_rid`, and `handle` values above.)

Once we have a Stream, we can construct a Writer and send values to it:

```rust,ignore
let channel_descriptor = ChannelDescriptor::with_tags(
    "channel_1", [("experiment_id", "123")]
);

let mut writer = stream.double_writer(channel_descriptor);

// Stream single data point
let start_time = UNIX_EPOCH.elapsed().unwrap();
let value: f64 = 123;
writer.push(start_time, value);
```

Here, we are enqueuing data onto Channel 1, with tags "name" and "batch".
These are, of course, just examples, and you can choose your own.

## Example: streaming from memory to Nominal Core, with file fallback

This is the typical scenario where we want to stream some values from memory into a [Nominal Dataset](https://docs.nominal.io/core/sdk/python-client/streaming/overview#streaming-data-to-a-dataset).
If the upload fails (say because of network errors), we'd like to instead send the data to an Avro file. Note that the Avro spec does not support uint64 values, so those will be stored as signed int64 values.

Note that we set up the async [Tokio runtime](https://tokio.rs/), since that is required by the underlying [`NominalCoreConsumer`](https://docs.rs/nominal-streaming/latest/nominal_streaming/consumer/struct.NominalCoreConsumer.html).

```rust,no_run
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

use std::time::UNIX_EPOCH;


static DATASET_RID: &str = "ri.catalog....";  // your dataset ID here


fn main() {
    // The NominalCoreConsumer requires a tokio runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("tokio")
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(async_main());
}


async fn async_main() {
    // Configure token for authentication
    let token = BearerToken::new(
        std::env::var("NOMINAL_TOKEN")
            .expect("NOMINAL_TOKEN environment variable not set")
            .as_str(),
    )
    .expect("Invalid token");

    let dataset_rid = ResourceIdentifier::new(DATASET_RID).unwrap();
    let handle = tokio::runtime::Handle::current();

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset_rid, handle)
        .with_file_fallback("fallback.avro")
        .build();

    let channel_descriptor = ChannelDescriptor::with_tags("channel_1", [("experiment_id", "123")]);

    let mut writer = stream.double_writer(channel_descriptor);

    // Generate and upload 100,000 data points
    for i in 0..100_000 {
        let start_time = UNIX_EPOCH.elapsed().unwrap();
        let value = i % 50;
        writer.push(start_time, value as f64);
    }
}
```

## Checked shutdown

Use fallible admission methods and explicitly close the stream to observe delivery
and file-finalization failures. Writers must be dropped before `close(&mut self)`;
their accepted points are flushed even if later pushes are rejected.

```rust,no_run
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

fn write_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = NominalDatasetStreamBuilder::new()
        .stream_to_file("new-data.avro")
        .try_build()?;
    {
        let mut writer = stream.double_writer(ChannelDescriptor::new("temperature"));
        writer.try_push(0, 21.5)?;
        writer.try_flush()?;
    }
    let summary = stream.close()?;
    assert_eq!(summary.file_points, 1);
    Ok(())
}
```

`close` drains accepted points, joins workers and finalizes destinations. Repeated
calls return the same result. A failed close carries its delivery summary in
`StreamError::summary`. Backend acknowledgements and file writes may overlap for
dual writes. Opaque custom-consumer completion is counted separately and does not
establish backend or disk preservation. File counts in a live `delivery_summary`
are provisional, and live unpreserved counts include pending work. A finalization
error conservatively invalidates all non-backend evidence.

`try_enqueue` and `try_enqueue_many` reject new admissions after a delivery failure.
An admitted batch is reserved in full and its remaining chunks continue draining.
Worker failure while draining can return an error after acceptance; inspect the
error summary before retrying. Legacy `enqueue` and writer `push` methods panic on
rejection. `Drop` performs the same cleanup and logs errors; use `close` to handle
them explicitly. A custom consumer that never returns can still block shutdown.

## Additional configuration

### Stream options

Above, you saw an example using [`NominalStreamOpts::default`](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalStreamOpts.html).
Stream options can be customised with the `with_*` setters and set using `.with_options(...)` on the StreamBuilder:

```rust
use std::time::Duration;
use nominal_streaming::stream::NominalStreamOpts;

let opts = NominalStreamOpts::default()
    .with_max_points_per_record(100_000)
    .with_max_request_delay(Duration::from_millis(250))
    .with_max_buffered_requests(4)
    .with_request_dispatcher_tasks(8)
    .with_track_metrics(true) // defaults to false
    // caller-emitted metric channels excluded from latency bounds
    .with_additional_metric_channels(["my.metric.channel"]);
```

#### Metrics

The `track_metrics` option enables request latency metrics tracking directly on the dataset. It adds the following metric fields:

| Channel | Value | Point timestamp |
| --- | --- | --- |
| `__nominal.metric.largest_latency_before_request` | Wall time before HTTP send minus the oldest data timestamp in the batch | Request completion time |
| `__nominal.metric.smallest_latency_before_request` | Wall time before HTTP send minus the newest data timestamp in the batch | Request completion time |
| `__nominal.metric.request_rtt` | Elapsed HTTP send time, including client retries | Request completion time |
| `__nominal.metric.largest_latency_after_request` | Wall time after HTTP send minus the oldest data timestamp in the batch | Request completion time |
| `__nominal.metric.smallest_latency_after_request` | Wall time after HTTP send minus the newest data timestamp in the batch | Request completion time |

If there are metrics provided by upstream users of this library - such as the Python client - they can be provided as `additional_metric_channels`. They will
be excluded from the metrics above (where possible - e.g for metric-only requests and when fetching newest/oldest data timestamps). All metrics - including the
ones added by this library - are appended into the existing requests the library makes, so overhead is minimal.

### Logging errors

Most of the time, when things go wrong, we want some form of reporting. You can enable debug logging on the StreamBuilder by using `.enable_logging()`:

```rust,ignore
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset_rid, handle)
    .with_file_fallback("fallback.avro")
    .enable_logging()
    .build();
```
*/
#![recursion_limit = "256"]

pub mod client;
pub mod consumer;
pub mod listener;
mod metrics;
#[cfg(test)]
mod simulated_consumer;
pub mod stream;
pub mod types;
pub mod upload;

pub use nominal_api as api;

/// This includes the most common types in this crate, re-exported for your convenience.
pub mod prelude {
    pub use conjure_object::BearerToken;
    pub use conjure_object::ResourceIdentifier;
    pub use nominal_api::tonic::google::protobuf::Timestamp;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StructPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Points;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequest;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;

    pub use crate::consumer::NominalCoreConsumer;
    pub use crate::stream::DeliverySummary;
    pub use crate::stream::NominalDatasetStream;
    #[expect(deprecated)]
    pub use crate::stream::NominalDatasourceStream;
    pub use crate::stream::NominalStreamOpts;
    pub use crate::stream::StreamError;
    pub use crate::types::AuthProvider;
    pub use crate::types::ChannelDescriptor;
    pub use crate::types::IntoTimestamp;
}

#[cfg(test)]
mod tests;
