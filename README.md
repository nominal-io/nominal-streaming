# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

The library aims to balance three concerns:
1. Data should exist in-memory only for a limited, configurable amount of time before it's sent to Core.
1. Writes should fall back to disk if there are network failures.
1. Backpressure should be applied to incoming requests when network throughput is saturated.

The library provides configuration points to manage the tradeoff between these concerns.

> [!WARNING]
> This library is still under active development and may make breaking changes.

You can view the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

## Conceptual overview

Data points will be sent to a Consumer.
The Consumer is responsible for, e.g., sending the data to a dataset in Nominal Core, or for saving it to disk.
A [`NominalDatasetStream`](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalDatasetStream.html) is the mechanism by which data points are fed to the consumer.

We construct a stream from a consumer as follows:

```rust
use nominal_streaming::consumer::AvroFileConsumer;

let avro_consumer = AvroFileConsumer::new_with_full_path("/tmp/my_stream.avro").expect("Could not open Avro file");
let stream = NominalDatasetStream::new_with_consumer(avro_consumer, NominalStreamOpts::default());
```

Recall that the consumer takes the data points, and sends it somewhere—in this case, into an Avro file.
We can now push data onto the stream:

```rust
let mut points = Vec::new();

// ... add data onto points ...
points.push(DoublePoint {
    timestamp: Timestamp {
      seconds: 0,
      nanos: 0
    },
    value: 123.45
});

// Stream to Avro file
stream.enqueue(
    &ChannelDescriptor::with_tags("channel_1", [("name", "my stream"), ("batch", "1")]),
    points,
);
```

Note that we are enquing our data onto Channel 1, with tags "name" and "batch".
These are just examples, you can choose your own.

### Builder interface

A shorthand for setting up the stream as above uses the [builder interface](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalDatasetStream.html#method.builder):

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_file("/tmp/fallback.avro")
    .build();

```

### Writer interface

A shorthand for enqueuing values onto the stream is the [writer interface]():

```rust
let mut writer = stream.double_writer(&channel_descriptor);

// Stream single data point
let start_time = UNIX_EPOCH.elapsed().unwrap();
let value = i % 50;
writer.push(start_Time, value as f64);
}

```

### Stream options

Above, you saw an example using [`NominalStreamOpts::default`](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalStreamOpts.html). The
following stream options can be set:

```rust
NominalStreamOpts {
  max_points_per_record: usize,
  max_request_delay: Duration,
  max_buffered_requests: usize,
  request_dispatcher_tasks: usize,
}
```

When using the builder, you can set it using `.with_options(...)`.

## Full example: streaming from memory to Nominal Core, with file fallback

In this typical scenario, we want to stream some values from memory into a [Nominal Dataset](https://docs.nominal.io/core/sdk/python-client/streaming/overview#streaming-data-to-a-dataset).
If the upload fails, we'd like to store the data points to an AVRO file.

Note that we set up the async [Tokio runtime](https://tokio.rs/), since that is required by the underlying [`NominalCoreConsumer`](https://docs.rs/nominal-streaming/latest/nominal_streaming/consumer/struct.NominalCoreConsumer.html).

For simplicity, we use the builder interface.
We'll discuss further down how to build the various components explicitly.

```rust
use nominal_streaming::prelude::*;
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

    let mut writer = stream.double_writer(&channel_descriptor);

    // Generate and upload 100,000 data points
    for i in 0..100_000 {
        let start_time = UNIX_EPOCH.elapsed().unwrap();
        let value = i % 50;
        writer.push(start_Time, value as f64);
    }
}
```

Instead of using the builder interface, we could have constructed the stream explicitly from consumers:

```rust
use nominal_streaming::consumer::RequestConsumerWithFallback;

fn core_consumer() -> NominalCoreConsumer<BearerToken> {
    let token = BearerToken::new(
        std::env::var("NOMINAL_TOKEN")
            .expect("NOMINAL_TOKEN environment variable not set")
            .as_str(),
    )
    .expect("Invalid token");

    let dataset_rid = ResourceIdentifier::new(DATASET_RID).unwrap();

    NominalCoreConsumer::new(
        STAGING_STREAMING_CLIENT.clone(),
        tokio::runtime::Handle::current(),
        token.clone(),
        dataset_rid.clone(),
    )
}

let avro_consumer = AvroFileConsumer::new_with_full_path("/tmp/my_stream.avro").expect("Could not open Avro file");

let stream = NominalDatasetStream::new_with_consumer(
    RequestConsumerWithFallback::new(core_consumer(), avro_consumer),
    NominalStreamOpts::default(),
);
```

Similarly, you can use `DualWriteRequestConsumer` to send data to two consumers simultaneously.

## Logging errors

Most of the time, when things go wrong, we also want some form of
reporting, which you can enable in `main()`:

```rust
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

tracing_subscriber::registry()
.with(
    tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true),
)
.with(
    EnvFilter::builder()
        .with_default_directive(LevelFilter::DEBUG.into())
        .from_env_lossy()
)
.init();
```

And add the necessary tracing dependencies to `Cargo.toml`:

```toml
tracing = "^0.1"
tracing-subscriber = { version = "0.3.19", features = ["env-filter"] }
```
