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
The Consumer is responsible for, e.g., sending the data to Nominal Core, or for saving it to disk.
A [NominalDatasourceStream](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalDatasourceStream.html) is the mechanism by which data points are fed to the consumer.

We construct a stream from a consumer as follows:

```rust
use nominal_streaming::consumer::AvroFileConsumer;

let avro_consumer = AvroFileConsumer::new_with_full_path("/tmp/my_stream.avro").expect("Could not open Avro file");
let stream = NominalDatasourceStream::new_with_consumer(avro_consumer, NominalStreamOpts::default());
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
    &ChannelDescriptor::new("channel_1", [("name", "my stream"), ("batch", "1")]),
    points,
);
```

Note that we are enquing our data onto Channel 1, with tags "name" and "batch".

## Stream options

Above, you saw an example using `NominalStreamOpts::default`. The
following stream options can be set:

```rust
NominalStreamOpts {
  max_points_per_record: usize,
  max_request_delay: Duration,
  max_buffered_requests: usize,
  request_dispatcher_tasks: usize,
}
```

## Full example: streaming from memory to Nominal Core

In this simplest case, we want to stream some values from memory into a [Nominal Dataset](https://docs.nominal.io/core/sdk/python-client/streaming/overview#streaming-data-to-a-dataset).

Note that the `NominalCoreConsumer` requires the async [Tokio runtime](https://tokio.rs/).

```rust
use nominal_streaming::prelude::*;
use std::time::UNIX_EPOCH;


static DATA_SOURCE_RID: &str =
      "ri.catalog.gov-staging.dataset.d61e3cf9-094b-4612-9c1d-619b48c335f9";


fn core_consumer() -> NominalCoreConsumer<BearerToken> {
    let token = BearerToken::new(
        std::env::var("NOMINAL_TOKEN")
            .expect("NOMINAL_TOKEN environment variable not set")
            .as_str(),
    )
    .expect("Invalid token");

    let data_source_rid = ResourceIdentifier::new(DATA_SOURCE_RID).unwrap();

    NominalCoreConsumer::new(
        STAGING_STREAMING_CLIENT.clone(),
        tokio::runtime::Handle::current(),
        token.clone(),
        data_source_rid.clone(),
    )
}


fn main() {
    // Standard scaffolding for multi-threaded app
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("tokio")
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(async_main());
}


async fn async_main() {
    let stream = NominalDatasourceStream::new_with_consumer(
        core_consumer(),
        NominalStreamOpts::default()
    );

    // Generate 500 batches of test data, each containing 100,000 data points
    for batch in 0..50 {
        let mut points = Vec::new();

        for i in 0..100_000 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            points.push(DoublePoint {
                timestamp: Some(Timestamp {
                    seconds: start_time.as_secs() as i64,
                    nanos: start_time.subsec_nanos() as i32 + i,
                }),
                value: (i % 50) as f64,
            });
        }

        // Push current batch onto the upload queue
        println!("Enqueue batch: {}", batch);
        stream.enqueue(
            &ChannelDescriptor::new("channel_1", [("batch_id", batch.to_string())]),
            points,
        );
    }
}
```

The `Cargo.toml` will contain the following dependencies:

```toml
[dependencies]
nominal-api = "0.867.0"
nominal-streaming = "0.2.0"
tokio = { version = "1", features = ["full", "tracing"] }
```

## Streaming with fallback

Often, it is imperative that we capture data values even when a
network connection is interrupted. For that purpose, the library has
support for a fallback, so that it attempts to write to a secondary
consumer if the first one fails:

```rust
use nominal_streaming::consumer::RequestConsumerWithFallback;

let stream = NominalDatasourceStream::new_with_consumer(
    RequestConsumerWithFallback::new(core_consumer(), avro_consumer),
    NominalStreamOpts::default(),
);
```

Similarly, you can use `DualWriteRequestConsumer` to send data to two consumers simultaneously.

## Logging errors

Most of the time, when things go wrong, we also want some form of reporting.
That is the purpose of the `ListeningWriteRequestConsumer`:

```rust
use nominal_streaming::consumer::ListeningWriteRequestConsumer;
use nominal_streaming::notifier::LoggingListener;
use std::sync::Arc;

let consumer_with_logging = ListeningWriteRequestConsumer::new(
    core_consumer(),
    vec![Arc::new(LoggingListener)]
);
```

You'll also need to enable tracing in `main`:

```rust
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

If you want to avoid printing full tracebacks for errors, customize the error printing:

```rust
use nominal_streaming::notifier::NominalStreamListener;
use std::error::Error;
use tracing::error;

#[derive(Debug, Default, Clone)]
pub struct MyListener;

impl NominalStreamListener for MyListener {
    fn on_error(&self, message: &str, _error: &dyn Error) {
        error!("{}", message);
    }
}

let stream = ListeningWriteRequestConsumer::new(core_consumer(), vec![Arc::new(MyListener)]);
```
