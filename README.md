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

## Concepts

While streaming, there will be a provider (the origin of the data) and a consumer (which receives the data).
E.g., when streaming to the Nominal platform, we will be streaming from memory into a `NominalCoreConsumer`.

## Stream options

Below, you will see examples using `NominalStreamOpts::default`. The following stream options can be set:

```rust
NominalStreamOpts {
  max_points_per_record: usize,
  max_request_delay: Duration,
  max_buffered_requests: usize,
  request_dispatcher_tasks: usize,
}
```

## Usage examples

### Streaming from memory to Nominal

In this simplest case, we want to stream some values from memory in a [Nominal Dataset](https://docs.nominal.io/core/sdk/python-client/streaming/overview#streaming-data-to-a-dataset).

```rust
use nominal_streaming::NominalDatasourceStream;
use nominal_streaming::api::{
    BearerToken, ResourceIdentifier, Timestamp, DoublePoint
};
use nominal_streaming::client::STAGING_STREAMING_CLIENT;
use nominal_streaming::consumer::NominalCoreConsumer;
use nominal_streaming::stream::NominalStreamOpts;

use std::time::UNIX_EPOCH;
use std::collections::HashMap;


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
            "channel_1".into(),
            HashMap::from([("batch_id".to_string(), batch.to_string())]),
            points,
        );
    }
}
```

The `Cargo.toml` will contain the following dependencies:

```toml
[dependencies]
nominal-api = "0.867.0"
nominal-streaming = "0.1.1"
tokio = { version = "1", features = ["full", "tracing"] }
```
