# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

The library aims to balance three concerns:
1. Data should exist in-memory only for a limited, configurable amount of time before it's sent to Core.
1. Writes should fall back to disk if there are network failures.
1. Backpressure should be applied to incoming requests when network throughput is saturated.

This library streams data to Nominal Core, to a file, or to Nominal Core with a file as backup (recommended to protect against network failures).
It also provides configuration to manage the tradeoff between above listed concerns.

> [!WARNING]
> This library is still under active development and may make breaking changes.

You can view the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

## Conceptual overview

Data is sent to a [Stream](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalDatasetStream.html) via a Writer.
For example, a file stream is constructed as follows:

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_file("my_data.avro")
    .build();
```

Or, a stream to Nominal Core, writing failed requests to a file, is created as follows:

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset_rid, handle)
    .with_file_fallback("fallback.avro")
    .build();
```

(See below for a full example, that also shows how to create the `token`, `dataset_rid`, and `handle` values above.)

Once we have a Stream, we can construct a Writer and send values to it:

```rust
let channel_descriptor = ChannelDescriptor::with_tags("channel_1", [("experiment_id", "123")]);

let mut writer = stream.double_writer(&channel_descriptor);

// Stream single data point
let start_time = UNIX_EPOCH.elapsed().unwrap();
let value: f64 = 123;
writer.push(start_time, value);
}
```

Here, we are enquing data onto Channel 1, with tags "name" and "batch".
These are, of course, just examples, and you can choose your own.

## Full example: streaming from memory to Nominal Core, with file fallback

This is the typical scenario where we want to stream some values from memory into a [Nominal Dataset](https://docs.nominal.io/core/sdk/python-client/streaming/overview#streaming-data-to-a-dataset).
If the upload fails (say because of network errors), we'd like to instead send the data to an AVRO file.

Note that we set up the async [Tokio runtime](https://tokio.rs/), since that is required by the underlying [`NominalCoreConsumer`](https://docs.rs/nominal-streaming/latest/nominal_streaming/consumer/struct.NominalCoreConsumer.html).

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

    let mut writer = stream.double_writer(&channel_descriptor);

    // Generate and upload 100,000 data points
    for i in 0..100_000 {
        let start_time = UNIX_EPOCH.elapsed().unwrap();
        let value = i % 50;
        writer.push(start_time, value as f64);
    }
}
```

## Additional configuration

### Stream options

Above, you saw an example using [`NominalStreamOpts::default`](https://docs.rs/nominal-streaming/latest/nominal_streaming/stream/struct.NominalStreamOpts.html).
The following stream options can be set using `.with_options(...)` on the StreamBuilder:

```rust
NominalStreamOpts {
  max_points_per_record: usize,
  max_request_delay: Duration,
  max_buffered_requests: usize,
  request_dispatcher_tasks: usize,
}
```

### Logging errors

Most of the time, when things go wrong, we want some form of reporting. You can enable debug logging on the StreamBuilder using `.enable_logging()`:

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset_rid, handle)
    .with_file_fallback("fallback.avro")
    .enable_logging()
    .build();
```
