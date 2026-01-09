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

let mut writer = stream.double_writer(&channel_descriptor);

// Stream single data point
let start_time = UNIX_EPOCH.elapsed().unwrap();
let value: f64 = 123;
writer.push(start_time, value);
```

Here, we are enquing data onto Channel 1, with tags "name" and "batch".
These are, of course, just examples, and you can choose your own.

## Example: streaming from memory to Nominal Core, with file fallback

This is the typical scenario where we want to stream some values from memory into a [Nominal Dataset](https://docs.nominal.io/core/sdk/python-client/streaming/overview#streaming-data-to-a-dataset).
If the upload fails (say because of network errors), we'd like to instead send the data to an AVRO file.

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

```text
NominalStreamOpts {
  max_points_per_record: usize,
  max_request_delay: Duration,
  max_buffered_requests: usize,
  request_dispatcher_tasks: usize,
}
```

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

pub mod client;
pub mod consumer;
pub mod listener;
pub mod stream;
pub mod types;
pub mod upload;

pub use nominal_api as api;

/// This includes the most common types in this crate, re-exported for your convenience.
pub mod prelude {
    pub use conjure_object::BearerToken;
    pub use conjure_object::ResourceIdentifier;
    pub use nominal_api::tonic::google::protobuf::Timestamp;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Points;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequest;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;

    pub use crate::consumer::NominalCoreConsumer;
    pub use crate::stream::NominalDatasetStream;
    #[expect(deprecated)]
    pub use crate::stream::NominalDatasourceStream;
    pub use crate::stream::NominalStreamOpts;
    pub use crate::types::AuthProvider;
    pub use crate::types::ChannelDescriptor;
    pub use crate::types::IntoTimestamp;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;

    use crate::client::PRODUCTION_API_URL;
    use crate::consumer::ConsumerResult;
    use crate::consumer::WriteRequestConsumer;
    use crate::prelude::*;

    #[derive(Debug)]
    struct TestDatasourceStream {
        requests: Mutex<Vec<WriteRequestNominal>>,
    }

    impl WriteRequestConsumer for Arc<TestDatasourceStream> {
        fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(())
        }
    }

    fn create_test_stream() -> (Arc<TestDatasourceStream>, NominalDatasetStream) {
        let test_consumer = Arc::new(TestDatasourceStream {
            requests: Mutex::new(vec![]),
        });
        let stream = NominalDatasetStream::new_with_consumer(
            test_consumer.clone(),
            NominalStreamOpts {
                max_points_per_record: 1000,
                max_request_delay: Duration::from_millis(100),
                max_buffered_requests: 2,
                request_dispatcher_tasks: 4,
                base_api_url: PRODUCTION_API_URL.to_string(),
            },
        );

        (test_consumer, stream)
    }

    #[test_log::test]
    fn test_stream() {
        let (test_consumer, stream) = create_test_stream();

        for batch in 0..5 {
            let mut points = Vec::new();
            for i in 0..1000 {
                let start_time = UNIX_EPOCH.elapsed().unwrap();
                points.push(DoublePoint {
                    timestamp: Some(Timestamp {
                        seconds: start_time.as_secs() as i64,
                        nanos: start_time.subsec_nanos() as i32 + i,
                    }),
                    value: (i % 50) as f64,
                });
            }

            stream.enqueue(
                &ChannelDescriptor::with_tags("channel_1", [("batch_id", batch.to_string())]),
                points,
            );
        }

        drop(stream); // wait for points to flush

        let requests = test_consumer.requests.lock().unwrap();

        // validate that the requests were flushed based on the max_records value, not the
        // max request delay
        assert_eq!(requests.len(), 5);
        let series = requests.first().unwrap().series.first().unwrap();
        if let Some(PointsType::DoublePoints(points)) =
            series.points.as_ref().unwrap().points_type.as_ref()
        {
            assert_eq!(points.points.len(), 1000);
        } else {
            panic!("unexpected data type");
        }
    }

    #[test_log::test]
    fn test_stream_types() {
        let (test_consumer, stream) = create_test_stream();

        for batch in 0..5 {
            let mut doubles = Vec::new();
            let mut strings = Vec::new();
            let mut ints = Vec::new();
            for i in 0..1000 {
                let start_time = UNIX_EPOCH.elapsed().unwrap();
                doubles.push(DoublePoint {
                    timestamp: Some(start_time.into_timestamp()),
                    value: (i % 50) as f64,
                });
                strings.push(StringPoint {
                    timestamp: Some(start_time.into_timestamp()),
                    value: format!("{}", i % 50),
                });
                ints.push(IntegerPoint {
                    timestamp: Some(start_time.into_timestamp()),
                    value: i % 50,
                })
            }

            stream.enqueue(
                &ChannelDescriptor::with_tags("double", [("batch_id", batch.to_string())]),
                doubles,
            );
            stream.enqueue(
                &ChannelDescriptor::with_tags("string", [("batch_id", batch.to_string())]),
                strings,
            );
            stream.enqueue(
                &ChannelDescriptor::with_tags("int", [("batch_id", batch.to_string())]),
                ints,
            );
        }

        drop(stream); // wait for points to flush

        let requests = test_consumer.requests.lock().unwrap();

        // validate that the requests were flushed based on the max_records value, not the
        // max request delay
        assert_eq!(requests.len(), 15);

        let r = requests
            .iter()
            .flat_map(|r| r.series.clone())
            .map(|s| {
                (
                    s.channel.unwrap().name,
                    s.points.unwrap().points_type.unwrap(),
                )
            })
            .collect::<HashMap<_, _>>();
        let PointsType::DoublePoints(dp) = r.get("double").unwrap() else {
            panic!("invalid double points type");
        };

        let PointsType::IntegerPoints(ip) = r.get("int").unwrap() else {
            panic!("invalid int points type");
        };

        let PointsType::StringPoints(sp) = r.get("string").unwrap() else {
            panic!("invalid string points type");
        };

        // collect() overwrites into a single request
        assert_eq!(dp.points.len(), 1000);
        assert_eq!(sp.points.len(), 1000);
        assert_eq!(ip.points.len(), 1000);
    }

    #[test_log::test]
    fn test_writer() {
        let (test_consumer, stream) = create_test_stream();

        let cd = ChannelDescriptor::new("channel_1");
        let mut writer = stream.double_writer(&cd);

        for i in 0..5000 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            let value = i % 50;
            writer.push(start_time, value as f64);
        }

        drop(writer); // flush points to stream
        drop(stream); // flush stream to nominal

        let requests = test_consumer.requests.lock().unwrap();

        assert_eq!(requests.len(), 5);
        let series = requests.first().unwrap().series.first().unwrap();
        if let Some(PointsType::DoublePoints(points)) =
            series.points.as_ref().unwrap().points_type.as_ref()
        {
            assert_eq!(points.points.len(), 1000);
        } else {
            panic!("unexpected data type");
        }
    }

    #[test_log::test]
    fn test_time_flush() {
        let (test_consumer, stream) = create_test_stream();

        let cd = ChannelDescriptor::new("channel_1");
        let mut writer = stream.double_writer(&cd);

        writer.push(UNIX_EPOCH.elapsed().unwrap(), 1.0);
        thread::sleep(Duration::from_millis(101));
        writer.push(UNIX_EPOCH.elapsed().unwrap(), 2.0); // first flush
        thread::sleep(Duration::from_millis(101));
        writer.push(UNIX_EPOCH.elapsed().unwrap(), 3.0); // second flush

        drop(writer);
        drop(stream);

        let requests = test_consumer.requests.lock().unwrap();
        dbg!(&requests);
        assert_eq!(requests.len(), 2);
    }

    #[test_log::test]
    fn test_writer_types() {
        let (test_consumer, stream) = create_test_stream();

        let cd1 = ChannelDescriptor::new("double");
        let cd2 = ChannelDescriptor::new("string");
        let cd3 = ChannelDescriptor::new("int");
        let mut double_writer = stream.double_writer(&cd1);
        let mut string_writer = stream.string_writer(&cd2);
        let mut integer_writer = stream.integer_writer(&cd3);

        for i in 0..5000 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            let value = i % 50;
            double_writer.push(start_time, value as f64);
            string_writer.push(start_time, format!("{}", value));
            integer_writer.push(start_time, value);
        }

        drop(double_writer);
        drop(string_writer);
        drop(integer_writer);
        drop(stream);

        let requests = test_consumer.requests.lock().unwrap();

        assert_eq!(requests.len(), 15);

        let r = requests
            .iter()
            .flat_map(|r| r.series.clone())
            .map(|s| {
                (
                    s.channel.unwrap().name,
                    s.points.unwrap().points_type.unwrap(),
                )
            })
            .collect::<HashMap<_, _>>();

        let PointsType::DoublePoints(dp) = r.get("double").unwrap() else {
            panic!("invalid double points type");
        };

        let PointsType::IntegerPoints(ip) = r.get("int").unwrap() else {
            panic!("invalid int points type");
        };

        let PointsType::StringPoints(sp) = r.get("string").unwrap() else {
            panic!("invalid string points type");
        };

        // collect() overwrites into a single request
        assert_eq!(dp.points.len(), 1000);
        assert_eq!(sp.points.len(), 1000);
        assert_eq!(ip.points.len(), 1000);
    }
}
