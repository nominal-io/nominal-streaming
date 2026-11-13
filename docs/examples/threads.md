# Stream from several threads

{.lead}
Share one stream between threads, with a writer in each. A stream can be shared across threads, and each writer borrows the stream, so scoped threads can use it without an `Arc`.

This program runs one thread per motor, each streaming its torque at 100 Hz for 10 seconds, tagged with the motor's name. It builds its own tokio runtime, because the threads aren't async.

```shell
cargo add nominal-streaming chrono
cargo add tokio --features full
```

```rust
use std::time::Duration;

use nominal_streaming::prelude::*;
use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};

fn main() {
    // The stream sends its requests on this runtime
    let runtime = tokio::runtime::Runtime::new().expect("failed to start a tokio runtime");
    let token = BearerToken::new("<api key>").expect("invalid API key"); // DO NOT COMMIT THIS
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>").expect("invalid dataset RID");

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, runtime.handle().clone())
        .with_options(NominalStreamOpts::default().with_base_api_url("<base url>"))
        .with_file_fallback("fallback.avro")
        .build();

    std::thread::scope(|scope| {
        for motor in ["left", "right"] {
            let stream = &stream;
            scope.spawn(move || {
                let mut torque = stream.double_writer(ChannelDescriptor::with_tags("motor.torque", [("motor", motor)]));
                for i in 0..1_000 {
                    torque.push(chrono::Utc::now(), f64::from(i % 100) / 10.0);
                    std::thread::sleep(Duration::from_millis(10));
                }
                // Dropping the writer here passes its last points to the stream
            });
        }
    });

    drop(stream); // waits until every point has been sent
}
```

Both threads write to the channel `motor.torque`, told apart by the `motor` tag. To give each thread its own channel instead, use a different name in each `ChannelDescriptor`.
