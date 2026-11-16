# Stream from several threads

{.lead}
Share one stream between threads, with a writer in each. A stream can be shared across threads, and each writer borrows the stream, so scoped threads can use it without an `Arc`.

This program runs one thread per motor, each streaming 10 seconds of its torque at 100 Hz, tagged with the motor's name. It builds its own tokio runtime, because the threads aren't async.

```shell
cargo add nominal-streaming nominal
cargo add tokio --features full
```

```rust
use std::time::{Duration, UNIX_EPOCH};

use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

fn main() {
    // Load the API key and base URL that API Keys Setup saved
    let config = nominal::Config::load().expect("failed to read the Nominal config file");
    let profile = config.get_profile("default").expect("no profile named default");

    // The stream sends its requests on this runtime
    let runtime = tokio::runtime::Runtime::new().expect("failed to start a tokio runtime");
    let token = BearerToken::new(profile.token()).expect("invalid API key");
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>").expect("invalid dataset RID");

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, runtime.handle().clone())
        .with_options(NominalStreamOpts::default().with_base_api_url(profile.base_url()))
        .with_file_fallback("fallback.avro")
        .build();

    let start = UNIX_EPOCH.elapsed().unwrap();
    std::thread::scope(|scope| {
        for motor in ["left", "right"] {
            let stream = &stream;
            scope.spawn(move || {
                let mut torque = stream.double_writer(ChannelDescriptor::with_tags("motor.torque", [("motor", motor)]));
                // 1,000 points, 10 ms apart
                let mut timestamp = start;
                for i in 0..1_000 {
                    torque.push(timestamp, f64::from(i % 100) / 10.0);
                    timestamp += Duration::from_millis(10);
                }
                // Dropping the writer here passes its last points to the stream
            });
        }
    });

    drop(stream); // waits until every point has been sent
}
```

Both threads write to the channel `motor.torque`, told apart by the `motor` tag. To give each thread its own channel instead, use a different name in each {rust:struct}`ChannelDescriptor`.
