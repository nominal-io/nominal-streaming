# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

Please refer to the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

### Bounded upload retries

Core uploads use Conjure's retry policy: five additional attempts, a 250 ms
exponential-backoff slot with jitter, 5 second connect and 15 second read/write
timeouts. A 60 second delivery deadline bounds all HTTP attempts and retry sleeps,
including `Retry-After` when a response provides it. Queueing, protobuf/zstd
encoding, and file fallback are outside that deadline. Each request is encoded
once; retries replay its bytes. A timed-out request may already have reached the
server, so retries and fallback can duplicate data.

The pinned Conjure runtime retries 429, 503, and (with idempotent requests) 500
and transport failures; it does not retry 502 or 504. Exhaustion or a deadline
returns the existing upload error and invokes a configured Avro fallback.

```rust
use nominal_streaming::client::TransportOptions;
use nominal_streaming::stream::NominalStreamOpts;
use std::time::Duration;

let mut transport = TransportOptions::default();
transport.max_retries = 3; // additional attempts; zero disables retries
let options = NominalStreamOpts::default()
    .with_transport_options(transport)
    .with_delivery_timeout(Duration::from_secs(30));
```

```python
from nominal_streaming import PyNominalStreamOpts

options = PyNominalStreamOpts(max_retries=3, delivery_timeout_secs=30.0)
```

Python also accepts these settings in `NominalDatasetStream.create`. Both APIs
expose the backoff slot and individual socket timeouts. Durations must be positive
and Python durations must be finite; retry counts range from 0 through 31.
Manually constructed Rust consumers can use `NominalCoreConsumer::with_delivery_timeout`;
`NominalApiClients::from_uri_with_options` configures socket timeouts and retries,
while its `send_with_timeout` method sets a per-call deadline (`send` uses 60 seconds).
