# Stream a sensor loop

{.lead}
Read two sensors every 100 ms for a minute, and stream both readings to a dataset. Points that can't be sent go to a fallback file, and errors are logged.

Replace `read_sensor` with your hardware reads, and the placeholders with your API key, base URL and dataset RID, from [API Keys Setup](../guides/api-keys-setup.md).

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```shell
cargo add nominal-streaming chrono
cargo add tokio --features full
```

```rust
use std::time::Duration;

use nominal_streaming::prelude::*;
use nominal_streaming::stream::{NominalDatasetStreamBuilder, NominalStreamOpts};

/// Stands in for your hardware: temperature in °C and pressure in kPa
fn read_sensor(i: u32) -> (f64, f64) {
    let t = f64::from(i) / 10.0;
    (24.0 + (t / 10.0).sin(), 101.3 + 0.2 * (t / 3.0).cos())
}

#[tokio::main]
async fn main() {
    let token = BearerToken::new("<api key>").expect("invalid API key"); // DO NOT COMMIT THIS
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>").expect("invalid dataset RID");

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, tokio::runtime::Handle::current())
        .with_options(NominalStreamOpts::default().with_base_api_url("<base url>"))
        .with_file_fallback("fallback.avro")
        .enable_logging()
        .build();

    let mut temperature = stream.double_writer(ChannelDescriptor::new("temperature"));
    let mut pressure = stream.double_writer(ChannelDescriptor::new("pressure"));

    let mut tick = tokio::time::interval(Duration::from_millis(100));
    for i in 0..600 {
        tick.tick().await;
        let now = chrono::Utc::now();
        let (t, p) = read_sensor(i);
        temperature.push(now, t);
        pressure.push(now, p);
    }

    // Pass the writers' last points to the stream, then wait until everything is sent
    drop(temperature);
    drop(pressure);
    drop(stream);
}
```
:::

:::{tab-item} Python
:sync: python

```shell
pip install nominal-streaming
```

```python
import math
import pathlib
import time

from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts


def read_sensor(i: int) -> tuple[float, float]:
    """Stands in for your hardware: temperature in °C and pressure in kPa."""
    t = i / 10
    return 24.0 + math.sin(t / 10), 101.3 + 0.2 * math.cos(t / 3)


stream = (
    NominalDatasetStream(
        auth_header="<api key>",  # DO NOT COMMIT THIS
        opts=PyNominalStreamOpts(base_api_url="<base url>"),
    )
    .enable_logging("warn")
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(pathlib.Path("fallback.avro"))
)

# Leaving the block sends what's buffered and closes the stream
with stream:
    for i in range(600):
        now = time.time_ns()
        temperature, pressure = read_sensor(i)
        stream.enqueue("temperature", now, temperature)
        stream.enqueue("pressure", now, pressure)
        time.sleep(0.1)
```
:::
::::

If the network drops, the stream keeps taking readings: requests it can't send go to `fallback.avro`, with the stream's start time added to the name. [Stream files](../guides/files.md) covers uploading that file later.
