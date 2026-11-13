# Streaming

{.lead}
`nominal-streaming` sends data points to a Nominal dataset as your program produces them. It batches points in memory, applies backpressure when the network can't keep up, and can write to a file when requests fail. Use it from Rust, or from Python through the `nominal-streaming` package.

```{toctree}
:hidden:
:caption: Quickstart

Quickstart <self>
```

```{toctree}
:hidden:
:caption: Guides

guides/index
guides/api-keys-setup
guides/write
guides/configure
guides/files
guides/logs
```

```{toctree}
:hidden:
:caption: Examples

examples/index
examples/sensor-loop
examples/threads
examples/many-units
```

```{toctree}
:hidden:
:caption: Reference

ref/index
ref/python/nominal_streaming/index
```

This quickstart creates a dataset, streams 10 seconds of engine values to it, and opens the result in Nominal.

## 1. Install

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

Create a project, and add the crate, tokio to run it, and chrono for timestamps:

```shell
cargo new streaming-quickstart
cd streaming-quickstart
cargo add nominal-streaming chrono
cargo add tokio --features full
```
:::

:::{tab-item} Python
:sync: python

```shell
pip install nominal-streaming
```

It needs Python 3.11 or later.
:::
::::

## 2. Set up your API key

Follow [API Keys Setup](guides/api-keys-setup.md). It gives you an API key and a base URL, which the code below uses, and installs nomctl, which the next step uses.

## 3. Create a dataset

A stream writes to an existing dataset. Create one with nomctl:

```shell
nomctl --profile default dataset create --name "Streaming quickstart"
```

It prints the dataset's RID and its URL in Nominal. To stream to a dataset you already have, use its RID instead.

## 4. Stream values

This streams two channels for 10 seconds: `engine.rpm`, a number, and `engine.state`, text. Points that can't be sent go to a fallback file. Replace the API key, base URL and dataset RID with yours.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

Replace `src/main.rs` with:

```rust
use std::time::Duration;

use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = BearerToken::new("<api key>")?; // DO NOT COMMIT THIS
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>")?;

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, tokio::runtime::Handle::current())
        .with_options(NominalStreamOpts::default().with_base_api_url("<base url>"))
        .with_file_fallback("fallback.avro")
        .try_build()?;

    let mut rpm = stream.double_writer(ChannelDescriptor::new("engine.rpm"));
    let mut state = stream.string_writer(ChannelDescriptor::new("engine.state"));
    for i in 0..100 {
        let now = chrono::Utc::now();
        rpm.push(now, 3000.0 + 50.0 * (i % 20) as f64);
        state.push(now, if i < 20 { "warming up" } else { "running" });
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Writers pass their last points to the stream when dropped; dropping the stream
    // then waits until every point has been sent or saved to the fallback file
    drop(rpm);
    drop(state);
    drop(stream);
    Ok(())
}
```

Run it with `cargo run`.
:::

:::{tab-item} Python
:sync: python

Save this as `quickstart.py`:

```python
import time
from pathlib import Path

from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

stream = (
    NominalDatasetStream(
        auth_header="<api key>",  # DO NOT COMMIT THIS
        opts=PyNominalStreamOpts(base_api_url="<base url>"),
    )
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(Path("fallback.avro"))
)

# Leaving the block sends what's buffered and closes the stream
with stream:
    for i in range(100):
        now = time.time_ns()
        stream.enqueue("engine.rpm", now, 3000.0 + 50.0 * (i % 20))
        stream.enqueue("engine.state", now, "warming up" if i < 20 else "running")
        time.sleep(0.1)
```

Run it with `python quickstart.py`.
:::
::::

## 5. See the data

Open the URL nomctl printed in step 3. The dataset has two channels, `engine.rpm` and `engine.state`.

If a request failed, its points are in a file named after the stream's start time, such as `fallback_20260101_120000.avro`, and the file isn't created when every request succeeds. To upload it, see [Stream files](guides/files.md#upload-a-file-to-nominal).

## Next

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} Guides
:link: guides/index
:link-type: doc

Write each value type and tags, set batching and backpressure, handle errors, keep files of what you stream, and stream logs.
:::

:::{grid-item-card} Reference
:link: ref/index
:link-type: doc

The Rust crate and the Python package.
:::
::::
