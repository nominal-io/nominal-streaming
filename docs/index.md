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
guides/configure
```

```{toctree}
:hidden:
:caption: Reference

ref/index
ref/python/nominal_streaming/index
```

## 1. Install

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```shell
cargo add nominal-streaming
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

## 2. Stream to a dataset

You need an API key, from [API Keys Setup](guides/api-keys-setup.md), and the RID of the dataset to stream to. Points that can't be sent go to `fallback.avro`.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use std::time::UNIX_EPOCH;

use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

#[tokio::main]
async fn main() {
    let token = BearerToken::new("<api key>").expect("invalid API key"); // DO NOT COMMIT THIS
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>").expect("invalid RID");

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, tokio::runtime::Handle::current())
        .with_file_fallback("fallback.avro")
        .build();

    let mut voltage = stream.double_writer(ChannelDescriptor::new("voltage"));
    for i in 0..1_000 {
        voltage.push(UNIX_EPOCH.elapsed().unwrap(), 12.0 + (i % 10) as f64 / 10.0);
    }
    // Dropping the stream waits until every buffered point has been sent
}
```
:::

:::{tab-item} Python
:sync: python

```python
import pathlib
import time

from nominal_streaming import NominalDatasetStream

stream = (
    NominalDatasetStream(auth_header="<api key>")  # DO NOT COMMIT THIS
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(pathlib.Path("fallback.avro"))
)

# Leaving the block sends the remaining points and closes the stream
with stream:
    for i in range(1_000):
        stream.enqueue("voltage", time.time_ns(), 12.0 + (i % 10) / 10)
```
:::
::::

Streams send to `https://api.gov.nominal.io/api` by default. To use another Nominal host, see [Stream options](guides/configure.md#stream-options).

## Next

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} Guides
:link: guides/index
:link-type: doc

Set batching, backpressure and the Nominal URL, keep files of what you stream, and turn on logging.
:::

:::{grid-item-card} Reference
:link: ref/index
:link-type: doc

The Rust crate and the Python package.
:::
::::
