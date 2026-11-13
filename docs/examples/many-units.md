# Stream many units to one dataset

{.lead}
Test units one after another on one tester, and stream every unit's readings to the tester's dataset, each point tagged with the unit's serial number. This is the [single dataset for many identical assets](https://dev.nominal.io/platform/data-model/organization/tagged-data/) approach.

In Nominal, each unit's asset then adds the tester's dataset filtered to its own tag value, so each asset shows only its unit's points. Every point needs the tag: a point without it shows on no unit's asset.

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

#[tokio::main]
async fn main() {
    let token = BearerToken::new("<api key>").expect("invalid API key"); // DO NOT COMMIT THIS
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>").expect("invalid dataset RID");

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, tokio::runtime::Handle::current())
        .with_options(NominalStreamOpts::default().with_base_api_url("<base url>"))
        .with_file_fallback("fallback.avro")
        .build();

    for serial in ["SN-001", "SN-002", "SN-003"] {
        // Each unit's points carry its serial number
        let mut current = stream.double_writer(ChannelDescriptor::with_tags("supply_current", [("UUT", serial)]));
        for i in 0..50 {
            current.push(chrono::Utc::now(), 1.2 + f64::from(i % 5) / 100.0);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        drop(current); // the unit's test is done
    }

    drop(stream); // waits until every point has been sent
}
```
:::

:::{tab-item} Python
:sync: python

```shell
pip install nominal-streaming
```

```python
import time

from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

stream = NominalDatasetStream(
    auth_header="<api key>",  # DO NOT COMMIT THIS
    opts=PyNominalStreamOpts(base_api_url="<base url>"),
).with_core_consumer("<ri.catalog.instance.dataset.identifier>")

with stream:
    for serial in ["SN-001", "SN-002", "SN-003"]:
        # Each unit's points carry its serial number
        for i in range(50):
            stream.enqueue("supply_current", time.time_ns(), 1.2 + (i % 5) / 100, tags={"UUT": serial})
            time.sleep(0.1)
```
:::
::::
