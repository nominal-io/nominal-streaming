# Stream many units to one dataset

{.lead}
Test units one after another on one tester, and stream every unit's readings to the tester's dataset, each point tagged with the unit's serial number. This is the [single dataset for many identical assets](https://dev.nominal.io/platform/data-model/organization/tagged-data/) approach.

In Nominal, each unit's asset then adds the tester's dataset filtered to its own tag value, so each asset shows only its unit's points. Every point needs the tag: a point without it shows on no unit's asset.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```shell
cargo add nominal-streaming nominal
cargo add tokio --features full
```

```rust
use std::time::{Duration, UNIX_EPOCH};

use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

#[tokio::main]
async fn main() {
    // Load the API key and base URL that API Keys Setup saved
    let config = nominal::Config::load().expect("failed to read the Nominal config file");
    let profile = config.get_profile("default").expect("no profile named default");

    let token = BearerToken::new(profile.token()).expect("invalid API key");
    let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>").expect("invalid dataset RID");

    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_core(token, dataset, tokio::runtime::Handle::current())
        .with_options(NominalStreamOpts::default().with_base_api_url(profile.base_url()))
        .with_file_fallback("fallback.avro")
        .build();

    // 50 points per unit, 100 ms apart
    let mut timestamp = UNIX_EPOCH.elapsed().unwrap();
    for serial in ["SN-001", "SN-002", "SN-003"] {
        // Each unit's points carry its serial number
        let mut current = stream.double_writer(ChannelDescriptor::with_tags("supply_current", [("UUT", serial)]));
        for i in 0..50 {
            current.push(timestamp, 1.2 + f64::from(i % 5) / 100.0);
            timestamp += Duration::from_millis(100);
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
pip install nominal-streaming nominal
```

```python
import time

from nominal.config import NominalConfig
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

# Load the API key and base URL that API Keys Setup saved
profile = NominalConfig.from_yaml().get_profile("default")

stream = NominalDatasetStream(
    auth_header=profile.token,
    opts=PyNominalStreamOpts(base_api_url=profile.base_url),
).with_core_consumer("<ri.catalog.instance.dataset.identifier>")

with stream:
    # 50 points per unit, 100 ms apart
    timestamp = time.time_ns()
    for serial in ["SN-001", "SN-002", "SN-003"]:
        # Each unit's points carry its serial number
        for i in range(50):
            stream.enqueue("supply_current", timestamp, 1.2 + (i % 5) / 100, tags={"UUT": serial})
            timestamp += 100_000_000
```
:::
::::
