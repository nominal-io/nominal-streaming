# Fallback file

{.lead}
A fallback file saves the points a stream couldn't send to Nominal. If one is generated, upload it to the same dataset when the network is back.

## Add a fallback file

Data that cannot be sent to Nominal due to a network disruption or saturation goes to a fallback file. This file is only created if data would otherwise be lost, so no file present means no data was dropped. 

The library takes a filename, but that filename is not used because the stream adds its start time (UTC) to the file name automatically. For example, `fallback.avro` becomes `fallback_20260101_120000.avro`.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStreamBuilder;

// Load the API key and base URL that API Keys Setup saved
let config = nominal::Config::load()?;
let profile = config.get_profile("default").ok_or("no profile named default")?;
let token = BearerToken::new(profile.token())?;
let dataset = ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>")?;

let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_options(NominalStreamOpts::default().with_base_api_url(profile.base_url()))
    .with_file_fallback("fallback.avro")
    .build();
```
:::

:::{tab-item} Python
:sync: python

```python
from pathlib import Path

from nominal.config import NominalConfig
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

# Load the API key and base URL that API Keys Setup saved
profile = NominalConfig.from_yaml().get_profile("default")

stream = (
    NominalDatasetStream(auth_header=profile.token, opts=PyNominalStreamOpts(base_api_url=profile.base_url))
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(Path("fallback.avro"))
)
```
:::
::::

## Upload the file

When the network is back, upload the file with nomctl, installed in step 2 of the [API Keys setup](../api-keys-setup.md).

```shell
nomctl --profile default ingest avro-stream --dataset <ri.catalog.instance.dataset.identifier> fallback_20260101_120000.avro
```

To upload it from Rust or Python, see [Upload a file to Nominal](files.md#upload-a-file-to-nominal).
