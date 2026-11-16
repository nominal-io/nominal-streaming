# Stream files

{.lead}
A stream can optionally write every point it streams to an Avro file. 

This is different from a [fallback file](fallback.md), which only stores data that couldn't reach Nominal servers.

A stream can be configured to optionally transmit data to Nominal and _either_ write all data to disk _or_ store failed samples in a fallback file.

## Write every point to a file

This section covers the full redundancy case: stream to Nominal and also write all data to a file.

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

// Send to Nominal, and write every point to all-data-redundant.avro
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_options(NominalStreamOpts::default().with_base_api_url(profile.base_url()))
    .stream_to_file("all-data-redundant.avro")
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

# Send to Nominal, and write every point to all-data-redundant.avro
stream = (
    NominalDatasetStream(auth_header=profile.token, opts=PyNominalStreamOpts(base_api_url=profile.base_url))
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>") #send to Nominal
    .to_file(Path("all-data-redundant.avro"))
)
```
:::
::::

Once the stream is configured, all of the normal [write](write.md) methods work on the stream.

## Disabling network transmission

To record with no network, write only to a file. This requires no API key or dataset RID, but you must [upload the file](#upload-a-file-to-nominal) later.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use nominal_streaming::stream::NominalDatasetStreamBuilder;

// Write only to the file, with no API key or network
let offline = NominalDatasetStreamBuilder::new().stream_to_file("offline.avro").build();
```
:::

:::{tab-item} Python
:sync: python

```python
import time
from pathlib import Path

from nominal_streaming import NominalDatasetStream

# Write only to the file, with no API key or network
with NominalDatasetStream().to_file(Path("offline.avro")) as offline:
    offline.enqueue("engine.rpm", time.time_ns(), 3000.0)
```
:::
::::


## Upload a file to Nominal

Nominal ingests these files directly. Upload one with nomctl, installed when you [configure your API keys](../api-keys-setup.md) or with a Nominal client:

::::{tab-set}
:sync-group: lang

:::{tab-item} nomctl

```shell
nomctl --profile default ingest avro-stream --dataset <ri.catalog.instance.dataset.identifier> fallback_20260101_120000.avro
```

To ingest into a new dataset instead, replace `--dataset` and the RID with `--name "<dataset name>"`.
:::

:::{tab-item} Rust
:sync: rust

With the `nominal` crate:

```rust
use nominal::core::AvroStreamIngest;

let client = nominal::NominalClient::from_profile("default")?;
let (job, _) = client
    .ingest()
    .upload_avro_stream("fallback_20260101_120000.avro", "<ri.catalog.instance.dataset.identifier>", AvroStreamIngest::new())
    .await?;
client.ingest().wait_for_ingest_job(job.rid()).await?;
```
:::

:::{tab-item} Python
:sync: python

With the `nominal` package:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<ri.catalog.instance.dataset.identifier>")
dataset.add_avro_stream("fallback_20260101_120000.avro").poll_until_ingestion_completed()
```
:::
::::


## What's in a file

A stream file is an [Avro](https://avro.apache.org/docs/) object container file, compressed with Snappy. Each `record` in the file holds one batch of points for one channel and set of tags:

| Field | Type | Holds |
| --- | --- | --- |
| `channel` | string | The channel name |
| `timestamps` | array of long | Each point's time, in nanoseconds since the Unix epoch |
| `values` | array | Each point's value, as typed by the streaming library |
| `tags` | map of strings | The points' tags |

:::{warning}
Avro has no unsigned 64-bit integers, so `u64` values are stored by casting the value to `i64`.
:::


::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

This example uses [apache-avro](https://docs.rs/apache-avro), added with `cargo add apache-avro --features snappy`.

To simplify handling of records, this example also uses [serde](https://docs.rs/serde), added with `cargo add serde --features derive`:

```rust
use std::collections::HashMap;

use apache_avro::Reader;

// A point's value: a number or string, or a record holding an array or a struct.
// Long comes before Double so that longs aren't read as floats.
#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum PointValue {
    Long(i64),
    Double(f64),
    String(String),
    DoubleArray { items: Vec<f64> },
    StringArray { items: Vec<String> },
    JsonStruct { json: String },
}

#[derive(serde::Deserialize)]
struct Batch {
    channel: String,
    tags: HashMap<String, String>,
    timestamps: Vec<i64>,
    values: Vec<PointValue>,
}

let file = std::fs::File::open("fallback_20260101_120000.avro")?;
for record in Reader::new(file)? {
    let batch: Batch = apache_avro::from_value(&record?)?;
    for (timestamp, value) in batch.timestamps.iter().zip(&batch.values) {
        println!("{} {:?} {timestamp} {value:?}", batch.channel, batch.tags);
    }
}
```
:::

:::{tab-item} Python
:sync: python

This example uses [fastavro](https://fastavro.readthedocs.io), installed with `pip install "fastavro[snappy]"`:

```python
from fastavro import reader

with open("fallback_20260101_120000.avro", "rb") as f:
    for record in reader(f):
        # Arrays come back as {"items": [...]}, and structs as {"json": "..."}
        for timestamp, value in zip(record["timestamps"], record["values"]):
            print(record["channel"], record["tags"], timestamp, value)
```
:::
::::