# Stream files

{.lead}
A stream can write points to an Avro file: every point it streams, or only the points it couldn't send. Upload the file to Nominal later, or read it yourself.

The Rust examples use `token` and `dataset` as created in the [quickstart](../index.md#4-stream-values).

## Keep a fallback file

A fallback file holds the points of requests that failed after retries. It's created when a request first fails, so a stream that sends everything leaves no file. Its name gets the stream's start time (UTC), so each run writes its own file: `fallback.avro` becomes `fallback_20260101_120000.avro`.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .with_file_fallback("fallback.avro")
    .build();
```
:::

:::{tab-item} Python
:sync: python

```python
from pathlib import Path

from nominal_streaming import NominalDatasetStream

stream = (
    NominalDatasetStream(auth_header="<api key>")  # DO NOT COMMIT THIS
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(Path("fallback.avro"))
)
```
:::
::::

## Write every point to a file

A stream can write every point to a file, along with sending it to Nominal or in place of it. With no network, record to a file and upload it later. The file is created when the stream starts, and replaces any file already at that path.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
// Send to Nominal, and write every point to run-1.avro
let stream = NominalDatasetStreamBuilder::new()
    .stream_to_core(token, dataset, tokio::runtime::Handle::current())
    .stream_to_file("run-1.avro")
    .build();

// Write only to the file: no API key or network needed
let offline = NominalDatasetStreamBuilder::new().stream_to_file("run-2.avro").build();
```
:::

:::{tab-item} Python
:sync: python

```python
import time
from pathlib import Path

from nominal_streaming import NominalDatasetStream

# Write only to the file: no API key or network needed
with NominalDatasetStream().to_file(Path("run-2.avro")) as stream:
    stream.enqueue("engine.rpm", time.time_ns(), 3000.0)
```

To send to Nominal too, add `with_core_consumer` as in the quickstart.
:::
::::

A stream that sends to Nominal can have a file or a fallback file, not both.

## What's in a file

A stream file is an [Avro](https://avro.apache.org/docs/) object container file, compressed with Snappy. Each record holds one batch of points for one channel and set of tags:

| Field | Type | Holds |
| --- | --- | --- |
| `channel` | string | The channel name |
| `timestamps` | array of long | Each point's time, in nanoseconds since the Unix epoch |
| `values` | array | Each point's value: a double, string or long, or a record holding an array of doubles (`DoubleArray`), an array of strings (`StringArray`), or a struct as JSON text (`JsonStruct`) |
| `tags` | map of strings | The points' tags |

Avro has no unsigned 64-bit integers, so `u64` values are written as signed longs. When the stream also sends to Nominal, the file's metadata names the dataset under the key `nominal.dataset_rid`.

To read a file in Python, use an Avro library such as [fastavro](https://fastavro.readthedocs.io):

```python
from fastavro import reader

with open("fallback_20260101_120000.avro", "rb") as f:
    for record in reader(f):
        print(record["channel"], record["tags"], len(record["timestamps"]))
```

## Upload a file to Nominal

Nominal ingests these files directly. Upload one with nomctl, which [API Keys Setup](api-keys-setup.md) installs, or with a Nominal client:

::::{tab-set}
:sync-group: lang

:::{tab-item} nomctl

```shell
nomctl --profile default ingest avro-stream --dataset <ri.catalog.instance.dataset.identifier> fallback_20260101_120000.avro
```

To ingest into a new dataset instead, replace `--dataset <rid>` with `--name "<dataset name>"`.
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
