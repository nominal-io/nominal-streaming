# Stream logs

{.lead}
A log stream sends timestamped messages, each with string arguments such as a severity, to log channels in an existing dataset. It needs an API key and the dataset's RID.

## Send log messages

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

The log stream's calls block, so in async code, call them from a blocking context. It uploads on the multi-thread Tokio runtime you give it: keep the runtime until `close` returns.

```rust
use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use nominal_streaming::log::NominalLogStream;
use nominal_streaming::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let stream = NominalLogStream::builder()
        .stream_to_core(
            BearerToken::new("<api key>")?, // DO NOT COMMIT THIS
            ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>")?,
            runtime.handle().clone(),
        )
        .with_file_fallback("log-backup")
        .build()?;

    // Every message from this writer gets the engine=left argument
    let engine = stream.log_writer("engine", HashMap::from([("engine".to_string(), "left".to_string())]));
    engine.push(UNIX_EPOCH.elapsed()?, "started")?;
    engine.push(UNIX_EPOCH.elapsed()?, "running")?;

    let stats = stream.close()?;
    println!("{} acknowledged", stats.acknowledged_records);
    Ok(())
}
```

To give each message its own arguments, enqueue a `LogRecord`, whose timestamp is in nanoseconds since the Unix epoch:

```rust
use nominal_streaming::log::LogRecord;

let args = HashMap::from([("level".to_string(), "WARN".to_string())]);
stream.enqueue("engine", LogRecord::new(1_789_392_441_123_456_789, "Temperature above target", args))?;
```
:::

:::{tab-item} Python
:sync: python

```python
from pathlib import Path

from nominal_streaming import NominalLogStream

with (
    NominalLogStream("<api key>")  # DO NOT COMMIT THIS
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(Path("log-backup"))
) as stream:
    stream.enqueue("engine", "2026-09-14T12:00:00.123456789Z", "started", args={"engine": "left"})
    stream.enqueue("engine", 1_789_392_441_123_456_789, "running", args={"engine": "left", "phase": "test"})
    print(stream.flush().acknowledged_records)
```

Timestamps can be `int` nanoseconds since the Unix epoch, timezone-aware `datetime` values, or ISO 8601 strings with a timezone and up to nine fractional digits. Messages, argument names and argument values must all be strings. `tags` is another name for `args`.
:::
::::

One stream can write to several log channels: name the channel in each call.

## Check delivery

`flush` waits until every accepted message has been delivered or saved to the fallback directory, and returns the stream's counts. `close` stops new writes, does the same, and returns the counts. Both report a failure as an error.

| Count | Means |
| --- | --- |
| `accepted_records` | Messages the stream has taken |
| `acknowledged_records` | Messages Nominal has confirmed |
| `backed_up_records` | Messages saved to the fallback directory instead |
| `failed_records` | Messages neither delivered nor saved |

A message can reach Nominal without being confirmed. If the stream then saves it to the fallback directory too, uploading that file later duplicates it.

## Keep a copy on disk

With a fallback directory, messages that can't be delivered are saved there instead of dropped. To record only to disk, with no network, use `stream_to_file` in Rust or `to_file` in Python in place of the Nominal target. Either way the destination is a directory, with a JSON Lines journal for each log channel, and a manifest describing it.

## Options

Set the Nominal URL, batch sizes and the memory the stream may use with `NominalLogStreamOpts` in Rust, or `PyNominalLogStreamOpts` in Python. When the buffered messages reach `max_buffered_bytes`, 64 MiB by default, writes wait.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
use nominal_streaming::log::NominalLogStreamOpts;

let mut opts = NominalLogStreamOpts::default();
opts.base_api_url = "https://api.gov.nominal.io/api".to_string();
opts.max_buffered_bytes = 32 * 1024 * 1024;
let builder = NominalLogStream::builder().with_options(opts);
```
:::

:::{tab-item} Python
:sync: python

```python
from nominal_streaming import PyNominalLogStreamOpts

opts = PyNominalLogStreamOpts(base_api_url="https://api.gov.nominal.io/api", max_buffered_bytes=32 * 1024 * 1024)
stream = NominalLogStream("<api key>", opts)  # DO NOT COMMIT THIS
```
:::
::::
