# Stream logs

{.lead}
A log stream sends timestamped messages to log channels in an existing dataset.

:::{note}
While similar in structure, logs use a different API from other data streams.
:::

## Send log messages

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

A log stream's calls block, so in async code, call them from a blocking context. The log stream uploads on the multi-thread tokio runtime you give it, so keep that runtime until {rust:method}`close <NominalLogStream::close>` returns.

```rust
use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use nominal_streaming::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the API key and base URL that API Keys Setup saved
    let config = nominal::Config::load()?;
    let profile = config.get_profile("default").ok_or("no profile named default")?;

    let mut opts = NominalLogStreamOpts::default();
    opts.base_api_url = profile.base_url().to_string();

    // The stream sends its requests on this runtime
    let runtime = tokio::runtime::Runtime::new()?;
    let stream = NominalLogStream::builder()
        .stream_to_core(
            BearerToken::new(profile.token())?,
            ResourceIdentifier::new("<ri.catalog.instance.dataset.identifier>")?,
            runtime.handle().clone(),
        )
        .with_options(opts)
        .with_file_fallback("log-backup")
        .build()?;

    // Every message from this writer gets the engine_id=left argument
    let left_engine_logger = stream.log_writer("engine", HashMap::from([("engine_id".to_string(), "left".to_string())]));
    left_engine_logger.push(UNIX_EPOCH.elapsed()?, "started")?;
    // things happen in real life
    left_engine_logger.push(UNIX_EPOCH.elapsed()?, "running")?;

    let stats = stream.close()?;
    println!("{} acknowledged", stats.acknowledged_records);
    Ok(())
}
```

{rust:method}`push <NominalLogWriter::push>` takes the same timestamp types as a [dataset writer's `push`](write.md#write-values). To give each message its own arguments (e.g. to change a tag for `severity`), enqueue a {rust:struct}`LogRecord`:

```rust
let args = HashMap::from([("level".to_string(), "WARN".to_string())]);
// 2026-01-01T12:00:00Z, in nanoseconds since the Unix epoch
stream.enqueue("engine", LogRecord::new(1_767_268_800_000_000_000, "Temperature above target", args))?;
```
:::

:::{tab-item} Python
:sync: python

```python
from pathlib import Path

from nominal.config import NominalConfig
from nominal_streaming import NominalLogStream, PyNominalLogStreamOpts

# Load the API key and base URL that API Keys Setup saved
profile = NominalConfig.from_yaml().get_profile("default")

opts = PyNominalLogStreamOpts(base_api_url=profile.base_url)
with (
    NominalLogStream(profile.token, opts)
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
    .with_file_fallback(Path("log-backup"))
) as stream:
    stream.enqueue("engine", "2026-01-01T12:00:00Z", "started", args={"engine_id": "left"})
    stream.enqueue("engine", 1_767_268_801_000_000_000, "running", args={"engine_id": "left", "phase": "test"})
    print(stream.flush().acknowledged_records)
```

Any of these types may be used as a timestamp:

- an `int` count of nanoseconds since the Unix epoch
- a `datetime` with a time zone
- an ISO 8601 string with a time zone and at most nine fractional digits

Messages, argument names and argument values must all be strings. `tags` is another name for `args`.
:::
::::

One stream can write to several log channels. Name the channel in each call.

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

With a fallback directory, messages that can't be delivered are saved there instead of dropped. With no network, record only to disk in place of the Nominal target. Either way the destination is a directory, with a JSON Lines journal and a manifest for each log channel.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
// Record only to disk, with no API key or network
let offline = NominalLogStream::builder().stream_to_file("log-records").build()?;
```
:::

:::{tab-item} Python
:sync: python

```python
from pathlib import Path

from nominal_streaming import NominalLogStream

# Record only to disk, with no API key or network
with NominalLogStream().to_file(Path("log-records")) as offline:
    offline.enqueue("engine", "2026-01-01T12:00:00Z", "started")
```
:::
::::

## Options

Set the Nominal URL, batch sizes and the memory the stream may use with {rust:struct}`NominalLogStreamOpts` in Rust, or {py:class}`~nominal_streaming.PyNominalLogStreamOpts` in Python. When the buffered messages reach `max_buffered_bytes`, 64 MiB by default, writes wait.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
let mut opts = NominalLogStreamOpts::default();
opts.base_api_url = profile.base_url().to_string();
opts.max_buffered_bytes = 32 * 1024 * 1024;
let builder = NominalLogStream::builder().with_options(opts);
```
:::

:::{tab-item} Python
:sync: python

```python
from nominal_streaming import NominalLogStream, PyNominalLogStreamOpts

opts = PyNominalLogStreamOpts(base_api_url=profile.base_url, max_buffered_bytes=32 * 1024 * 1024)
stream = NominalLogStream(profile.token, opts)
```
:::
::::
