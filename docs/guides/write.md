# Write points

{.lead}
A point is a timestamp and a value on a [channel](https://dev.nominal.io/platform/data-model/#channels), in a [time series](https://dev.nominal.io/platform/data-model/#time-series-data). Write points as your program produces them and the stream will send them to Nominal.

## Stream setup

Every streaming program starts by creating a stream like the one below. A stream writes to one dataset, which you name by its RID. [Stream configuration](configure.md) covers the stream's other options.

:::{note}
To create datasets as your program needs them, use the Nominal client for [Rust](https://dev.nominal.io/rust-client/) or [Python](https://dev.nominal.io/python/).
:::

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
    .try_build()?;
```
:::

:::{tab-item} Python
:sync: python

```python
from nominal.config import NominalConfig
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

# Load the API key and base URL that API Keys Setup saved
profile = NominalConfig.from_yaml().get_profile("default")

stream = (
    NominalDatasetStream(auth_header=profile.token, opts=PyNominalStreamOpts(base_api_url=profile.base_url))
    .with_core_consumer("<ri.catalog.instance.dataset.identifier>")
)
stream.open()  # call stream.close() when done
```
:::
::::

## Write values

A value can be a number, a string, a struct of named values such as a GPS fix, or an array of numbers or strings such as a spectrum.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

In Rust, configure a strongly-typed `writer` for each channel and use that `writer` to push data to Nominal.

```rust
use std::time::UNIX_EPOCH;

use nominal_streaming::prelude::*;

let mut voltage = stream.double_writer(ChannelDescriptor::new("voltage"));
let mut state = stream.string_writer(ChannelDescriptor::new("state"));

// Push each reading with the time it was measured
let now = UNIX_EPOCH.elapsed().unwrap();
voltage.push(now, 12.1);
state.push(now, "running");
```

| Values | Writer | `push` takes |
| --- | --- | --- |
| Floating point | {rust:method}`double_writer <NominalDatasetStream::double_writer>` | `f64` |
| Signed integers | {rust:method}`integer_writer <NominalDatasetStream::integer_writer>` | `i64` |
| Unsigned integers | {rust:method}`uint64_writer <NominalDatasetStream::uint64_writer>` | `u64` |
| Text | {rust:method}`string_writer <NominalDatasetStream::string_writer>` | anything that converts to `String` |
| Structs | {rust:method}`struct_writer <NominalDatasetStream::struct_writer>` | the struct as JSON text |
| Floating-point arrays | {rust:method}`double_array_writer <NominalDatasetStream::double_array_writer>` | `Vec<f64>` |
| Text arrays | {rust:method}`string_array_writer <NominalDatasetStream::string_array_writer>` | any iterator of values that convert to `String` |



Use the time when each value was measured.  Any of these types may be used:

- a `std::time::Duration` since the Unix epoch, such as `UNIX_EPOCH.elapsed()`
- an `i64` count of nanoseconds since the Unix epoch
- a `chrono::DateTime`

:::

:::{tab-item} Python
:sync: python

In Python, write directly to the stream with the channel name and data as parameters.

```python
import time

# Enqueue each reading with the time it was measured
now = time.time_ns()
stream.enqueue("voltage", now, 12.1)
stream.enqueue("state", now, "running")
```

In addition to basic types, {py:meth}`~nominal_streaming.NominalDatasetStream.enqueue_struct`, {py:meth}`~nominal_streaming.NominalDatasetStream.enqueue_float_array` and {py:meth}`~nominal_streaming.NominalDatasetStream.enqueue_string_array` write dicts and arrays.

```python
stream.enqueue_struct("gps.fix", now, {"lat": 34.05, "lon": -118.25, "satellites": 9})
stream.enqueue_float_array("vibration.spectrum", now, [0.01, 0.4, 0.02])
stream.enqueue_string_array("faults.active", now, ["E07", "W12"])
```

Use the time when each value was measured. Any of these types may be used:

- an `int` count of nanoseconds since the Unix epoch, such as `time.time_ns()`
- a `datetime`
- a date and time string, such as ISO 8601

A `datetime` or string without a time zone is read as local time. Both keep at most microsecond precision, so for nanoseconds, use integers.
:::
::::


## Tag data points

[Tags](https://dev.nominal.io/platform/data-model/organization/tagged-data/) tell apart points in the same channel, such as two motors on one test stand.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

```rust
let mut left = stream.double_writer(ChannelDescriptor::with_tags("motor.temperature", [("motor_id", "left")]));
let mut right = stream.double_writer(ChannelDescriptor::with_tags("motor.temperature", [("motor_id", "right")]));
```
:::

:::{tab-item} Python
:sync: python

```python
stream.enqueue("motor.temperature", now, 295.0, tags={"motor_id": "left"})
stream.enqueue("motor.temperature", now, 297.0, tags={"motor_id": "right"})
```
:::
::::

An asset or a run in Nominal can then select one tag value.

## Write several values at once

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

Writers [batch for you](#batching-and-backpressure), so push each value as it arrives. 

If your points already come in batches, you can write a vector of `(channel, timestamp, value)` entries with {rust:method}`enqueue_many <NominalDatasetStream::enqueue_many>` or a vector of `(timestamp, value)` entries to a single `channel` with {rust:method}`enqueue <NominalDatasetStream::enqueue>`.

```rust
use std::time::{Duration, UNIX_EPOCH};

use nominal_streaming::prelude::*;
use nominal_streaming::types::IntoPoints;

let now = UNIX_EPOCH.elapsed().unwrap();

// One value on each of three channels, at one timestamp
let timestamp = Some(now.into_timestamp());
stream.enqueue_many(vec![
    (ChannelDescriptor::new("voltage"), vec![DoublePoint { timestamp, value: 12.1 }].into_points()),
    (ChannelDescriptor::new("current"), vec![DoublePoint { timestamp, value: 0.52 }].into_points()),
    (ChannelDescriptor::new("state"), vec![StringPoint { timestamp, value: "running".into() }].into_points()),
]);

// Many points on one channel: vibration readings taken 1 ms apart
let readings = [0.01, 0.4, 0.02];
let points: Vec<DoublePoint> = readings
    .iter()
    .enumerate()
    .map(|(i, &value)| DoublePoint {
        timestamp: Some((now + Duration::from_millis(i as u64)).into_timestamp()),
        value,
    })
    .collect();
stream.enqueue(&ChannelDescriptor::new("vibration"), points);
```
:::

:::{tab-item} Python
:sync: python

If your points already come in batches, you can write a dict of `{channel: value}` entries at one timestamp with {py:meth}`~nominal_streaming.NominalDatasetStream.enqueue_from_dict` or lists of timestamps and lists of values to a single `channel` with {py:meth}`~nominal_streaming.NominalDatasetStream.enqueue_batch`.

```python
now = time.time_ns()
stream.enqueue_from_dict(now, {"voltage": 12.1, "current": 0.52, "state": "running"})

timestamps = [now + i * 1_000_000 for i in range(1_000)]  # 1 kHz
stream.enqueue_batch("vibration", timestamps, [0.01 * (i % 50) for i in range(1_000)])
```

`enqueue_batch` will stream native floats _or_ strings, but can also accept 1D NumPy arrays:

```python
import numpy as np

timestamps = np.array([1_767_268_800_000_000_000, 1_767_268_800_001_000_000], dtype="uint64")
stream.enqueue_batch("temperature", timestamps, np.array([1.25, 2.5], dtype="float64"))
```
:::
::::



## Batching and backpressure

The purpose of this library is to stream data as efficiently as possible to Nominal. 

One way to increase network utilization is to batch small requests together. At one extreme, each small data chunk transmitted to the server is outweighed by the protocol that carries it. At the other extreme, data never leaves your device and latency increases. The goal is in between, keeping latency acceptable while reducing protocol overhead to a manageable level. This concept is batching and users can adjust both the size of the batch and the maximum allowable latency.

Backpressure is feedback from the network indicating that it is overloaded. This is transparently handled at many layers with the end result being that transmitting onto an overloaded network get slower and slower, eventually failing. This library handles that by queueing up a limited number of outbound requests, which helps handle transient conditions, and then blocking further writes to avoid memory consumption increasing without bounds. The size of this internal queue can be configured. If your application regularly hits this blocking mode, consider [writing directly to disk](files.md#write-every-point-to-a-file).

To adjust these mechanisms, use the [Batching and backpressure options](configure.md#batching-and-backpressure) on stream creation.

## Watch points arrive

In a workbook on the dataset's asset or run, click **GO LIVE** in the bottom bar to follow new points as the stream sends them. See [Workbooks](https://dev.nominal.io/platform/workbooks/).
