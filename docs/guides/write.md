# Write points

{.lead}
A point is a timestamp and a value on a channel. Write points one at a time as your program produces them, or in batches. The stream sends them to Nominal in the background.

The Rust examples use a `stream` built as in the [quickstart](../index.md#4-stream-values). The Python examples use a `stream` opened there, inside its `with` block.

## Write values

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

Each channel gets a writer for its value type:

```rust
use nominal_streaming::prelude::*;

let mut voltage = stream.double_writer(ChannelDescriptor::new("voltage"));
let mut state = stream.string_writer(ChannelDescriptor::new("state"));

for i in 0..100 {
    let now = chrono::Utc::now();
    voltage.push(now, 12.0 + (i % 10) as f64 / 10.0);
    state.push(now, if i < 10 { "idle" } else { "running" });
}
```

| Values | Writer | `push` takes |
| --- | --- | --- |
| Floating point | `double_writer` | `f64` |
| Signed integers | `integer_writer` | `i64` |
| Unsigned integers | `uint64_writer` | `u64` |
| Text | `string_writer` | anything that converts to `String` |
| Structs | `struct_writer` | the struct as JSON text |
| Arrays | `double_array_writer`, `string_array_writer` | `Vec<f64>`; strings as any iterator |

A writer holds its points until it has `max_points_per_record` of them, or until `max_request_delay` has passed when you next push, and then passes them to the stream. Dropping a writer passes on the rest, so drop a writer when its channel is done, and drop every writer before the stream.
:::

:::{tab-item} Python
:sync: python

`enqueue` writes one value. Its type sets the point's type: `float`, `int` or `str`.

```python
import time

for i in range(100):
    now = time.time_ns()
    stream.enqueue("voltage", now, 12.0 + (i % 10) / 10)
    stream.enqueue("state", now, "idle" if i < 10 else "running")
```

`enqueue_struct`, `enqueue_float_array` and `enqueue_string_array` write structs and arrays.
:::
::::

## Timestamps

Use the time each value was measured, not the time you write it.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

`push` takes a `chrono::DateTime`, a `std::time::Duration` since the Unix epoch, or an `i64` count of nanoseconds since the Unix epoch.
:::

:::{tab-item} Python
:sync: python

Timestamps can be timezone-aware `datetime` values, ISO 8601 strings, or `int` nanoseconds since the Unix epoch. Strings and `datetime` values lose precision below a microsecond: for nanoseconds, use integers.
:::
::::

## Write several values at once

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

Writers batch for you, so push each value as it arrives.
:::

:::{tab-item} Python
:sync: python

`enqueue_from_dict` writes one value to each of several channels at one timestamp, and `enqueue_batch` writes many points to one channel:

```python
now = time.time_ns()
stream.enqueue_from_dict(now, {"voltage": 12.1, "current": 0.52, "state": "running"})

timestamps = [now + i * 1_000_000 for i in range(1_000)]  # 1 kHz
stream.enqueue_batch("vibration", timestamps, [0.01 * (i % 50) for i in range(1_000)])
```

`enqueue_batch` treats every value as the first value's type. It also takes one-dimensional NumPy arrays, which it copies without making a Python object per value. Install NumPy with `pip install "nominal-streaming[numpy]"`, or use the one your program already has:

```python
import numpy as np

timestamps = np.array([1_700_000_000_000_000_000, 1_700_000_000_000_000_001], dtype="uint64")
stream.enqueue_batch("temperature", timestamps, np.array([1.25, 2.5], dtype="float64"))
```
:::
::::

## Structs and arrays

A struct point holds named values, such as a GPS fix. An array point holds a list of numbers or strings at one time, such as a spectrum.

::::{tab-set}
:sync-group: lang

:::{tab-item} Rust
:sync: rust

A struct is written as JSON text:

```rust
let now = chrono::Utc::now();

let mut fix = stream.struct_writer(ChannelDescriptor::new("gps.fix"));
fix.push(now, r#"{"lat": 34.05, "lon": -118.25, "satellites": 9}"#);

let mut spectrum = stream.double_array_writer(ChannelDescriptor::new("vibration.spectrum"));
spectrum.push(now, vec![0.01, 0.4, 0.02]);

let mut faults = stream.string_array_writer(ChannelDescriptor::new("faults.active"));
faults.push(now, ["E07", "W12"]);
```
:::

:::{tab-item} Python
:sync: python

A struct is a `dict` of JSON values: strings, numbers, booleans, `None`, lists and dicts.

```python
now = time.time_ns()
stream.enqueue_struct("gps.fix", now, {"lat": 34.05, "lon": -118.25, "satellites": 9})
stream.enqueue_float_array("vibration.spectrum", now, [0.01, 0.4, 0.02])
stream.enqueue_string_array("faults.active", now, ["E07", "W12"])
```
:::
::::

## Tag points

Tags tell apart points in the same channel, such as two motors on one test stand. Tag with a small set of values, such as component, vehicle or test configuration, not with values that change every point.

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

## Watch points arrive

In a workbook on the dataset's asset or run, click **GO LIVE** in the bottom bar to follow new points as the stream sends them. It isn't available once the workbook's runs have ended. See [Workbooks](https://dev.nominal.io/platform/workbooks/).
