# Opt-in runtime metrics

Runtime metrics are disabled by default. Enable them per stream:

```rust
use nominal_streaming::stream::NominalStreamOpts;

let opts = NominalStreamOpts {
    track_metrics: true,
    ..Default::default()
};
// Pass opts to NominalDatasetStream::builder().with_options(opts).
```

```python
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

opts = PyNominalStreamOpts(track_metrics=True)
# Pass opts to NominalDatasetStream(auth_header, opts).
# NominalDatasetStream.create(..., track_metrics=True) is also supported.
```

`PyNominalStreamOpts.with_track_metrics(True)` provides the fluent option setter.
For a manually constructed `NominalCoreConsumer`, use `.with_track_metrics(true)`;
`new_with_consumer` does not configure arbitrary consumers.

## Inventory from nominal-client

The reference is the `experimental` Python backend in
`nominal/experimental/stream_v2/_write_stream.py`, with request calculations in
`nominal/core/_clientsbunch.py` (`RequestMetrics` and
`ProtoWriteService.write_nominal_batches_with_metrics`). All seven metrics are
untagged double-valued channels measured in **seconds**:

| Channel | Value | Point timestamp |
| --- | --- | --- |
| `enque_dict_start_staleness` | Wall time at dictionary enqueue start minus the supplied data timestamp | Supplied data timestamp |
| `enque_dict_end_staleness` | Wall time after dictionary enqueue minus the supplied data timestamp | Supplied data timestamp |
| `__nominal.metric.largest_latency_before_request` | Wall time before HTTP send minus the oldest data timestamp in the batch | Request completion time |
| `__nominal.metric.smallest_latency_before_request` | Wall time before HTTP send minus the newest data timestamp in the batch | Request completion time |
| `__nominal.metric.request_rtt` | Elapsed HTTP send time, including client retries | Request completion time |
| `__nominal.metric.largest_latency_after_request` | Wall time after HTTP send minus the oldest data timestamp in the batch | Request completion time |
| `__nominal.metric.smallest_latency_after_request` | Wall time after HTTP send minus the newest data timestamp in the batch | Request completion time |

The spelling `enque_dict` and the lack of a prefix on those two channels are
intentional compatibility details. Staleness/latency values may be negative for
future-dated data and are meaningful as live latency only for UTC data timestamps.
RTT uses a monotonic clock so system-clock adjustments do not make it negative.

## Emission and delivery

The two dictionary metrics are emitted by Python's `enqueue_from_dict`, including
the low-level binding, after successfully queuing the dictionary. They measure
conversion and enqueue/backpressure time in that binding. They use the ordinary
stream pipeline, so file-only streams also receive these two metrics. Individual
scalar, array, struct, and batch enqueue methods do not emit dictionary metrics.

The five request metrics are emitted by the Core consumer after a successful HTTP
send, with serialization and compression outside the measured interval. Timestamp
bounds cover all scalar, array, and struct point types, regardless of point order;
missing timestamps and the seven metric channels are excluded. Empty batches and
metric-only batches do not generate request metrics.

Completed request metrics piggyback on the next data request prepared for the same
session and dataset, preserving their original measurement timestamps. No additional
HTTP requests are sent for request metrics, including at idle or shutdown. The last
request's measurements are discarded if no later data request arrives.

The pending buffer is shared across dispatcher threads and consumer clones. It holds
at most 64 completed measurements (320 points), dropping the oldest on overflow.
Concurrent requests already in flight cannot carry measurements that complete later;
a subsequent data request takes all matching pending measurements. Encoding and
network I/O run outside the buffer lock. Metrics do not affect latency bounds or
feed back into the stream's batching queue.

Delivery is best-effort. Measurements attached to a request that fails encoding or
upload are discarded; failed data requests produce no new measurements. The original
user request remains unchanged for listeners, dual-write files, and file fallback.
Normal HTTP retries resend the same encoded data-plus-metrics payload. Request metrics
are not copied to files, and file-only streams have no HTTP request metrics. The two
dictionary metrics continue to use ordinary batching as described above.

The seven channel names are reserved for this feature. Metrics have no tags,
matching the reference implementation.
