# Streaming logs

`nominal_streaming::log::NominalLogStream` and Python's `NominalLogStream` stream timestamped messages with per-event string arguments. They use the existing columnar protobuf channel-writer endpoint with zstd level 1 compression. They do not treat logs as telemetry string channels.

## Python

```python
from pathlib import Path
from nominal_streaming import NominalLogStream, PyNominalLogStreamOpts

opts = PyNominalLogStreamOpts(
    base_api_url="https://api.gov.nominal.io/api",
    max_batch_bytes=16 * 1024 * 1024,
    max_buffered_bytes=64 * 1024 * 1024,
)
with (
    NominalLogStream("YOUR_TOKEN", opts)
    .with_core_consumer("YOUR_DATASET_RID")
    .with_file_fallback(Path("log-backup"))
) as stream:
    stream.enqueue("application", 1_789_392_441_123_456_789, "Started", args={"service": "api"})
    stream.enqueue_batch(
        "application",
        [1_789_392_442_123_456_789, 1_789_392_442_123_456_790],
        ["Connected", "Ready"],
        args={"service": "api"},
        per_record_args=[{"peer": "one"}, {"peer": "two"}],
    )
    stats = stream.flush()
    print(stats.acknowledged_records, stats.backed_up_records, stats.last_error)
```

Arguments supplied through legacy `tags=` or the fourth positional argument are log arguments. `tags` and `args` cannot both be provided. Common batch arguments are merged with per-record arguments; the per-record value wins. Batch operations cross into native code once, with validation before acceptance.

Integer timestamps are signed Unix nanoseconds. Timezone-aware datetimes and explicit-timezone ISO 8601 strings with up to nine fractional digits are supported. Integers are the preferred high-throughput input. Naive datetimes are rejected rather than guessing a timezone. This API intentionally omits telemetry array/struct enqueue methods.

## Rust

```rust,no_run
use std::collections::HashMap;
use nominal_streaming::log::{LogRecord, NominalLogStream};
use nominal_streaming::prelude::{BearerToken, ResourceIdentifier};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let runtime = tokio::runtime::Runtime::new()?;
let token = BearerToken::new(std::env::var("NOMINAL_TOKEN")?)?;
let dataset = ResourceIdentifier::new(std::env::var("NOMINAL_DATASET_RID")?)?;
let stream = NominalLogStream::builder()
    .stream_to_core(token, dataset, runtime.handle().clone())
    .with_file_fallback("log-backup")
    .build()?;

let writer = stream.writer("application", HashMap::from([("service".into(), "api".into())]));
writer.push(1_789_392_441_123_456_789, "Started")?;
stream.enqueue_batch("application", vec![
    LogRecord::new(1_789_392_442_123_456_789, "Ready", HashMap::new()),
])?;
let stats = stream.close()?;
assert_eq!(stats.failed_records, 0);
# Ok(())
# }
```

Supply a multi-thread Tokio runtime with I/O and timers enabled, and keep it alive until the stream is closed. Current-thread runtimes are rejected because uploader threads cannot drive their I/O. The stream's enqueue and close methods are synchronous; call them from a blocking context if used inside an async application. Python owns its uploader runtime automatically.

## Delivery and resource limits

- `enqueue` acknowledges acceptance into bounded memory. It is not a disk or backend durability guarantee; process crashes can lose buffered records.
- Defaults: 16 MiB accounted bytes or 50,000 records per request, 64 MiB total accounted bytes, four upload workers, and a 250 ms maximum buffering delay. These are client resource settings, not API limits.
- Accounted bytes conservatively include records, strings, argument maps and framing. The bound covers pending, queued, in-flight and retained failed data. It is not an exact RSS ceiling: caller-owned inputs, codec/request scratch buffers, runtimes and allocator overhead also consume memory.
- A record exceeding the request budget or an input batch exceeding the total memory budget is rejected before any of that call is accepted. Split large caller batches explicitly. When previously accepted work occupies the budget, producers block; Python releases the GIL during this wait.
- Buffering delay is a dispatch target under available capacity, not an end-to-end visibility SLA. Slow uploads and retry waits apply backpressure. Concurrent uploads do not preserve request order.
- `flush()` blocks new enqueue calls while draining accepted work. `close()` stops acceptance, wakes blocked producers, drains and joins upload workers. Both report fatal delivery/preservation errors.
- Python `close(wait=False)` stops acceptance and drains in a background native thread. Follow with `close(wait=True)` to wait and inspect the outcome. It does not cancel or deliberately discard accepted records. Explicit close is required before interpreter/process exit when delivery matters.
- Stats distinguish accepted, acknowledged, backed-up and unpreserved failed records. `last_error` reports the most recent delivery/preservation error even if backup succeeded. A successful flush can therefore include backed-up records; it does not mean all records reached Core.

## Retry and backup

The stream makes an initial attempt plus at most three retries. Transport failures and HTTP 408, 429, 500, 502, 503 and 504 are retryable. Other HTTP statuses go directly to backup. There is one retry loop, with no additional reqwest retry layer.

Default exponential backoff starts at 100 ms and caps at 5 seconds. Each HTTP attempt has a 30 second total timeout. Retry-After seconds and HTTP dates are honored; a server delay exceeding the configured 30 second retry-wait limit sends the batch to backup instead of retrying earlier than the server requested. Request bytes are encoded/compressed once and shared across attempts.

**A positively acknowledged batch is never written to backup.** After retry exhaustion or a non-retryable error, only the unconfirmed batch is written to journal files. A request whose acknowledgement was lost can already exist remotely; retries or later recovery may duplicate it. This tradeoff favors preservation and requires no exactly-once protocol.

Journal writes flush and sync before completion is reported. Disk errors stop admission, surface through flush/close, and leave unpreserved batches owned by the stream. After correcting the disk problem, `save_failed(other_directory)` can rescue them. Do not discard the stream after a failed close unless you accept losing those retained records. Partial multi-channel backup/rescue may leave some completed segments before an error; inspect manifests to avoid unnecessary re-import.

File-only operation is available through `.stream_to_file(directory)` in Rust or `.to_file(directory)` in Python. It cannot be combined with a Core target; that prevents accidentally configuring a duplicate write of every acknowledged record.

## Journal files and recovery

Each failed channel batch creates `logs-*.jsonl` and a matching `logs-*.json` manifest in the configured directory. Channel names never become filesystem paths. The manifest carries dataset RID (empty in file-only mode), channel, record count, timestamp column, and timestamp unit. A `.jsonl.partial` file is not a completed segment and must not be submitted without inspecting/repairing its complete-record boundary. Files are never appended to or automatically deleted/imported.

The JSONL shape uses `MESSAGE`, `__REALTIME_TIMESTAMP`, and top-level string argument fields. **Timestamp values use nanoseconds and require the manifest's explicit timestamp metadata**, rather than journald's default microsecond interpretation. `MESSAGE` and `__REALTIME_TIMESTAMP` argument-key collisions are rejected before acceptance when journaling is configured. Rescue also rejects those collisions if the original live-only stream accepted them.

For each finalized segment, ordinary SDK ingestion can use:

```python
import json
from pathlib import Path
from nominal.core import NominalClient
from nominal.core.dataset_file import IngestStatus

client = NominalClient.from_profile("YOUR_PROFILE")
manifest_path = Path("log-backup/logs-EXAMPLE.json")
metadata = json.loads(manifest_path.read_text())
dataset = client.get_dataset(metadata["dataset_rid"])
file = dataset.add_journal_json(
    manifest_path.parent / metadata["file"],
    channel=metadata["channel"],
    timestamp_column=metadata["timestamp_column"],
    timestamp_type=metadata["timestamp_type"],
)
file.poll_until_ingestion_completed()
assert file.ingest_status is IngestStatus.SUCCESS
```

Retain the local segment until terminal success is verified. Importing it again can create duplicates. Backend-added internal ingest metadata is normal bookkeeping. Journal files are uncompressed JSONL for compatibility; zstd here describes wire content encoding, not a claim that `.jsonl.zst` file ingestion is supported.
