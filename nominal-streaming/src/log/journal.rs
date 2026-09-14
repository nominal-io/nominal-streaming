//! Failed batches become independent, per-channel JSONL segments. Never append to an old segment.
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;

static SEGMENT_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn save(directory: &Path, request: &wire::WriteBatchesRequest) -> std::io::Result<()> {
    // Rescue may be invoked for a stream that originally had no journal target and therefore
    // accepted arbitrary argument keys. Reject before writing any part of that request.
    for batch in &request.batches {
        if let Some(wire::points::Points::LogPoints(logs)) =
            batch.points.as_ref().and_then(|p| p.points.as_ref())
        {
            if logs
                .points
                .iter()
                .filter_map(|p| p.value.as_ref())
                .any(|v| {
                    v.args.contains_key("MESSAGE") || v.args.contains_key("__REALTIME_TIMESTAMP")
                })
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "journal keys MESSAGE and __REALTIME_TIMESTAMP conflict with log arguments",
                ));
            }
        }
    }
    fs::create_dir_all(directory)?;
    for batch in &request.batches {
        let points = batch.points.as_ref().expect("constructed log batch");
        let Some(wire::points::Points::LogPoints(logs)) = &points.points else {
            unreachable!()
        };
        let id = SEGMENT_ID.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let stem = format!("logs-{}-{now}-{id}", std::process::id());
        let pending = directory.join(format!("{stem}.jsonl.partial"));
        let final_path = directory.join(format!("{stem}.jsonl"));
        let mut file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&pending)?,
        );
        for (timestamp, point) in points.timestamps.iter().zip(&logs.points) {
            let value = point.value.as_ref().expect("constructed log value");
            let nanos = timestamp.seconds.unwrap_or_default() as i128 * 1_000_000_000
                + timestamp.nanos.unwrap_or_default() as i128;
            let mut row = serde_json::Map::with_capacity(value.args.len() + 2);
            row.extend(
                value
                    .args
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone()))),
            );
            row.insert("MESSAGE".into(), value.message.clone().into());
            row.insert("__REALTIME_TIMESTAMP".into(), nanos.to_string().into());
            serde_json::to_writer(&mut file, &row)?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        file.get_ref().sync_all()?;
        // Persist the import instructions before exposing a finalized JSONL segment.
        let manifest = serde_json::json!({
            "version": 1, "file": format!("{stem}.jsonl"), "dataset_rid": request.data_source_rid,
            "channel": batch.channel, "timestamp_column": "__REALTIME_TIMESTAMP",
            "timestamp_type": "epoch_nanoseconds", "records": logs.points.len(),
        });
        let mut metadata = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{stem}.json")))?;
        serde_json::to_writer(&mut metadata, &manifest)?;
        metadata.write_all(b"\n")?;
        metadata.sync_all()?;
        fs::rename(pending, final_path)?;
        #[cfg(unix)]
        fs::File::open(directory)?.sync_all()?;
    }
    Ok(())
}
