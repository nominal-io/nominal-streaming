//! Journal fallback: one append-only JSONL file per channel per stream, each with a manifest.
//!
//! `add_journal_json` imports one channel per file, so files cannot be shared across channels.
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use parking_lot::Mutex;

pub(super) const MESSAGE_KEY: &str = "MESSAGE";
pub(super) const TIMESTAMP_KEY: &str = "__REALTIME_TIMESTAMP";
static JOURNAL_ID: AtomicU64 = AtomicU64::new(0);

pub(super) struct Journal {
    directory: PathBuf,
    stem: String,
    // Paths, not open files, so a stream with many channels does not hold many descriptors.
    files: Mutex<HashMap<String, PathBuf>>,
    // Never reuse a file name, even when creating a file failed partway.
    created: AtomicUsize,
}

impl Journal {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            directory: directory.into(),
            stem: format!(
                "logs-{}-{started}-{}",
                std::process::id(),
                JOURNAL_ID.fetch_add(1, Ordering::Relaxed)
            ),
            files: Mutex::new(HashMap::new()),
            created: AtomicUsize::new(0),
        }
    }

    /// Append every batch in the request, syncing each file before returning.
    pub fn append(&self, request: &wire::WriteBatchesRequest) -> std::io::Result<()> {
        let batches = request
            .batches
            .iter()
            .map(|batch| Ok((batch.channel.as_str(), rows(batch)?)))
            .collect::<std::io::Result<Vec<_>>>()?;
        let mut files = self.files.lock();
        for (channel, rows) in batches {
            let path = match files.entry(channel.to_owned()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.create(channel, &request.data_source_rid)?)
                }
            };
            let mut file = OpenOptions::new().append(true).open(path)?;
            let length = file.metadata()?.len();
            if let Err(error) = file.write_all(&rows).and_then(|()| file.sync_data()) {
                // Never leave a partial line behind; later batches append to the same file.
                let _ = file.set_len(length);
                return Err(error);
            }
        }
        Ok(())
    }

    fn create(&self, channel: &str, dataset_rid: &str) -> std::io::Result<PathBuf> {
        fs::create_dir_all(&self.directory)?;
        let index = self.created.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}-{index}.jsonl", self.stem);
        let path = self.directory.join(&name);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let manifest = serde_json::json!({
            "version": 1, "file": name, "dataset_rid": dataset_rid, "channel": channel,
            "timestamp_column": TIMESTAMP_KEY, "timestamp_type": "epoch_nanoseconds",
        });
        let mut metadata = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.directory.join(format!("{}-{index}.json", self.stem)))?;
        serde_json::to_writer(&mut metadata, &manifest)?;
        metadata.write_all(b"\n")?;
        metadata.sync_all()?;
        #[cfg(unix)]
        File::open(&self.directory)?.sync_all()?;
        tracing::debug!(file = %name, channel, "Created log journal");
        Ok(path)
    }
}

fn rows(batch: &wire::RecordsBatch) -> std::io::Result<Vec<u8>> {
    let points = batch.points.as_ref().expect("constructed log batch");
    let Some(wire::points::Points::LogPoints(logs)) = &points.points else {
        unreachable!("log streams only build log points")
    };
    let mut out = Vec::new();
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
        row.insert(MESSAGE_KEY.into(), value.message.clone().into());
        row.insert(TIMESTAMP_KEY.into(), nanos.to_string().into());
        serde_json::to_writer(&mut out, &row)?;
        out.push(b'\n');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::batch::LogBatch;
    use crate::log::batch::LogRecordSize;
    use crate::log::LogRecord;

    #[test]
    fn failed_file_creation_does_not_block_later_appends() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path());
        let record = LogRecord::new(0, "message", HashMap::new());
        let mut batch = LogBatch::default();
        batch.push("a", record.clone(), LogRecordSize::new(&record), 1);
        let request = batch.into_request("fixture");
        // A directory where the first manifest goes makes the first creation fail partway.
        let blocker = dir.path().join(format!("{}-0.json", journal.stem));
        fs::create_dir(&blocker).unwrap();
        assert!(journal.append(&request).is_err());
        fs::remove_dir(&blocker).unwrap();
        journal.append(&request).unwrap();
    }
}
