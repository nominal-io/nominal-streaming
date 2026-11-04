//! Journal fallback: one append-only JSONL file per channel per stream, each with a manifest.
//!
//! `add_journal_json` imports one channel per file, so files cannot be shared across channels.
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
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
    files: Mutex<HashMap<String, File>>,
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
            if !files.contains_key(channel) {
                let file = self.create(files.len(), channel, &request.data_source_rid)?;
                files.insert(channel.to_owned(), file);
            }
            let file = files.get_mut(channel).expect("inserted above");
            let length = file.metadata()?.len();
            if let Err(error) = file.write_all(&rows).and_then(|()| file.sync_data()) {
                // Never leave a partial line behind; a later rescue appends to the same file.
                let _ = file.set_len(length);
                return Err(error);
            }
        }
        Ok(())
    }

    fn create(&self, index: usize, channel: &str, dataset_rid: &str) -> std::io::Result<File> {
        fs::create_dir_all(&self.directory)?;
        let name = format!("{}-{index}.jsonl", self.stem);
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(self.directory.join(&name))?;
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
        Ok(file)
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
        // A stream without a journal accepts these keys, so a rescue can still meet them.
        if value.args.contains_key(MESSAGE_KEY) || value.args.contains_key(TIMESTAMP_KEY) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "journal keys MESSAGE and __REALTIME_TIMESTAMP conflict with log arguments",
            ));
        }
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
