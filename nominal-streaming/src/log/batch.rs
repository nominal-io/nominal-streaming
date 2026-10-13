//! Incremental wire sizing for the log-only WriteBatchesRequest shape.
//! Keep the framing tests in sync with the generated protobuf schema.
use std::collections::HashMap;
use std::time::Instant;

use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use nominal_api::tonic::nominal::types::time::Timestamp;
use prost::encoding;
use prost::Message;

use super::LogRecord;

// Every populated field in this request shape has a one-byte protobuf key.
fn framed(length: usize) -> usize {
    1 + encoding::encoded_len_varint(length as u64) + length
}

fn string(length: usize) -> usize {
    if length == 0 {
        0
    } else {
        framed(length)
    }
}

fn timestamp(nanos: i64) -> Timestamp {
    Timestamp {
        seconds: Some(nanos.div_euclid(1_000_000_000)),
        nanos: Some(nanos.rem_euclid(1_000_000_000)),
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct RecordSize {
    timestamps: usize,
    logs: usize,
}

impl RecordSize {
    pub fn new(record: &LogRecord) -> Self {
        let value = string(record.message.len())
            + encoding::hash_map::encoded_len(
                encoding::string::encoded_len,
                encoding::string::encoded_len,
                2,
                &record.args,
            );
        Self {
            timestamps: framed(timestamp(record.timestamp_ns).encoded_len()),
            logs: framed(framed(value)),
        }
    }

    fn channel_len(self, channel: &str) -> usize {
        framed(string(channel.len()) + framed(self.timestamps + framed(self.logs)))
    }

    pub fn singleton_len(self, channel: &str, dataset_rid: &str) -> usize {
        string(dataset_rid.len()) + self.channel_len(channel)
    }

    pub fn encoding_reservation(self, channel: &str, dataset_rid: &str) -> usize {
        let raw = self.singleton_len(channel, dataset_rid);
        // Covers raw protobuf plus zstd compressBound(raw), including its small-input
        // overhead. Summing singleton envelopes over-reserves shared batch framing.
        raw.saturating_mul(2)
            .saturating_add(raw / 128)
            .saturating_add(128)
    }

    fn plus(self, other: Self) -> Self {
        Self {
            timestamps: self.timestamps + other.timestamps,
            logs: self.logs + other.logs,
        }
    }
}

#[derive(Clone, Default)]
struct ChannelBatch {
    records: Vec<LogRecord>,
    size: RecordSize,
}

#[derive(Clone, Default)]
pub(super) struct Batch {
    channels: HashMap<String, ChannelBatch>,
    channel_bytes: usize,
    pub bytes: usize,
    pub count: usize,
    pub first_record: Option<Instant>,
}

impl Batch {
    pub fn encoded_len(&self, dataset_rid: &str) -> usize {
        string(dataset_rid.len()) + self.channel_bytes
    }

    pub fn encoded_len_after(&self, channel: &str, size: RecordSize, dataset_rid: &str) -> usize {
        let (old, updated) = self.channels.get(channel).map_or((0, size), |batch| {
            (batch.size.channel_len(channel), batch.size.plus(size))
        });
        self.encoded_len(dataset_rid) - old + updated.channel_len(channel)
    }

    pub fn push(
        &mut self,
        channel: &str,
        record: LogRecord,
        size: RecordSize,
        charged_bytes: usize,
    ) {
        if let Some(batch) = self.channels.get_mut(channel) {
            self.channel_bytes -= batch.size.channel_len(channel);
            batch.size = batch.size.plus(size);
            self.channel_bytes += batch.size.channel_len(channel);
            batch.records.push(record);
        } else {
            self.channel_bytes += size.channel_len(channel);
            self.channels.insert(
                channel.into(),
                ChannelBatch {
                    records: vec![record],
                    size,
                },
            );
        }
        self.bytes += charged_bytes;
        self.count += 1;
        self.first_record.get_or_insert_with(Instant::now);
    }

    pub fn into_request(self, dataset_rid: &str) -> wire::WriteBatchesRequest {
        let batches = self
            .channels
            .into_iter()
            .map(|(channel, batch)| {
                let mut timestamps = Vec::with_capacity(batch.records.len());
                let mut points = Vec::with_capacity(batch.records.len());
                for record in batch.records {
                    timestamps.push(timestamp(record.timestamp_ns));
                    points.push(wire::LogPoint {
                        value: Some(wire::LogValue {
                            message: record.message,
                            args: record.args,
                        }),
                    });
                }
                wire::RecordsBatch {
                    channel,
                    tags: HashMap::new(),
                    unit: None,
                    points: Some(wire::Points {
                        timestamps,
                        points: Some(wire::points::Points::LogPoints(wire::LogPoints { points })),
                    }),
                }
            })
            .collect();
        wire::WriteBatchesRequest {
            batches,
            data_source_rid: dataset_rid.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_lengths_match_prost_across_nested_boundaries() {
        let mut batch = Batch::default();
        let rid = "r".repeat(128);
        let mut reserved = 0;
        for i in 0..64 {
            let channel = ["a", "多字节", "another-channel"][i % 3];
            let len = [0, 1, 127, 128, 16383, 16384][i % 6];
            let record = LogRecord::new(
                [0, -1, i64::MIN, i64::MAX][i % 4],
                "x".repeat(len),
                HashMap::from([
                    (String::new(), String::new()),
                    ("key".into(), "🚀".repeat(i)),
                    ("".into(), "nonempty".into()),
                    ("empty-value".into(), String::new()),
                ]),
            );
            let size = RecordSize::new(&record);
            let predicted = batch.encoded_len_after(channel, size, &rid);
            reserved += size.encoding_reservation(channel, &rid);
            batch.push(channel, record, size, 0);
            let request = batch.clone().into_request(&rid);
            assert_eq!(predicted, request.encoded_len(), "insertion {i}");
            let raw = request.encode_to_vec();
            assert!(reserved >= raw.len() + zstd::zstd_safe::compress_bound(raw.len()));
        }
    }

    #[test]
    fn empty_map_entries_and_long_channel_envelopes_match_prost() {
        for length in [0, 127, 128, 16383, 16384] {
            let record = LogRecord::new(0, "", HashMap::from([(String::new(), String::new())]));
            let channel = "x".repeat(length);
            let size = RecordSize::new(&record);
            let mut batch = Batch::default();
            batch.push(&channel, record, size, 0);
            assert_eq!(
                size.singleton_len(&channel, ""),
                batch.into_request("").encoded_len()
            );
        }
    }
}
