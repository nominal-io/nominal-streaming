use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use prost::Message;

use super::transport::LogTransport;
use super::*;

struct ScriptedTransport {
    attempts: AtomicUsize,
    failures: usize,
    requests: Mutex<Vec<wire::WriteBatchesRequest>>,
}

impl LogTransport for ScriptedTransport {
    fn send(&self, body: &bytes::Bytes) -> Result<(), String> {
        let request =
            wire::WriteBatchesRequest::decode(zstd::decode_all(body.as_ref()).unwrap().as_slice())
                .unwrap();
        self.requests.lock().unwrap().push(request);
        if self.attempts.fetch_add(1, Ordering::Relaxed) < self.failures {
            Err("scripted delivery error".into())
        } else {
            Ok(())
        }
    }
}

fn target(failures: usize) -> Arc<ScriptedTransport> {
    Arc::new(ScriptedTransport {
        attempts: AtomicUsize::new(0),
        failures,
        requests: Mutex::new(Vec::new()),
    })
}

fn record(message: &str) -> LogRecord {
    LogRecord::new(1_789_392_441_123_456_789, message, HashMap::new())
}

#[test]
fn file_only_flush_preserves_nanoseconds_and_message() {
    let dir = tempfile::tempdir().unwrap();
    let stream = NominalLogStream::builder()
        .stream_to_file(dir.path())
        .build()
        .unwrap();
    stream.enqueue("app", record("café 🚀\nnext line")).unwrap();
    let stats = stream.flush().unwrap();
    assert_eq!(stats.accepted_records, 1);
    assert_eq!(stats.backed_up_records, 1);
    assert_eq!(stats.acknowledged_records, 0);
    let files = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect::<Vec<_>>();
    let jsonl = files
        .iter()
        .find(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .unwrap();
    let line: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(jsonl).unwrap()).unwrap();
    assert_eq!(line["__REALTIME_TIMESTAMP"], "1789392441123456789");
    assert_eq!(line["MESSAGE"], "café 🚀\nnext line");
    stream.close().unwrap();
    assert!(matches!(
        stream.enqueue("app", record("late")),
        Err(LogStreamError::Closed)
    ));
}

#[test]
fn invalid_options_and_reserved_keys_fail_before_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let opts = NominalLogStreamOpts {
        max_batch_bytes: 0,
        ..Default::default()
    };
    assert!(NominalLogStream::builder()
        .stream_to_file(dir.path())
        .with_options(opts)
        .build()
        .is_err());
    let stream = NominalLogStream::builder()
        .stream_to_file(dir.path())
        .build()
        .unwrap();
    let mut bad = record("bad");
    bad.args.insert("MESSAGE".into(), "collision".into());
    assert!(stream
        .enqueue_batch("app", vec![record("valid"), bad])
        .is_err());
    assert_eq!(stream.stats().accepted_records, 0);
    stream.close().unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn byte_and_record_limits_rotate_batches_and_timer_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let opts = NominalLogStreamOpts {
        max_records_per_batch: 2,
        max_request_delay: Duration::from_millis(10),
        ..Default::default()
    };
    let stream = NominalLogStream::builder()
        .stream_to_file(dir.path())
        .with_options(opts)
        .build()
        .unwrap();
    stream
        .enqueue_batch("app", vec![record("a"), record("b"), record("c")])
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while stream.stats().backed_up_records < 3 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(stream.stats().backed_up_records, 3);
    stream.close().unwrap();
    let count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|p| {
            p.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|e| e == "jsonl")
        })
        .count();
    assert_eq!(count, 2);
}

#[test]
fn confirmed_delivery_never_creates_backup() {
    let dir = tempfile::tempdir().unwrap();
    let backup = dir.path().join("should-not-exist");
    let transport = target(0);
    let stream = NominalLogStream::start(
        NominalLogStreamOpts::default(),
        Some(transport.clone()),
        "dataset".into(),
        Some(backup.clone()),
    )
    .unwrap();
    stream.enqueue("a", record("delivery")).unwrap();
    stream.enqueue("b", record("second channel")).unwrap();
    let stats = stream.close().unwrap();
    assert_eq!(stats.accepted_records, 2);
    assert_eq!(stats.acknowledged_records, 2);
    assert_eq!(stats.requests, 1);
    assert_eq!(stats.backed_up_records, 0);
    assert!(!backup.exists());
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].batches.len(), 2);
    assert!(requests[0].batches.iter().all(|b| b.tags.is_empty()));
}

#[test]
fn failed_delivery_preserves_records_and_reports_delivery_error() {
    let dir = tempfile::tempdir().unwrap();
    let stream = NominalLogStream::start(
        NominalLogStreamOpts::default(),
        Some(target(usize::MAX)),
        "dataset".into(),
        Some(dir.path().into()),
    )
    .unwrap();
    stream.enqueue("a", record("backup")).unwrap();
    let stats = stream.close().unwrap();
    assert_eq!(stats.requests, 1);
    assert_eq!(stats.backed_up_records, 1);
    assert_eq!(stats.acknowledged_records, 0);
    assert_eq!(stats.failed_records, 0);
    assert_eq!(stats.buffered_bytes, 0);
    assert_eq!(stats.last_error.as_deref(), Some("scripted delivery error"));
}

#[test]
fn concurrent_producers_upload_every_record_once() {
    let transport = target(0);
    let stream = NominalLogStream::start(
        NominalLogStreamOpts {
            max_records_per_batch: 73,
            max_request_delay: Duration::from_millis(1),
            num_upload_workers: 4,
            ..NominalLogStreamOpts::default()
        },
        Some(transport.clone()),
        "dataset".into(),
        None,
    )
    .unwrap();
    std::thread::scope(|scope| {
        for producer in 0..4 {
            let stream = &stream;
            scope.spawn(move || {
                for sequence in 0..1_000 {
                    stream
                        .enqueue("a", record(&format!("{producer}:{sequence}")))
                        .unwrap();
                }
            });
        }
    });
    let stats = stream.close().unwrap();
    assert_eq!(stats.accepted_records, 4_000);
    assert_eq!(stats.acknowledged_records, 4_000);
    let mut messages = std::collections::HashSet::new();
    let requests = transport.requests.lock().unwrap();
    for request in requests.iter() {
        for batch in &request.batches {
            let points = batch.points.as_ref().unwrap();
            let Some(wire::points::Points::LogPoints(logs)) = &points.points else {
                panic!("expected logs");
            };
            assert_eq!(points.timestamps.len(), logs.points.len());
            for point in &logs.points {
                assert!(messages.insert(point.value.as_ref().unwrap().message.clone()));
            }
        }
    }
    assert_eq!(messages.len(), 4_000);
}

#[test]
fn mixed_delivery_backs_up_only_the_unconfirmed_batch() {
    let dir = tempfile::tempdir().unwrap();
    let stream = NominalLogStream::start(
        NominalLogStreamOpts {
            num_upload_workers: 1,
            max_records_per_batch: 1,
            ..NominalLogStreamOpts::default()
        },
        Some(target(1)),
        "dataset".into(),
        Some(dir.path().into()),
    )
    .unwrap();
    stream.enqueue("a", record("unconfirmed")).unwrap();
    stream.enqueue("a", record("acknowledged")).unwrap();
    let stats = stream.close().unwrap();
    assert_eq!(stats.accepted_records, 2);
    assert_eq!(stats.acknowledged_records, 1);
    assert_eq!(stats.backed_up_records, 1);
    assert_eq!(stats.failed_records, 0);
    let rows: Vec<serde_json::Value> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .flat_map(|path| {
            std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect::<Vec<serde_json::Value>>()
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["MESSAGE"], "unconfirmed");
}

#[test]
fn disk_failure_is_observable_and_retained_records_can_be_rescued() {
    let unusable = tempfile::NamedTempFile::new().unwrap();
    let stream = NominalLogStream::builder()
        .stream_to_file(unusable.path())
        .build()
        .unwrap();
    stream.enqueue("a", record("must survive")).unwrap();
    assert!(stream.close().is_err());
    assert_eq!(stream.stats().failed_records, 1);
    assert!(stream.stats().buffered_bytes > 0);
    let rescue = tempfile::tempdir().unwrap();
    let stats = stream.save_failed(rescue.path()).unwrap();
    assert_eq!(stats.failed_records, 0);
    assert_eq!(stats.backed_up_records, 1);
    assert_eq!(stats.buffered_bytes, 0);
    assert!(stream.close().is_ok());
}

#[test]
fn oversized_record_and_batch_are_rejected_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let options = NominalLogStreamOpts {
        max_batch_bytes: 512,
        max_buffered_bytes: 512,
        ..Default::default()
    };
    let stream = NominalLogStream::builder()
        .with_options(options)
        .stream_to_file(dir.path())
        .build()
        .unwrap();
    assert!(stream.enqueue("a", record(&"x".repeat(513))).is_err());
    assert!(stream
        .enqueue_batch("a", vec![record("one"), record("two")])
        .is_err());
    assert_eq!(stream.close().unwrap().accepted_records, 0);
}

#[test]
fn backpressure_counts_inflight_bytes_and_close_wakes_blocked_producer() {
    struct Blocked {
        started: crossbeam_channel::Sender<()>,
        finish: crossbeam_channel::Receiver<()>,
    }
    impl LogTransport for Blocked {
        fn send(&self, _body: &bytes::Bytes) -> Result<(), String> {
            self.started.send(()).unwrap();
            self.finish.recv().unwrap();
            Ok(())
        }
    }
    let (started_tx, started_rx) = crossbeam_channel::bounded(1);
    let (finish_tx, finish_rx) = crossbeam_channel::bounded(1);
    let options = NominalLogStreamOpts {
        max_batch_bytes: 512,
        max_buffered_bytes: 512,
        max_records_per_batch: 1,
        num_upload_workers: 1,
        ..NominalLogStreamOpts::default()
    };
    let stream = NominalLogStream::start(
        options,
        Some(Arc::new(Blocked {
            started: started_tx,
            finish: finish_rx,
        })),
        "dataset".into(),
        None,
    )
    .unwrap();
    stream.enqueue("a", record("one")).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    std::thread::scope(|scope| {
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let stream_ref = &stream;
        scope.spawn(move || {
            done_tx
                .send(stream_ref.enqueue("a", record("two")))
                .unwrap();
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert_eq!(stream.stats().accepted_records, 1);
        stream.stop_accepting_writes();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(LogStreamError::Closed)
        ));
        finish_tx.send(()).unwrap();
    });
    assert_eq!(stream.close().unwrap().acknowledged_records, 1);
}

#[test]
fn partial_rescue_keeps_accounting_and_never_overwrites_reserved_arguments() {
    let options = NominalLogStreamOpts {
        max_records_per_batch: 1,
        num_upload_workers: 1,
        ..NominalLogStreamOpts::default()
    };
    let stream =
        NominalLogStream::start(options, Some(target(usize::MAX)), "dataset".into(), None).unwrap();
    let mut collision = record("real message");
    collision
        .args
        .insert("MESSAGE".into(), "user argument".into());
    stream
        .enqueue_batch("a", vec![collision, record("rescuable")])
        .unwrap();
    assert!(stream.close().is_err());
    let dir = tempfile::tempdir().unwrap();
    assert!(stream.save_failed(dir.path()).is_err());
    assert_eq!(stream.stats().backed_up_records, 1);
    assert_eq!(stream.stats().failed_records, 1);
    assert!(stream.stats().buffered_bytes > 0);
}

#[test]
fn serialized_limit_splits_unicode_and_arguments_independently_of_memory_budget() {
    let transport = target(0);
    let opts = NominalLogStreamOpts {
        max_request_bytes: 512,
        ..NominalLogStreamOpts::default()
    };
    let stream =
        NominalLogStream::start(opts, Some(transport.clone()), "fixture".into(), None).unwrap();
    let records: Vec<_> = (0..40)
        .map(|i| {
            LogRecord::new(
                -1 - i,
                "🚀".repeat(30),
                HashMap::from([
                    (String::new(), String::new()),
                    ("属性".into(), "é".repeat(45)),
                ]),
            )
        })
        .collect();
    stream.enqueue_batch("channel", records).unwrap();
    assert_eq!(stream.close().unwrap().acknowledged_records, 40);
    let requests = transport.requests.lock().unwrap();
    assert!(requests.len() > 1);
    assert!(requests.iter().all(|r| r.encoded_len() <= 512));
    let mut nanos: Vec<_> = requests
        .iter()
        .flat_map(|r| &r.batches)
        .flat_map(|b| &b.points.as_ref().unwrap().timestamps)
        .map(|ts| {
            assert_eq!(ts.seconds, Some(-1));
            ts.nanos.unwrap()
        })
        .collect();
    nanos.sort();
    assert_eq!(nanos, (999_999_960..1_000_000_000).collect::<Vec<_>>());
}

#[test]
fn serialized_oversized_singleton_rejects_entire_input() {
    let transport = target(0);
    let opts = NominalLogStreamOpts {
        max_request_bytes: 512,
        ..NominalLogStreamOpts::default()
    };
    let stream =
        NominalLogStream::start(opts, Some(transport.clone()), "fixture".into(), None).unwrap();
    assert!(stream
        .enqueue_batch("channel", vec![record("small"), record(&"x".repeat(512))])
        .is_err());
    assert_eq!(stream.close().unwrap().accepted_records, 0);
    assert!(transport.requests.lock().unwrap().is_empty());
}

#[test]
fn admission_reserves_encoding_capacity_before_accepting() {
    let input = record(&"x".repeat(2048));
    let record_only = input.accounted_bytes("a");
    let transport = target(0);
    let opts = NominalLogStreamOpts {
        max_batch_bytes: record_only,
        max_buffered_bytes: record_only,
        ..NominalLogStreamOpts::default()
    };
    let stream =
        NominalLogStream::start(opts, Some(transport.clone()), "fixture".into(), None).unwrap();
    assert!(stream.enqueue("a", input).is_err());
    assert_eq!(stream.close().unwrap().accepted_records, 0);
    assert!(transport.requests.lock().unwrap().is_empty());
}

#[test]
fn request_limit_includes_multiple_channels_and_dataset_envelope() {
    let transport = target(0);
    let opts = NominalLogStreamOpts {
        max_request_bytes: 512,
        max_request_delay: Duration::from_secs(60),
        ..NominalLogStreamOpts::default()
    };
    let stream =
        NominalLogStream::start(opts, Some(transport.clone()), "r".repeat(250), None).unwrap();
    for i in 0..80 {
        stream
            .enqueue(&format!("channel-{i}"), record("small"))
            .unwrap();
    }
    assert_eq!(stream.close().unwrap().acknowledged_records, 80);
    let requests = transport.requests.lock().unwrap();
    assert!(requests.len() > 1);
    assert!(requests.iter().all(|r| r.encoded_len() <= 512));
    assert_eq!(requests.iter().map(|r| r.batches.len()).sum::<usize>(), 80);
}

#[test]
fn encoder_rejects_oversize_before_encoding_and_preserves_roundtrip() {
    let mut batch = super::batch::Batch::default();
    let record = record(&"abcdef".repeat(200));
    let size = super::batch::RecordSize::new(&record);
    batch.push("channel", record, size, 0);
    let request = batch.into_request("fixture");
    let limit = request.encoded_len();
    assert!(transport::encode(&request, limit - 1).is_err());
    let body = transport::encode(&request, limit).unwrap();
    assert_eq!(
        request,
        wire::WriteBatchesRequest::decode(zstd::decode_all(body.as_ref()).unwrap().as_slice())
            .unwrap()
    );
}

#[test]
fn exactly_full_serialized_batch_dispatches_without_waiting_for_timer() {
    let input = record(&"x".repeat(600));
    let limit = super::batch::RecordSize::new(&input).singleton_len("channel", "fixture");
    let transport = target(0);
    let opts = NominalLogStreamOpts {
        max_request_bytes: limit,
        max_request_delay: Duration::from_secs(60),
        ..NominalLogStreamOpts::default()
    };
    let stream =
        NominalLogStream::start(opts, Some(transport.clone()), "fixture".into(), None).unwrap();
    stream.enqueue("channel", input).unwrap();
    for _ in 0..100 {
        if transport.attempts.load(Ordering::Relaxed) != 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let sent_without_close = transport.attempts.load(Ordering::Relaxed) != 0;
    stream.close().unwrap();
    assert!(sent_without_close);
}

#[test]
fn panicking_delivery_retains_records_and_flush_returns_error() {
    struct PanickingTransport;
    impl LogTransport for PanickingTransport {
        fn send(&self, _body: &bytes::Bytes) -> Result<(), String> {
            panic!("auth provider failed");
        }
    }

    let stream = Arc::new(
        NominalLogStream::start(
            NominalLogStreamOpts::default(),
            Some(Arc::new(PanickingTransport)),
            "fixture".into(),
            None,
        )
        .unwrap(),
    );
    stream.enqueue("app", record("preserve me")).unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let flushing = stream.clone();
    let thread = std::thread::spawn(move || done_tx.send(flushing.flush()).unwrap());
    assert!(done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_err());
    thread.join().unwrap();
    assert!(stream.enqueue("app", record("late")).is_err());
    let stats = stream.stats();
    assert_eq!(stats.failed_records, 1);
    assert_eq!(stats.acknowledged_records, 0);
    assert!(stats.buffered_bytes > 0);
    assert!(stream.close().is_err());

    let directory = tempfile::tempdir().unwrap();
    let stats = stream.save_failed(directory.path()).unwrap();
    assert_eq!(stats.backed_up_records, 1);
    assert_eq!(stats.failed_records, 0);
    assert_eq!(stats.buffered_bytes, 0);
    stream.close().unwrap();
}

#[test]
fn channel_writer_merges_common_arguments_with_record_overrides() {
    let target = target(0);
    let stream = NominalLogStream::start(
        NominalLogStreamOpts::default(),
        Some(target.clone()),
        "fixture".into(),
        None,
    )
    .unwrap();
    let writer = stream.log_writer("app", HashMap::from([("service".into(), "api".into())]));
    writer.push(1, "started").unwrap();
    writer
        .enqueue_batch(vec![LogRecord::new(
            2,
            "ready",
            HashMap::from([("service".into(), "worker".into())]),
        )])
        .unwrap();
    assert_eq!(stream.close().unwrap().acknowledged_records, 2);
    let requests = target.requests.lock().unwrap();
    let messages: Vec<_> = requests
        .iter()
        .flat_map(|r| &r.batches)
        .flat_map(|batch| {
            assert_eq!(batch.channel, "app");
            assert!(batch.tags.is_empty());
            let Some(wire::points::Points::LogPoints(logs)) =
                &batch.points.as_ref().unwrap().points
            else {
                panic!("expected logs");
            };
            logs.points
                .iter()
                .map(|point| point.value.as_ref().unwrap())
        })
        .collect();
    let by_message: HashMap<_, _> = messages
        .iter()
        .map(|v| (v.message.as_str(), v.args["service"].as_str()))
        .collect();
    assert_eq!(by_message["started"], "api");
    assert_eq!(by_message["ready"], "worker");
}

#[test]
fn file_only_target_rejects_fallback_in_either_configuration_order() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    let fallback = directory.path().join("fallback");
    for builder in [
        NominalLogStream::builder()
            .stream_to_file(&file)
            .with_file_fallback(&fallback),
        NominalLogStream::builder()
            .with_file_fallback(&fallback)
            .stream_to_file(&file),
    ] {
        assert!(matches!(builder.build(), Err(LogStreamError::Invalid(_))));
    }
    assert!(!file.exists());
    assert!(!fallback.exists());
}

#[test]
fn public_builder_and_writer_accept_timeseries_timestamp_inputs() {
    use crate::prelude::NominalLogStreamBuilder;
    use crate::prelude::NominalLogStreamOpts;
    let directory = tempfile::tempdir().unwrap();
    let stream = NominalLogStreamBuilder::new()
        .with_options(NominalLogStreamOpts::default())
        .stream_to_file(directory.path())
        .build()
        .unwrap();
    let writer = stream.log_writer("app", HashMap::new());
    writer.push(-1_i64, "before epoch").unwrap();
    writer.push(Duration::from_nanos(1), "duration").unwrap();
    writer
        .push(chrono::DateTime::from_timestamp(1, 2).unwrap(), "datetime")
        .unwrap();
    assert!(writer
        .push(Duration::from_secs(10_000_000_000), "outside range")
        .is_err());
    assert!(writer
        .push(Duration::from_secs(u64::MAX), "overflow seconds")
        .is_err());
    assert!(writer.push(Duration::MAX, "overflow duration").is_err());
    assert_eq!(stream.close().unwrap().accepted_records, 3);
    let mut timestamps = Vec::new();
    for entry in std::fs::read_dir(directory.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "jsonl") {
            for line in std::fs::read_to_string(path).unwrap().lines() {
                let row: serde_json::Value = serde_json::from_str(line).unwrap();
                timestamps.push(
                    row["__REALTIME_TIMESTAMP"]
                        .as_str()
                        .unwrap()
                        .parse::<i64>()
                        .unwrap(),
                );
            }
        }
    }
    timestamps.sort();
    assert_eq!(timestamps, vec![-1, 1, 1_000_000_002]);
}
