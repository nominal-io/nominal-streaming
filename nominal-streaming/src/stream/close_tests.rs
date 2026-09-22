use super::*;
use crate::consumer::ConsumerError;
use crate::consumer::ConsumerResult;

#[derive(Debug)]
struct CheckedConsumer {
    calls: Arc<AtomicUsize>,
    finishes: Arc<AtomicUsize>,
    panic: bool,
    fail_finish: bool,
}
impl WriteRequestConsumer for CheckedConsumer {
    fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "intentional consumer panic");
        Ok(())
    }
    fn finish(&self) -> ConsumerResult<()> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        if self.fail_finish {
            Err(ConsumerError::Configuration("finish failed".into()))
        } else {
            Ok(())
        }
    }
}
fn checked(
    panic: bool,
    fail_finish: bool,
) -> (NominalDatasetStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(AtomicUsize::new(0));
    let stream = NominalDatasetStream::new_with_consumer(
        CheckedConsumer {
            calls: calls.clone(),
            finishes: finishes.clone(),
            panic,
            fail_finish,
        },
        NominalStreamOpts::default()
            .with_max_points_per_record(1)
            .with_request_dispatcher_tasks(1),
    );
    (stream, calls, finishes)
}
fn points(n: usize) -> Vec<DoublePoint> {
    (0..n)
        .map(|i| DoublePoint {
            timestamp: Some((i as i64).into_timestamp()),
            value: i as f64,
        })
        .collect()
}
#[test]
fn close_joins_finalizes_once_and_is_sticky() {
    let (mut stream, calls, finishes) = checked(false, false);
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(7))
        .unwrap();
    let summary = stream.close().unwrap();
    assert_eq!(summary.accepted_points, 7);
    assert_eq!(summary.custom_consumer_points, 7);
    assert_eq!(summary.acknowledged_points, 0);
    assert_eq!(summary.unpreserved_points, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    assert_eq!(stream.close().unwrap(), summary);
    assert_eq!(stream.delivery_summary(), summary);
    assert!(stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(1))
        .is_err());
    drop(stream);
    assert_eq!(finishes.load(Ordering::SeqCst), 1);
}
#[test]
fn consumer_panics_drain_and_close_reports_unpreserved_points() {
    let (mut stream, calls, finishes) = checked(true, false);
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(7))
        .unwrap();
    let error = stream.close().unwrap_err();
    assert_eq!(error.summary.accepted_points, 7);
    assert_eq!(error.summary.unpreserved_points, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    assert_eq!(finishes.load(Ordering::SeqCst), 1);
    assert_eq!(stream.close().unwrap_err().summary, error.summary);
}
#[test]
fn finalization_failure_is_reported() {
    let (mut stream, _, _) = checked(false, true);
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(1))
        .unwrap();
    let error = stream.close().unwrap_err();
    assert!(error.to_string().contains("finish"));
    assert_eq!(error.summary.unpreserved_points, 1);
}

#[derive(Debug)]
struct FailFirst(AtomicUsize);
impl WriteRequestConsumer for FailFirst {
    fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(ConsumerError::Configuration("first request failed".into()))
        } else {
            Ok(())
        }
    }
}
fn wait_for_failure(stream: &NominalDatasetStream) {
    let mut state = stream.progress.state.lock();
    while !state.failed {
        assert!(
            !stream
                .progress
                .capacity
                .wait_for(&mut state, Duration::from_secs(2))
                .timed_out(),
            "consumer did not fail"
        );
    }
}
#[test]
fn accepted_writer_points_drain_after_admission_failure() {
    let mut stream = NominalDatasetStream::new_with_consumer(
        FailFirst(AtomicUsize::new(0)),
        NominalStreamOpts::default()
            .with_max_points_per_record(10)
            .with_request_dispatcher_tasks(1),
    );
    {
        let mut writer = stream.double_writer(ChannelDescriptor::new("writer"));
        writer.try_push(1, 42.0).unwrap();
        stream
            .try_enqueue(&ChannelDescriptor::new("trigger"), points(1))
            .unwrap();
        stream.primary_handle.unpark();
        wait_for_failure(&stream);
        assert!(writer.try_push(2, 43.0).is_err());
        writer.try_flush().unwrap();
    }
    let summary = stream.close().unwrap_err().summary;
    assert_eq!(summary.accepted_points, 2);
    assert_eq!(summary.custom_consumer_points, 1);
    assert_eq!(summary.unpreserved_points, 1);
}
#[test]
fn admitted_many_drains_every_chunk_after_failure() {
    let mut stream = NominalDatasetStream::new_with_consumer(
        FailFirst(AtomicUsize::new(0)),
        NominalStreamOpts::default()
            .with_max_points_per_record(1)
            .with_max_buffered_requests(0)
            .with_request_dispatcher_tasks(1),
    );
    stream
        .try_enqueue_many(
            (0..40)
                .map(|i| {
                    (
                        ChannelDescriptor::new(format!("x{i}")),
                        points(1).into_points(),
                    )
                })
                .collect(),
        )
        .unwrap();
    let summary = stream.close().unwrap_err().summary;
    assert_eq!(summary.accepted_points, 40);
    assert_eq!(summary.custom_consumer_points, 39);
    assert_eq!(summary.unpreserved_points, 1);
}
#[test]
fn file_fallback_reports_preserved_points_and_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fallback.avro");
    let file = AvroFileConsumer::new_with_full_path(&path, false, None).unwrap();
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(FailFirst(AtomicUsize::new(0)), file),
        NominalStreamOpts::default().with_request_dispatcher_tasks(1),
    );
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(7))
        .unwrap();
    let summary = stream.close().unwrap();
    assert_eq!(summary.file_points, 7);
    assert_eq!(summary.acknowledged_points, 0);
    assert_eq!(summary.custom_consumer_points, 0);
    assert_eq!(summary.unpreserved_points, 0);
    assert_eq!(summary.file_paths, vec![path.clone()]);
    assert!(summary.failures[0].contains("first request failed"));
    let records = apache_avro::Reader::new(std::fs::File::open(path).unwrap())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let count: usize = records
        .into_iter()
        .map(|record| {
            let apache_avro::types::Value::Record(fields) = record else {
                panic!("expected Avro record")
            };
            let (_, apache_avro::types::Value::Array(values)) =
                fields.into_iter().find(|(key, _)| key == "values").unwrap()
            else {
                panic!("expected point values")
            };
            values.len()
        })
        .sum();
    assert_eq!(count, 7);
}
#[test]
fn both_destinations_fail_with_bounded_diagnostics() {
    #[derive(Debug)]
    struct Fails(&'static str);
    impl WriteRequestConsumer for Fails {
        fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
            Err(ConsumerError::Configuration(self.0.into()))
        }
    }
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(Fails("primary failed"), Fails("fallback failed")),
        NominalStreamOpts::default().with_max_points_per_record(1),
    );
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(30))
        .unwrap();
    let summary = stream.close().unwrap_err().summary;
    assert_eq!(summary.unpreserved_points, 30);
    assert_eq!(summary.failures.len(), 16);
    assert!(summary
        .failures
        .iter()
        .any(|e| e.contains("primary failed")));
    assert!(summary
        .failures
        .iter()
        .any(|e| e.contains("fallback failed")));
}
#[test]
fn disconnected_dispatch_channel_reports_all_accepted_points() {
    let buffer = Arc::new(SeriesBuffer::new(1));
    buffer
        .lock()
        .extend(&ChannelDescriptor::new("x"), points(5));
    let progress = Progress::new();
    progress.state.lock().summary.accepted_points = 5;
    let (tx, rx) = crossbeam_channel::bounded(0);
    drop(rx);
    batch_processor(
        Arc::new(AtomicBool::new(false)),
        buffer,
        tx,
        Duration::ZERO,
        &progress,
        #[cfg(feature = "instrument")]
        Arc::new(AtomicU64::new(0)),
    );
    let state = progress.state.lock();
    assert!(state.worker_failed);
    assert_eq!(state.snapshot().unpreserved_points, 5);
}
#[test]
fn worker_failure_wakes_a_blocked_submitter() {
    let (stream, _, _) = checked(false, false);
    // A synthetic capacity wait isolates failure wakeup from consumer scheduling.
    std::thread::scope(|scope| {
        let progress = &stream.progress;
        let mut state = progress.state.lock();
        scope.spawn(move || progress.worker_failure("worker stopped"));
        while !state.worker_failed {
            assert!(!progress
                .capacity
                .wait_for(&mut state, Duration::from_secs(2))
                .timed_out());
        }
    });
    assert!(stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(1))
        .is_err());
}
#[test]
#[should_panic(expected = "request_dispatcher_tasks must be greater than zero")]
fn zero_dispatch_workers_are_rejected() {
    NominalDatasetStream::new_with_consumer(
        FailFirst(AtomicUsize::new(0)),
        NominalStreamOpts::default().with_request_dispatcher_tasks(0),
    );
}

#[test]
fn finish_panic_is_sticky_and_drop_does_not_panic() {
    #[derive(Debug)]
    struct PanicFinish;
    impl WriteRequestConsumer for PanicFinish {
        fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
            Ok(())
        }
        fn finish(&self) -> ConsumerResult<()> {
            panic!("finish panic")
        }
    }
    let mut stream =
        NominalDatasetStream::new_with_consumer(PanicFinish, NominalStreamOpts::default());
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(2))
        .unwrap();
    let error = stream.close().unwrap_err();
    assert!(error.message.contains("finish panic"));
    assert_eq!(error.summary.unpreserved_points, 2);
    assert_eq!(stream.close().unwrap_err().message, error.message);
    drop(stream);
}

#[test]
fn dual_write_failure_retains_acknowledgement_and_both_finish_errors() {
    #[derive(Debug)]
    struct Ack;
    impl WriteRequestConsumer for Ack {
        fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
            Ok(())
        }
        fn consume_delivery(&self, _: &WriteRequestNominal) -> ConsumerDelivery {
            ConsumerDelivery {
                acknowledged: true,
                ..Default::default()
            }
        }
        fn finish(&self) -> ConsumerResult<()> {
            Err(ConsumerError::Configuration("primary finish".into()))
        }
    }
    #[derive(Debug)]
    struct Fails;
    impl WriteRequestConsumer for Fails {
        fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
            Err(ConsumerError::Configuration("secondary write".into()))
        }
        fn finish(&self) -> ConsumerResult<()> {
            Err(ConsumerError::Configuration("secondary finish".into()))
        }
    }
    let mut stream = NominalDatasetStream::new_with_consumer(
        DualWriteRequestConsumer::new(Ack, Fails),
        NominalStreamOpts::default(),
    );
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(3))
        .unwrap();
    let error = stream.close().unwrap_err();
    assert_eq!(error.summary.acknowledged_points, 3);
    assert_eq!(error.summary.unpreserved_points, 0);
    for message in ["secondary write", "primary finish", "secondary finish"] {
        assert!(error.message.contains(message));
    }
}

#[test]
fn primary_panic_still_preserves_the_request_in_avro() {
    let dir = tempfile::tempdir().unwrap();
    let file =
        AvroFileConsumer::new_with_full_path(dir.path().join("panic.avro"), false, None).unwrap();
    let consumer = CheckedConsumer {
        calls: Arc::new(AtomicUsize::new(0)),
        finishes: Arc::new(AtomicUsize::new(0)),
        panic: true,
        fail_finish: false,
    };
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(consumer, file),
        NominalStreamOpts::default(),
    );
    stream
        .try_enqueue(&ChannelDescriptor::new("x"), points(3))
        .unwrap();
    let summary = stream.close().unwrap();
    assert_eq!(summary.file_points, 3);
    assert_eq!(summary.unpreserved_points, 0);
    assert!(summary
        .failures
        .iter()
        .any(|message| message.contains("panic")));
}

#[test]
fn consumer_destructor_panic_does_not_escape_stream_drop() {
    #[derive(Debug)]
    struct PanicDrop;
    impl WriteRequestConsumer for PanicDrop {
        fn consume(&self, _: &WriteRequestNominal) -> ConsumerResult<()> {
            Ok(())
        }
    }
    impl Drop for PanicDrop {
        fn drop(&mut self) {
            panic!("consumer destructor panic")
        }
    }
    let mut stream =
        NominalDatasetStream::new_with_consumer(PanicDrop, NominalStreamOpts::default());
    let error = stream.close().unwrap_err();
    assert!(error.message.contains("consumer destructor panic"));
    drop(stream);
}
