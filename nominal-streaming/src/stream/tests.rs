use super::*;

#[test]
#[should_panic(expected = "mismatched types")]
fn test_mismatched_array_types_panics() {
    // Protects the exhaustive match in SeriesBufferGuard::extend from being
    // silently simplified to a catch-all: pushing a DoubleArray and then a
    // StringArray to the same channel must panic at buffer merge time.
    //
    // Exercise the buffer directly under one lock. Between public enqueue
    // calls, a worker could flush the first array and prevent the mismatch.
    // This also avoids the stream's shutdown hang during panic without using
    // ManuallyDrop, which would leave its workers running.
    let buffer = SeriesBuffer::new(100);
    let mut guard = buffer.lock();
    let descriptor = ChannelDescriptor::new("mixed_array");
    guard.extend(
        &descriptor,
        vec![DoubleArrayPoint {
            timestamp: None,
            value: vec![1.0, 2.0],
        }],
    );
    guard.extend(
        &descriptor,
        vec![StringArrayPoint {
            timestamp: None,
            value: vec!["a".into()],
        }],
    );
}

#[derive(Debug)]
struct FailedConsumer;
impl WriteRequestConsumer for FailedConsumer {
    fn consume(&self, _: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        Err(crate::consumer::ConsumerError::RequestError(
            "upload failed".into(),
        ))
    }
}

#[test]
fn close_reports_delivery_failure_and_retains_it() {
    let mut stream =
        NominalDatasetStream::new_with_consumer(FailedConsumer, NominalStreamOpts::default());
    {
        let mut writer = stream.double_writer(ChannelDescriptor::new("buffered"));
        writer.push(Duration::from_secs(1), 42.0);
    }
    let first = stream.close().unwrap_err().to_string();
    assert!(first.contains("upload failed"));
    assert_eq!(first, stream.close().unwrap_err().to_string());
}

#[derive(Debug)]
struct FinishFailure;
impl WriteRequestConsumer for FinishFailure {
    fn consume(&self, _: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        Ok(())
    }
    fn finish(&self) -> crate::consumer::ConsumerResult<()> {
        Err(crate::consumer::ConsumerError::RequestError(
            "finish failed".into(),
        ))
    }
}

#[test]
fn close_reports_finish_failure_through_wrappers() {
    let consumer = ListeningWriteRequestConsumer::new(
        DualWriteRequestConsumer::new(FinishFailure, FinishFailure),
        vec![],
    );
    let mut stream =
        NominalDatasetStream::new_with_consumer(consumer, NominalStreamOpts::default());
    assert!(stream
        .close()
        .unwrap_err()
        .to_string()
        .contains("finish failed"));
}

#[derive(Debug)]
struct PanicOnceConsumer(Arc<AtomicUsize>);
impl WriteRequestConsumer for PanicOnceConsumer {
    fn consume(&self, _: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("consumer panic");
        }
        Ok(())
    }
}

#[test]
fn close_reports_panic_and_keeps_draining() {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let count = Arc::new(AtomicUsize::new(0));
        let mut stream = NominalDatasetStream::new_with_consumer(
            PanicOnceConsumer(count.clone()),
            NominalStreamOpts::default()
                .with_max_points_per_record(1)
                .with_request_dispatcher_tasks(1),
        );
        stream.enqueue(
            &ChannelDescriptor::new("points"),
            vec![
                DoublePoint {
                    timestamp: None,
                    value: 1.0
                };
                3
            ],
        );
        let error = stream.close().unwrap_err().to_string();
        tx.send((error, count.load(Ordering::SeqCst))).unwrap();
    });
    let (error, count) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("close hung after panic");
    assert!(error.contains("consumer panic"));
    assert_eq!(count, 3);
}

#[test]
fn close_drains_typed_writer_to_fallback_and_finishes_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fallback.avro");
    let file = AvroFileConsumer::new_with_full_path(&path, false, None).unwrap();
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(FailedConsumer, file),
        NominalStreamOpts::default(),
    );
    {
        let mut writer = stream.double_writer(ChannelDescriptor::new("buffered"));
        writer.push(Duration::from_secs(1), 42.0);
    }
    stream.close().unwrap();
    stream.close().unwrap();
    let records = apache_avro::Reader::new(std::fs::File::open(path).unwrap())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(records.len(), 1);
    let apache_avro::types::Value::Record(fields) = &records[0] else {
        panic!("expected record")
    };
    assert!(fields.iter().any(|(key, value)| key == "values"
        && *value
            == apache_avro::types::Value::Array(vec![apache_avro::types::Value::Union(
                0,
                Box::new(apache_avro::types::Value::Double(42.0))
            )])));
}

#[test]
fn close_reports_primary_and_fallback_failure() {
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(FailedConsumer, FailedConsumer),
        NominalStreamOpts::default(),
    );
    stream.enqueue(
        &ChannelDescriptor::new("point"),
        vec![DoublePoint {
            timestamp: None,
            value: 1.0,
        }],
    );
    assert!(stream
        .close()
        .unwrap_err()
        .to_string()
        .contains("upload failed"));
}

#[test]
fn close_observes_unexpected_worker_panic() {
    let mut stream =
        NominalDatasetStream::new_with_consumer(FailedConsumer, NominalStreamOpts::default());
    stream
        .workers
        .push(thread::spawn(|| panic!("batch worker failed")));
    assert!(stream
        .close()
        .unwrap_err()
        .to_string()
        .contains("batch worker failed"));
}

#[test]
#[should_panic(expected = "stream is closed")]
fn enqueue_after_close_is_rejected() {
    let mut stream =
        NominalDatasetStream::new_with_consumer(FailedConsumer, NominalStreamOpts::default());
    stream.close().unwrap();
    stream.enqueue(
        &ChannelDescriptor::new("late"),
        vec![DoublePoint {
            timestamp: None,
            value: 1.0,
        }],
    );
}

#[derive(Debug)]
struct MissingTokenConsumer;
impl WriteRequestConsumer for MissingTokenConsumer {
    fn consume(&self, _: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        Err(crate::consumer::ConsumerError::MissingTokenError)
    }
}

#[test]
fn missing_token_with_successful_fallback_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fallback.avro");
    let file = AvroFileConsumer::new_with_full_path(&path, false, None).unwrap();
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(MissingTokenConsumer, file),
        NominalStreamOpts::default(),
    );
    stream.enqueue(
        &ChannelDescriptor::new("point"),
        vec![DoublePoint {
            timestamp: Some(crate::prelude::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            value: 1.0,
        }],
    );
    stream.close().unwrap();
    assert_eq!(
        apache_avro::Reader::new(std::fs::File::open(path).unwrap())
            .unwrap()
            .count(),
        1
    );
}

#[derive(Debug)]
struct PanicFinish;
impl WriteRequestConsumer for PanicFinish {
    fn consume(&self, _: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        Ok(())
    }
    fn finish(&self) -> crate::consumer::ConsumerResult<()> {
        panic!("finish panicked")
    }
}
#[derive(Debug)]
struct CountFinish(Arc<AtomicUsize>);
impl WriteRequestConsumer for CountFinish {
    fn consume(&self, _: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        Ok(())
    }
    fn finish(&self) -> crate::consumer::ConsumerResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn finish_panic_still_finalizes_other_destination() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut stream = NominalDatasetStream::new_with_consumer(
        DualWriteRequestConsumer::new(PanicFinish, CountFinish(count.clone())),
        NominalStreamOpts::default(),
    );
    assert!(stream
        .close()
        .unwrap_err()
        .to_string()
        .contains("finish panicked"));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut fallback_stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(PanicFinish, CountFinish(count.clone())),
        NominalStreamOpts::default(),
    );
    assert!(fallback_stream.close().is_err());
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn primary_panic_is_preserved_by_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fallback.avro");
    let file = AvroFileConsumer::new_with_full_path(&path, false, None).unwrap();
    let mut stream = NominalDatasetStream::new_with_consumer(
        RequestConsumerWithFallback::new(PanicOnceConsumer(Arc::new(AtomicUsize::new(0))), file),
        NominalStreamOpts::default(),
    );
    stream.enqueue(
        &ChannelDescriptor::new("point"),
        vec![DoublePoint {
            timestamp: Some(crate::prelude::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            value: 1.0,
        }],
    );
    stream.close().unwrap();
    assert_eq!(
        apache_avro::Reader::new(std::fs::File::open(path).unwrap())
            .unwrap()
            .count(),
        1
    );
}
