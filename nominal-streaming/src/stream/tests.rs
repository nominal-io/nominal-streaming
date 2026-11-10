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

#[test]
fn empty_batches_do_not_block_waiting_producers() {
    // A zero-point batch must not leave an entry in the buffer: the processor only flushes, and so
    // only notifies waiting producers, when the count is non-zero, so a producer waiting on a
    // buffer holding only empty entries would never be woken.
    let buffer = Arc::new(SeriesBuffer::new(100));
    buffer
        .lock()
        .extend(&ChannelDescriptor::new("empty"), Vec::<DoublePoint>::new());
    assert!(buffer.points.lock().is_empty());
    assert!(buffer.is_empty());

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn({
        let buffer = Arc::clone(&buffer);
        move || {
            buffer.on_notify(|mut guard| {
                guard.extend(
                    &ChannelDescriptor::new("value"),
                    vec![DoublePoint {
                        timestamp: None,
                        value: 1.0,
                    }],
                )
            });
            let _ = done_tx.send(());
        }
    });
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("on_notify waited on a buffer holding no points");
    assert_eq!(buffer.count(), 1);
}

fn core_builder(runtime: &tokio::runtime::Runtime) -> NominalDatasetStreamBuilder {
    NominalDatasetStreamBuilder::new().stream_to_core(
        BearerToken::new("unused").unwrap(),
        ResourceIdentifier::new("ri.catalog.main.dataset.00000000-0000-0000-0000-000000000000")
            .unwrap(),
        runtime.handle().clone(),
    )
}

#[test]
fn file_fallback_is_not_created_without_a_failed_request() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fallback.avro");

    let stream = core_builder(&runtime)
        .with_file_fallback(&path)
        .try_build()
        .unwrap();
    assert!(!path.exists());

    drop(stream);
    assert!(!path.exists());
}

#[test]
fn existing_file_fallback_is_left_untouched() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fallback.avro");
    std::fs::write(&path, b"undelivered points").unwrap();

    let stream = core_builder(&runtime)
        .with_file_fallback(&path)
        .try_build()
        .unwrap();
    drop(stream);

    assert_eq!(std::fs::read(&path).unwrap(), b"undelivered points");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn unusable_file_fallback_leaves_stream_to_file_untouched() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("data.avro");
    let parent = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(&file, b"previous data").unwrap();

    let result = NominalDatasetStreamBuilder::new()
        .stream_to_file(&file)
        .with_file_fallback(parent.path().join("fallback.avro"))
        .try_build();

    assert!(matches!(result, Err(StreamBuildError::File { .. })));
    assert_eq!(std::fs::read(&file).unwrap(), b"previous data");
}

#[test]
fn try_build_returns_error_when_file_fallback_cannot_be_opened() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let parent = tempfile::NamedTempFile::new().unwrap();

    let result = core_builder(&runtime)
        .with_file_fallback(parent.path().join("fallback.avro"))
        .try_build();

    assert!(matches!(result, Err(StreamBuildError::File { .. })));
}

#[test]
fn try_build_returns_error_when_stream_to_file_cannot_be_opened() {
    let parent = tempfile::NamedTempFile::new().unwrap();

    let result = NominalDatasetStreamBuilder::new()
        .stream_to_file(parent.path().join("data.avro"))
        .try_build();

    assert!(matches!(result, Err(StreamBuildError::File { .. })));
}

#[test]
fn try_build_returns_error_for_invalid_targets() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("data.avro");

    let no_target = NominalDatasetStreamBuilder::new().try_build();
    assert!(matches!(no_target, Err(StreamBuildError::Invalid(_))));

    let all_targets = core_builder(&runtime)
        .stream_to_file(&file)
        .with_file_fallback(directory.path().join("fallback.avro"))
        .try_build();
    assert!(matches!(all_targets, Err(StreamBuildError::Invalid(_))));
    assert!(!file.exists());
}

#[test]
#[should_panic(expected = "failed to open")]
fn build_panics_when_file_fallback_cannot_be_opened() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let parent = tempfile::NamedTempFile::new().unwrap();

    core_builder(&runtime)
        .with_file_fallback(parent.path().join("fallback.avro"))
        .build();
}
