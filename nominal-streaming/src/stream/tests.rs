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
fn fallback_builder_preserves_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fallback = dir.path().join("fallback.avro");
    std::fs::write(&fallback, b"existing data").unwrap();
    let result = std::panic::catch_unwind(|| {
        super::NominalDatasetStreamBuilder::new()
            .stream_to_core(
                conjure_object::BearerToken::new("test").unwrap(),
                conjure_object::ResourceIdentifier::new("ri.catalog.main.dataset.test").unwrap(),
                runtime.handle().clone(),
            )
            .with_file_fallback_overwrite(&fallback, false)
            .build()
    });
    assert!(result.is_err(), "existing fallback must be rejected");
    assert_eq!(std::fs::read(fallback).unwrap(), b"existing data");
}

#[test]
fn file_output_and_fallback_are_rejected_before_opening_files() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let primary = dir.path().join("primary.avro");
    let fallback = dir.path().join("fallback.avro");
    std::fs::write(&primary, b"previous run").unwrap();
    for with_core in [false, true] {
        let mut builder = NominalDatasetStreamBuilder::new()
            .stream_to_file(&primary)
            .with_file_fallback(&fallback);
        if with_core {
            builder = builder.stream_to_core(
                conjure_object::BearerToken::new("test").unwrap(),
                conjure_object::ResourceIdentifier::new("ri.catalog.main.dataset.test").unwrap(),
                runtime.handle().clone(),
            );
        }
        let error = builder
            .try_build()
            .err()
            .expect("two file destinations must be rejected");
        assert!(error.to_string().contains("file output and file fallback"));
        assert_eq!(std::fs::read(&primary).unwrap(), b"previous run");
        assert!(!fallback.exists());
    }
}

#[test]
fn invalid_core_url_is_reported_before_file_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.avro");
    std::fs::write(&path, b"existing data").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let opts = NominalStreamOpts {
        base_api_url: "not a URL".into(),
        ..Default::default()
    };
    let result = NominalDatasetStreamBuilder::new()
        .with_options(opts)
        .stream_to_core(
            conjure_object::BearerToken::new("test").unwrap(),
            conjure_object::ResourceIdentifier::new("ri.catalog.main.dataset.test").unwrap(),
            runtime.handle().clone(),
        )
        .stream_to_file(&path)
        .try_build();
    assert!(result.is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"existing data");
}

#[test]
fn file_output_overwrites_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.avro");
    std::fs::write(&path, b"previous run").unwrap();
    let stream = NominalDatasetStreamBuilder::new()
        .stream_to_file(&path)
        .try_build()
        .unwrap();
    drop(stream);
    assert_ne!(std::fs::read(path).unwrap(), b"previous run");
}

#[test]
fn primary_can_refuse_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.avro");
    std::fs::write(&path, b"previous run").unwrap();
    let error = NominalDatasetStreamBuilder::new()
        .stream_to_file_overwrite(&path, false)
        .try_build()
        .err()
        .unwrap();
    assert!(
        matches!(error, crate::consumer::ConsumerError::FileError { source, .. }
        if source.kind() == std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(std::fs::read(path).unwrap(), b"previous run");
}

#[test]
fn zero_point_limit_is_rejected_before_file_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.avro");
    std::fs::write(&path, b"previous run").unwrap();
    let result = std::panic::catch_unwind(|| {
        NominalDatasetStreamBuilder::new()
            .with_options(NominalStreamOpts::default().with_max_points_per_record(0))
            .stream_to_file(&path)
            .try_build()
    });
    assert_eq!(std::fs::read(path).unwrap(), b"previous run");
    assert!(matches!(
        result.unwrap(),
        Err(ConsumerError::Configuration(_))
    ));
}
