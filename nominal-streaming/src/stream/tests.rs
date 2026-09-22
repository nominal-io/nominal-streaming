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
            .with_file_fallback(&fallback)
            .build()
    });
    assert!(result.is_err(), "existing fallback must be rejected");
    assert_eq!(std::fs::read(fallback).unwrap(), b"existing data");
}

#[test]
fn file_fallback_requires_core_before_opening_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("same.avro");
    let result = super::NominalDatasetStreamBuilder::new()
        .stream_to_file(&path)
        .with_file_fallback(&path)
        .try_build();
    assert!(result.is_err());
    assert!(!path.exists());
}
