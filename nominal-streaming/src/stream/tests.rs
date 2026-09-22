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
