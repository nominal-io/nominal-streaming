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
