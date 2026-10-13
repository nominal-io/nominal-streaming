use super::*;

#[derive(Debug)]
struct ControlledConsumer {
    entered: crossbeam_channel::Sender<()>,
    release: crossbeam_channel::Receiver<()>,
    requests: Mutex<Vec<WriteRequestNominal>>,
}

impl WriteRequestConsumer for Arc<ControlledConsumer> {
    fn consume(&self, request: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
        self.requests.lock().push(request.clone());
        let _ = self.entered.send(());
        // A token advances one request. Dropping the sender releases all requests, including
        // during assertion unwinding, so a failed test cannot strand its stream behind this gate.
        let _ = self.release.recv();
        Ok(())
    }
}

fn controlled_consumer() -> (
    Arc<ControlledConsumer>,
    crossbeam_channel::Sender<()>,
    crossbeam_channel::Receiver<()>,
) {
    let (entered, entered_rx) = crossbeam_channel::unbounded();
    let (release_tx, release) = crossbeam_channel::unbounded();
    (
        Arc::new(ControlledConsumer {
            entered,
            release,
            requests: Mutex::new(Vec::new()),
        }),
        release_tx,
        entered_rx,
    )
}

fn assert_delivered_points(requests: &[WriteRequestNominal], cap: usize, total: usize) {
    let mut values = Vec::new();
    for request in requests {
        let start = values.len();
        for series in &request.series {
            let PointsType::DoublePoints(points) = series
                .points
                .as_ref()
                .unwrap()
                .points_type
                .as_ref()
                .unwrap()
            else {
                panic!("expected double points");
            };
            values.extend(points.points.iter().map(|point| point.value as usize));
        }
        let count = values.len() - start;
        assert!(
            count > 0 && count <= cap,
            "record contains {count} points, limit is {cap}"
        );
    }
    values.sort_unstable();
    assert_eq!(values, (0..total).collect::<Vec<_>>());
}

#[derive(Clone, Copy, Debug)]
enum BatchShape {
    SingleChannel,
    ManyChannels,
    MixedChannels,
}

fn enqueue_batch(
    stream: &NominalDatasetStream,
    shape: BatchShape,
    batch: usize,
    points_per_batch: usize,
) {
    let point = |index| DoublePoint {
        timestamp: Some(Default::default()),
        value: (batch * points_per_batch + index) as f64,
    };
    match shape {
        BatchShape::SingleChannel => stream
            .enqueue(
                &ChannelDescriptor::new(format!("batch-{batch}")),
                (0..points_per_batch).map(point).collect::<Vec<_>>(),
            )
            .unwrap(),
        BatchShape::ManyChannels => stream
            .enqueue_many(
                (0..points_per_batch)
                    .map(|index| {
                        (
                            ChannelDescriptor::new(format!("batch-{batch}-point-{index}")),
                            vec![point(index)].into_points(),
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        BatchShape::MixedChannels => {
            let split = points_per_batch / 2;
            let mut entries = vec![(
                ChannelDescriptor::new(format!("batch-{batch}-grouped")),
                (0..split).map(point).collect::<Vec<_>>().into_points(),
            )];
            entries.extend((split..points_per_batch).map(|index| {
                (
                    ChannelDescriptor::new(format!("batch-{batch}-point-{index}")),
                    vec![point(index)].into_points(),
                )
            }));
            stream.enqueue_many(entries).unwrap();
        }
    }
}

#[test]
fn drop_wakes_partial_batches_before_flush_deadline() {
    #[derive(Debug)]
    struct CountingConsumer(Arc<AtomicUsize>);
    impl WriteRequestConsumer for CountingConsumer {
        fn consume(&self, request: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
            let count: usize = request
                .series
                .iter()
                .map(|s| points_len(s.points.as_ref().unwrap().points_type.as_ref().unwrap()))
                .sum();
            self.0.fetch_add(count, Ordering::Relaxed);
            Ok(())
        }
    }
    let accepted = Arc::new(AtomicUsize::new(0));
    let stream = NominalDatasetStream::new_with_consumer(
        CountingConsumer(accepted.clone()),
        NominalStreamOpts {
            max_request_delay: Duration::from_secs(60),
            ..Default::default()
        },
    );
    // Let empty processors enter their long idle wait.
    thread::sleep(Duration::from_millis(100));
    stream
        .enqueue(
            &ChannelDescriptor::new("value"),
            vec![DoublePoint {
                timestamp: Some(Default::default()),
                value: 1.0,
            }],
        )
        .unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        drop(stream);
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("drop waited for the flush deadline");
    assert_eq!(accepted.load(Ordering::Relaxed), 1);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Arc::strong_count(&accepted) != 1 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        Arc::strong_count(&accepted),
        1,
        "idle workers retained the consumer"
    );
}

#[test]
fn drop_drains_oversized_detached_and_buffered_batches() {
    const POINTS_PER_BATCH: usize = 4;
    const BATCHES: usize = 3;
    let (consumer, release_tx, entered_rx) = controlled_consumer();
    let stream = NominalDatasetStream::new_with_consumer(
        consumer.clone(),
        NominalStreamOpts::default()
            .with_max_points_per_record(1)
            .with_max_buffered_requests(0)
            .with_request_dispatcher_tasks(1)
            .with_max_request_delay(Duration::from_millis(1)),
    );

    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let producer = thread::spawn(move || {
        for batch in 0..BATCHES {
            stream
                .enqueue(
                    &ChannelDescriptor::new(format!("batch-{batch}")),
                    (0..POINTS_PER_BATCH)
                        .map(|point| DoublePoint {
                            timestamp: Some(Default::default()),
                            value: (batch * POINTS_PER_BATCH + point) as f64,
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            if batch == 0 {
                entered_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("oversized batch did not reach the consumer");
            }
        }
        let _ = admitted_tx.send(());
        drop(stream);
        let _ = done_tx.send(());
    });
    admitted_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("oversized batches were not admitted");
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "drop completed while the consumer was blocked"
    );

    drop(release_tx);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("drop did not drain every split request");
    producer.join().unwrap();

    assert_delivered_points(&consumer.requests.lock(), 1, POINTS_PER_BATCH * BATCHES);
}

#[rstest::rstest]
#[case::rendezvous_oversized_narrow(0, 1, 1, 8, 12, BatchShape::SingleChannel)]
#[case::single_slot_exact_capacity(1, 1, 2, 2, 24, BatchShape::SingleChannel)]
#[case::multi_dispatcher_remainder(3, 2, 3, 8, 24, BatchShape::SingleChannel)]
#[case::wide_fitting(1, 2, 4, 4, 24, BatchShape::ManyChannels)]
#[case::wide_underfilled(0, 1, 4, 1, 32, BatchShape::ManyChannels)]
#[case::mixed_oversized(2, 3, 5, 12, 24, BatchShape::MixedChannels)]
fn saturated_dispatchers_apply_backpressure_and_resume_incrementally(
    #[case] queue_capacity: usize,
    #[case] dispatcher_tasks: usize,
    #[case] record_capacity: usize,
    #[case] points_per_batch: usize,
    #[case] batches: usize,
    #[case] shape: BatchShape,
) {
    let total_points = points_per_batch * batches;
    let (consumer, release_tx, entered_rx) = controlled_consumer();
    let stream = NominalDatasetStream::new_with_consumer(
        consumer.clone(),
        NominalStreamOpts::default()
            .with_max_points_per_record(record_capacity)
            .with_max_buffered_requests(queue_capacity)
            .with_request_dispatcher_tasks(dispatcher_tasks)
            .with_max_request_delay(Duration::from_millis(1)),
    );
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let producer = thread::spawn(move || {
        for batch in 0..batches {
            enqueue_batch(&stream, shape, batch, points_per_batch);
            admitted_tx.send(()).unwrap();
        }
        drop(stream);
        done_tx.send(()).unwrap();
    });

    // Consume every initial notification so the next one must follow a gate release.
    for _ in 0..dispatcher_tasks {
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("dispatcher did not reach the controlled consumer");
    }
    thread::sleep(Duration::from_millis(50));
    let initially_admitted = admitted_rx.try_iter().count();
    assert!(initially_admitted > 0);
    assert!(
        initially_admitted < batches,
        "all batches bypassed backpressure with queue={queue_capacity}, dispatchers={dispatcher_tasks}, cap={record_capacity}, shape={shape:?}"
    );

    release_tx.send(()).unwrap();
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("releasing one request did not resume dispatch");
    thread::sleep(Duration::from_millis(50));
    let admitted_after_release = initially_admitted + admitted_rx.try_iter().count();
    assert!(
        admitted_after_release < batches,
        "one release drained an unbounded amount of producer work"
    );

    drop(release_tx);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("producer and stream did not finish draining");
    producer.join().unwrap();

    assert_delivered_points(&consumer.requests.lock(), record_capacity, total_points);
}

#[test]
fn oversized_enqueue_does_not_wait_for_consumer() {
    let (consumer, release_tx, _entered_rx) = controlled_consumer();
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let producer = thread::spawn(move || {
        let stream = NominalDatasetStream::new_with_consumer(
            consumer,
            NominalStreamOpts::default()
                .with_max_points_per_record(2)
                .with_max_buffered_requests(1)
                .with_request_dispatcher_tasks(1),
        );
        stream
            .enqueue(
                &ChannelDescriptor::new("large"),
                vec![
                    DoublePoint {
                        timestamp: Some(Default::default()),
                        value: 0.0
                    };
                    100
                ],
            )
            .unwrap();
        let _ = admitted_tx.send(());
        drop(stream);
        let _ = done_tx.send(());
    });
    let admitted = admitted_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    drop(release_tx);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("stream did not drain");
    producer.join().unwrap();
    assert!(
        admitted,
        "oversized input waited for downstream consumption"
    );
}
