use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;

use crate::client::PRODUCTION_API_URL;
use crate::consumer::ConsumerResult;
use crate::consumer::RequestConsumerWithFallback;
use crate::consumer::WriteRequestConsumer;
use crate::prelude::*;
use crate::simulated_consumer::SimulatedNetworkConfig;
use crate::simulated_consumer::SimulatedNetworkConsumer;
use crate::simulated_consumer::SimulatedNetworkFailure;
use crate::simulated_consumer::SimulatedRetryPolicy;
use crate::types::IntoPoints;

#[derive(Debug)]
struct TestDatasourceStream {
    requests: Mutex<Vec<WriteRequestNominal>>,
}

impl WriteRequestConsumer for Arc<TestDatasourceStream> {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(())
    }
}

fn create_test_stream() -> (Arc<TestDatasourceStream>, NominalDatasetStream) {
    let test_consumer = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let stream = create_stream_with_consumer(test_consumer.clone(), 1000);

    (test_consumer, stream)
}

fn create_stream_with_consumer<C>(consumer: C, max_points_per_record: usize) -> NominalDatasetStream
where
    C: WriteRequestConsumer + 'static,
{
    NominalDatasetStream::new_with_consumer(
        consumer,
        NominalStreamOpts {
            max_points_per_record,
            max_request_delay: Duration::from_millis(100),
            max_buffered_requests: 2,
            request_dispatcher_tasks: 4,
            transport: crate::client::TransportOptions::default(),
            delivery_timeout: crate::client::DEFAULT_DELIVERY_TIMEOUT,
            base_api_url: PRODUCTION_API_URL.to_string(),
            track_metrics: false,
            additional_metric_channels: Vec::new(),
        },
    )
}

fn create_stream_with_consumer_and_options<C>(
    consumer: C,
    opts: NominalStreamOpts,
) -> NominalDatasetStream
where
    C: WriteRequestConsumer + 'static,
{
    NominalDatasetStream::new_with_consumer(consumer, opts)
}

fn total_double_points(requests: &[WriteRequestNominal], channel_name: &str) -> usize {
    requests
        .iter()
        .flat_map(|request| request.series.iter())
        .filter(|series| {
            series
                .channel
                .as_ref()
                .is_some_and(|channel| channel.name == channel_name)
        })
        .map(|series| {
            let Some(Points {
                points_type: Some(PointsType::DoublePoints(points)),
            }) = series.points.as_ref()
            else {
                return 0;
            };
            points.points.len()
        })
        .sum()
}

fn point_counts_by_channel(
    requests: &[WriteRequestNominal],
) -> HashMap<String, (&'static str, usize)> {
    let mut counts = HashMap::new();

    for series in requests.iter().flat_map(|request| request.series.iter()) {
        let channel_name = series
            .channel
            .as_ref()
            .expect("series should have a channel")
            .name
            .clone();
        let points_type = series
            .points
            .as_ref()
            .and_then(|points| points.points_type.as_ref())
            .expect("series should have points");

        let (kind, point_count) = match points_type {
            PointsType::DoublePoints(points) => ("double", points.points.len()),
            PointsType::StringPoints(points) => ("string", points.points.len()),
            PointsType::IntegerPoints(points) => ("int", points.points.len()),
            PointsType::Uint64Points(points) => ("uint64", points.points.len()),
            PointsType::StructPoints(points) => ("struct", points.points.len()),
            PointsType::ArrayPoints(ArrayPoints {
                array_type: Some(ArrayType::DoubleArrayPoints(points)),
            }) => ("double_array", points.points.len()),
            PointsType::ArrayPoints(ArrayPoints {
                array_type: Some(ArrayType::StringArrayPoints(points)),
            }) => ("string_array", points.points.len()),
            PointsType::ArrayPoints(ArrayPoints { array_type: None }) => ("array", 0),
        };

        let entry = counts.entry(channel_name).or_insert((kind, 0));
        assert_eq!(entry.0, kind, "mismatched point type for channel");
        entry.1 += point_count;
    }

    counts
}

fn assert_record_limit(requests: &[WriteRequestNominal], cap: usize) -> usize {
    requests
        .iter()
        .map(|request| {
            let count: usize = point_counts_by_channel(std::slice::from_ref(request))
                .into_values()
                .map(|(_, count)| count)
                .sum();
            assert!(
                count <= cap,
                "request contains {count} points, limit is {cap}"
            );
            count
        })
        .sum()
}

#[test]
#[should_panic(expected = "max_points_per_record must be greater than zero")]
fn zero_record_limit_is_rejected() {
    create_stream_with_consumer(
        Arc::new(TestDatasourceStream {
            requests: Mutex::new(vec![]),
        }),
        0,
    );
}

#[test_log::test]
fn test_stream() {
    let (test_consumer, stream) = create_test_stream();

    for batch in 0..5 {
        let mut points = Vec::new();
        for i in 0..1000 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            points.push(DoublePoint {
                timestamp: Some(Timestamp {
                    seconds: start_time.as_secs() as i64,
                    nanos: start_time.subsec_nanos() as i32 + i,
                }),
                value: (i % 50) as f64,
            });
        }

        stream.enqueue(
            &ChannelDescriptor::with_tags("channel_1", [("batch_id", batch.to_string())]),
            points,
        );
    }

    drop(stream); // wait for points to flush

    let requests = test_consumer.requests.lock().unwrap();

    // validate that the requests were flushed based on the max_records value, not the
    // max request delay
    assert_eq!(requests.len(), 5);
    let series = requests.first().unwrap().series.first().unwrap();
    if let Some(PointsType::DoublePoints(points)) =
        series.points.as_ref().unwrap().points_type.as_ref()
    {
        assert_eq!(points.points.len(), 1000);
    } else {
        panic!("unexpected data type");
    }
}

#[test_log::test]
fn enqueue_many_writes_every_channel_in_one_request() {
    let (test_consumer, stream) = create_test_stream();
    let timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 0,
    };

    // a wide record: many channels sharing one timestamp, written as a single unit
    let entries: Vec<(ChannelDescriptor, PointsType)> = (0..250)
        .map(|i| {
            (
                ChannelDescriptor::new(format!("wide_{i}")),
                vec![DoublePoint {
                    timestamp: Some(timestamp),
                    value: i as f64,
                }]
                .into_points(),
            )
        })
        .collect();

    stream.enqueue_many(entries);
    drop(stream); // wait for points to flush

    let requests = test_consumer.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "the whole record should share one request"
    );
    assert_eq!(requests[0].series.len(), 250);
    for i in 0..250 {
        assert_eq!(total_double_points(&requests, &format!("wide_{i}")), 1);
    }
}

#[test_log::test]
fn enqueue_many_splits_a_batch_larger_than_a_record() {
    // max_points_per_record is 1000 here, so 2500 points cannot ride in one request
    let (test_consumer, stream) = create_test_stream();
    let timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanos: 0,
    };

    let mut entries: Vec<(ChannelDescriptor, PointsType)> = (0..2500)
        .map(|i| {
            (
                ChannelDescriptor::new(format!("big_{i}")),
                vec![DoublePoint {
                    timestamp: Some(timestamp),
                    value: i as f64,
                }]
                .into_points(),
            )
        })
        .collect();

    entries.push((
        ChannelDescriptor::with_tags("oversized", [("site", "test")]),
        (0..2501)
            .map(|i| DoublePoint {
                timestamp: Some(timestamp),
                value: i as f64,
            })
            .collect::<Vec<_>>()
            .into_points(),
    ));
    stream.enqueue_many(entries);
    drop(stream);

    let requests = test_consumer.requests.lock().unwrap();
    assert!(requests.len() > 1, "an oversized batch should be split");
    assert_eq!(assert_record_limit(&requests, 1000), 5001);
    assert_eq!(total_double_points(&requests, "oversized"), 2501);
    for series in requests
        .iter()
        .flat_map(|r| &r.series)
        .filter(|s| s.channel.as_ref().unwrap().name == "oversized")
    {
        assert_eq!(series.tags.get("site").map(String::as_str), Some("test"));
    }
    // and nothing is lost in the splitting
    let total: usize = (0..2500)
        .map(|i| total_double_points(&requests, &format!("big_{i}")))
        .sum();
    assert_eq!(total, 2500);
}

#[test_log::test]
fn enqueue_many_appends_to_channels_already_buffered() {
    let (test_consumer, stream) = create_test_stream();
    let channel = ChannelDescriptor::with_tags("shared", [("site", "a1")]);
    let point = |nanos| {
        vec![DoublePoint {
            timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos,
            }),
            value: nanos as f64,
        }]
        .into_points()
    };

    // the same channel reached once through enqueue and once through enqueue_many must land in
    // one series rather than splitting
    stream.enqueue(&channel, point(1));
    stream.enqueue_many(vec![(channel.clone(), point(2))]);
    drop(stream);

    let requests = test_consumer.requests.lock().unwrap();
    assert_eq!(total_double_points(&requests, "shared"), 2);
    let series: Vec<_> = requests
        .iter()
        .flat_map(|r| r.series.iter())
        .filter(|s| s.channel.as_ref().is_some_and(|c| c.name == "shared"))
        .collect();
    assert_eq!(series.len(), 1, "points should share one series");
}

#[test_log::test]
fn test_stream_types() {
    let (test_consumer, stream) = create_test_stream();

    for batch in 0..5 {
        let mut doubles = Vec::new();
        let mut strings = Vec::new();
        let mut structs = Vec::new();
        let mut ints = Vec::new();
        let mut uints = Vec::new();
        let mut double_arrays = Vec::new();
        let mut string_arrays = Vec::new();
        for i in 0..1001 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            doubles.push(DoublePoint {
                timestamp: Some(start_time.into_timestamp()),
                value: (i % 50) as f64,
            });
            strings.push(StringPoint {
                timestamp: Some(start_time.into_timestamp()),
                value: format!("{}", i % 50),
            });
            structs.push(StructPoint {
                timestamp: Some(start_time.into_timestamp()),
                json_string: format!("{{\"v\":{}}}", i % 50),
            });
            ints.push(IntegerPoint {
                timestamp: Some(start_time.into_timestamp()),
                value: i % 50,
            });
            uints.push(Uint64Point {
                timestamp: Some(start_time.into_timestamp()),
                value: (i % 50) as u64,
            });
            double_arrays.push(DoubleArrayPoint {
                timestamp: Some(start_time.into_timestamp()),
                value: vec![(i % 50) as f64, ((i + 1) % 50) as f64],
            });
            string_arrays.push(StringArrayPoint {
                timestamp: Some(start_time.into_timestamp()),
                value: vec![format!("{}", i % 50)],
            });
        }

        stream.enqueue(
            &ChannelDescriptor::with_tags("double", [("batch_id", batch.to_string())]),
            doubles,
        );
        stream.enqueue(
            &ChannelDescriptor::with_tags("string", [("batch_id", batch.to_string())]),
            strings,
        );
        stream.enqueue(
            &ChannelDescriptor::with_tags("struct", [("batch_id", batch.to_string())]),
            structs,
        );
        stream.enqueue(
            &ChannelDescriptor::with_tags("int", [("batch_id", batch.to_string())]),
            ints,
        );
        stream.enqueue(
            &ChannelDescriptor::with_tags("uint64", [("batch_id", batch.to_string())]),
            uints,
        );
        stream.enqueue(
            &ChannelDescriptor::with_tags("double_array", [("batch_id", batch.to_string())]),
            double_arrays,
        );
        stream.enqueue(
            &ChannelDescriptor::with_tags("string_array", [("batch_id", batch.to_string())]),
            string_arrays,
        );
    }

    drop(stream); // wait for points to flush

    let requests = test_consumer.requests.lock().unwrap();

    assert_eq!(assert_record_limit(&requests, 1000), 7 * 5 * 1001);
    let counts = point_counts_by_channel(&requests);
    assert_eq!(counts.len(), 7);
    for name in [
        "double",
        "string",
        "struct",
        "int",
        "uint64",
        "double_array",
        "string_array",
    ] {
        assert_eq!(counts[name], (name, 5 * 1001));
    }
}

#[test]
fn concurrent_producers_respect_record_limit() {
    let consumer = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let stream = create_stream_with_consumer(consumer.clone(), 16);
    let start = std::sync::Barrier::new(12);
    thread::scope(|scope| {
        for producer in 0..12 {
            let stream = &stream;
            let start = &start;
            scope.spawn(move || {
                let channel = ChannelDescriptor::new(format!("producer-{producer}"));
                start.wait();
                let mut writer = stream.double_writer(channel.clone());
                for i in 0..128 {
                    let points = vec![
                        DoublePoint {
                            timestamp: None,
                            value: i as f64
                        };
                        8
                    ];
                    match producer % 3 {
                        0 => stream.enqueue(&channel, points),
                        1 => stream.enqueue_many(vec![(channel.clone(), points.into_points())]),
                        _ => {
                            for _ in points {
                                writer.push(i as i64, i as f64);
                            }
                        }
                    }
                }
            });
        }
    });
    drop(stream);
    assert_eq!(
        assert_record_limit(&consumer.requests.lock().unwrap(), 16),
        12 * 128 * 8
    );
}

#[test_log::test]
fn test_writer() {
    let (test_consumer, stream) = create_test_stream();

    let cd = ChannelDescriptor::new("channel_1");
    let mut writer = stream.double_writer(cd);

    for i in 0..5000 {
        let start_time = UNIX_EPOCH.elapsed().unwrap();
        let value = i % 50;
        writer.push(start_time, value as f64);
    }

    drop(writer); // flush points to stream
    drop(stream); // flush stream to nominal

    let requests = test_consumer.requests.lock().unwrap();

    assert_eq!(requests.len(), 5);
    let series = requests.first().unwrap().series.first().unwrap();
    if let Some(PointsType::DoublePoints(points)) =
        series.points.as_ref().unwrap().points_type.as_ref()
    {
        assert_eq!(points.points.len(), 1000);
    } else {
        panic!("unexpected data type");
    }
}

#[test_log::test]
fn test_time_flush() {
    // A writer should flush on the next push after the time limit, even when
    // it has too few points to trigger a size-based flush.
    let (test_consumer, stream) = create_test_stream();

    let cd = ChannelDescriptor::new("channel_1");
    let mut writer = stream.double_writer(cd);

    writer.push(UNIX_EPOCH.elapsed().unwrap(), 1.0);
    thread::sleep(Duration::from_millis(101));
    writer.push(UNIX_EPOCH.elapsed().unwrap(), 2.0); // first flush

    // Wait for delivery before the next write so worker scheduling cannot
    // combine the two writer flushes into one request. The request-count
    // assertion below should reflect writer behavior, not worker timing.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while test_consumer.requests.lock().unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed batch did not arrive"
        );
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(101));
    writer.push(UNIX_EPOCH.elapsed().unwrap(), 3.0); // second flush

    drop(writer);
    drop(stream);

    let requests = test_consumer.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
}

#[test_log::test]
fn simulated_network_retries_transient_failures_and_delivers_data() {
    let test_consumer = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let simulated_network = SimulatedNetworkConsumer::new(
        test_consumer.clone(),
        SimulatedNetworkConfig::default()
            .with_latency(Duration::from_micros(10), Duration::from_micros(10))
            .with_bandwidth_limit(32 * 1024 * 1024)
            .with_failure_pattern(SimulatedNetworkFailure::FailFirstAttemptsPerRequest {
                attempts: 1,
            })
            .with_failure_pattern(SimulatedNetworkFailure::InitialOutageAttempts { attempts: 1 })
            .with_retry_policy(
                SimulatedRetryPolicy::new(2, Duration::from_micros(10))
                    .with_jitter(Duration::from_micros(10)),
            ),
    );
    let stats = simulated_network.stats();
    let stream = create_stream_with_consumer(simulated_network, 4);

    let cd = ChannelDescriptor::new("networked_double");
    let mut writer = stream.double_writer(cd);
    for i in 0..12 {
        writer.push(UNIX_EPOCH.elapsed().unwrap(), i as f64);
    }

    drop(writer);
    drop(stream);

    let requests = test_consumer.requests.lock().unwrap();
    assert_eq!(total_double_points(&requests, "networked_double"), 12);

    let stats = stats.snapshot();
    assert_eq!(stats.successful_requests as usize, requests.len());
    assert_eq!(stats.simulated_failures, stats.successful_requests);
    assert_eq!(stats.retries, stats.simulated_failures);
    assert!(stats.attempts > stats.successful_requests);
    assert!(stats.delivered_bytes > 0);
    assert!(stats.simulated_sleep >= Duration::from_micros(stats.attempts * 10));
}

#[test]
fn oversized_batch_delivers_failed_chunks_to_fallback_exactly_once() {
    let primary = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let fallback = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let network = SimulatedNetworkConsumer::new(
        primary.clone(),
        SimulatedNetworkConfig::default().with_failure_pattern(
            SimulatedNetworkFailure::FailEveryNthRequest {
                every: 2,
                attempts_per_request: 1,
            },
        ),
    );
    let stats = network.stats();
    let stream = create_stream_with_consumer_and_options(
        RequestConsumerWithFallback::new(network, fallback.clone()),
        NominalStreamOpts::default()
            .with_max_points_per_record(3)
            .with_max_buffered_requests(1)
            .with_request_dispatcher_tasks(1),
    );
    let channel = ChannelDescriptor::with_tags("oversized", [("sensor", "left")]);
    let expected = (0..11)
        .map(|i| DoublePoint {
            timestamp: Some(i.into_timestamp()),
            value: i as f64,
        })
        .collect::<Vec<_>>();
    stream.enqueue(&channel, expected.clone());
    drop(stream);

    let primary = primary.requests.lock().unwrap();
    let fallback = fallback.requests.lock().unwrap();
    assert_eq!(primary.len(), 2);
    assert_eq!(fallback.len(), 2);
    assert_eq!(
        assert_record_limit(&primary, 3) + assert_record_limit(&fallback, 3),
        11
    );
    let mut actual = Vec::new();
    for request in primary.iter().chain(fallback.iter()) {
        for series in &request.series {
            assert_eq!(series.channel.as_ref().unwrap().name, "oversized");
            assert_eq!(
                series.tags,
                [("sensor".to_owned(), "left".to_owned())].into()
            );
            let Some(Points {
                points_type: Some(PointsType::DoublePoints(points)),
            }) = &series.points
            else {
                panic!("expected double points");
            };
            actual.extend(points.points.iter().cloned());
        }
    }
    actual.sort_by_key(|point| {
        let timestamp = point.timestamp.as_ref().unwrap();
        (timestamp.seconds, timestamp.nanos)
    });
    assert_eq!(actual, expected);
    let stats = stats.snapshot();
    assert_eq!(stats.attempts, 4);
    assert_eq!(stats.successful_requests, 2);
    assert_eq!(stats.simulated_failures, 2);
}

#[test_log::test]
fn simulated_network_all_requests_timeout_falls_back_under_backpressure() {
    let primary_destination = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let fallback_destination = Arc::new(TestDatasourceStream {
        requests: Mutex::new(vec![]),
    });
    let timeout = Duration::from_millis(5);
    let simulated_network = SimulatedNetworkConsumer::new(
        primary_destination.clone(),
        SimulatedNetworkConfig::default()
            .with_latency(timeout, Duration::ZERO)
            .with_failure_pattern(SimulatedNetworkFailure::AllRequestsTimeout),
    );
    let stats = simulated_network.stats();
    let stream = create_stream_with_consumer_and_options(
        RequestConsumerWithFallback::new(simulated_network, fallback_destination.clone()),
        NominalStreamOpts {
            max_points_per_record: 2,
            max_request_delay: Duration::from_millis(20),
            max_buffered_requests: 1,
            request_dispatcher_tasks: 1,
            transport: crate::client::TransportOptions::default(),
            delivery_timeout: crate::client::DEFAULT_DELIVERY_TIMEOUT,
            base_api_url: PRODUCTION_API_URL.to_string(),
            track_metrics: false,
            additional_metric_channels: Vec::new(),
        },
    );

    let cd = ChannelDescriptor::new("timeout_double");
    let mut writer = stream.double_writer(cd);
    for i in 0..12 {
        writer.push(UNIX_EPOCH.elapsed().unwrap(), i as f64);
    }

    drop(writer);
    drop(stream);

    let primary_requests = primary_destination.requests.lock().unwrap();
    assert!(primary_requests.is_empty());

    let fallback_requests = fallback_destination.requests.lock().unwrap();
    assert_eq!(fallback_requests.len(), 6);
    assert_eq!(
        total_double_points(&fallback_requests, "timeout_double"),
        12
    );

    let stats = stats.snapshot();
    assert_eq!(stats.attempts, 6);
    assert_eq!(stats.simulated_failures, 6);
    assert_eq!(stats.successful_requests, 0);
    assert_eq!(stats.retries, 0);
    assert_eq!(stats.delivered_bytes, 0);
    assert!(stats.simulated_sleep >= timeout * stats.attempts as u32);
}

#[test_log::test]
fn test_writer_types() {
    let (test_consumer, stream) = create_test_stream();

    let cd1 = ChannelDescriptor::new("double");
    let cd2 = ChannelDescriptor::new("string");
    let cd3 = ChannelDescriptor::new("int");
    let cd4 = ChannelDescriptor::new("uint64");
    let cd5 = ChannelDescriptor::new("struct");
    let cd6 = ChannelDescriptor::new("double_array");
    let cd7 = ChannelDescriptor::new("string_array");
    let mut double_writer = stream.double_writer(cd1);
    let mut string_writer = stream.string_writer(cd2);
    let mut integer_writer = stream.integer_writer(cd3);
    let mut uint64_writer = stream.uint64_writer(cd4);
    let mut struct_writer = stream.struct_writer(cd5);
    let mut double_array_writer = stream.double_array_writer(cd6);
    let mut string_array_writer = stream.string_array_writer(cd7);

    for i in 0..5000 {
        let start_time = UNIX_EPOCH.elapsed().unwrap();
        let value = i % 50;
        double_writer.push(start_time, value as f64);
        string_writer.push(start_time, format!("{}", value));
        integer_writer.push(start_time, value);
        uint64_writer.push(start_time, value as u64);
        struct_writer.push(start_time, format!("{{\"v\":{}}}", value));
        double_array_writer.push(start_time, vec![value as f64, (value + 1) as f64]);
        string_array_writer.push(start_time, vec![format!("{}", value)]);
    }

    drop(double_writer);
    drop(string_writer);
    drop(integer_writer);
    drop(uint64_writer);
    drop(struct_writer);
    drop(double_array_writer);
    drop(string_array_writer);
    drop(stream);

    let requests = test_consumer.requests.lock().unwrap();
    let counts = point_counts_by_channel(&requests);

    assert_eq!(counts.get("double"), Some(&("double", 5000)));
    assert_eq!(counts.get("string"), Some(&("string", 5000)));
    assert_eq!(counts.get("int"), Some(&("int", 5000)));
    assert_eq!(counts.get("uint64"), Some(&("uint64", 5000)));
    assert_eq!(counts.get("struct"), Some(&("struct", 5000)));
    assert_eq!(counts.get("double_array"), Some(&("double_array", 5000)));
    assert_eq!(counts.get("string_array"), Some(&("string_array", 5000)));
}
