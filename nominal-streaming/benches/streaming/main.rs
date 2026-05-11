/// Each iteration creates a fresh `NominalDatasetStream`, fires `n_enqueues`
/// calls with bus-stop inter-arrival delays, then drops the stream — which
/// blocks until `unflushed_points == 0`. Two timing signals are captured:
///
/// - `Mean(ms)` (Criterion): producer active time — enqueue overhead only,
///   inter-arrival sleeps subtracted. Approximates throughput under continuous load.
/// - Throughput: derived from session time — wall clock from first enqueue until
///   `drop(stream)` returns (all in-flight requests acknowledged). Includes
///   inter-arrival sleeps and final drain; represents end-to-end delivery rate.
///
/// Bus-stop model: every 5th arrival is back-to-back (burst); the rest wait
/// `inter_arrival_ms`. The total session length exceeds `max_request_delay`
/// (100ms default) so the batch processor fires from time to time mid-session.
///
/// Latency measurement: each enqueue records its wall-clock timestamp as the
/// first point's timestamp field. The mock server decompresses and decodes the
/// protobuf body, reads the timestamp, and records `arrival_ns - enqueue_ns`
/// in a shared vec. p50/p95/p99 are printed after each benchmark function
/// completes.
mod helpers;
mod mock_server;

// --- benchmark --------------------------------------------------------------
#[cfg(feature = "bench")]
use criterion::Criterion;
use helpers::*;
use mock_server::*;
use nominal_streaming::prelude::*;

#[cfg(feature = "bench")]
fn bench_streaming(c: &mut Criterion) {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::UNIX_EPOCH;

    use criterion::Throughput;
    use nominal_streaming::client::NominalApiClients;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .thread_name("bench-tokio")
        .build()
        .unwrap();

    let latency_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let port = spawn_mock_http_server(Arc::clone(&latency_samples));
    let base_url = format!("http://127.0.0.1:{port}");
    let clients = NominalApiClients::from_uri(&base_url);
    let cd = ChannelDescriptor::new("bench");

    // (n_enqueues, pts_per_enqueue, inter_arrival_delay_ms)
    //
    // 160 gaps × 19ms ≈ 3040ms session; max_request_delay fires ~30 times.
    let workloads: &[(usize, usize, u64)] = &[
        (100, 10_000_000, 19), // representative of real user streams
        (100, 1_000_000, 19),
        (100, 10_000, 19),
        (100, 100, 19),
        (100, 1, 19),
    ];

    let mut group = c.benchmark_group("http");
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(3));

    for &(n_enqueues, pts_per, delay_ms) in workloads {
        let total_pts = n_enqueues * pts_per;
        let label = format!("{}x{}pts_{}ms", n_enqueues, pts_per, delay_ms);

        let make = || make_stream(clients.clone(), rt.handle().clone());

        for _ in 0..2 {
            let stream = make();
            run_session(&stream, &cd, n_enqueues, pts_per, delay_ms);
            drop(stream);
        }
        latency_samples.lock().unwrap().clear();

        let work_ns_accum: Arc<Mutex<Vec<(u64, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

        group.throughput(Throughput::Elements(total_pts as u64));
        group.bench_function(&label, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let stream = make();
                    let bp_arc = Arc::clone(&stream.batch_processor_ns);
                    let disp_arc = Arc::clone(&stream.dispatcher_ns);
                    let mut work = Duration::ZERO;
                    let t_session = Instant::now();
                    let mut t = t_session;
                    for i in 0..n_enqueues {
                        let enqueue_ns = UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64;
                        stream.enqueue(&cd, make_double_points(pts_per, enqueue_ns));
                        if delay_ms > 0 && i % 5 != 0 && i + 1 < n_enqueues {
                            work += t.elapsed();
                            std::thread::sleep(Duration::from_millis(delay_ms));
                            t = Instant::now();
                        }
                    }
                    work += t.elapsed();
                    total += work;
                    drop(stream); // blocks until all in-flight requests are acknowledged
                    let session_ns = t_session.elapsed().as_nanos() as u64;
                    work_ns_accum.lock().unwrap().push((
                        bp_arc.load(std::sync::atomic::Ordering::Relaxed),
                        disp_arc.load(std::sync::atomic::Ordering::Relaxed),
                        session_ns,
                    ));
                }
                total
            });
        });

        let (bp_ms, disp_ms, session_ms) = {
            let triples = work_ns_accum.lock().unwrap();
            let (bp_total, disp_total, session_total): (u64, u64, u64) =
                triples.iter().fold((0, 0, 0), |(ba, da, sa), &(b, d, s)| {
                    (ba + b, da + d, sa + s)
                });
            let n = triples.len().max(1) as u64;
            (
                bp_total as f64 / n as f64 / 1e6,
                disp_total as f64 / n as f64 / 1e6,
                session_total as f64 / n as f64 / 1e6,
            )
        };

        let (p50_ms, p95_ms, p99_ms) = {
            let raw = latency_samples.lock().unwrap();
            let mut sorted = raw.clone();
            sorted.sort_unstable();
            let n = sorted.len();
            let pct = |p: usize| {
                sorted
                    .get(((n * p + 99) / 100).saturating_sub(1))
                    .copied()
                    .unwrap_or(0) as f64
                    / 1e6
            };
            (pct(50), pct(95), pct(99))
        };

        println!(
            "BENCH_STATS:{{\"label\":\"{label}\",\"total_elements\":{total_pts},\
             \"bp_ms\":{bp_ms:.1},\"disp_ms\":{disp_ms:.1},\"session_ms\":{session_ms:.1},\
             \"p50_ms\":{p50_ms:.1},\"p95_ms\":{p95_ms:.1},\"p99_ms\":{p99_ms:.1}}}"
        );
    }

    group.finish();
}

#[cfg(feature = "bench")]
criterion::criterion_group!(benches, bench_streaming);
#[cfg(feature = "bench")]
criterion::criterion_main!(benches);

#[cfg(not(feature = "bench"))]
fn main() {}

#[cfg(all(test, not(feature = "bench")))]
mod tests {
    use std::io::Write;

    use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
    use nominal_api::tonic::io::nominal::scout::api::proto::Points;
    use nominal_api::tonic::io::nominal::scout::api::proto::Series;
    use prost::Message;

    use super::*;

    #[allow(dead_code)]
    fn make_compressed_request(enqueue_ns: u64, n_extra_points: usize) -> Vec<u8> {
        let mut points = vec![DoublePoint {
            timestamp: Some(Timestamp {
                seconds: (enqueue_ns / 1_000_000_000) as i64,
                nanos: (enqueue_ns % 1_000_000_000) as i32,
            }),
            value: 0.0,
        }];
        points.extend((1..=n_extra_points).map(|i| DoublePoint {
            timestamp: Some(Timestamp {
                seconds: i as i64,
                nanos: 0,
            }),
            value: i as f64,
        }));
        let req = WriteRequestNominal {
            series: vec![Series {
                channel: Some(Channel {
                    name: "test".to_string(),
                }),
                tags: Default::default(),
                points: Some(Points {
                    points_type: Some(PointsType::DoublePoints(DoublePoints { points })),
                }),
            }],
            ..Default::default()
        };
        let mut proto_bytes = Vec::new();
        req.encode(&mut proto_bytes).unwrap();
        let mut compressed = Vec::new();
        let mut enc = snap::write::FrameEncoder::new(&mut compressed);
        enc.write_all(&proto_bytes).unwrap();
        enc.flush().unwrap();
        drop(enc);
        compressed
    }

    #[test]
    fn decode_single_point() {
        let ns = 1_234_567_890_123_456_789u64;
        assert_eq!(decode_enqueue_ns(&make_compressed_request(ns, 0)), Some(ns));
    }

    #[test]
    fn decode_many_points() {
        let ns = 9_876_543_210_987_654_321u64;
        assert_eq!(
            decode_enqueue_ns(&make_compressed_request(ns, 10_000)),
            Some(ns)
        );
    }
}
