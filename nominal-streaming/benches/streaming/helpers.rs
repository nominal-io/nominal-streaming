use std::time::Duration;
use std::time::UNIX_EPOCH;

use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_streaming::client::NominalApiClients;
use nominal_streaming::consumer::NominalCoreConsumer;
use nominal_streaming::prelude::*;
use nominal_streaming::stream::NominalDatasetStream;
use nominal_streaming::stream::NominalStreamOpts;

pub fn make_double_points(n: usize, enqueue_ns: u64) -> Vec<DoublePoint> {
    let now_ns = UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64;
    (0..n)
        .map(|i| {
            let ts_ns = if i == 0 {
                enqueue_ns as i64
            } else {
                now_ns + i as i64
            };
            DoublePoint {
                timestamp: Some(Timestamp {
                    seconds: ts_ns / 1_000_000_000,
                    nanos: (ts_ns % 1_000_000_000) as i32,
                }),
                value: i as f64,
            }
        })
        .collect()
}

pub fn make_stream(
    clients: NominalApiClients,
    handle: tokio::runtime::Handle,
) -> NominalDatasetStream {
    let token = BearerToken::new("bench-token").unwrap();
    let dataset_rid = ResourceIdentifier::new("ri.catalog.main.dataset.bench").unwrap();
    let consumer = NominalCoreConsumer::new(clients, handle, token, dataset_rid);
    NominalDatasetStream::new_with_consumer(consumer, NominalStreamOpts::default())
}

pub fn run_session(
    stream: &NominalDatasetStream,
    cd: &ChannelDescriptor,
    n_enqueues: usize,
    pts_per: usize,
    delay_ms: u64,
) {
    for i in 0..n_enqueues {
        let enqueue_ns = UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64;
        stream.enqueue(cd, make_double_points(pts_per, enqueue_ns));
        if delay_ms > 0 && i % 5 != 0 && i + 1 < n_enqueues {
            std::thread::sleep(Duration::from_millis(delay_ms));
        }
    }
}
