use std::thread;
use std::time::Duration;

use conjure_object::ResourceIdentifier;
use nominal_streaming::consumer::ReuploadOpts;
use nominal_streaming::prelude::DoublePoint;
use nominal_streaming::stream::NominalDatasetStream;
use nominal_streaming::stream::NominalStreamOpts;
use nominal_streaming::types::ChannelDescriptor;
use nominal_streaming::types::IntoTimestamp;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    let handle = rt.handle().clone();

    let data_source_rid = ResourceIdentifier::new(
        &*std::env::var("DATA_SOURCE_RID").expect("DATA_SOURCE_RID must be set"),
    )
    .unwrap();

    let reupload_opts = ReuploadOpts {
        idle_threshold: Duration::from_millis(200),
        time_since_last_failure_threshold: Duration::from_millis(200),
        reupload_interval: Duration::from_millis(200),
        upload_opts: Default::default(),
    };

    let bearer_token = std::env::var("PROD_API_KEY").unwrap().parse().unwrap();
    let stream = NominalDatasetStream::builder()
        .stream_to_core(bearer_token, data_source_rid, handle)
        .with_file_fallback("test.avro")
        .with_streaming_options(NominalStreamOpts::default())
        .with_reupload_options(reupload_opts)
        .with_workspace_rid(
            ResourceIdentifier::new(
                "ri.security.cerulean-staging.workspace.7d802d4e-7f1c-45b9-ba05-f7f6323504d6",
            )
            .unwrap(),
        )
        .build();

    // Burst 1: write some points, then idle to trigger rotation
    let cd = ChannelDescriptor::new("saf_channel");
    for i in 0..300_000 {
        let ts = std::time::UNIX_EPOCH.elapsed().unwrap().into_timestamp();
        stream.enqueue(
            &cd,
            vec![DoublePoint {
                timestamp: Some(ts),
                value: (i % 10) as f64,
            }],
        );
    }
    thread::sleep(Duration::from_millis(1000));

    for i in 0..300_000 {
        let ts = std::time::UNIX_EPOCH.elapsed().unwrap().into_timestamp();
        stream.enqueue(
            &cd,
            vec![DoublePoint {
                timestamp: Some(ts),
                value: (i % 10) as f64,
            }],
        );
    }
    thread::sleep(Duration::from_millis(1000));

    for i in 0..300_000 {
        let ts = std::time::UNIX_EPOCH.elapsed().unwrap().into_timestamp();
        stream.enqueue(
            &cd,
            vec![DoublePoint {
                timestamp: Some(ts),
                value: (i % 10) as f64,
            }],
        );
    }
    thread::sleep(Duration::from_millis(3000));

    drop(stream);
    thread::sleep(Duration::from_millis(3000));
}
