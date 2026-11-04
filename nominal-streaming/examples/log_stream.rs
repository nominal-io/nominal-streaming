//! Stream log messages to Nominal, with a local journal as fallback.
//!
//! With `NOMINAL_TOKEN` and `NOMINAL_DATASET_RID` set, logs go to that dataset, and batches
//! that fail to upload are written to `./log-fallback`. `NOMINAL_API_URL` overrides the API.
//! Without credentials, the example records to `./log-journal` instead.
//!
//! ```sh
//! cargo run --example log_stream
//! ```
use std::collections::HashMap;
use std::time::Duration;

use nominal_streaming::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Uploads run on this runtime. Keep it alive until the stream is closed.
    let runtime = tokio::runtime::Runtime::new()?;

    let mut opts = NominalLogStreamOpts::default();
    opts.max_request_delay = Duration::from_millis(100);
    if let Ok(url) = std::env::var("NOMINAL_API_URL") {
        opts.base_api_url = url;
    }

    let builder = NominalLogStreamBuilder::new().with_options(opts);
    let builder = match (
        std::env::var("NOMINAL_TOKEN"),
        std::env::var("NOMINAL_DATASET_RID"),
    ) {
        (Ok(token), Ok(dataset_rid)) => builder
            .stream_to_core(
                BearerToken::new(&token)?,
                ResourceIdentifier::new(&dataset_rid)?,
                runtime.handle().clone(),
            )
            .with_file_fallback("log-fallback"),
        _ => builder.stream_to_file("log-journal"),
    };
    let stream = builder.build()?;

    // A writer fixes the channel and the arguments that every message shares.
    let writer = stream.log_writer(
        "application",
        HashMap::from([("service".to_string(), "example".to_string())]),
    );
    writer.push(chrono::Utc::now(), "Service started")?;

    // Each record can add its own arguments to the shared ones.
    let records = (1..=3)
        .map(|attempt| {
            LogRecord::new(
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                format!("Connection attempt {attempt}"),
                HashMap::from([("attempt".to_string(), attempt.to_string())]),
            )
        })
        .collect();
    writer.enqueue_batch(records)?;

    // flush waits until every accepted record is uploaded or written to the journal.
    let stats = stream.flush()?;
    println!(
        "flushed: {} acknowledged, {} in the journal",
        stats.acknowledged_records, stats.backed_up_records
    );

    writer.push(chrono::Utc::now(), "Service stopping")?;
    // close stops new writes, drains what is left, and reports any dropped batch.
    let stats = stream.close()?;
    println!(
        "closed: {} accepted, {} acknowledged, {} in the journal, {} dropped",
        stats.accepted_records,
        stats.acknowledged_records,
        stats.backed_up_records,
        stats.failed_records
    );
    Ok(())
}
