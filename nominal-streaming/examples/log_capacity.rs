//! Opt-in finite staging load probe. Credentials are read only from the environment.
//! Run through benchmarks/log_streaming_rust_capacity.py in the research worktree.
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use nominal_streaming::log::LogRecord;
use nominal_streaming::log::LogStreamOptions;
use nominal_streaming::log::NominalLogStreamBuilder;
use serde_json::json;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"))
}

fn record(run: &str, phase: &str, sequence: usize, base: i64) -> LogRecord {
    let level = ["DEBUG", "INFO", "WARNING", "ERROR"][sequence % 4];
    let service = ["telemetry-gateway", "scheduler", "worker", "api"][sequence % 4];
    let component = ["ingest", "routing", "validation", "dispatch"][sequence % 4];
    let trace = format!(
        "{sequence:016x}{:08x}",
        (sequence as u64 * 0x9e3779b1) & 0xffffffff
    );
    let message = format!("{level} service={service} component={component} handled synthetic request run={run} sequence={sequence:016x} trace={trace} operation=stream_log outcome=accepted route=/api/v1/events retryable=false payload_class=structured benchmark=true");
    let args = HashMap::from([
        ("benchmark_run".into(), run.into()),
        ("benchmark_phase".into(), phase.into()),
        ("sequence".into(), sequence.to_string()),
        ("level".into(), level.into()),
        ("service".into(), service.into()),
        ("component".into(), component.into()),
        (
            "host".into(),
            format!("synthetic-load-{:02}", sequence % 32),
        ),
        ("thread".into(), (sequence % 16).to_string()),
        ("request_id".into(), trace),
        ("filename".into(), "log_streaming_baseline.py".into()),
        ("function".into(), "produce_paced_records".into()),
        ("line".into(), "0".into()),
    ]);
    LogRecord::new(base + sequence as i64, message, args)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("nominal_streaming::log::attempt=info")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();
    let url = env("NOMINAL_API_URL");
    if url != "https://api-staging.gov.nominal.io/api" && url != "http://127.0.0.1:9/api" {
        return Err("only staging or the loopback failure fixture is allowed".into());
    }
    let count: usize = env("BENCH_RECORDS").parse()?;
    let batch: usize = env("BENCH_BATCH").parse()?;
    let workers: usize = env("BENCH_WORKERS").parse()?;
    if count == 0
        || count > 5_000_000
        || batch == 0
        || batch > 200_000
        || workers == 0
        || workers > 32
    {
        return Err("probe bounds exceeded".into());
    }
    let run = env("BENCH_RUN");
    let phase = env("BENCH_PHASE");
    let base: i64 = env("BENCH_BASE_NS").parse()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    // 4096 accounted bytes per record leaves headroom above this fixture's ~2600 bytes.
    // Total includes queued and in-flight work, capped at 6 GiB; the launcher checks available host memory.
    let batch_bytes = batch * 4096;
    let buffer_bytes = (batch_bytes * (workers + 1)).min(6 * 1024 * 1024 * 1024);
    let opts = LogStreamOptions {
        base_api_url: url,
        max_records_per_batch: batch,
        max_batch_bytes: batch_bytes,
        max_buffered_bytes: buffer_bytes,
        max_request_delay: Duration::from_secs(2),
        num_upload_workers: workers,
        ..Default::default()
    };
    let stream = Arc::new(
        NominalLogStreamBuilder::default()
            .with_options(opts)
            .stream_to_core(
                env("NOMINAL_TOKEN").parse::<conjure_object::BearerToken>()?,
                env("NOMINAL_DATASET_RID").parse()?,
                runtime.handle().clone(),
            )
            .with_file_fallback(env("BENCH_BACKUP"))
            .build()?,
    );
    let done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let monitor_stream = stream.clone();
    let monitor_done = done.clone();
    let monitor = std::thread::spawn(move || {
        while !monitor_done.load(Ordering::Relaxed) {
            let s = monitor_stream.stats();
            println!(
                "{}",
                json!({"kind":"progress", "seconds":started.elapsed().as_secs_f64(), "accepted":s.accepted_records,"acknowledged":s.acknowledged_records,"requests":s.requests,"retries":s.retries,"buffered_bytes":s.buffered_bytes})
            );
            std::thread::sleep(Duration::from_millis(500));
        }
    });
    let mut input_error = None;
    for start in (0..count).step_by(1000) {
        let records = (start..(start + 1000).min(count))
            .map(|i| record(&run, &phase, i, base))
            .collect();
        if let Err(error) = stream.enqueue_batch(&phase, records) {
            input_error = Some(error.to_string());
            break;
        }
        let s = stream.stats();
        if s.backed_up_records > 0 || s.failed_records > 0 || s.retries > 0 {
            input_error = Some("stopped producer after delivery pressure".into());
            break;
        }
    }
    let producer_seconds = started.elapsed().as_secs_f64();
    let close_error = stream.close().err().map(|e| e.to_string());
    let total_seconds = started.elapsed().as_secs_f64();
    done.store(true, Ordering::Relaxed);
    monitor.join().unwrap();
    let s = stream.stats();
    println!(
        "{}",
        json!({"kind":"result","run":run,"phase":phase,"base_ns":base,"configured_records":count,"batch_records":batch,"workers":workers,"max_batch_bytes":batch_bytes,"max_buffered_bytes":buffer_bytes,"producer_seconds":producer_seconds,"total_seconds":total_seconds,"drain_seconds":total_seconds-producer_seconds,"ack_per_second":s.acknowledged_records as f64/total_seconds,"requests_per_second":s.requests as f64/total_seconds,"accepted":s.accepted_records,"acknowledged":s.acknowledged_records,"backed_up":s.backed_up_records,"failed":s.failed_records,"requests":s.requests,"retries":s.retries,"buffered_bytes":s.buffered_bytes,"last_error":s.last_error,"input_error":input_error,"close_error":close_error})
    );
    if s.acknowledged_records != count as u64 || input_error.is_some() || close_error.is_some() {
        std::process::exit(2);
    }
    Ok(())
}
