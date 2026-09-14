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

fn synthetic_text(mut state: u64, count: usize) -> String {
    let mut output = String::with_capacity(count);
    for _ in 0..count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        output.push((b'a' + (state % 26) as u8) as char);
    }
    output
}

fn record(
    run: &str,
    phase: &str,
    sequence: usize,
    base: i64,
    extra_args: usize,
    extra_message_bytes: usize,
) -> LogRecord {
    let level = ["DEBUG", "INFO", "WARNING", "ERROR"][sequence % 4];
    let service = ["telemetry-gateway", "scheduler", "worker", "api"][sequence % 4];
    let component = ["ingest", "routing", "validation", "dispatch"][sequence % 4];
    let trace = format!(
        "{sequence:016x}{:08x}",
        (sequence as u64 * 0x9e3779b1) & 0xffffffff
    );
    let mut message = format!("{level} service={service} component={component} handled synthetic request run={run} sequence={sequence:016x} trace={trace} operation=stream_log outcome=accepted route=/api/v1/events retryable=false payload_class=structured benchmark=true");
    let mut args = HashMap::from([
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
    message.push_str(&synthetic_text(
        sequence as u64 ^ 0xabcdef,
        extra_message_bytes,
    ));
    for index in 0..extra_args {
        args.insert(
            format!("extra_{index:03}"),
            synthetic_text(sequence as u64 ^ ((index as u64 + 1) << 32), 64),
        );
    }
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
    let extra_args: usize = std::env::var("BENCH_EXTRA_ARGS")
        .unwrap_or_else(|_| "0".into())
        .parse()?;
    let extra_message_bytes: usize = std::env::var("BENCH_EXTRA_MESSAGE_BYTES")
        .unwrap_or_else(|_| "0".into())
        .parse()?;
    if extra_args > 116 || extra_message_bytes > 8192 {
        return Err("payload bounds exceeded".into());
    }
    let enqueue_chunk: usize = std::env::var("BENCH_ENQUEUE_CHUNK")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    if !(1..=1000).contains(&enqueue_chunk) {
        return Err("enqueue chunk must be 1..=1000".into());
    }
    let run = env("BENCH_RUN");
    let phase = env("BENCH_PHASE");
    let base: i64 = env("BENCH_BASE_NS").parse()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    // Scale the accounted byte budgets with the optional richer fixture payload.
    // Total includes queued and in-flight work, capped at 6 GiB; the launcher checks available host memory.
    let batch_bytes = batch * (4096 + extra_args * 512 + extra_message_bytes * 4);
    let buffer_bytes = (batch_bytes * (workers + 1)).min(6 * 1024 * 1024 * 1024);
    let policy = std::env::var("BENCH_POLICY").unwrap_or_else(|_| "stress".into());
    let opts = match policy.as_str() {
        "defaults" => LogStreamOptions {
            base_api_url: url,
            ..Default::default()
        },
        "bounded" => {
            let batch_mib: usize = env("BENCH_BATCH_MIB").parse()?;
            let buffer_mib: usize = env("BENCH_BUFFER_MIB").parse()?;
            if !(16..=64).contains(&batch_mib) || !(64..=512).contains(&buffer_mib) {
                return Err("bounded probe memory limits exceeded".into());
            }
            LogStreamOptions {
                base_api_url: url,
                max_batch_bytes: batch_mib * 1024 * 1024,
                max_buffered_bytes: buffer_mib * 1024 * 1024,
                num_upload_workers: workers,
                ..Default::default()
            }
        }
        "stress" => LogStreamOptions {
            base_api_url: url,
            max_request_bytes: batch_bytes,
            max_records_per_batch: batch,
            max_batch_bytes: batch_bytes,
            max_buffered_bytes: buffer_bytes,
            max_request_delay: Duration::from_secs(2),
            num_upload_workers: workers,
            ..Default::default()
        },
        _ => return Err("BENCH_POLICY must be defaults, bounded or stress".into()),
    };
    let batch = opts.max_records_per_batch;
    let workers = opts.num_upload_workers;
    let batch_bytes = opts.max_batch_bytes;
    let buffer_bytes = opts.max_buffered_bytes;
    let request_bytes = opts.max_request_bytes;
    let flush_delay_ms = opts.max_request_delay.as_millis();
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
    for start in (0..count).step_by(enqueue_chunk) {
        let records = (start..(start + enqueue_chunk).min(count))
            .map(|i| record(&run, &phase, i, base, extra_args, extra_message_bytes))
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
        json!({"kind":"result","run":run,"phase":phase,"base_ns":base,"configured_records":count,"enqueue_chunk":enqueue_chunk,"policy":policy,"max_request_bytes":request_bytes,"flush_delay_ms":flush_delay_ms,"extra_args":extra_args,"extra_message_bytes":extra_message_bytes,"batch_records":batch,"workers":workers,"max_batch_bytes":batch_bytes,"max_buffered_bytes":buffer_bytes,"producer_seconds":producer_seconds,"total_seconds":total_seconds,"drain_seconds":total_seconds-producer_seconds,"ack_per_second":s.acknowledged_records as f64/total_seconds,"requests_per_second":s.requests as f64/total_seconds,"accepted":s.accepted_records,"acknowledged":s.acknowledged_records,"backed_up":s.backed_up_records,"failed":s.failed_records,"requests":s.requests,"retries":s.retries,"buffered_bytes":s.buffered_bytes,"last_error":s.last_error,"input_error":input_error,"close_error":close_error})
    );
    if s.acknowledged_records != count as u64 || input_error.is_some() || close_error.is_some() {
        std::process::exit(2);
    }
    Ok(())
}
