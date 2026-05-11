# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

Please refer to the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

## Benchmarks

End-to-end benchmarks simulate an Application-like workload: `enqueue()` calls with inter-arrival delays, routed through the full pipeline (batch processor, dispatcher, HTTP round trip) against a mock server.

### Running

```sh
cargo xtask bench
```

This compiles and runs the benchmark suite with the `bench` feature enabled, then prints a summary table to stderr. Criterion's per-run output is printed to stdout as it runs.

### Saving and comparing baselines

Save the current run as a named baseline:

```sh
cargo xtask bench --save-baseline main
```

Baselines are stored under `target/xtask-baselines/`. Run again to compare:

```sh
cargo xtask bench --baseline main
```

The summary table gains delta columns (`ΔBP%`, `ΔDisp%`, `Δp50%`, `Δp95%`, `Δp99%`) and a `Δ vs baseline` group header.

### Summary table columns

| Column | Description |
|--------|-------------|
| `Workload` | `{enqueues}x{pts/enqueue}pts_{inter-arrival}ms` |
| `Mean(ms)` | Criterion wall-time mean per iteration (enqueue loop only; drain excluded) |
| `±σ(ms)` | Standard deviation of the mean |
| `Throughput` | Points per second: `total_points / session_time`, where session time is wall clock from first enqueue until the last in-flight request is acknowledged (includes inter-arrival sleeps and final drain) |
| `BP(ms)` | Cumulative CPU time spent in the batch processor per iteration |
| `Disp(ms)` | Cumulative CPU time spent in the dispatcher (HTTP send) per iteration |
| `p50 / p95 / p99` | Enqueue-to-server-arrival latency percentiles in ms (measured by the mock server) |
| `p-val` | Criterion's Student's t-test p-value vs the previous saved baseline (`n/a` when no baseline) |
