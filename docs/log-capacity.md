# Log capacity probe

Development diagnostics for [log streaming](log-streaming.md). These probes upload data; run them explicitly against a test dataset.

The `log_capacity` Rust example is an opt-in finite staging probe. Build with
`cargo build --release -p nominal-streaming --example log_capacity`.
It enables debug tracing for `nominal_streaming::log` and records elapsed request time,
compressed bytes, encoding time, retry decisions and delivery outcomes.

These are the same structured tracing events used by the library. Log filtering does
not change request bodies, connection behavior or tracing headers. The existing
`instrument` feature remains available for time-series work counters; logs do not
require it for diagnostics.

Request elapsed time spans the send until response headers arrive. It does not split
network, ingress and backend time. Credentials and message contents are not logged.

## Fixture settings

For the finite capacity example, `BENCH_POLICY=defaults` uses the actual library defaults (except staging URL). `bounded` defaults to the 8 MiB serialized request cap (`BENCH_REQUEST_MIB` permits 1–8 MiB) while configuring batch memory (16–64 MiB), total memory (64–512 MiB) and workers. `BENCH_ENQUEUE_CHUNK` controls caller batch size (1–1,000 records). `stress` deliberately raises limits; result metadata records the effective request, memory, count, worker and flush-delay settings. Do not treat stress results as default-policy behavior.

The capacity probe supports `BENCH_SIMPLE_MESSAGE_BYTES=64..8192` for exact-size ASCII messages with no arguments. This fixture cannot be combined with the extra-argument or extra-message knobs. It uses a fixed log prefix and deterministic varying text; result metadata records the selected size.
