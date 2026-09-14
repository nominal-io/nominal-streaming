# Log capacity probe

Development diagnostics for [log streaming](log-streaming.md). These probes upload data; run them explicitly against a test dataset.

The `log_capacity` Rust example is an opt-in finite staging probe. Build with
`cargo build --release -p nominal-streaming --example log_capacity --features instrument`.
The existing `instrument` feature enables per-attempt tracing under
`nominal_streaming::log::attempt`, including elapsed microseconds, compressed wire bytes,
success, and a sanitized error. It does not emit credentials or record contents.

## Diagnostic timing

Build with `instrument` to record protobuf encoding and zstd durations, aggregate
connection setup time (DNS/TCP/TLS together), negotiated HTTP version/status, and
first/final request-body handoff offsets. The diagnostic body supplies exact-length,
64 KiB slices of the already compressed buffer; it does not re-encode per retry.
Non-instrumented builds retain the original reusable byte body.

`body_last_chunk_micros` means the HTTP stack consumed the last chunk. It is **not**
a socket-write completion or TCP acknowledgement. The remaining interval until response
headers includes HTTP/TLS/socket buffering, network transit, ingress and backend work.
Connection timings are independent events, not reliably attributable to a single request
because pooled HTTP/2 connections can be shared. They combine DNS, TCP and TLS; they do
not split those phases. Instrumentation may affect scheduling and chunking, so compare
runs using the same build. No payloads, headers or tokens are logged.

## Fixture settings

For the finite capacity example, `BENCH_POLICY=defaults` uses the actual library defaults (except staging URL). `bounded` defaults to the 8 MiB serialized request cap (`BENCH_REQUEST_MIB` permits 1–8 MiB) while configuring batch memory (16–64 MiB), total memory (64–512 MiB) and workers. `BENCH_ENQUEUE_CHUNK` controls caller batch size (1–1,000 records). `stress` deliberately raises limits; result metadata records the effective request, memory, count, worker and flush-delay settings. Do not treat stress results as default-policy behavior.

The capacity probe supports `BENCH_SIMPLE_MESSAGE_BYTES=64..8192` for exact-size ASCII messages with no arguments. This fixture cannot be combined with the extra-argument or extra-message knobs. It uses a fixed log prefix and deterministic varying text; result metadata records the selected size.
