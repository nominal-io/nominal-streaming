# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

Please refer to the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

## Log streaming

For timestamped messages with per-record arguments, use the dedicated Rust/Python `NominalLogStream`. It provides bounded buffering, protobuf + zstd uploads, exponential-backoff retries, and journal JSON backup of failed deliveries. See [Python log stream usage](py-nominal-streaming/README.md#log-streams).
