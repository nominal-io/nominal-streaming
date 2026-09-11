# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

Please refer to the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

### Runtime metrics

Set `NominalStreamOpts { track_metrics: true, ..Default::default() }` in Rust or
`PyNominalStreamOpts(track_metrics=True)` in Python to opt into runtime metric
channels. See [the metric inventory and delivery semantics](docs/runtime-metrics.md).
