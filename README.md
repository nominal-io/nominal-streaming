# Nominal Streaming

nominal-streaming is a Rust library for streaming data into Nominal Core.

The library aims to balance three concerns:
1. Data should exist in-memory only for a limited, configurable amount of time before it's sent to Core.
1. Writes should fall back to disk if there are network failures.
1. Backpressure should be applied to incoming requests when network throughput is saturated.

The library provides configuration points to manage the tradeoff between these concerns.

> [!WARNING]  
> This library is still under active development and may make breaking changes.
