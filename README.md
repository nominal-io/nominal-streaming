# Nominal Streaming

nominal-streaming is a Rust library for streaming data into Nominal Core.

The library aims to balance three concerns:
1. Data should exist in-memory only for a limited, configurable amount of time before it's sent to Core.
1. Writes should fall back to disk if there are network failures.
1. Backpressure should be applied to incoming requests when network throughput is saturated.

As a library, the balance between these concerns can be configured (and even disabled entirely) to accommodate various use cases.

> [!WARNING]  
> This library is still under active development and may make breaking changes.
