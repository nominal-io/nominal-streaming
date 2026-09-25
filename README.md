# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

Please refer to the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

### Checked graceful close

Rust callers can observe background delivery and file-finalization failures with
`stream.close()`. Drop scoped channel writers first so their buffered points are
enqueued, then call close through an exclusive mutable reference:

```rust
{
    let mut writer = stream.double_writer(channel);
    writer.push(timestamp, value);
}
stream.close()?;
```

Python `close()` and context-manager exit raise `RuntimeError` on a delivery or
finalization failure. The runtime is shut down even after a failed close, and
repeated close calls retain the error. If the context body also failed, its
exception remains chained as the cause. Both values of `close(wait=...)` currently
drain; `wait=False` is not an asynchronous completion API.

Successful fallback is successful preservation, including when the primary
failed authentication. Close continues draining after a failed request and
attempts every destination's finalizer. It reports the first failure rather than
point counts. Drop alone can only log failures. This covers graceful shutdown,
not abrupt process loss, and a custom consumer that never returns can block it.
