# Nominal Streaming

`nominal-streaming` is a Rust library for streaming data into Nominal Core.

Please refer to the crate documentation at https://docs.rs/nominal-streaming/latest/nominal_streaming/

### File overwrite policy and startup errors

File destinations overwrite existing contents by default. Opt out for either
primary output or Core upload fallback:

```python
stream.to_file(primary_path, overwrite=False)
```

Or, for a stream configured to send to Core:

```python
stream.with_file_fallback(fallback_path, overwrite=False)
```

Rust provides `stream_to_file_overwrite(path, false)` and
`with_file_fallback_overwrite(path, false)`; the existing methods keep overwriting.
With overwrite disabled, an existing path causes a startup error without changing
its contents. This uses exclusive creation, not append mode.

Rust callers can handle initialization errors with `builder.try_build()`;
`build()` remains the panic-on-error convenience method. Python `open()` raises
`RuntimeError` with the destination and cause, and a failed open can be retried.
A stream supports Core only, file only, Core with a file mirror, or Core with a
file fallback. File output and file fallback cannot be combined; invalid
combinations fail before any file is opened. Destination validation and Core
client initialization precede file opening; stream workers start after the
single configured file destination opens.
