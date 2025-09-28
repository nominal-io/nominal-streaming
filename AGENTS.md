# Repository Guidelines

## Project Structure & Module Organization

- Library crate lives under `src/` with `lib.rs` re-exporting the main modules.
- `src/consumer.rs`, `src/stream.rs`, and `src/client.rs` handle the streaming pipeline and external I/O.
- Supporting utilities sit in `src/types.rs` and `src/notifier.rs`; place new helper types alongside these.
- Integration tests live inline with modules via `#[cfg(test)]`; add new tests next to the code they cover.

## Build, Test, and Development Commands

- `cargo build` compiles the library and ensures new APIs integrate cleanly.
- `cargo test` runs unit tests (with `test-log` enabled) and should pass before every push.
- `cargo +nightly fmt --all` applies the repository rustfmt settings; run before committing.
- `cargo clippy --all-targets --all-features` catches common mistakes; address warnings or justify them.
- `cargo doc --open` renders API docs; check public items for clarity when adding features.

## Coding Style & Naming Conventions

- Use Rust 2021 idioms, keeping modules single-purpose and aligned with filenames.
- Functions and module-level helpers use `snake_case`; types and traits use `PascalCase`; constants stay `SCREAMING_SNAKE_CASE`.
- Use `tracing` macros for logging and instrumentation.
- Keep async code on Tokio-friendly APIs; clone handles instead of creating ad-hoc runtimes.

## Testing Guidelines

- Co-locate tests in `mod tests` blocks using descriptive function names like `handles_disk_fallback`.
- Unit tests should cover specific functionality, integration and end-to-end tests are not required.
- Mock external clients with in-memory channels; avoid network calls in CI.
- Cover both happy paths and failure recovery (e.g., disk fallback, retry logic).
- Run `cargo test -- --nocapture` when debugging to view output.

## Commit & Pull Request Guidelines

- Use Conventional Commit prefixes (`feat:`, `fix:`, `chore:`, `docs:`) mirroring existing history and reference issues as `(#123)` when applicable.
- PR descriptions should include a concise summary, motivation for the change, and the commands/tests executed (paste output when failure-driven).

## Security & Configuration Tips

- Never commit tokens or dataset identifiers; load them via `NOMINAL_TOKEN` and other environment variables at runtime.
