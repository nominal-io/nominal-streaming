set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# TODO(drake): switch away from nightly when stable rustfmt supports group_imports and imports_granularity.
fmt-toolchain := "nightly-2026-01-05"

# List available commands by default
default:
    @just --list rust

# Install all necessary dependencies for building / running rust code
install:
    @echo "Running 'install' for Rust code"
    rustup toolchain install {{fmt-toolchain}} --profile minimal --component rustfmt
    cargo fetch

# Lint all targets with core features and the Python 3.11 buffer path enabled.
lint:
    cargo clippy --workspace --all-targets --no-default-features --features nominal-streaming/logging,nominal-streaming/instrument,py-nominal-streaming/python311 --locked -- -D warnings

# Build all rust code
build:
    @echo "Running 'build' for Rust code"
    cargo build

# Validate style and formatting of all rust code
check:
    @echo "Running 'check' for Rust code"
    cargo +{{fmt-toolchain}} fmt --all --check

# Fix formatting of all rust code
fix:
    @echo "Running 'fix' for Rust code"
    cargo +{{fmt-toolchain}} fmt --all

# Clean rust-based build artifacts
clean:
    @echo "Running 'clean' for Rust code"
    cargo clean

# Run rust tests
test:
    @echo "Running 'test' for Rust code"
    cargo test --workspace
