set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# List available commands by default
default:
    @just --list rust

# Install all necessary dependencies for building / running rust code
install:
    @echo "Running 'install' for Rust code"
    cargo fetch

# Build all rust code
build:
    @echo "Running 'build' for Rust code"
    cargo build

# Validate style and formatting of all rust code
check:
    @echo "Running 'check' for Rust code"
    cargo +nightly fmt --all --check

# Fix formatting of all rust code
fix:
    @echo "Running 'fix' for Rust code"
    cargo +nightly fmt --all

# Clean rust-based build artifacts
clean:
    @echo "Running 'clean' for Rust code"
    cargo clean

# Run rust tests
test:
    @echo "Running 'test' for Rust code"
    cargo test --workspace