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

# Validate style and formatting of all rust code (rustfmt + taplo + ASCII)
check:
    @echo "Running 'check' for Rust code"
    bash scripts/fmt.sh --check

# Run all lint gates (clippy -D warnings + taplo lint + cargo machete)
lint:
    @echo "Running 'lint' for Rust code"
    bash scripts/lint.sh

# Format all rust code (rustfmt + taplo)
fmt:
    @echo "Running 'fmt' for Rust code"
    bash scripts/fmt.sh

# Fix formatting of all rust code (alias for fmt)
fix:
    @echo "Running 'fix' for Rust code"
    bash scripts/fmt.sh

# Clean rust-based build artifacts
clean:
    @echo "Running 'clean' for Rust code"
    cargo clean

# Run rust tests (workspace, all features)
test:
    @echo "Running 'test' for Rust code"
    bash scripts/test.sh