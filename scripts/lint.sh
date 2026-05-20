#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/tooling.sh"

ensure_taplo
ensure_cargo_machete

echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings

echo "Running taplo lint..."
taplo lint

echo "Running cargo machete..."
cargo machete

echo "Linting complete."
