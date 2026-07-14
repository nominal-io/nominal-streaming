#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/tooling.sh"

check_flag=""
if [[ "${1:-}" == "--check" ]]; then
    check_flag="--check"
fi

ensure_nightly
ensure_taplo

echo "Running rustfmt ($NIGHTLY_TOOLCHAIN)..."
cargo +"$NIGHTLY_TOOLCHAIN" fmt --all $check_flag

echo "Running taplo fmt..."
if [[ -n "$check_flag" ]]; then
    taplo fmt --check --diff
else
    taplo fmt
fi

echo "Checking for non-ASCII characters..."
if [[ -n "$check_flag" ]]; then
    check_ascii_files "$REPO_ROOT"
else
    check_ascii_files "$REPO_ROOT" || true
fi

echo "Formatting complete."
