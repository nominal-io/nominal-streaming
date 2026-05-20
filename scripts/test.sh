#!/usr/bin/env bash
set -euo pipefail

EXTRA_ARGS=()

if [[ "${1:-}" == "--release" ]]; then
    EXTRA_ARGS+=(--release)
    shift
fi

echo "Running cargo test ${EXTRA_ARGS[*]:-}..."
cargo test --all --all-features ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

echo "Tests complete."
