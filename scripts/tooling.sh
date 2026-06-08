#!/usr/bin/env bash
# Pinned tool versions -- single source of truth for all scripts and CI.

NIGHTLY_TOOLCHAIN="nightly-2026-03-10"
TAPLO_VERSION="0.9.3"
CARGO_MACHETE_VERSION="0.9.1"

ASCII_FILE_GLOBS="*.rs *.toml *.md *.sh *.yml *.yaml *.json *.html"

# check_ascii_files <repo_root>: scan tracked text files for non-ASCII bytes.
# Returns 1 if any are found. Used by fmt.sh and the commit-msg hook.
check_ascii_files() {
    local repo_root="$1"
    local globs=()
    read -ra globs <<< "$ASCII_FILE_GLOBS"
    local fail=()
    while IFS= read -r file; do
        if LC_ALL=C grep -Pn '[^\x00-\x7F]' "$repo_root/$file" >/dev/null 2>&1; then
            local lines
            lines="$(LC_ALL=C grep -Pn '[^\x00-\x7F]' "$repo_root/$file")"
            fail+=("$file"$'\n'"$lines")
        fi
    done < <(git -C "$repo_root" ls-files --full-name -- "${globs[@]}")
    if [[ ${#fail[@]} -gt 0 ]]; then
        echo "ERROR: non-ASCII characters found:" >&2
        for entry in "${fail[@]}"; do
            echo "$entry" >&2
        done
        echo "" >&2
        echo "Only ASCII (0x00-0x7F) is allowed. Replace smart quotes, em dashes," >&2
        echo "and other non-ASCII characters with their ASCII equivalents." >&2
        return 1
    fi
    return 0
}

# check_ascii_text <file>: check a single file for non-ASCII bytes.
# Used by the commit-msg hook.
check_ascii_text() {
    local file="$1"
    if LC_ALL=C grep -Pn '[^\x00-\x7F]' "$file" >/dev/null 2>&1; then
        echo "ERROR: non-ASCII characters found:" >&2
        LC_ALL=C grep -Pn '[^\x00-\x7F]' "$file" >&2
        echo "" >&2
        echo "Only ASCII (0x00-0x7F) is allowed." >&2
        return 1
    fi
    return 0
}

check_ascii_staged_files() {
    local repo_root="$1"
    local globs=()
    read -ra globs <<< "$ASCII_FILE_GLOBS"
    local fail=()
    while IFS= read -r file; do
        [[ -z "$file" ]] && continue
        if git -C "$repo_root" show ":0:$file" 2>/dev/null | LC_ALL=C grep -Pn '[^\x00-\x7F]' >/dev/null 2>&1; then
            local lines
            lines="$(git -C "$repo_root" show ":0:$file" 2>/dev/null | LC_ALL=C grep -Pn '[^\x00-\x7F]')"
            fail+=("$file"$'\n'"$lines")
        fi
    done < <(git -C "$repo_root" diff --cached --name-only --diff-filter=ACM -- "${globs[@]}")
    if [[ ${#fail[@]} -gt 0 ]]; then
        echo "ERROR: non-ASCII characters found in staged files:" >&2
        for entry in "${fail[@]}"; do
            echo "$entry" >&2
        done
        echo "" >&2
        echo "Only ASCII (0x00-0x7F) is allowed. Replace smart quotes, em dashes," >&2
        echo "and other non-ASCII characters with their ASCII equivalents." >&2
        return 1
    fi
    return 0
}

# ensure_nightly [--check]: install the pinned nightly toolchain with rustfmt if
# missing. With --check, verify the nightly toolchain and rustfmt are available.
ensure_nightly() {
    if [[ "${1:-}" == "--check" ]]; then
        if ! rustup run "$NIGHTLY_TOOLCHAIN" cargo fmt --version &>/dev/null; then
            echo "ERROR: rustfmt not available for $NIGHTLY_TOOLCHAIN" >&2
            echo "Run: rustup toolchain install $NIGHTLY_TOOLCHAIN --component rustfmt --profile minimal" >&2
            return 1
        fi
        return 0
    fi
    if ! rustup run "$NIGHTLY_TOOLCHAIN" cargo fmt --version &>/dev/null; then
        echo "Installing $NIGHTLY_TOOLCHAIN with rustfmt..."
        rustup toolchain install "$NIGHTLY_TOOLCHAIN" --component rustfmt --profile minimal
    fi
}

# ensure_taplo [--check]: install taplo-cli if missing. With --check, verify the
# installed version matches the pinned version and fail if not.
ensure_taplo() {
    local installed
    if [[ "${1:-}" == "--check" ]]; then
        installed="$(taplo --version 2>/dev/null | awk '{print $2}')" || true
        if [[ "$installed" != "$TAPLO_VERSION" ]]; then
            echo "ERROR: taplo version mismatch: installed=$installed, expected=$TAPLO_VERSION" >&2
            return 1
        fi
        return 0
    fi
    if ! command -v taplo &>/dev/null; then
        echo "Installing taplo-cli@$TAPLO_VERSION..."
        cargo install "taplo-cli@$TAPLO_VERSION" --locked
    fi
}

# ensure_cargo_machete [--check]: install cargo-machete if missing. With --check,
# verify the installed version matches the pinned version and fail if not.
ensure_cargo_machete() {
    local installed
    if [[ "${1:-}" == "--check" ]]; then
        installed="$(cargo machete --version 2>/dev/null)" || true
        if [[ "$installed" != "$CARGO_MACHETE_VERSION" ]]; then
            echo "ERROR: cargo-machete version mismatch: installed=$installed, expected=$CARGO_MACHETE_VERSION" >&2
            return 1
        fi
        return 0
    fi
    if ! command -v cargo-machete &>/dev/null; then
        echo "Installing cargo-machete@$CARGO_MACHETE_VERSION..."
        cargo install "cargo-machete@$CARGO_MACHETE_VERSION" --locked
    fi
}
