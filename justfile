# Set shell for Windows OSs:
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Rust specific commands
mod rust

# Python specific commands
mod python

######################
# Repo-wide Commands #
######################

# List available commands by default
default:
    @just --list

# Install all necessary dependencies for building and running code
install: rust::install python::install install-toml

# Build all crates / packages
build: rust::build python::build

# Build / prepare development environment
dev: rust::build python::dev

# Check formatting / codestyle and run linting
check: rust::check python::check check-toml

# Fix formatting / codestyle
fix: rust::fix python::fix fix-toml

# Clean cached artifacts
clean: rust::clean python::clean

# Install the pinned TOML formatter
install-toml:
    cargo install taplo-cli --version 0.10.0 --locked

# Check repository TOML formatting
check-toml:
    taplo format --check

# Apply repository TOML formatting
fix-toml:
    taplo format

# Build the docs (docs/) into docs/_build/dirhtml; warnings fail the build, as in CI.
# The Rust API reference is built from `cargo rustdoc`'s JSON output, by sphinx-rustdoc.
# Published by .github/workflows/deploy-docs.yml. Uses its own venv, .venv-docs.
build-docs $UV_PROJECT_ENVIRONMENT=".venv-docs":
    uv run --only-group docs --python ">=3.12" sphinx-build -E -W --keep-going -b dirhtml docs docs/_build/dirhtml

# Live-preview the docs on http://127.0.0.1:<port>, rebuilding on edits (`just serve-docs 8001` for another port)
serve-docs port="8000" $UV_PROJECT_ENVIRONMENT=".venv-docs":
    uv run --only-group docs --python ">=3.12" --with sphinx-autobuild sphinx-autobuild -b dirhtml --port {{port}} docs docs/_build/dirhtml --ignore "docs/ref/python/*" --ignore "docs/ref/rust/*"
