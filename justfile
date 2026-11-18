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

# The theme and sphinx-rustdoc at their newest commits on GitHub, in place of the ones uv.lock pins.
# @main is needed: without a branch, uv keeps the commits uv.lock pins.
newest := '--with "nominal-sphinx-theme @ git+https://github.com/nominal-io/nominal-sphinx-theme@main" --with "sphinx-rustdoc @ git+https://github.com/nominal-io/sphinx-rustdoc@main" --refresh-package nominal-sphinx-theme --refresh-package sphinx-rustdoc'
docs := '--only-group docs --python ">=3.12"'
sphinx := 'python -m sphinx -E -W --keep-going -b dirhtml docs docs/_build/dirhtml'
fallback := "Building with the newest theme and sphinx-rustdoc failed; building with the ones uv.lock pins"

# Build the docs (docs/) into docs/_build/dirhtml with the theme and sphinx-rustdoc uv.lock pins;
# warnings fail the build, as in CI. The Rust API reference is built from `cargo rustdoc`'s JSON
# output, by sphinx-rustdoc. Uses its own venv, .venv-docs.
build-docs $UV_PROJECT_ENVIRONMENT=".venv-docs":
    uv run {{docs}} {{sphinx}}

# As build-docs, with the newest theme and sphinx-rustdoc, and the ones uv.lock pins if that fails.
# CI builds and publishes with this (.github/workflows/docs.yml and deploy-docs.yml).
[unix]
build-docs-newest $UV_PROJECT_ENVIRONMENT=".venv-docs":
    uv run {{docs}} {{newest}} {{sphinx}} || { echo "{{fallback}}"; uv run {{docs}} {{sphinx}}; }

# As build-docs, with the newest theme and sphinx-rustdoc, and the ones uv.lock pins if that fails.
# CI builds and publishes with this (.github/workflows/docs.yml and deploy-docs.yml).
[windows]
build-docs-newest $UV_PROJECT_ENVIRONMENT=".venv-docs":
    uv run {{docs}} {{newest}} {{sphinx}}; if (-not $?) { Write-Host "{{fallback}}"; uv run {{docs}} {{sphinx}} }

# Live-preview the docs on http://127.0.0.1:<port>, rebuilding on edits (`just serve-docs 8001` for another port)
serve-docs port="8000" $UV_PROJECT_ENVIRONMENT=".venv-docs":
    uv run {{docs}} {{newest}} --with sphinx-autobuild sphinx-autobuild -b dirhtml --port {{port}} docs docs/_build/dirhtml --ignore "docs/ref/python/*" --ignore "docs/ref/rust/*"
