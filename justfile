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
