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
install: rust::install python::install

# Build all crates / packages
build: rust::build python::build

# Build / prepare development environment
dev: rust::build python::dev

# Check formatting / codestyle and run linting
check: rust::check python::check

# Fix formatting / codestyle
fix: rust::fix python::fix

# Clean cached artifacts
clean: rust::clean python::clean