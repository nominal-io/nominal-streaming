set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# List available commands by default
default:
    @just --list python

# Install all neessary python packages for building / running python code
install:
    @echo "Running 'install' for Python code"
    uv --directory py-nominal-streaming sync

# Build python code + rust bindings into wheel file
build:
    @echo "Running 'build' for Python code"
    uv --directory py-nominal-streaming run maturin build

# Build python code + rust bindings into local dev mode
dev:
    @echo "Running 'dev' for Python code"
    uv --directory py-nominal-streaming run maturin develop

# Start python repl with package prepared
repl: dev
    @echo "Running 'python' for Python code"
    uv --directory py-nominal-streaming run python

# Validate typehints of all python code
check-types:
    @echo "Running 'check-types' for Python code"
    uv --directory py-nominal-streaming run mypy

# Validate typehints of all python code across all supported python versions
check-types-all:
    @echo "Running 'check-types-all' for Python code"
    uv --directory py-nominal-streaming run mypy --python-version 3.14
    uv --directory py-nominal-streaming run mypy --python-version 3.13
    uv --directory py-nominal-streaming run mypy --python-version 3.12
    uv --directory py-nominal-streaming run mypy --python-version 3.11
    uv --directory py-nominal-streaming run mypy --python-version 3.10

# Validate formatting of all python code
check-format:
    @echo "Running 'check-format' for Python code"
    uv --directory py-nominal-streaming run ruff format --check

# Validate formatting of all python imports
check-imports:
    @echo "Running 'check-imports' for Python code"
    uv --directory py-nominal-streaming run ruff check

# Validate formatting and typehints of all python code
check: check-format check-types check-imports
    @echo "Running 'check' for Python code"

# Automatically fix formatting of all python imports
fix-imports:
    @echo "Running 'fix-imports' for Python code"
    uv --directory py-nominal-streaming run ruff check --fix

# Automatically fix formatting of all python code
fix-format:
    @echo "Running 'fix-format' for Python code"
    uv --directory py-nominal-streaming run ruff format

# Automatically apply all python codestyle fixes
fix: fix-format fix-imports

# Clean any python build artifacts
clean:
    @echo "Running 'clean' for Python code"
    uv --directory py-nominal-streaming cache clean
