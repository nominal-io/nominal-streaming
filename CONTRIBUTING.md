
# Contributing

## Justfile Setup

Developer workflows are run with [`just`](https://github.com/casey/just).
You can use `just -l` to list commands, or view the `justfile` for the specific commands.

Some common commands are as follows:

- `just install`: Installs relevant dependencies for rust and python bindings.
  - `just rust::install` or `just python::install` for language specific versions.
- `just build`: Builds rust crate and python bindings wheel.
  - `just rust::build` or `just python::build` for language specific versions
  - `just dev`: Builds rust crate and builds / installs python bindings in developer mode instead.
- `just check`: Validate linting and formatting across rust and python bindings.
  - `just rust::check` or `just python::check` for language specific versions.
- `just fix`: Automatically format across rust and python bindings.
  - `just rust::fix` or `just python::fix` for language specific versions.


## Nix Setup

A dev shell for the Rust crate is also available via nix. 

If it is your first time developing against this repo, make sure you have direnv installed and run:

```shell
direnv allow
```

Then, to get a Rust dev shell:

```shell
nix develop
```

This will spin up a minimal dev environment with some Rust utilities.


## Python bindings

The py-nominal-streaming crate contains pyo3 and maturin bindings to allow users to stream data performantly from python.

To build, you may run `uv build --package nominal-streaming` from the repository root.
Alternatively, you may run `uv run maturin develop -m py-nominal-streaming/Cargo.toml` to build in developer (hot reload) mode, or `uv run maturin build -m py-nominal-streaming/Cargo.toml` to build a wheel with maturin directly.

Working with the rust => python bindings is done using `uv` and `maturin` and exposed to developers using `justfile`s.

The most common workflows are as follows:

```shell
# Install dependencies and initialize workspace
just install

# Build python bindings in developer mode
just python::dev

# (Optional) check codestyle / formatting
just python::check

# (Optional) fix codestyle / formatting
just python::fix

# Run test script
uv run py-nominal-streaming/examples/test.py

# Build wheel file for installing in other environments
just python::build
```

To run common workflows manually, either `cd` into the `py-nominal-streaming` directory or use `--directory` to specify the working directory in all `uv` commands (the following details will assume you did the former).

- Build and run in developer mode:
  
  ```shell
  uv run maturin develop    # Build bindings / dependent rust code
  uv run python             # Run python interpreter with bindings loaded 
  ```

- Build wheel file for distributing / installing:

  ```shell
  uv run maturin build  # Places the `whl` file in the `target/wheels` directory
  ```

If updating any public-facing bindings from the rust side (e.g. updating `PyNominalDatasetStream` or `PyNominalStreamOpts`), ensure that you make the appropriate changes to [the python bindings](py-nominal-streaming/python/nominal_streaming/_nominal_streaming.pyi).

