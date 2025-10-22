# nominal-streaming python bindings

The py-nominal-streaming crate contains pyo3 and maturin bindings to allow users to stream data performantly from python.

To build, you may run `uv build --package nominal-streaming` from the repository root.
Alternatively, you may run `uv --directory py-nominal-streaming run maturin develop` to build in developer (hot reload) mode, or `uv --directory py-nominal-streaming run maturin build` to build a wheel with maturin directly.

In general, it is recommended to use the provided justfile recipes-- e.g., `just python::check`, `just python::build`, `just python::fix`, etc.
