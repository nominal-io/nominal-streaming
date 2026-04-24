//! PyO3 facade for [`nominal_streaming::avro_writer::AvroWriter`].
//!
//! This module provides [`NominalAvroWriter`] and [`NominalAvroWriterOpts`],
//! the Python-facing pyclass wrappers around the pure-Rust `AvroWriter` /
//! `AvroWriterOpts` types that live in the `nominal-streaming` crate. The
//! pure-Rust core is enabled with the `polars` feature (transitively, via
//! py-nominal-streaming's Cargo.toml) so `write_dataframe` is available.
//!
//! # Example (Python)
//!
//! ```python
//! from nominal_streaming import NominalAvroWriter, NominalAvroWriterOpts
//! with NominalAvroWriter("out.avro") as w:
//!     w.write("speed", ts_ns, 3.14)
//! ```

mod error_map;
mod opts;
mod polars_ffi;
mod pymethods;
mod writer;

pub use opts::NominalAvroWriterOpts;
pub use writer::NominalAvroWriter;
