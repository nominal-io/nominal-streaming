//! Purpose-built avro file writer for the Nominal schema.
//!
//! This module provides the pure-Rust file-writer primitive. It writes
//! snappy-compressed avro in the Nominal `AvroStream` schema (same schema as
//! [`crate::consumer::AvroFileConsumer`]).
//!
//! For PyO3 bindings, see the `py-nominal-streaming` crate, which wraps
//! [`AvroWriter`] / [`AvroWriterOpts`] with `#[pyclass]`.
//!
//! # When to use this vs. [`crate::stream::NominalDatasetStream`]
//!
//! - Use [`AvroWriter`] when you only need a file: no network, no fallback,
//!   no retries. Fewer moving parts, simpler opts surface.
//! - Use [`crate::stream::NominalDatasetStream`] when you need to upload to
//!   Nominal Core, with or without a file fallback.
//!
//! # Example
//!
//! ```rust,ignore
//! use nominal_streaming::avro_writer::{AvroWriter, AvroWriterOpts};
//! use nominal_streaming::prelude::*;
//! use std::path::PathBuf;
//!
//! let writer = AvroWriter::new(PathBuf::from("out.avro"), AvroWriterOpts::default())?;
//! let desc = ChannelDescriptor::new("speed");
//! writer.write(&desc, vec![DoublePoint { timestamp: None, value: 3.14 }])?;
//! writer.close()?;
//! ```

mod consumer;
mod error;
mod helpers;
mod opts;
#[cfg(feature = "polars")]
mod polars;
mod state;
mod stats;
mod writer;

pub use error::AvroWriterError;
pub use opts::AvroWriterOpts;
pub use stats::PipelineStats;
pub use writer::AvroWriter;
