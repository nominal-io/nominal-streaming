use crate::consumer::ConsumerError;

/// Errors produced by [`super::AvroWriter`].
///
/// Latched errors surface via `Consumer` for any variant that couldn't be
/// preserved across the `&ConsumerError` → `AvroWriterError` boundary (avro
/// errors specifically, because `apache_avro::Error` isn't `Clone`).
///
/// These errors are wrapped in `Arc` for idiomatic sharing of a sticky,
/// cached error across producer threads and repeated calls — once a writer
/// latches a failure, every subsequent method call returns the same
/// `Arc<AvroWriterError>` without re-walking the failure path.
#[derive(Debug, thiserror::Error)]
pub enum AvroWriterError {
    /// I/O error from the underlying file (open, write, sync, etc.).
    /// Latched I/O errors are reconstructed from `io::ErrorKind` + message
    /// so the `Io` variant is preserved through the error-latching consumer layer.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Error from the avro writer (schema mismatch, serialization failure).
    /// Avro errors latch as `Consumer` because `apache_avro::Error` isn't `Clone`.
    #[error("avro error: {0}")]
    Avro(#[from] Box<apache_avro::Error>),

    /// Error from the underlying [`crate::consumer::AvroFileConsumer`] sink,
    /// or a latched avro error (avro errors flatten here because
    /// `apache_avro::Error` isn't `Clone`).
    #[error("consumer error: {0}")]
    Consumer(String),

    /// `write` was called after `close()` completed. Always means caller
    /// misuse, not encoder failure.
    #[error("write attempted after close")]
    SendAfterClose,
}

impl From<ConsumerError> for AvroWriterError {
    fn from(e: ConsumerError) -> Self {
        match e {
            ConsumerError::IoError(io) => AvroWriterError::Io(io),
            ConsumerError::AvroError(avro) => AvroWriterError::Avro(avro),
            other => AvroWriterError::Consumer(other.to_string()),
        }
    }
}

/// Construct an `AvroWriterError` from a `ConsumerError` that the caller
/// needs to retain (so we can't consume it). Preserves the `Io` variant
/// via `io::Error::new(kind, msg)`; avro errors flatten to `Consumer`
/// because `apache_avro::Error` isn't Clone.
pub(super) fn avro_error_from_consumer_ref(e: &ConsumerError) -> AvroWriterError {
    match e {
        ConsumerError::IoError(io) => {
            AvroWriterError::Io(std::io::Error::new(io.kind(), io.to_string()))
        }
        other => AvroWriterError::Consumer(other.to_string()),
    }
}
