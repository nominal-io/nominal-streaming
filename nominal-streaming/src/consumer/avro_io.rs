use std::io::Write;
use std::io::{self};

/// Avro 0.17 uses `write` for complete blocks and markers. Supply complete writes
/// and refuse to reuse a stream after a partial failure could have damaged it.
pub(super) struct CompleteWriter<W> {
    inner: W,
    failure: Option<(io::ErrorKind, String)>,
}

impl<W: Write> CompleteWriter<W> {
    pub(super) fn new(inner: W) -> Self {
        Self {
            inner,
            failure: None,
        }
    }

    fn check(&self) -> io::Result<()> {
        match &self.failure {
            Some((kind, message)) => Err(io::Error::new(*kind, message.clone())),
            None => Ok(()),
        }
    }

    fn record(&mut self, result: io::Result<()>) -> io::Result<()> {
        if let Err(error) = &result {
            self.failure = Some((error.kind(), error.to_string()));
        }
        result
    }
}

impl<W: Write> Write for CompleteWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.check()?;
        let result = self.inner.write_all(bytes);
        self.record(result)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.check()?;
        let result = self.inner.flush();
        self.record(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct ShortWriter {
        bytes: Vec<u8>,
        interrupted: bool,
    }
    impl Write for ShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(io::ErrorKind::Interrupted.into());
            }
            let len = bytes.len().min(2);
            self.bytes.extend_from_slice(&bytes[..len]);
            Ok(len)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn completes_short_and_interrupted_writes() {
        let mut writer = CompleteWriter::new(ShortWriter::default());
        assert_eq!(writer.write(b"complete block").unwrap(), 14);
        assert_eq!(writer.inner.bytes, b"complete block");
    }

    #[test]
    fn partial_failure_cannot_be_followed_by_apparent_success() {
        let mut bytes = [0u8; 3];
        let mut writer = CompleteWriter::new(bytes.as_mut_slice());
        assert_eq!(
            writer.write(b"too long").unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert!(writer.write(b"").is_err());
        assert!(writer.flush().is_err());
    }
}
