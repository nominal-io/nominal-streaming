use std::io::Write;
use std::io::{self};
use std::sync::Arc;

use apache_avro::types::Record;

pub(super) trait DurableWrite: Write {
    fn sync_all(&self) -> io::Result<()>;
}

impl DurableWrite for std::fs::File {
    fn sync_all(&self) -> io::Result<()> {
        std::fs::File::sync_all(self)
    }
}

// apache-avro 0.17 uses Write::write for container framing and block payloads,
// and assumes it writes every byte. Complete each write, including EINTR retries.
pub(super) struct CompleteWrite<W>(pub W);

impl<W: Write> Write for CompleteWrite<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write_all(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

pub(super) struct AvroWriter<W: DurableWrite> {
    writer: Option<apache_avro::Writer<'static, CompleteWrite<W>>>,
    failure: Option<Arc<io::Error>>,
    path: std::path::PathBuf,
}

impl<W: DurableWrite> AvroWriter<W> {
    pub(super) fn new(
        writer: apache_avro::Writer<'static, CompleteWrite<W>>,
        path: std::path::PathBuf,
    ) -> Self {
        Self {
            writer: Some(writer),
            failure: None,
            path,
        }
    }

    fn check_failure(&self) -> io::Result<()> {
        match &self.failure {
            Some(error) => Err(io::Error::new(error.kind(), Arc::clone(error))),
            None => Ok(()),
        }
    }

    fn remember(&mut self, result: io::Result<()>) -> io::Result<()> {
        if let Err(error) = result {
            self.failure = Some(Arc::new(error));
            // A partial block or a compressed buffer cannot safely be retried.
            self.writer = None;
        }
        self.check_failure()
    }

    pub(super) fn append(&mut self, records: Vec<Record<'_>>) -> io::Result<()> {
        self.check_failure()?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("Avro writer is finished"))?;
        let result = writer.extend(records).map(|_| ()).map_err(avro_error);
        self.remember(result)
    }

    pub(super) fn finish(&mut self) -> io::Result<()> {
        self.check_failure()?;
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let result = (|| {
            // into_inner also writes a header for an empty container.
            let mut sink = writer.into_inner().map_err(avro_error)?;
            sink.flush()?;
            sink.0.sync_all()
        })();
        self.remember(result)
    }
}

// Preserve the actual I/O cause: apache-avro's display text omits it.
fn avro_error(error: apache_avro::Error) -> io::Error {
    match error {
        apache_avro::Error::WriteBytes(source) | apache_avro::Error::WriteMarker(source) => source,
        other => io::Error::other(other),
    }
}

impl<W: DurableWrite> Drop for AvroWriter<W> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::warn!(path = ?self.path, %error, "failed to finalize Avro writer on drop");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::io::{self};

    use super::*;

    #[derive(Default)]
    struct FaultyWriter {
        bytes: Vec<u8>,
        interrupted: bool,
        fail_after: Option<usize>,
        fail_sync: bool,
        fail_flush: bool,
    }

    impl Write for FaultyWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(io::ErrorKind::Interrupted.into());
            }
            if self
                .fail_after
                .is_some_and(|limit| self.bytes.len() >= limit)
            {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "injected write failure",
                ));
            }
            let len = bytes.len().min(3);
            self.bytes.extend_from_slice(&bytes[..len]);
            Ok(len)
        }
        fn flush(&mut self) -> io::Result<()> {
            if self.fail_flush {
                Err(io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    impl DurableWrite for FaultyWriter {
        fn sync_all(&self) -> io::Result<()> {
            if self.fail_sync {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected sync failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn writer(sink: FaultyWriter) -> AvroWriter<FaultyWriter> {
        AvroWriter::new(
            apache_avro::Writer::with_codec(
                &super::super::CORE_AVRO_SCHEMA,
                CompleteWrite(sink),
                apache_avro::Codec::Snappy,
            ),
            "test.avro".into(),
        )
    }

    fn record() -> apache_avro::types::Record<'static> {
        let mut record = apache_avro::types::Record::new(&super::super::CORE_AVRO_SCHEMA).unwrap();
        record.put("channel", "ch");
        record.put("timestamps", apache_avro::types::Value::Array(vec![]));
        record.put("values", apache_avro::types::Value::Array(vec![]));
        record.put("tags", std::collections::HashMap::<String, String>::new());
        record
    }

    #[test]
    fn drop_flushes_buffered_records() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut state = AvroWriter::new(
            apache_avro::Writer::with_codec(
                &super::super::CORE_AVRO_SCHEMA,
                CompleteWrite(file.reopen().unwrap()),
                apache_avro::Codec::Snappy,
            ),
            file.path().to_path_buf(),
        );
        // append buffers the record; dropping the owner must flush it.
        state.writer.as_mut().unwrap().append(record()).unwrap();
        drop(state);
        let records = apache_avro::Reader::new(file.reopen().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn short_and_interrupted_writes_produce_readable_avro() {
        let mut state = writer(FaultyWriter::default());
        state.append(vec![record()]).unwrap();
        let sink = state.writer.take().unwrap().into_inner().unwrap();
        let records = apache_avro::Reader::new(sink.0.bytes.as_slice())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn partial_write_failure_is_sticky() {
        let mut baseline = writer(FaultyWriter::default());
        baseline.append(vec![record()]).unwrap();
        let length = baseline
            .writer
            .take()
            .unwrap()
            .into_inner()
            .unwrap()
            .0
            .bytes
            .len();
        // Fail both during the header and near the end of the compressed block.
        for limit in [6, length - 3] {
            let mut state = writer(FaultyWriter {
                fail_after: Some(limit),
                ..Default::default()
            });
            let first = state.append(vec![record()]).unwrap_err();
            assert_eq!(first.kind(), io::ErrorKind::WriteZero);
            assert!(first.to_string().contains("injected write failure"));
            let error = first.to_string();
            assert_eq!(state.append(vec![record()]).unwrap_err().to_string(), error);
            assert_eq!(state.finish().unwrap_err().to_string(), error);
            assert!(state.writer.is_none());
        }
    }

    #[test]
    fn underlying_flush_failure_is_sticky() {
        let mut state = writer(FaultyWriter {
            fail_flush: true,
            ..Default::default()
        });
        state.append(vec![record()]).unwrap();
        let error = state.finish().unwrap_err().to_string();
        assert!(error.contains("injected flush failure"));
        assert_eq!(state.finish().unwrap_err().to_string(), error);
    }

    #[test]
    fn sync_failure_is_sticky() {
        let mut state = writer(FaultyWriter {
            fail_sync: true,
            ..Default::default()
        });
        state.append(vec![record()]).unwrap();
        let first = state.finish().unwrap_err();
        let repeated = state.finish().unwrap_err();
        assert_eq!(first.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(repeated.kind(), first.kind());
        let original = repeated
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert_eq!(original.kind(), first.kind());
        assert_eq!(original.to_string(), "injected sync failure");
        let error = first.to_string();
        assert!(error.contains("injected sync failure"));
        assert_eq!(state.finish().unwrap_err().to_string(), error);
        assert_eq!(state.append(vec![record()]).unwrap_err().to_string(), error);
    }
}
