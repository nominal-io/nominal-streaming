use std::io::Write;
use std::io::{self};

use apache_avro::types::Record;

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

pub(super) fn append_records<W: Write>(
    writer: &mut Option<apache_avro::Writer<'static, CompleteWrite<W>>>,
    records: Vec<Record<'_>>,
) -> io::Result<()> {
    let result = writer
        .as_mut()
        .ok_or_else(|| io::Error::other("Avro writer is no longer available"))?
        .extend(records)
        .map(|_| ())
        .map_err(avro_error);
    if result.is_err() {
        // extend flushes each request. After a partial write its compressed buffer
        // cannot safely be retried, so discard the writer rather than reuse it.
        *writer = None;
    }
    result
}

// Preserve the actual I/O cause: apache-avro's display text omits it.
pub(super) fn avro_error(error: apache_avro::Error) -> io::Error {
    match error {
        apache_avro::Error::WriteBytes(source) | apache_avro::Error::WriteMarker(source) => source,
        other => io::Error::other(other),
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
            Ok(())
        }
    }

    fn writer(
        sink: FaultyWriter,
    ) -> Option<apache_avro::Writer<'static, CompleteWrite<FaultyWriter>>> {
        Some(apache_avro::Writer::with_codec(
            &super::super::CORE_AVRO_SCHEMA,
            CompleteWrite(sink),
            apache_avro::Codec::Snappy,
        ))
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
    fn short_and_interrupted_writes_produce_readable_avro() {
        let mut state = writer(FaultyWriter::default());
        append_records(&mut state, vec![record()]).unwrap();
        let sink = state.take().unwrap().into_inner().unwrap();
        let records = apache_avro::Reader::new(sink.0.bytes.as_slice())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn partial_write_failure_is_sticky() {
        let mut baseline = writer(FaultyWriter::default());
        append_records(&mut baseline, vec![record()]).unwrap();
        let length = baseline.take().unwrap().into_inner().unwrap().0.bytes.len();
        // Fail both during the header and near the end of the compressed block.
        for limit in [6, length - 3] {
            let mut state = writer(FaultyWriter {
                fail_after: Some(limit),
                ..Default::default()
            });
            let first = append_records(&mut state, vec![record()]).unwrap_err();
            assert_eq!(first.kind(), io::ErrorKind::WriteZero);
            assert!(first.to_string().contains("injected write failure"));
            assert!(append_records(&mut state, vec![record()]).is_err());
            assert!(state.is_none());
        }
    }
}
