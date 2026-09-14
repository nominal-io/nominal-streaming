//! Runtime ownership and a single background drain for Python log streams.
use std::sync::Arc;
use std::sync::Mutex;

use nominal_streaming::log::LogStreamError;
use nominal_streaming::log::LogStreamStats;
use nominal_streaming::log::NominalLogStream;

pub(crate) struct LogRuntime {
    // Keep the runtime alive while the stream drains, including on drop.
    pub stream: NominalLogStream,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    draining: Mutex<bool>,
}

impl LogRuntime {
    pub fn new(stream: NominalLogStream, runtime: tokio::runtime::Runtime) -> Self {
        Self {
            stream,
            runtime: Mutex::new(Some(runtime)),
            draining: Mutex::new(false),
        }
    }

    pub fn close(&self) -> Result<LogStreamStats, LogStreamError> {
        *self.draining.lock().unwrap() = true;
        let result = self.stream.close();
        // close joins upload workers even when records need disk rescue. Recovery does not
        // use the HTTP runtime, so those records can outlive it without keeping threads alive.
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            runtime.shutdown_background();
        }
        result
    }

    pub fn close_in_background(self: &Arc<Self>) -> std::io::Result<()> {
        self.stream.stop_accepting_writes();
        let mut draining = self.draining.lock().unwrap();
        if *draining {
            return Ok(());
        }
        let owned = Arc::clone(self);
        std::thread::Builder::new()
            .name("nominal-log-drain".into())
            .spawn(move || {
                let _ = owned.close();
            })?;
        *draining = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_close_releases_runtime_and_keeps_records_for_rescue() {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join("occupied");
        std::fs::write(&occupied, "not a directory").unwrap();
        let stream = NominalLogStream::builder()
            .stream_to_file(occupied)
            .build()
            .unwrap();
        stream
            .enqueue(
                "app",
                nominal_streaming::log::LogRecord::new(1, "retained", Default::default()),
            )
            .unwrap();
        let owned = Arc::new(LogRuntime::new(
            stream,
            tokio::runtime::Runtime::new().unwrap(),
        ));
        owned.close_in_background().unwrap();
        assert!(owned.close().is_err());
        assert!(owned.runtime.lock().unwrap().is_none());
        assert_eq!(owned.stream.stats().failed_records, 1);
        for _ in 0..10 {
            owned.close_in_background().unwrap();
        }
        let stats = owned
            .stream
            .save_failed(directory.path().join("rescued"))
            .unwrap();
        assert_eq!(stats.backed_up_records, 1);
        assert_eq!(stats.failed_records, 0);
        owned.close().unwrap();
    }

    #[test]
    fn background_close_is_started_only_once() {
        let directory = tempfile::tempdir().unwrap();
        let stream = NominalLogStream::builder()
            .stream_to_file(directory.path())
            .build()
            .unwrap();
        let owned = Arc::new(LogRuntime::new(
            stream,
            tokio::runtime::Runtime::new().unwrap(),
        ));
        // Hold runtime teardown so the first background drain stays alive during repeated calls.
        let runtime = owned.runtime.lock().unwrap();
        owned.close_in_background().unwrap();
        for _ in 0..20 {
            owned.close_in_background().unwrap();
        }
        assert_eq!(Arc::strong_count(&owned), 2);
        drop(runtime);
        owned.close().unwrap();
        assert!(owned.runtime.lock().unwrap().is_none());
    }
}
