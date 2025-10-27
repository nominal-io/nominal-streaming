use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use parking_lot::Mutex;
use tracing::error;

pub trait NominalStreamListener: Send + Sync + Debug {
    fn on_error(&self, message: &str, error: &dyn Error);

    #[expect(unused_variables)]
    fn on_file_written(&self, path: &Path, num_points: u64) {}
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn on_error(&self, message: &str, error: &dyn Error) {
        error!("{}: {}", message, error);
    }
}

#[derive(Debug, Clone)]
pub struct FileSummary {
    pub total_points: u64,
    pub last_write_time: Instant,
}

#[derive(Debug, Clone)]
pub struct StreamHealthSnapshot {
    pub total_failed: u64,
    pub last_enqueue_time: Instant,
    pub last_failed_time: Instant,
}

#[derive(Debug)]
pub struct HealthReporter {
    health: Arc<Mutex<StreamHealthSnapshot>>,
    file_inventory: Arc<Mutex<HashMap<PathBuf, FileSummary>>>,
}

impl HealthReporter {
    pub fn new() -> Self {
        Self {
            health: Arc::new(Mutex::new(StreamHealthSnapshot {
                total_failed: 0,
                last_enqueue_time: Instant::now(),
                last_failed_time: Instant::now(),
            })),
            file_inventory: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn health_snapshot(&self) -> StreamHealthSnapshot {
        self.health.lock().clone()
    }

    pub fn file_inventory(&self) -> Vec<FileSummary> {
        self.file_inventory.lock().values().cloned().collect()
    }
}

impl NominalStreamListener for HealthReporter {
    fn on_error(&self, _message: &str, _error: &dyn Error) {
        let mut health = self.health.lock();
        health.total_failed += 1;
        health.last_failed_time = Instant::now();
    }

    fn on_file_written(&self, path: &Path, num_points: u64) {
        let mut inventory = self.file_inventory.lock();
        let now = Instant::now();
        let path_buf = path.to_path_buf();

        inventory
            .entry(path_buf.clone())
            .and_modify(|summary| {
                summary.total_points += num_points;
                summary.last_write_time = now;
            })
            .or_insert(FileSummary {
                total_points: num_points,
                last_write_time: now,
            });
    }
}