use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tracing::error;

pub trait NominalStreamListener: Send + Sync + Debug {
    fn on_error(&self, message: &str, error: &dyn Error);
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn on_error(&self, message: &str, error: &dyn Error) {
        error!("{}: {}", message, error);
    }
}
//
// #[derive(Debug, Clone)]
// pub struct StreamHealthSnapshot {
//     pub total_failed: u64,
//     pub last_enqueue_time: Instant,
//     pub last_failed_time: Instant,
// }
//
// #[derive(Debug)]
// pub struct HealthReporter {
//     health: Arc<Mutex<StreamHealthSnapshot>>,
// }
//
// impl HealthReporter {
//     pub fn new() -> Self {
//         Self {
//             health: Arc::new(Mutex::new(StreamHealthSnapshot {
//                 total_failed: 0,
//                 last_enqueue_time: Instant::now(),
//                 last_failed_time: Instant::now(),
//             })),
//         }
//     }
//
//     pub fn health_snapshot(&self) -> StreamHealthSnapshot {
//         self.health.lock().clone()
//     }
// }
//
// impl NominalStreamListener for HealthReporter {
//     fn on_error(&self, _message: &str, _error: &dyn Error) {
//         let mut health = self.health.lock();
//         health.total_failed += 1;
//         health.last_failed_time = Instant::now();
//     }
// }
