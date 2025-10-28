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
    fn emit_error(&self, message: &str, error: &dyn Error);

    fn on_points_failed(&self, num_points: usize) {}

    fn on_points_succeeded(&self, num_points: usize) {}
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn emit_error(&self, message: &str, error: &dyn Error) {
        error!("{}: {}", message, error);
    }
}