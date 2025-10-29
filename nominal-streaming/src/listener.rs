use std::error::Error;
use std::fmt::Debug;
use std::panic::RefUnwindSafe;

use tracing::error;

pub trait NominalStreamListener: Send + Sync + Debug + RefUnwindSafe {
    fn emit_error(&self, message: &str, error: &dyn Error);

    fn on_points_failed(&self, _num_points: usize) {}

    fn on_points_succeeded(&self, _num_points: usize) {}
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn emit_error(&self, message: &str, error: &dyn Error) {
        error!("{}: {}", message, error);
    }
}
