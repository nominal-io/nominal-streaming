use std::error::Error;
use std::fmt::Debug;

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
