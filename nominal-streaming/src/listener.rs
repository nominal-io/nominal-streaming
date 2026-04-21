use std::error::Error;
use std::fmt::Debug;
use std::panic::RefUnwindSafe;
use std::sync::Arc;

use tracing::error;

use crate::client::StreamWriteRequest;

pub trait NominalStreamListener: Send + Sync + Debug + RefUnwindSafe {
    fn on_error(&self, error: &dyn Error, request: &StreamWriteRequest);

    fn on_success(&self, _request: &StreamWriteRequest) {}
}

impl NominalStreamListener for Vec<Arc<dyn NominalStreamListener>> {
    fn on_error(&self, error: &dyn Error, request: &StreamWriteRequest) {
        for listener in self {
            listener.on_error(error, request);
        }
    }

    fn on_success(&self, request: &StreamWriteRequest) {
        for listener in self {
            listener.on_success(request);
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn on_error(&self, error: &dyn Error, request: &StreamWriteRequest) {
        let summary = crate::format::request_summary(request);
        error!("Failed to consume request with {summary}: {error}");
    }
}
