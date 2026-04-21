use std::error::Error;
use std::fmt::Debug;
use std::panic::RefUnwindSafe;
use std::sync::Arc;

use nominal_api::tonic::nominal::direct_channel_writer::v2::WriteBatchesRequest;
use tracing::error;

pub trait NominalStreamListener: Send + Sync + Debug + RefUnwindSafe {
    fn on_error(&self, error: &dyn Error, request: &WriteBatchesRequest);

    fn on_success(&self, _request: &WriteBatchesRequest) {}
}

impl NominalStreamListener for Vec<Arc<dyn NominalStreamListener>> {
    fn on_error(&self, error: &dyn Error, request: &WriteBatchesRequest) {
        for listener in self {
            listener.on_error(error, request);
        }
    }

    fn on_success(&self, request: &WriteBatchesRequest) {
        for listener in self {
            listener.on_success(request);
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn on_error(&self, error: &dyn Error, request: &WriteBatchesRequest) {
        let len = request.batches.len();
        let message = format!("Failed to consume request with {len} batches");
        error!("{message}: {error}");
    }
}
