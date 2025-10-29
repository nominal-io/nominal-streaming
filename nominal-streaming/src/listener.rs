use std::error::Error;
use std::fmt::Debug;
use std::panic::RefUnwindSafe;
use std::sync::Arc;

use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use tracing::error;

pub trait NominalStreamListener: Send + Sync + Debug + RefUnwindSafe {
    fn on_error(&self, error: &dyn Error, request: &WriteRequestNominal);

    fn on_success(&self, _request: &WriteRequestNominal) {}
}

impl NominalStreamListener for Vec<Arc<dyn NominalStreamListener>> {
    fn on_error(&self, error: &dyn Error, request: &WriteRequestNominal) {
        for listener in self {
            listener.on_error(error, request);
        }
    }

    fn on_success(&self, request: &WriteRequestNominal) {
        for listener in self {
            listener.on_success(request);
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct LoggingListener;

impl NominalStreamListener for LoggingListener {
    fn on_error(&self, error: &dyn Error, request: &WriteRequestNominal) {
        let len = request.series.len();
        let message = format!("Failed to consume request with {len} series");
        error!("{message}: {error}");
    }
}
