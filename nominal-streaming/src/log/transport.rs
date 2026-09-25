use std::sync::Arc;

use bytes::Bytes;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use prost::Message;

use super::LogStreamError;
use super::NominalLogStreamOpts;
use crate::client::NominalApiClients;
use crate::client::{self};

type TokenProvider = Arc<dyn Fn() -> Option<BearerToken> + Send + Sync>;

// A complete delivery, including retries owned by the shared HTTP client.
pub(super) trait LogTransport: Send + Sync {
    fn send(&self, body: &Bytes) -> Result<(), String>;
}

pub(super) struct HttpTransport {
    client: NominalApiClients,
    auth: TokenProvider,
    handle: tokio::runtime::Handle,
}

impl HttpTransport {
    pub fn new(
        auth: TokenProvider,
        handle: tokio::runtime::Handle,
        opts: &NominalLogStreamOpts,
    ) -> Result<Self, LogStreamError> {
        if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(LogStreamError::Invalid(
                "log uploads require a multi-thread Tokio runtime with I/O and timers enabled"
                    .into(),
            ));
        }
        let endpoint = url::Url::parse(&opts.base_api_url)
            .map_err(|_| LogStreamError::Invalid("invalid base_api_url".into()))?;
        let local = endpoint
            .host_str()
            .is_some_and(|h| matches!(h, "localhost" | "127.0.0.1" | "[::1]"));
        if (endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && local))
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(LogStreamError::Invalid("base_api_url must use HTTPS (HTTP permitted on loopback only), without credentials, query or fragment".into()));
        }
        let client = NominalApiClients::try_from_url(endpoint)
            .map_err(|e| LogStreamError::Invalid(crate::consumer::describe_request_error(&e)))?;
        Ok(Self {
            client,
            auth,
            handle,
        })
    }
}

impl LogTransport for HttpTransport {
    fn send(&self, body: &Bytes) -> Result<(), String> {
        let token = (self.auth)().ok_or("missing auth token")?;
        let started = std::time::Instant::now();
        let result = self
            .client
            .send_blocking(&self.handle, client::columnar_request(body.clone(), &token));
        tracing::debug!(
            elapsed_micros = started.elapsed().as_micros() as u64,
            wire_bytes = body.len(),
            success = result.is_ok(),
            error = result.as_ref().err().map(String::as_str),
            "Log request completed"
        );
        result
    }
}

pub(super) fn encode(
    request: &wire::WriteBatchesRequest,
    max_request_bytes: usize,
) -> std::io::Result<Bytes> {
    let encode_started = std::time::Instant::now();
    if request.encoded_len() > max_request_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "protobuf request exceeds max_request_bytes",
        ));
    }
    let raw = request.encode_to_vec();
    let compression_started = std::time::Instant::now();
    let compressed = crate::client::compress(raw.as_slice())?;
    tracing::debug!(
        raw_bytes = raw.len(),
        wire_bytes = compressed.len(),
        protobuf_micros = compression_started
            .duration_since(encode_started)
            .as_micros() as u64,
        zstd_micros = compression_started.elapsed().as_micros() as u64,
        "Encoded log request"
    );
    Ok(compressed)
}

pub(super) struct CoreTarget {
    pub auth: TokenProvider,
    pub dataset_rid: ResourceIdentifier,
    pub handle: tokio::runtime::Handle,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_current_thread_runtime_that_cannot_drive_worker_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(matches!(
            HttpTransport::new(
                Arc::new(|| None),
                runtime.handle().clone(),
                &NominalLogStreamOpts::default()
            ),
            Err(LogStreamError::Invalid(_))
        ));
    }
}
