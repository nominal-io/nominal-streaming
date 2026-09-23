use std::sync::Arc;

use bytes::Bytes;
use conjure_http::client::ConjureRuntime;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use prost::Message;

use super::LogStreamError;
use super::NominalLogStreamOpts;
use crate::client::NominalApiClients;
use crate::client::WriteRequest;
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
        let streaming = client::async_conjure_streaming_client(endpoint.clone())
            .map_err(|e| LogStreamError::Invalid(crate::consumer::describe_request_error(&e)))?;
        let services = client::async_conjure_client("upload-ingest", endpoint)
            .map_err(|e| LogStreamError::Invalid(crate::consumer::describe_request_error(&e)))?;
        let client = NominalApiClients::from_conjure_clients(
            streaming,
            services,
            &Arc::new(ConjureRuntime::default()),
        );
        Ok(Self {
            client,
            auth,
            handle,
        })
    }
}

fn request(body: Bytes, token: &BearerToken) -> WriteRequest<'static> {
    let mut request = client::compressed_protobuf_request(body, token);
    *request.uri_mut() = "/storage/writer/v1/nominal-columnar".parse().unwrap();
    request
        .extensions_mut()
        .insert(conjure_http::client::Endpoint::new(
            "NominalChannelWriterService",
            None,
            "writeNominalColumnarBatches",
            "/storage/writer/v1/nominal-columnar",
        ));
    request
}

impl LogTransport for HttpTransport {
    fn send(&self, body: &Bytes) -> Result<(), String> {
        let token = (self.auth)().ok_or("missing auth token")?;
        let started = std::time::Instant::now();
        let result = self
            .handle
            .block_on(self.client.send(request(body.clone(), &token)))
            .map(|_| ())
            .map_err(|e| crate::consumer::describe_request_error(&e));
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
    fn columnar_request_uses_shared_compression_and_auth() {
        let token = BearerToken::new("test-token").unwrap();
        let body = client::compress(b"protobuf payload").unwrap();
        let rid = ResourceIdentifier::new("ri.catalog.main.dataset.test").unwrap();
        let timeseries = client::encode_request(b"protobuf payload", &token, &rid).unwrap();
        assert_eq!(
            timeseries.uri(),
            "/storage/writer/v1/nominal/ri.catalog.main.dataset.test"
        );
        let request = request(body.clone(), &token);
        assert_eq!(request.headers(), timeseries.headers());
        let conjure_http::client::AsyncRequestBody::Fixed(encoded) = timeseries.into_body() else {
            panic!("time-series request must be replayable");
        };
        assert_eq!(
            zstd::decode_all(encoded.as_ref()).unwrap(),
            b"protobuf payload"
        );
        assert_eq!(request.method(), "POST");
        assert_eq!(request.uri(), "/storage/writer/v1/nominal-columnar");
        assert_eq!(request.headers()["content-type"], "application/x-protobuf");
        assert_eq!(request.headers()["content-encoding"], "zstd");
        assert_eq!(request.headers()["authorization"], "Bearer test-token");
        let conjure_http::client::AsyncRequestBody::Fixed(encoded) = request.into_body() else {
            panic!("request must be replayable");
        };
        assert_eq!(encoded, body);
        assert_eq!(
            zstd::decode_all(encoded.as_ref()).unwrap(),
            b"protobuf payload"
        );
    }

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
