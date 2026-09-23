use std::fmt::Debug;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use conjure_error::Error;
use conjure_http::client::AsyncClient;
use conjure_http::client::AsyncRequestBody;
use conjure_http::client::AsyncService;
use conjure_http::client::ConjureRuntime;
use conjure_http::private::header::CONTENT_ENCODING;
use conjure_http::private::header::CONTENT_TYPE;
use conjure_http::private::Request;
use conjure_http::private::Response;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use conjure_runtime_rustls_platform_verifier::Agent;
use conjure_runtime_rustls_platform_verifier::BodyWriter;
use conjure_runtime_rustls_platform_verifier::Client;
use conjure_runtime_rustls_platform_verifier::Idempotency;
use conjure_runtime_rustls_platform_verifier::ResponseBody;
use conjure_runtime_rustls_platform_verifier::UserAgent;
use nominal_api::clients::ingest::api::AsyncIngestServiceClient;
use nominal_api::clients::upload::api::AsyncUploadServiceClient;
use nominal_api::objects::api::rids::NominalDataSourceOrDatasetRid;
use nominal_api::objects::api::rids::WorkspaceRid;
use url::Url;

use crate::types::AuthProvider;

pub mod conjure {
    pub use conjure_error as error;
    pub use conjure_http as http;
    pub use conjure_object as object;
    pub use conjure_runtime_rustls_platform_verifier as runtime;
}

/// The URL that points toward's Nominal's default production deployment.
pub const PRODUCTION_API_URL: &str = "https://api.gov.nominal.io/api";

const USER_AGENT: &str = "nominal-streaming";

/// Default overall deadline for HTTP attempts and retry sleeps.
pub const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);

/// HTTP attempt and retry settings for Core uploads.
///
/// Conjure owns retries, including jitter and Retry-After. The pinned runtime retries
/// 429, 503, and (for these idempotent requests) 500 and transport errors. It does not
/// retry 502 or 504. Retries replay the same encoded body and may duplicate delivery.
/// Configure the overall deadline separately with `NominalStreamOpts::with_delivery_timeout`
/// or `NominalApiClients::send_with_timeout`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TransportOptions {
    /// Additional attempts after the initial request (0 disables retries; maximum 31).
    pub max_retries: u32,
    pub backoff_slot: Duration,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            max_retries: 5,
            backoff_slot: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(15),
        }
    }
}

impl TransportOptions {
    /// Check positive durations and the runtime's exponential-backoff arithmetic limits.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_retries > 31 {
            return Err("max_retries must be at most 31");
        }
        if [
            self.backoff_slot,
            self.connect_timeout,
            self.read_timeout,
            self.write_timeout,
        ]
        .iter()
        .any(Duration::is_zero)
        {
            return Err("transport durations must be greater than zero");
        }
        let scale = 1_u32 << self.max_retries.saturating_sub(1);
        if self.backoff_slot.checked_mul(scale).is_none() {
            return Err("retry backoff exceeds duration range");
        }
        Ok(())
    }
}

impl AuthProvider for BearerToken {
    fn token(&self) -> Option<BearerToken> {
        Some(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TokenAndWorkspaceRid {
    pub token: BearerToken,
    pub workspace_rid: Option<WorkspaceRid>,
}

impl AuthProvider for TokenAndWorkspaceRid {
    fn token(&self) -> Option<BearerToken> {
        Some(self.token.clone())
    }

    fn workspace_rid(&self) -> Option<WorkspaceRid> {
        self.workspace_rid.clone()
    }
}

#[derive(Clone)]
pub struct NominalApiClients {
    pub streaming: Client,
    pub upload: AsyncUploadServiceClient<Client>,
    pub ingest: AsyncIngestServiceClient<Client>,
}

impl Debug for NominalApiClients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalApiClients")
            .field("streaming", &"Client")
            .field("upload", &"UploadServiceAsyncClient<Client>")
            .field("ingest", &"IngestServiceAsyncClient<Client>")
            .finish()
    }
}

impl NominalApiClients {
    pub fn from_uri(base_uri: &str) -> Self {
        Self::from_uri_with_options(base_uri, &TransportOptions::default())
    }

    /// Configure the streaming HTTP client. Set the send deadline separately with
    /// [`Self::send_with_timeout`]; [`Self::send`] uses the default 60-second deadline.
    pub fn from_uri_with_options(base_uri: &str, options: &TransportOptions) -> Self {
        Self::try_from_uri_with_options(base_uri, options).expect("Failed to create API clients")
    }

    /// Fallible variant of [`Self::from_uri_with_options`].
    pub fn try_from_uri_with_options(
        base_uri: &str,
        options: &TransportOptions,
    ) -> Result<Self, Error> {
        let base_uri = base_uri.parse::<url::Url>().map_err(Error::internal_safe)?;
        let streaming = async_conjure_streaming_client_with_options(base_uri.clone(), options)?;
        let services = async_conjure_client("upload-ingest", base_uri)?;
        Ok(Self::from_conjure_clients(
            streaming,
            services,
            &Arc::new(ConjureRuntime::default()),
        ))
    }

    /// NOTE: the conjure client type is a shared handle, and cheap to clone.
    pub fn from_conjure_clients(
        streaming: Client,
        services: Client,
        runtime: &Arc<ConjureRuntime>,
    ) -> Self {
        Self {
            streaming,
            upload: AsyncUploadServiceClient::new(services.clone(), runtime),
            ingest: AsyncIngestServiceClient::new(services, runtime),
        }
    }

    pub async fn send(&self, req: WriteRequest<'_>) -> Result<Response<ResponseBody>, Error> {
        self.send_with_timeout(req, DEFAULT_DELIVERY_TIMEOUT).await
    }

    /// Bound all attempts and retry sleeps. A timeout has an unknown delivery outcome.
    pub async fn send_with_timeout(
        &self,
        req: WriteRequest<'_>,
        timeout: Duration,
    ) -> Result<Response<ResponseBody>, Error> {
        tokio::time::timeout(timeout, self.streaming.send(req))
            .await
            .map_err(Error::internal_safe)?
    }
}

pub static PRODUCTION_CLIENTS: LazyLock<NominalApiClients> =
    LazyLock::new(|| NominalApiClients::from_uri(PRODUCTION_API_URL));

pub fn async_conjure_streaming_client(uri: Url) -> Result<Client, Error> {
    async_conjure_streaming_client_with_options(uri, &TransportOptions::default())
}

pub fn async_conjure_streaming_client_with_options(
    uri: Url,
    options: &TransportOptions,
) -> Result<Client, Error> {
    options.validate().map_err(|message| {
        Error::internal_safe(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            message,
        ))
    })?;
    Client::builder()
        .service("core-streaming-rs")
        .user_agent(UserAgent::new(Agent::new(
            USER_AGENT,
            env!("CARGO_PKG_VERSION"),
        )))
        .uri(uri)
        .connect_timeout(options.connect_timeout)
        .read_timeout(options.read_timeout)
        .write_timeout(options.write_timeout)
        .backoff_slot_size(options.backoff_slot)
        .max_num_retries(options.max_retries)
        // enables retries for POST endpoints like the streaming ingest one
        .idempotency(Idempotency::Always)
        .build()
}

pub fn async_conjure_client(service: &'static str, uri: Url) -> Result<Client, Error> {
    Client::builder()
        .service(service)
        .user_agent(UserAgent::new(Agent::new(
            USER_AGENT,
            env!("CARGO_PKG_VERSION"),
        )))
        .uri(uri)
        .build()
}

pub type WriteRequest<'a> = Request<AsyncRequestBody<'a, BodyWriter>>;

/// Zstd compression level for request bodies.
///
/// Level 1 compresses at speeds comparable to snappy while producing a substantially smaller body
/// (snappy has no entropy coder), and every byte saved feeds straight into per-request upload
/// time. Higher levels shrink telemetry payloads little further and cost disproportionate CPU.
const ZSTD_LEVEL: i32 = 1;

pub fn encode_request(
    write_request_bytes: &[u8],
    api_key: &BearerToken,
    data_source_rid: &ResourceIdentifier,
) -> std::io::Result<WriteRequest<'static>> {
    let mut request = compressed_protobuf_request(compress(write_request_bytes)?, api_key);
    let mut path = conjure_http::private::UriBuilder::new();
    path.push_literal("/storage/writer/v1/nominal");

    let nominal_data_source_or_dataset_rid = NominalDataSourceOrDatasetRid(data_source_rid.clone());
    path.push_path_parameter(&nominal_data_source_or_dataset_rid);

    *request.uri_mut() = path.build();
    request
        .extensions_mut()
        .insert(conjure_http::client::Endpoint::new(
            "NominalChannelWriterService",
            None,
            "writeNominalBatches",
            "/storage/writer/v1/nominal/{dataSourceRid}",
        ));
    Ok(request)
}

#[cfg(test)]
mod transport_tests {
    use std::time::Duration;

    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    use super::*;

    async fn scripted_server(
        statuses: Vec<u16>,
        retry_after: Option<u64>,
    ) -> (
        Url,
        tokio::task::JoinHandle<Vec<Vec<u8>>>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        let task = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for status in statuses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let byte = socket.read_u8().await.unwrap();
                    bytes.push(byte);
                    if bytes.ends_with(b"\r\n\r\n") {
                        break bytes.len();
                    }
                };
                let headers = String::from_utf8_lossy(&bytes);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                assert_eq!(header_end, bytes.len());
                bodies.push(body);
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let extra = retry_after
                    .map(|secs| format!("Retry-After: {secs}\r\n"))
                    .unwrap_or_default();
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n{extra}\r\n").as_bytes()).await.unwrap();
            }
            bodies
        });
        (url, task, attempts)
    }

    fn request() -> WriteRequest<'static> {
        encode_request(
            b"one immutable payload",
            &"test-token".parse().unwrap(),
            &"ri.catalog.main.dataset.test".parse().unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn defaults_allow_five_additional_attempts() {
        for status in [429, 500, 503] {
            let (url, server, _) =
                scripted_server(vec![status, status, status, status, status, 204], None).await;
            let clients = NominalApiClients::from_uri(url.as_str());
            clients.send(request()).await.unwrap();
            let bodies = server.await.unwrap();
            assert_eq!(bodies.len(), 6);
            assert!(bodies.windows(2).all(|pair| pair[0] == pair[1]));
        }
    }

    #[tokio::test]
    async fn retries_classified_statuses_with_identical_bytes() {
        for status in [429, 500, 503] {
            let (url, server, _) = scripted_server(vec![status, status, 204], None).await;
            let options = TransportOptions {
                backoff_slot: Duration::from_millis(1),
                ..Default::default()
            };
            let clients = NominalApiClients::from_uri_with_options(url.as_str(), &options);
            clients.send(request()).await.unwrap();
            let bodies = server.await.unwrap();
            assert_eq!(bodies.len(), 3);
            assert!(bodies.windows(2).all(|pair| pair[0] == pair[1]));
        }
    }

    #[tokio::test]
    async fn does_not_retry_unclassified_statuses() {
        for status in [400, 401, 403, 502, 504] {
            let (url, server, _) = scripted_server(vec![status, 204], None).await;
            let clients = NominalApiClients::from_uri(url.as_str());
            assert!(clients.send(request()).await.is_err());
            server.abort();
        }
    }

    #[tokio::test]
    async fn deadline_bounds_retry_after_sleep() {
        let (url, server, _) = scripted_server(vec![429], Some(3600)).await;
        let clients = NominalApiClients::from_uri(url.as_str());
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            clients.send_with_timeout(request(), Duration::from_millis(100)),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn deadline_bounds_a_stalled_attempt() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let clients = NominalApiClients::from_uri(&url);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            clients.send_with_timeout(request(), Duration::from_millis(100)),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        server.abort();
    }

    #[rstest::rstest]
    #[case::exhausted_retries(false)]
    #[case::delivery_deadline(true)]
    fn exhausted_upload_reaches_existing_avro_fallback(#[case] deadline: bool) {
        use apache_avro::types::Value;

        use crate::stream::NominalDatasetStreamBuilder;
        use crate::stream::NominalStreamOpts;
        use crate::types::ChannelDescriptor;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let statuses = if deadline {
            vec![429]
        } else {
            vec![503, 503, 503]
        };
        let expected_attempts = statuses.len();
        let (url, server, _) =
            runtime.block_on(scripted_server(statuses, deadline.then_some(3600)));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fallback.avro");
        let options = TransportOptions {
            max_retries: 2,
            backoff_slot: Duration::from_millis(1),
            ..Default::default()
        };
        let started = std::time::Instant::now();
        {
            let stream = NominalDatasetStreamBuilder::new()
                .stream_to_core(
                    "test-token".parse().unwrap(),
                    "ri.catalog.main.dataset.test".parse().unwrap(),
                    runtime.handle().clone(),
                )
                .with_file_fallback(&path)
                .with_options(
                    NominalStreamOpts::default()
                        .with_base_api_url(url.as_str())
                        .with_transport_options(options)
                        .with_delivery_timeout(if deadline {
                            Duration::from_millis(100)
                        } else {
                            DEFAULT_DELIVERY_TIMEOUT
                        }),
                )
                .build();
            let mut writer = stream.double_writer(ChannelDescriptor::new("temperature"));
            writer.push(123_i64, 42.5);
        }
        if deadline {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "configured delivery deadline was not applied"
            );
        }
        let bodies = runtime.block_on(server).unwrap();
        assert_eq!(bodies.len(), expected_attempts);
        assert!(bodies.windows(2).all(|pair| pair[0] == pair[1]));
        let records = apache_avro::Reader::new(std::fs::File::open(path).unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        let Value::Record(fields) = &records[0] else {
            panic!("expected record")
        };
        assert!(fields.contains(&("channel".into(), Value::String("temperature".into()))));
        assert!(fields.contains(&("timestamps".into(), Value::Array(vec![Value::Long(123)]))));
        assert!(fields.contains(&(
            "values".into(),
            Value::Array(vec![Value::Union(0, Box::new(Value::Double(42.5)))])
        )));
    }

    #[tokio::test]
    async fn retry_budget_is_additional_attempts() {
        for max_retries in [0, 2] {
            let mut statuses = vec![500; max_retries as usize + 1];
            // An extra attempt would succeed, exposing an off-by-one retry budget.
            statuses.push(204);
            let (url, server, attempts) = scripted_server(statuses, None).await;
            let options = TransportOptions {
                max_retries,
                backoff_slot: Duration::from_millis(1),
                ..Default::default()
            };
            let clients = NominalApiClients::from_uri_with_options(url.as_str(), &options);
            assert!(clients.send(request()).await.is_err());
            assert_eq!(
                attempts.load(std::sync::atomic::Ordering::SeqCst),
                max_retries as usize + 1
            );
            server.abort();
        }
    }
}

#[cfg(test)]
mod options_tests {
    use super::*;

    #[test]
    fn validates_retry_arithmetic_and_positive_durations() {
        let mut options = TransportOptions::default();
        assert!(options.validate().is_ok());
        options.max_retries = 32;
        assert!(options.validate().is_err());
        options.max_retries = 31;
        assert!(options.validate().is_ok());
        options.backoff_slot = Duration::MAX;
        assert!(options.validate().is_err());
        options = TransportOptions::default();
        options.connect_timeout = Duration::ZERO;
        assert!(options.validate().is_err());
        options = TransportOptions::default();
        options.max_retries = 0;
        assert!(options.validate().is_ok());
    }

    #[test]
    fn fallible_constructor_rejects_invalid_url() {
        assert!(NominalApiClients::try_from_uri_with_options(
            "invalid",
            &TransportOptions::default()
        )
        .is_err());
    }
}

/// Compress once into a fixed body that Conjure can replay without re-encoding.
pub(crate) fn compress(bytes: &[u8]) -> std::io::Result<bytes::Bytes> {
    zstd::bulk::compress(bytes, ZSTD_LEVEL).map(Into::into)
}

pub(crate) fn compressed_protobuf_request(
    body: bytes::Bytes,
    api_key: &BearerToken,
) -> WriteRequest<'static> {
    let mut request = Request::new(AsyncRequestBody::Fixed(body));
    let headers = request.headers_mut();
    headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
    headers.insert(CONTENT_ENCODING, "zstd".parse().unwrap());
    *request.method_mut() = conjure_http::private::http::Method::POST;
    conjure_http::private::encode_header_auth(&mut request, api_key);
    request
}
