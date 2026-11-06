use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::LazyLock;

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
use regex::Regex;
use tracing::warn;
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

/// Sent when nothing usable remains of a [`ProductAgent`] name or version.
const FALLBACK_NAME: &str = "unknown";
const FALLBACK_VERSION: &str = "0.0.0";

/// The version format the HTTP client accepts. It replaces any other version with `0.0.0`.
static VALID_VERSION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[0-9]+(\.[0-9]+)*(-rc[0-9]+)?(-[0-9]+-g[a-f0-9]+)?$").unwrap());
static LEADING_VERSION_NUMBERS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[0-9]+(\.[0-9]+)*").unwrap());

/// The program name and version at the start of the `User-Agent` header of every request this
/// crate sends to Nominal.
///
/// Nominal's usage reporting groups requests by this agent. With one set, the header starts with
/// `<name>/<version> nominal-streaming/<crate version>`. Without one, it starts with
/// `nominal-streaming/<crate version>`. The HTTP client appends its own agents after these.
///
/// Set it with [`NominalStreamOpts::with_user_agent`](crate::stream::NominalStreamOpts::with_user_agent),
/// [`NominalLogStreamOpts::user_agent`](crate::log::NominalLogStreamOpts::user_agent) or
/// [`NominalApiClients::try_from_url_with_agent`].
///
/// The HTTP client accepts only some names and versions, so `new` changes others to the closest
/// accepted value and logs a warning:
///
/// - A name keeps ASCII letters, digits and hyphens, and any other character becomes a hyphen.
///   Leading characters before the first letter are dropped. `nominal_matlab` becomes
///   `nominal-matlab`. A name with no letters becomes `unknown`.
/// - A version drops a leading `v` and keeps its leading dot-separated numbers. `v0.3.0` and
///   `0.3.0-beta.1` both become `0.3.0`. A version that doesn't start with a number becomes
///   `0.0.0`. Release candidates such as `0.3.0-rc1` are kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductAgent {
    name: String,
    version: String,
}

impl ProductAgent {
    pub fn new(name: &str, version: &str) -> Self {
        let agent = Self {
            name: clean_agent_name(name),
            version: clean_agent_version(version),
        };
        if agent.name != name || agent.version != version {
            warn!(
                requested = %format!("{name}/{version}"),
                sent = %agent,
                "changed user agent to a format the HTTP client accepts"
            );
        }
        agent
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

impl fmt::Display for ProductAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

fn clean_agent_name(name: &str) -> String {
    let replaced: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    match replaced.trim_start_matches(|c: char| !c.is_ascii_alphabetic()) {
        "" => FALLBACK_NAME.to_string(),
        cleaned => cleaned.to_string(),
    }
}

fn clean_agent_version(version: &str) -> String {
    let trimmed = version.trim();
    let unprefixed = trimmed.strip_prefix(['v', 'V']).unwrap_or(trimmed);
    if VALID_VERSION.is_match(unprefixed) {
        return unprefixed.to_string();
    }
    LEADING_VERSION_NUMBERS
        .find(unprefixed)
        .map_or(FALLBACK_VERSION, |numbers| numbers.as_str())
        .to_string()
}

fn user_agent(product: Option<&ProductAgent>) -> UserAgent {
    let streaming = Agent::new(USER_AGENT, env!("CARGO_PKG_VERSION"));
    let Some(product) = product else {
        return UserAgent::new(streaming);
    };
    let mut user_agent = UserAgent::new(Agent::new(&product.name, &product.version));
    user_agent.push_agent(streaming);
    user_agent
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
        Self::from_uri_with_agent(base_uri, None)
    }

    /// Like [`Self::from_uri`], with `agent` at the start of the `User-Agent` header.
    pub(crate) fn from_uri_with_agent(base_uri: &str, agent: Option<ProductAgent>) -> Self {
        Self::try_from_url_with_agent(base_uri.parse::<url::Url>().unwrap(), agent)
            .expect("Failed to create Nominal API clients")
    }

    pub fn try_from_url(base_url: Url) -> Result<Self, Error> {
        Self::try_from_url_with_agent(base_url, None)
    }

    /// Like [`Self::try_from_url`], with `agent` at the start of the `User-Agent` header.
    pub fn try_from_url_with_agent(
        base_url: Url,
        agent: Option<ProductAgent>,
    ) -> Result<Self, Error> {
        let streaming =
            async_conjure_streaming_client_with_agent(base_url.clone(), agent.as_ref())?;
        let services = async_conjure_client_with_agent("upload-ingest", base_url, agent.as_ref())?;
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
        self.streaming.send(req).await
    }

    /// Send from a synchronous worker thread, describing failures compactly.
    pub(crate) fn send_blocking(
        &self,
        handle: &tokio::runtime::Handle,
        req: WriteRequest<'_>,
    ) -> Result<(), String> {
        handle
            .block_on(self.send(req))
            .map(|_| ())
            .map_err(|e| crate::consumer::describe_request_error(&e))
    }
}

pub static PRODUCTION_CLIENTS: LazyLock<NominalApiClients> =
    LazyLock::new(|| NominalApiClients::from_uri(PRODUCTION_API_URL));

pub fn async_conjure_streaming_client(uri: Url) -> Result<Client, Error> {
    async_conjure_streaming_client_with_agent(uri, None)
}

/// Like [`async_conjure_streaming_client`], with `agent` at the start of the `User-Agent` header.
fn async_conjure_streaming_client_with_agent(
    uri: Url,
    agent: Option<&ProductAgent>,
) -> Result<Client, Error> {
    Client::builder()
        .service("core-streaming-rs")
        .user_agent(user_agent(agent))
        .uri(uri)
        .connect_timeout(std::time::Duration::from_secs(1))
        .read_timeout(std::time::Duration::from_secs(2))
        .write_timeout(std::time::Duration::from_secs(2))
        .backoff_slot_size(std::time::Duration::from_millis(10))
        .max_num_retries(2)
        // enables retries for POST endpoints like the streaming ingest one
        .idempotency(Idempotency::Always)
        .build()
}

pub fn async_conjure_client(service: &'static str, uri: Url) -> Result<Client, Error> {
    async_conjure_client_with_agent(service, uri, None)
}

/// Like [`async_conjure_client`], with `agent` at the start of the `User-Agent` header.
fn async_conjure_client_with_agent(
    service: &'static str,
    uri: Url,
    agent: Option<&ProductAgent>,
) -> Result<Client, Error> {
    Client::builder()
        .service(service)
        .user_agent(user_agent(agent))
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

const COLUMNAR_PATH: &str = "/storage/writer/v1/nominal-columnar";

pub(crate) fn columnar_request(body: Vec<u8>, api_key: &BearerToken) -> WriteRequest<'static> {
    let mut request = compressed_protobuf_request(body, api_key);
    *request.uri_mut() = COLUMNAR_PATH.parse().unwrap();
    request
        .extensions_mut()
        .insert(conjure_http::client::Endpoint::new(
            "NominalChannelWriterService",
            None,
            "writeNominalColumnarBatches",
            COLUMNAR_PATH,
        ));
    request
}

/// Compress once into a fixed body that Conjure can replay without re-encoding.
pub(crate) fn compress(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    zstd::bulk::compress(bytes, ZSTD_LEVEL)
}

pub(crate) fn compressed_protobuf_request(
    body: Vec<u8>,
    api_key: &BearerToken,
) -> WriteRequest<'static> {
    let mut request = Request::new(AsyncRequestBody::Fixed(body.into()));
    let headers = request.headers_mut();
    headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
    headers.insert(CONTENT_ENCODING, "zstd".parse().unwrap());
    *request.method_mut() = conjure_http::private::http::Method::POST;
    conjure_http::private::encode_header_auth(&mut request, api_key);
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    const CRATE_AGENT: &str = concat!("nominal-streaming/", env!("CARGO_PKG_VERSION"));

    /// The HTTP client panics on a name it rejects and replaces a version it rejects with `0.0.0`.
    fn assert_http_client_accepts(agent: &ProductAgent) {
        assert_eq!(
            Agent::new(agent.name(), agent.version()).version(),
            agent.version()
        );
    }

    #[test]
    fn user_agent_starts_with_crate_agent_when_unset() {
        assert_eq!(user_agent(None).to_string(), CRATE_AGENT);
    }

    #[test]
    fn user_agent_puts_product_agent_before_crate_agent() {
        let agent = ProductAgent::new("nominal-matlab", "0.3.0");
        assert_eq!(
            user_agent(Some(&agent)).to_string(),
            format!("nominal-matlab/0.3.0 {CRATE_AGENT}")
        );
    }

    #[rstest::rstest]
    #[case("nominal-matlab", "nominal-matlab")]
    #[case("NominalLabVIEW", "NominalLabVIEW")]
    #[case("nominal_matlab", "nominal-matlab")]
    #[case("nominal.matlab", "nominal-matlab")]
    #[case("nominal matlab", "nominal-matlab")]
    #[case(" nominal-matlab ", "nominal-matlab")]
    #[case("nominal-é", "nominal--")]
    #[case("3d-tool", "d-tool")]
    #[case("123", "unknown")]
    #[case("", "unknown")]
    fn product_agent_cleans_name(#[case] requested: &str, #[case] sent: &str) {
        let agent = ProductAgent::new(requested, "1.0.0");
        assert_eq!(agent.name(), sent);
        assert_http_client_accepts(&agent);
    }

    #[rstest::rstest]
    #[case("0.3.0", "0.3.0")]
    #[case("1", "1")]
    #[case("0.3.0-rc1", "0.3.0-rc1")]
    #[case("0.3.0-rc1-5-gabc123", "0.3.0-rc1-5-gabc123")]
    #[case("v0.3.0", "0.3.0")]
    #[case("V0.3.0-rc1", "0.3.0-rc1")]
    #[case(" 0.3.0 ", "0.3.0")]
    #[case("0.3.0-beta.1", "0.3.0")]
    #[case("0.3.0-dev", "0.3.0")]
    #[case("0.3.0+build.7", "0.3.0")]
    #[case("0.3.0-5-gABC123", "0.3.0")]
    #[case("dev", "0.0.0")]
    #[case("", "0.0.0")]
    fn product_agent_cleans_version(#[case] requested: &str, #[case] sent: &str) {
        let agent = ProductAgent::new("nominal-matlab", requested);
        assert_eq!(agent.version(), sent);
        assert_http_client_accepts(&agent);
    }

    #[test]
    fn columnar_request_shares_time_series_headers_and_compression() {
        let token = BearerToken::new("test-token").unwrap();
        let rid = ResourceIdentifier::new("ri.catalog.main.dataset.test").unwrap();
        let timeseries = encode_request(b"protobuf payload", &token, &rid).unwrap();
        assert_eq!(
            timeseries.uri(),
            "/storage/writer/v1/nominal/ri.catalog.main.dataset.test"
        );
        let body = compress(b"protobuf payload").unwrap();
        let request = columnar_request(body, &token);
        assert_eq!(request.headers(), timeseries.headers());
        assert_eq!(request.method(), "POST");
        assert_eq!(request.uri(), COLUMNAR_PATH);
        assert_eq!(request.headers()["authorization"], "Bearer test-token");
        for request in [timeseries, request] {
            let AsyncRequestBody::Fixed(encoded) = request.into_body() else {
                panic!("request must be replayable");
            };
            assert_eq!(
                zstd::decode_all(encoded.as_ref()).unwrap(),
                b"protobuf payload"
            );
        }
    }
}
