use std::fmt::Debug;
use std::io::Write;
use std::sync::LazyLock;

use conjure_error::Error;
use conjure_http::client::AsyncClient;
use conjure_http::client::AsyncRequestBody;
use conjure_http::client::AsyncService;
use conjure_http::private::header::CONTENT_ENCODING;
use conjure_http::private::header::CONTENT_TYPE;
use conjure_http::private::Request;
use conjure_http::private::Response;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use conjure_runtime::Agent;
use conjure_runtime::BodyWriter;
use conjure_runtime::Client;
use conjure_runtime::Idempotency;
use conjure_runtime::ResponseBody;
use conjure_runtime::UserAgent;
use nominal_api::api::rids::NominalDataSourceOrDatasetRid;
use nominal_api::ingest::api::IngestServiceAsyncClient;
use nominal_api::upload::api::UploadServiceAsyncClient;
use snap::write::FrameEncoder;
use url::Url;

use crate::types::AuthProvider;

pub mod conjure {
    pub use conjure_error as error;
    pub use conjure_http as http;
    pub use conjure_object as object;
    pub use conjure_runtime as runtime;
}

const PRODUCTION_API_URL: &str = "https://api.gov.nominal.io/api";
const STAGING_API_URL: &str = "https://api-staging.gov.nominal.io/api";
const USER_AGENT: &str = "nominal-streaming";

impl AuthProvider for BearerToken {
    fn token(&self) -> Option<BearerToken> {
        Some(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TokenAndWorkspaceRid {
    pub token: BearerToken,
    pub workspace_rid: ResourceIdentifier,
}

impl AuthProvider for TokenAndWorkspaceRid {
    fn token(&self) -> Option<BearerToken> {
        Some(self.token.clone())
    }

    fn workspace_rid(&self) -> Option<ResourceIdentifier> {
        Some(self.workspace_rid.clone())
    }
}

#[derive(Clone)]
pub struct NominalApiClients {
    pub streaming: Client,
    pub upload: UploadServiceAsyncClient<Client>,
    pub ingest: IngestServiceAsyncClient<Client>,
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
    pub async fn send(&self, req: WriteRequest<'_>) -> Result<Response<ResponseBody>, Error> {
        self.streaming.send(req).await
    }
}

pub static PRODUCTION_CLIENTS: LazyLock<NominalApiClients> = LazyLock::new(|| NominalApiClients {
    streaming: async_conjure_streaming_client(PRODUCTION_API_URL.try_into().unwrap())
        .expect("Failed to create streaming client"),
    upload: UploadServiceAsyncClient::new(
        async_conjure_client("upload", PRODUCTION_API_URL.try_into().unwrap())
            .expect("Failed to create upload client"),
    ),
    ingest: IngestServiceAsyncClient::new(
        async_conjure_client("ingest", PRODUCTION_API_URL.try_into().unwrap())
            .expect("Failed to create ingest client"),
    ),
});

pub static STAGING_CLIENTS: LazyLock<NominalApiClients> = LazyLock::new(|| NominalApiClients {
    streaming: async_conjure_streaming_client(STAGING_API_URL.try_into().unwrap())
        .expect("Failed to create streaming client"),
    upload: UploadServiceAsyncClient::new(
        async_conjure_client("upload", STAGING_API_URL.try_into().unwrap())
            .expect("Failed to create upload client"),
    ),
    ingest: IngestServiceAsyncClient::new(
        async_conjure_client("ingest", STAGING_API_URL.try_into().unwrap())
            .expect("Failed to create ingest client"),
    ),
});

fn async_conjure_streaming_client(uri: Url) -> Result<Client, Error> {
    Client::builder()
        .service("core-streaming-rs")
        .user_agent(UserAgent::new(Agent::new(
            USER_AGENT,
            env!("CARGO_PKG_VERSION"),
        )))
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

fn async_conjure_client(service: &'static str, uri: Url) -> Result<Client, Error> {
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

pub fn encode_request<'a, 'b>(
    write_request_bytes: Vec<u8>,
    api_key: &'a BearerToken,
    data_source_rid: &'a ResourceIdentifier,
) -> std::io::Result<WriteRequest<'b>> {
    let mut encoder = FrameEncoder::new(Vec::with_capacity(write_request_bytes.len()));

    encoder.write_all(&write_request_bytes)?;

    let mut request = Request::new(AsyncRequestBody::Fixed(
        encoder.into_inner().unwrap().into(),
    ));

    let headers = request.headers_mut();
    headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
    headers.insert(CONTENT_ENCODING, "x-snappy-framed".parse().unwrap());

    *request.method_mut() = conjure_http::private::http::Method::POST;
    let mut path = conjure_http::private::UriBuilder::new();
    path.push_literal("/storage/writer/v1/nominal");

    let nominal_data_source_or_dataset_rid = NominalDataSourceOrDatasetRid(data_source_rid.clone());
    path.push_path_parameter(&nominal_data_source_or_dataset_rid);

    *request.uri_mut() = path.build();
    conjure_http::private::encode_header_auth(&mut request, api_key);
    conjure_http::private::encode_empty_response_headers(&mut request);
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
