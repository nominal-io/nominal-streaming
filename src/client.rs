use crate::stream::AuthProvider;
use conjure_error::Error;
use conjure_http::client::AsyncClient;
use conjure_http::client::AsyncRequestBody;
use conjure_http::private::header::CONTENT_ENCODING;
use conjure_http::private::header::CONTENT_TYPE;
use conjure_http::private::{Request, Response};
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use conjure_runtime::{Agent, BodyWriter, Client};
use conjure_runtime::{ResponseBody, UserAgent};
use derive_more::From;
use nominal_api::api::rids::NominalDataSourceOrDatasetRid;
use snap::write::FrameEncoder;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::sync::LazyLock;
use url::Url;

pub mod conjure {
    pub use conjure_error as error;
    pub use conjure_http as http;
    pub use conjure_object as object;
    pub use conjure_runtime as runtime;
}

impl AuthProvider for BearerToken {
    fn token(&self) -> Option<BearerToken> {
        Some(self.clone())
    }
}

pub static PRODUCTION_STREAMING_CLIENT: LazyLock<StreamingClient> = LazyLock::new(|| {
    async_conjure_streaming_client("https://api.gov.nominal.io/api".try_into().unwrap())
        .expect("Failed to create client")
});

pub static STAGING_STREAMING_CLIENT: LazyLock<StreamingClient> = LazyLock::new(|| {
    async_conjure_streaming_client("https://api-staging.gov.nominal.io/api".try_into().unwrap())
        .expect("Failed to create client")
});

fn async_conjure_streaming_client(uri: Url) -> Result<StreamingClient, Error> {
    Client::builder()
        .service("core-streaming-rs")
        .user_agent(UserAgent::new(Agent::new(
            "core-streaming-rs",
            env!("CARGO_PKG_VERSION"),
        )))
        .uri(uri)
        .connect_timeout(std::time::Duration::from_secs(1))
        .read_timeout(std::time::Duration::from_secs(2))
        .write_timeout(std::time::Duration::from_secs(2))
        .backoff_slot_size(std::time::Duration::from_millis(10))
        .build()
        .map(|client| client.into())
}

#[derive(From, Clone)]
pub struct StreamingClient(Client);

impl Debug for StreamingClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamingClient")
    }
}

impl StreamingClient {
    pub async fn send(
        &self,
        req: WriteRequest<'_>,
    ) -> Result<Response<ResponseBody>, Error> {
        self.0.send(req).await
    }
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
