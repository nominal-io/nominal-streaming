use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use prost::Message;
use reqwest::header::CONTENT_ENCODING;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::RETRY_AFTER;

use super::LogStreamError;
use super::LogStreamOptions;

type TokenProvider = Arc<dyn Fn() -> Option<BearerToken> + Send + Sync>;

pub(super) struct AttemptError {
    pub message: String,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
}

// This narrow boundary lets lifecycle/retry tests exercise real batching without network I/O.
pub(super) trait LogTransport: Send + Sync {
    fn send(&self, body: &Bytes) -> Result<(), AttemptError>;
}

pub(super) struct HttpTransport {
    client: reqwest::Client,
    endpoint: url::Url,
    auth: TokenProvider,
    handle: tokio::runtime::Handle,
}

impl HttpTransport {
    pub fn new(
        auth: TokenProvider,
        handle: tokio::runtime::Handle,
        opts: &LogStreamOptions,
    ) -> Result<Self, LogStreamError> {
        if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(LogStreamError::Invalid(
                "log uploads require a multi-thread Tokio runtime with I/O and timers enabled"
                    .into(),
            ));
        }
        let mut endpoint = url::Url::parse(&opts.base_api_url)
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
        endpoint.set_path(&format!(
            "{}/storage/writer/v1/nominal-columnar",
            endpoint.path().trim_end_matches('/')
        ));
        let client = reqwest::Client::builder()
            .timeout(opts.request_timeout)
            .connect_timeout(opts.request_timeout.min(Duration::from_secs(10)))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .user_agent(concat!("nominal-streaming/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| LogStreamError::Invalid("could not construct HTTP client".into()))?;
        Ok(Self {
            client,
            endpoint,
            auth,
            handle,
        })
    }
}

impl LogTransport for HttpTransport {
    fn send(&self, body: &Bytes) -> Result<(), AttemptError> {
        let token = (self.auth)().ok_or_else(|| AttemptError {
            message: "missing auth token".into(),
            retryable: false,
            retry_after: None,
        })?;
        self.handle.block_on(async {
            let response = self
                .client
                .post(self.endpoint.clone())
                .bearer_auth(token.as_str())
                .header(CONTENT_TYPE, "application/x-protobuf")
                .header(CONTENT_ENCODING, "zstd")
                .body(body.clone())
                .send()
                .await
                .map_err(|e| AttemptError {
                    message: if e.is_timeout() {
                        "request timed out"
                    } else {
                        "request transport failed"
                    }
                    .into(),
                    retryable: !e.is_builder(),
                    retry_after: None,
                })?;
            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|h| h.to_str().ok())
                .and_then(parse_retry_after);
            Err(AttemptError {
                message: format!("HTTP {}", status.as_u16()),
                retryable: matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504),
                retry_after,
            })
        })
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            chrono::DateTime::parse_from_rfc2822(value)
                .ok()
                .map(|date| {
                    (date.with_timezone(&chrono::Utc) - chrono::Utc::now())
                        .to_std()
                        .unwrap_or_default()
                })
        })
}

pub(super) fn encode(request: &wire::WriteBatchesRequest) -> std::io::Result<Bytes> {
    zstd::encode_all(request.encode_to_vec().as_slice(), 1).map(Bytes::from)
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
    fn retry_after_supports_seconds_and_http_dates() {
        assert_eq!(parse_retry_after("17"), Some(Duration::from_secs(17)));
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(Duration::ZERO)
        );
        let future = (chrono::Utc::now() + chrono::Duration::seconds(90)).to_rfc2822();
        let delay = parse_retry_after(&future).unwrap();
        assert!(delay >= Duration::from_secs(89) && delay <= Duration::from_secs(90));
        assert_eq!(parse_retry_after("invalid"), None);
        assert_eq!(parse_retry_after("-1"), None);
    }

    #[test]
    fn rejects_current_thread_runtime_that_cannot_drive_worker_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let transport = HttpTransport::new(
            Arc::new(|| None),
            runtime.handle().clone(),
            &LogStreamOptions::default(),
        );
        assert!(matches!(transport, Err(LogStreamError::Invalid(_))));
    }
}
