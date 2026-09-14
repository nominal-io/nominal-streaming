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
            .user_agent(concat!("nominal-streaming/", env!("CARGO_PKG_VERSION")));
        let client = client
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

impl HttpTransport {
    fn request(&self, body: &Bytes, token: &BearerToken) -> reqwest::RequestBuilder {
        self.client
            .post(self.endpoint.clone())
            .bearer_auth(token.as_str())
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "zstd")
            .body(body.clone())
    }
}

impl LogTransport for HttpTransport {
    fn send(&self, body: &Bytes) -> Result<(), AttemptError> {
        let token = (self.auth)().ok_or_else(|| AttemptError {
            message: "missing auth token".into(),
            retryable: false,
            retry_after: None,
        })?;
        let started = std::time::Instant::now();
        tracing::debug!(wire_bytes = body.len(), "Sending log request");
        let result = self.handle.block_on(async {
            let response =
                self.request(body, &token)
                    .send()
                    .await
                    .map_err(|error| AttemptError {
                        message: if error.is_timeout() {
                            "request timed out"
                        } else {
                            "request transport failed"
                        }
                        .into(),
                        retryable: !error.is_builder(),
                        retry_after: None,
                    })?;
            classify_response(response.status(), response.headers())
        });
        tracing::debug!(
            elapsed_micros = started.elapsed().as_micros() as u64,
            wire_bytes = body.len(),
            success = result.is_ok(),
            error = result.as_ref().err().map(|e| e.message.as_str()),
            "Log request completed"
        );
        result
    }
}

fn classify_response(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> Result<(), AttemptError> {
    if status.is_success() {
        return Ok(());
    }
    Err(AttemptError {
        message: format!("HTTP {}", status.as_u16()),
        retryable: matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504),
        retry_after: headers
            .get(RETRY_AFTER)
            .and_then(|h| h.to_str().ok())
            .and_then(parse_retry_after),
    })
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
    let compressed = zstd::bulk::compress(raw.as_slice(), 1)?;
    tracing::debug!(
        raw_bytes = raw.len(),
        wire_bytes = compressed.len(),
        protobuf_micros = compression_started
            .duration_since(encode_started)
            .as_micros() as u64,
        zstd_micros = compression_started.elapsed().as_micros() as u64,
        "Encoded log request"
    );
    Ok(Bytes::from(compressed))
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
    fn response_status_controls_retries_and_retry_after() {
        let headers = reqwest::header::HeaderMap::from_iter([(RETRY_AFTER, "17".parse().unwrap())]);
        for code in [200, 201, 204, 299] {
            assert!(
                classify_response(reqwest::StatusCode::from_u16(code).unwrap(), &headers).is_ok()
            );
        }
        for code in [301, 400, 401, 403, 408, 413, 429, 500, 501, 502, 503, 504] {
            let error = classify_response(reqwest::StatusCode::from_u16(code).unwrap(), &headers)
                .err()
                .unwrap();
            assert_eq!(
                error.retryable,
                matches!(code, 408 | 429 | 500 | 502 | 503 | 504)
            );
            assert_eq!(error.retry_after, Some(Duration::from_secs(17)));
            assert_eq!(error.message, format!("HTTP {code}"));
        }
        let invalid =
            reqwest::header::HeaderMap::from_iter([(RETRY_AFTER, "invalid".parse().unwrap())]);
        assert!(
            classify_response(reqwest::StatusCode::TOO_MANY_REQUESTS, &invalid)
                .err()
                .unwrap()
                .retry_after
                .is_none()
        );
    }

    #[test]
    fn request_preserves_body_and_has_no_diagnostic_headers() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let transport = HttpTransport::new(
            Arc::new(|| None),
            runtime.handle().clone(),
            &LogStreamOptions::default(),
        )
        .unwrap();
        let body = Bytes::from_static(b"encoded request");
        let token = BearerToken::new("test-token").unwrap();
        let request = transport.request(&body, &token).build().unwrap();
        assert_eq!(request.method(), reqwest::Method::POST);
        assert!(request
            .url()
            .path()
            .ends_with("/storage/writer/v1/nominal-columnar"));
        assert_eq!(request.headers()[CONTENT_TYPE], "application/x-protobuf");
        assert_eq!(request.headers()[CONTENT_ENCODING], "zstd");
        assert_eq!(
            request.headers()[reqwest::header::AUTHORIZATION],
            "Bearer test-token"
        );
        for name in ["X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled"] {
            assert!(!request.headers().contains_key(name));
        }
        assert_eq!(request.body().unwrap().as_bytes().unwrap(), body.as_ref());
    }

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
