//! Deliver complete log requests to Core, with journal fallback for unconfirmed writes.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use conjure_object::BearerToken;
use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;
use prost::Message;

use super::journal::Journal;
use super::LogStreamError;
use super::NominalLogStreamOpts;
use crate::client::NominalApiClients;
use crate::client::{self};

pub(super) type TokenProvider = Arc<dyn Fn() -> Option<BearerToken> + Send + Sync>;

/// A complete delivery, including retries owned by the shared HTTP client.
pub(super) trait LogTransport: Send + Sync {
    fn send(&self, request: &wire::WriteBatchesRequest) -> Result<(), String>;
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
        let client = NominalApiClients::try_from_url_with_agent(endpoint, opts.user_agent.as_ref())
            .map_err(|e| LogStreamError::Invalid(crate::consumer::describe_request_error(&e)))?;
        Ok(Self {
            client,
            auth,
            handle,
        })
    }
}

impl LogTransport for HttpTransport {
    fn send(&self, request: &wire::WriteBatchesRequest) -> Result<(), String> {
        let body = encode(request).map_err(|error| format!("encoding failed: {error}"))?;
        let token = (self.auth)().ok_or("missing auth token")?;
        self.client
            .send_blocking(&self.handle, client::columnar_request(body, &token))
    }
}

/// Batching already keeps requests within `max_request_bytes`; the server rejects anything else.
pub(super) fn encode(request: &wire::WriteBatchesRequest) -> std::io::Result<Vec<u8>> {
    client::compress(&request.encode_to_vec())
}

pub(super) enum DeliveryOutcome {
    Acknowledged,
    BackedUp { delivery_error: Option<String> },
}

/// The stream owns admission and draining; the consumer owns delivery and preservation.
pub(super) struct LogConsumer {
    target: Option<Arc<dyn LogTransport>>,
    journal: Option<Journal>,
}

impl LogConsumer {
    pub fn new(target: Option<Arc<dyn LogTransport>>, journal: Option<Journal>) -> Self {
        Self { target, journal }
    }

    pub fn has_journal(&self) -> bool {
        self.journal.is_some()
    }

    /// `on_request` runs once before each upload, so the stream can count requests.
    pub fn consume(
        &self,
        request: &wire::WriteBatchesRequest,
        on_request: impl FnOnce(),
    ) -> Result<DeliveryOutcome, String> {
        let error = match &self.target {
            Some(target) => {
                on_request();
                // A panicking transport, such as a failing auth provider, still falls back.
                let sent = std::panic::catch_unwind(AssertUnwindSafe(|| target.send(request)))
                    .unwrap_or_else(|_| Err("log transport panicked".into()));
                match sent {
                    Ok(()) => return Ok(DeliveryOutcome::Acknowledged),
                    Err(error) => Some(error),
                }
            }
            None => None,
        };
        if let Some(error) = &error {
            tracing::warn!(error = %error, "Log batch delivery unconfirmed");
        }
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| error.clone().unwrap_or_else(|| "no log destination".into()))?;
        journal.append(request).map_err(|disk| {
            format!(
                "{}; journal backup failed: {disk}",
                error.as_deref().unwrap_or("file-only stream")
            )
        })?;
        Ok(DeliveryOutcome::BackedUp {
            delivery_error: error,
        })
    }
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
