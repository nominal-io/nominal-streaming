//! Deliver complete log requests to Core, with journal fallback for unconfirmed writes.

use std::path::PathBuf;
use std::sync::Arc;

use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;

use super::journal;
use super::transport::LogTransport;
use super::transport::{self};
use super::NominalLogStreamOpts;

pub(super) enum DeliveryOutcome {
    Acknowledged,
    BackedUp { delivery_error: Option<String> },
}

/// The stream owns admission and draining; the consumer owns delivery and preservation.
pub(super) struct LogConsumer {
    opts: NominalLogStreamOpts,
    target: Option<Arc<dyn LogTransport>>,
    backup: Option<PathBuf>,
}

impl LogConsumer {
    pub fn new(
        opts: NominalLogStreamOpts,
        target: Option<Arc<dyn LogTransport>>,
        backup: Option<PathBuf>,
    ) -> Self {
        Self {
            opts,
            target,
            backup,
        }
    }

    pub fn consume(
        &self,
        request: &wire::WriteBatchesRequest,
        mut on_request: impl FnMut(),
    ) -> Result<DeliveryOutcome, String> {
        let error = match &self.target {
            Some(target) => match self.upload(target.as_ref(), request, &mut on_request) {
                Ok(()) => return Ok(DeliveryOutcome::Acknowledged),
                Err(error) => Some(error),
            },
            None => None,
        };
        if let Some(error) = &error {
            tracing::warn!(error = %error, "Log batch delivery unconfirmed");
        }
        let directory = self
            .backup
            .as_ref()
            .ok_or_else(|| error.clone().unwrap_or_else(|| "no log destination".into()))?;
        journal::save(directory, request).map_err(|disk| {
            format!(
                "{}; journal backup failed: {disk}",
                error.as_deref().unwrap_or("file-only stream")
            )
        })?;
        tracing::debug!("Preserved log batch in journal");
        Ok(DeliveryOutcome::BackedUp {
            delivery_error: error,
        })
    }

    fn upload(
        &self,
        target: &dyn LogTransport,
        request: &wire::WriteBatchesRequest,
        on_request: &mut impl FnMut(),
    ) -> Result<(), String> {
        let body = transport::encode(request, self.opts.max_request_bytes)
            .map_err(|error| format!("encoding failed: {error}"))?;
        on_request();
        target.send(&body)
    }
}
