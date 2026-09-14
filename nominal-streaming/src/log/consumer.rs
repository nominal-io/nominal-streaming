//! Deliver complete log requests to Core, with journal fallback for unconfirmed writes.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use nominal_api::tonic::nominal::direct_channel_writer::v2 as wire;

use super::journal;
use super::transport::LogTransport;
use super::transport::{self};
use super::LogStreamOptions;

pub(super) enum DeliveryOutcome {
    Acknowledged,
    BackedUp { delivery_error: Option<String> },
}

/// The stream owns admission and draining; the consumer owns delivery and preservation.
pub(super) struct LogConsumer {
    opts: LogStreamOptions,
    target: Option<Arc<dyn LogTransport>>,
    backup: Option<PathBuf>,
}

impl LogConsumer {
    pub fn new(
        opts: LogStreamOptions,
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
        mut on_attempt: impl FnMut(bool),
    ) -> Result<DeliveryOutcome, String> {
        let error = match &self.target {
            Some(target) => match self.upload(target.as_ref(), request, &mut on_attempt) {
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
        on_attempt: &mut impl FnMut(bool),
    ) -> Result<(), String> {
        let body = transport::encode(request, self.opts.max_request_bytes)
            .map_err(|error| format!("encoding failed: {error}"))?;
        let mut delay = self.opts.initial_backoff;
        for attempt in 0..=self.opts.max_retries {
            on_attempt(attempt > 0);
            tracing::debug!(attempt = attempt + 1, "Attempting log delivery");
            let error = match target.send(&body) {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            let reason = if !error.retryable {
                Some("non_retryable_error")
            } else if attempt == self.opts.max_retries {
                Some("retry_budget_exhausted")
            } else if error
                .retry_after
                .is_some_and(|after| after > self.opts.max_retry_after)
            {
                Some("retry_after_exceeds_limit")
            } else {
                None
            };
            if let Some(reason) = reason {
                tracing::debug!(reason, error = %error.message, "Log delivery unconfirmed");
                return Err(error.message);
            }
            tracing::debug!(
                attempt = attempt + 1,
                backoff_ms = delay.max(error.retry_after.unwrap_or_default()).as_millis() as u64,
                error = %error.message,
                "Retrying log delivery"
            );
            thread::sleep(delay.max(error.retry_after.unwrap_or_default()));
            delay = delay.saturating_mul(2).min(self.opts.max_backoff);
        }
        unreachable!("the last attempt returns its delivery error")
    }
}
