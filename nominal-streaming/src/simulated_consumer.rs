use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use prost::Message;

use crate::consumer::ConsumerError;
use crate::consumer::ConsumerResult;
use crate::consumer::WriteRequestConsumer;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct SimulatedRetryPolicy {
    pub(crate) max_retries: usize,
    pub(crate) base_backoff: Duration,
    pub(crate) backoff_jitter: Duration,
}

impl SimulatedRetryPolicy {
    pub(crate) fn new(max_retries: usize, base_backoff: Duration) -> Self {
        Self {
            max_retries,
            base_backoff,
            backoff_jitter: Duration::ZERO,
        }
    }

    pub(crate) fn with_jitter(mut self, backoff_jitter: Duration) -> Self {
        self.backoff_jitter = backoff_jitter;
        self
    }
}

impl Default for SimulatedRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            base_backoff: Duration::ZERO,
            backoff_jitter: Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SimulatedNetworkFailure {
    /// Every request attempt times out after the configured network delay.
    AllRequestsTimeout,
    /// The first `attempts` attempts for each request fail before the request
    /// can reach the wrapped consumer.
    FailFirstAttemptsPerRequest { attempts: usize },
    /// Every nth global network attempt fails.
    FailEveryNthAttempt { every: u64 },
    /// The first `attempts` global network attempts fail, modeling a short
    /// outage during startup.
    InitialOutageAttempts { attempts: u64 },
    /// Every nth request fails for the first `attempts_per_request` attempts.
    FailEveryNthRequest {
        every: u64,
        attempts_per_request: usize,
    },
}

impl SimulatedNetworkFailure {
    fn should_fail(&self, request_id: u64, request_attempt: usize, global_attempt: u64) -> bool {
        match *self {
            Self::AllRequestsTimeout => true,
            Self::FailFirstAttemptsPerRequest { attempts } => request_attempt < attempts,
            Self::FailEveryNthAttempt { every } => {
                every != 0 && global_attempt != 0 && global_attempt.is_multiple_of(every)
            }
            Self::InitialOutageAttempts { attempts } => global_attempt <= attempts,
            Self::FailEveryNthRequest {
                every,
                attempts_per_request,
            } => {
                every != 0
                    && request_id != 0
                    && request_id.is_multiple_of(every)
                    && request_attempt < attempts_per_request
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SimulatedNetworkConfig {
    pub(crate) base_latency: Duration,
    pub(crate) latency_jitter: Duration,
    pub(crate) bandwidth_bytes_per_second: Option<u64>,
    pub(crate) failure_patterns: Vec<SimulatedNetworkFailure>,
    pub(crate) retry_policy: SimulatedRetryPolicy,
}

impl SimulatedNetworkConfig {
    pub(crate) fn with_latency(mut self, base_latency: Duration, latency_jitter: Duration) -> Self {
        self.base_latency = base_latency;
        self.latency_jitter = latency_jitter;
        self
    }

    pub(crate) fn with_bandwidth_limit(mut self, bytes_per_second: u64) -> Self {
        self.bandwidth_bytes_per_second = (bytes_per_second > 0).then_some(bytes_per_second);
        self
    }

    pub(crate) fn with_failure_pattern(mut self, failure_pattern: SimulatedNetworkFailure) -> Self {
        self.failure_patterns.push(failure_pattern);
        self
    }

    pub(crate) fn with_retry_policy(mut self, retry_policy: SimulatedRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    fn should_fail(&self, request_id: u64, request_attempt: usize, global_attempt: u64) -> bool {
        self.failure_patterns
            .iter()
            .any(|pattern| pattern.should_fail(request_id, request_attempt, global_attempt))
    }
}

impl Default for SimulatedNetworkConfig {
    fn default() -> Self {
        Self {
            base_latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            bandwidth_bytes_per_second: None,
            failure_patterns: Vec::new(),
            retry_policy: SimulatedRetryPolicy::default(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SimulatedNetworkStats {
    attempts: AtomicU64,
    retries: AtomicU64,
    simulated_failures: AtomicU64,
    successful_requests: AtomicU64,
    delivered_bytes: AtomicU64,
    simulated_sleep_ns: AtomicU64,
}

impl SimulatedNetworkStats {
    pub(crate) fn snapshot(&self) -> SimulatedNetworkStatsSnapshot {
        SimulatedNetworkStatsSnapshot {
            attempts: self.attempts.load(Ordering::Acquire),
            retries: self.retries.load(Ordering::Acquire),
            simulated_failures: self.simulated_failures.load(Ordering::Acquire),
            successful_requests: self.successful_requests.load(Ordering::Acquire),
            delivered_bytes: self.delivered_bytes.load(Ordering::Acquire),
            simulated_sleep: Duration::from_nanos(self.simulated_sleep_ns.load(Ordering::Acquire)),
        }
    }

    fn add_sleep(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.simulated_sleep_ns.fetch_add(nanos, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct SimulatedNetworkStatsSnapshot {
    pub(crate) attempts: u64,
    pub(crate) retries: u64,
    pub(crate) simulated_failures: u64,
    pub(crate) successful_requests: u64,
    pub(crate) delivered_bytes: u64,
    pub(crate) simulated_sleep: Duration,
}

#[derive(Debug)]
pub(crate) struct SimulatedNetworkConsumer<C> {
    consumer: C,
    config: SimulatedNetworkConfig,
    next_request_id: AtomicU64,
    stats: Arc<SimulatedNetworkStats>,
}

impl<C> SimulatedNetworkConsumer<C> {
    pub(crate) fn new(consumer: C, config: SimulatedNetworkConfig) -> Self {
        Self {
            consumer,
            config,
            next_request_id: AtomicU64::new(1),
            stats: Arc::new(SimulatedNetworkStats::default()),
        }
    }

    pub(crate) fn stats(&self) -> Arc<SimulatedNetworkStats> {
        Arc::clone(&self.stats)
    }

    fn network_delay(
        &self,
        request_id: u64,
        request_attempt: usize,
        encoded_len: usize,
    ) -> Duration {
        let jitter = deterministic_jitter(self.config.latency_jitter, request_id, request_attempt);
        self.config
            .base_latency
            .saturating_add(jitter)
            .saturating_add(throughput_delay(
                encoded_len,
                self.config.bandwidth_bytes_per_second,
            ))
    }

    fn retry_delay(&self, request_id: u64, request_attempt: usize) -> Duration {
        let retry_number = request_attempt.saturating_add(1) as u128;
        let backoff_ns = self
            .config
            .retry_policy
            .base_backoff
            .as_nanos()
            .saturating_mul(retry_number);
        duration_from_nanos_saturating(backoff_ns).saturating_add(deterministic_jitter(
            self.config.retry_policy.backoff_jitter,
            request_id,
            request_attempt,
        ))
    }

    fn sleep(&self, duration: Duration) {
        if !duration.is_zero() {
            self.stats.add_sleep(duration);
            thread::sleep(duration);
        }
    }
}

impl<C> WriteRequestConsumer for SimulatedNetworkConsumer<C>
where
    C: WriteRequestConsumer,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let encoded_len = request.encoded_len();
        let retry_policy = self.config.retry_policy;

        for request_attempt in 0..=retry_policy.max_retries {
            let global_attempt = self.stats.attempts.fetch_add(1, Ordering::Relaxed) + 1;
            self.sleep(self.network_delay(request_id, request_attempt, encoded_len));

            if self
                .config
                .should_fail(request_id, request_attempt, global_attempt)
            {
                self.stats
                    .simulated_failures
                    .fetch_add(1, Ordering::Relaxed);

                if request_attempt < retry_policy.max_retries {
                    self.stats.retries.fetch_add(1, Ordering::Relaxed);
                    self.sleep(self.retry_delay(request_id, request_attempt));
                    continue;
                }

                return Err(ConsumerError::RequestError(format!(
                    "simulated network failure for request {request_id} after {} attempt(s)",
                    request_attempt + 1
                )));
            }

            self.consumer.consume(request)?;
            self.stats
                .successful_requests
                .fetch_add(1, Ordering::Relaxed);
            self.stats
                .delivered_bytes
                .fetch_add(encoded_len as u64, Ordering::Relaxed);
            return Ok(());
        }

        unreachable!("retry loop always returns");
    }
}

fn deterministic_jitter(max_jitter: Duration, request_id: u64, request_attempt: usize) -> Duration {
    let max_nanos = max_jitter.as_nanos();
    if max_nanos == 0 {
        return Duration::ZERO;
    }

    let hash = request_id
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(request_attempt as u64)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    duration_from_nanos_saturating(u128::from(hash) % max_nanos.saturating_add(1))
}

fn throughput_delay(encoded_len: usize, bytes_per_second: Option<u64>) -> Duration {
    let Some(bytes_per_second) = bytes_per_second else {
        return Duration::ZERO;
    };
    if bytes_per_second == 0 || encoded_len == 0 {
        return Duration::ZERO;
    }

    let nanos = (encoded_len as u128)
        .saturating_mul(1_000_000_000)
        .checked_div(bytes_per_second as u128)
        .unwrap_or(0);
    duration_from_nanos_saturating(nanos)
}

fn duration_from_nanos_saturating(nanos: u128) -> Duration {
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fail_every_nth_attempt_matches_global_attempts() {
        let failure = SimulatedNetworkFailure::FailEveryNthAttempt { every: 3 };

        assert!(!failure.should_fail(1, 0, 1));
        assert!(!failure.should_fail(1, 0, 2));
        assert!(failure.should_fail(1, 0, 3));
    }

    #[test]
    fn fail_every_nth_request_matches_limited_request_attempts() {
        let failure = SimulatedNetworkFailure::FailEveryNthRequest {
            every: 2,
            attempts_per_request: 1,
        };

        assert!(!failure.should_fail(1, 0, 1));
        assert!(failure.should_fail(2, 0, 2));
        assert!(!failure.should_fail(2, 1, 3));
    }
}
