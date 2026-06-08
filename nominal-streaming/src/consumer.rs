use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;
use std::thread;
use std::time::Duration;

use apache_avro::types::Record;
use apache_avro::types::Value;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Points;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use parking_lot::Mutex;
use prost::Message;
use tracing::warn;

use crate::client::NominalApiClients;
use crate::client::{self};
use crate::listener::NominalStreamListener;
use crate::types::AuthProvider;

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("avro error: {0}")]
    AvroError(#[from] Box<apache_avro::Error>),
    #[error("No auth token provided. Please make sure you're authenticated.")]
    MissingTokenError,
    #[error("request error: {0}")]
    RequestError(String),
    #[error("consumer error occurred: {0}")]
    GenericConsumerError(#[from] Box<dyn Error + Send + Sync>),
}

pub type ConsumerResult<T> = Result<T, ConsumerError>;

pub trait WriteRequestConsumer: Send + Sync + Debug {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SimulatedRetryPolicy {
    pub max_retries: usize,
    pub base_backoff: Duration,
    pub backoff_jitter: Duration,
}

impl SimulatedRetryPolicy {
    pub fn new(max_retries: usize, base_backoff: Duration) -> Self {
        Self {
            max_retries,
            base_backoff,
            backoff_jitter: Duration::ZERO,
        }
    }

    pub fn with_jitter(mut self, backoff_jitter: Duration) -> Self {
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
pub enum SimulatedNetworkFailure {
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
pub struct SimulatedNetworkConfig {
    pub base_latency: Duration,
    pub latency_jitter: Duration,
    pub bandwidth_bytes_per_second: Option<u64>,
    pub failure_patterns: Vec<SimulatedNetworkFailure>,
    pub retry_policy: SimulatedRetryPolicy,
}

impl SimulatedNetworkConfig {
    pub fn with_latency(mut self, base_latency: Duration, latency_jitter: Duration) -> Self {
        self.base_latency = base_latency;
        self.latency_jitter = latency_jitter;
        self
    }

    pub fn with_bandwidth_limit(mut self, bytes_per_second: u64) -> Self {
        self.bandwidth_bytes_per_second = (bytes_per_second > 0).then_some(bytes_per_second);
        self
    }

    pub fn with_failure_pattern(mut self, failure_pattern: SimulatedNetworkFailure) -> Self {
        self.failure_patterns.push(failure_pattern);
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: SimulatedRetryPolicy) -> Self {
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
pub struct SimulatedNetworkStats {
    attempts: AtomicU64,
    retries: AtomicU64,
    simulated_failures: AtomicU64,
    successful_requests: AtomicU64,
    delivered_bytes: AtomicU64,
    simulated_sleep_ns: AtomicU64,
}

impl SimulatedNetworkStats {
    pub fn snapshot(&self) -> SimulatedNetworkStatsSnapshot {
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
pub struct SimulatedNetworkStatsSnapshot {
    pub attempts: u64,
    pub retries: u64,
    pub simulated_failures: u64,
    pub successful_requests: u64,
    pub delivered_bytes: u64,
    pub simulated_sleep: Duration,
}

#[derive(Debug)]
pub struct SimulatedNetworkConsumer<C> {
    consumer: C,
    config: SimulatedNetworkConfig,
    next_request_id: AtomicU64,
    stats: Arc<SimulatedNetworkStats>,
}

impl<C> SimulatedNetworkConsumer<C> {
    pub fn new(consumer: C, config: SimulatedNetworkConfig) -> Self {
        Self {
            consumer,
            config,
            next_request_id: AtomicU64::new(1),
            stats: Arc::new(SimulatedNetworkStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<SimulatedNetworkStats> {
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

#[derive(Clone)]
pub struct NominalCoreConsumer<A: AuthProvider> {
    client: NominalApiClients,
    handle: tokio::runtime::Handle,
    auth_provider: A,
    data_source_rid: ResourceIdentifier,
}

impl<A: AuthProvider> NominalCoreConsumer<A> {
    pub fn new(
        client: NominalApiClients,
        handle: tokio::runtime::Handle,
        auth_provider: A,
        data_source_rid: ResourceIdentifier,
    ) -> Self {
        Self {
            client,
            handle,
            auth_provider,
            data_source_rid,
        }
    }
}

impl<T: AuthProvider> Debug for NominalCoreConsumer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalCoreConsumer")
            .field("client", &self.client)
            .field("data_source_rid", &self.data_source_rid)
            .finish()
    }
}

impl<T: AuthProvider + 'static> WriteRequestConsumer for NominalCoreConsumer<T> {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let token = self
            .auth_provider
            .token()
            .ok_or(ConsumerError::MissingTokenError)?;
        let write_request =
            client::encode_request(request.encode_to_vec(), &token, &self.data_source_rid)?;
        self.handle.block_on(async {
            self.client
                .send(write_request)
                .await
                .map_err(|e| ConsumerError::RequestError(format!("{e:?}")))
        })?;
        Ok(())
    }
}

const DEFAULT_FILE_PREFIX: &str = "nominal_stream";

pub const DATASET_RID_METADATA_KEY: &str = "nominal.dataset_rid";

pub static CORE_SCHEMA_STR: &str = r#"{
  "type": "record",
  "name": "AvroStream",
  "namespace": "io.nominal.ingest",
  "fields": [
      {
          "name": "channel",
          "type": "string",
          "doc": "Channel/series name (e.g., 'vehicle_id', 'col_1', 'temperature')"
      },
      {
          "name": "timestamps",
          "type": {"type": "array", "items": "long"},
          "doc": "Array of Unix timestamps in nanoseconds"
      },
      {
          "name": "values",
          "type": {"type": "array", "items": [
              "double",
              "string",
              "long",
              {"type": "record", "name": "DoubleArray", "fields": [{"name": "items", "type": {"type": "array", "items": "double"}}]},
              {"type": "record", "name": "StringArray", "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}]},
              {"type": "record", "name": "JsonStruct", "fields": [{"name": "json", "type": "string"}]}
          ]},
          "doc": "Array of values. Can be doubles, longs, strings, arrays, or JSON structs"
      },
      {
          "name": "tags",
          "type": {"type": "map", "values": "string"},
          "default": {},
          "doc": "Key-value metadata tags"
      }
  ]
}
"#;

pub static CORE_AVRO_SCHEMA: LazyLock<apache_avro::Schema> = LazyLock::new(|| {
    let json = serde_json::from_str(CORE_SCHEMA_STR).expect("Failed to parse JSON schema");
    apache_avro::Schema::parse(&json).expect("Failed to parse Avro schema")
});

#[derive(Clone)]
pub struct AvroFileConsumer {
    writer: Arc<Mutex<apache_avro::Writer<'static, std::fs::File>>>,
    path: PathBuf,
}

impl Debug for AvroFileConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroFileConsumer")
            .field("path", &self.path)
            .finish()
    }
}

impl AvroFileConsumer {
    pub fn new(
        directory: impl Into<PathBuf>,
        file_prefix: Option<String>,
        dataset_rid: Option<ResourceIdentifier>,
    ) -> std::io::Result<Self> {
        let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let prefix = file_prefix.unwrap_or_else(|| DEFAULT_FILE_PREFIX.to_string());
        let filename = format!("{prefix}_{datetime}.avro");
        let directory = directory.into();
        let full_path = directory.join(&filename);

        Self::new_with_full_path(full_path, true, dataset_rid)
    }

    /// Opens `file_path` for writing and wraps it in an avro `Writer`.
    ///
    /// If `overwrite` is true and the path already exists, its prior contents
    /// are discarded. Truncation is required when reusing a path: the avro
    /// container format is single-header-and-blocks, so opening a longer
    /// existing file without truncating would leave leftover bytes from the
    /// previous run past the new content's end and produce a corrupt reader
    /// stream.
    ///
    /// If `overwrite` is false and the path already exists, an
    /// `io::ErrorKind::AlreadyExists` error is returned and no file is
    /// touched. This is the safe choice when the caller does not want to
    /// silently destroy prior data.
    ///
    /// If `dataset_rid` is provided, it is written to the avro file's user
    /// metadata under the `nominal.dataset_rid` key so downstream readers
    /// can identify the dataset the file belongs to.
    pub fn new_with_full_path(
        file_path: impl Into<PathBuf>,
        overwrite: bool,
        dataset_rid: Option<ResourceIdentifier>,
    ) -> std::io::Result<Self> {
        let path = file_path.into();
        std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true);
        if overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        }
        let file = options.open(&path)?;

        let mut writer = apache_avro::Writer::builder()
            .schema(&CORE_AVRO_SCHEMA)
            .writer(file)
            .codec(apache_avro::Codec::Snappy)
            .build();

        if let Some(rid) = dataset_rid {
            writer
                .add_user_metadata(DATASET_RID_METADATA_KEY.to_string(), rid.to_string())
                .map_err(|e| {
                    std::io::Error::other(format!("failed to write avro metadata: {e}"))
                })?;
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            path,
        })
    }

    fn append_series(&self, series: &[Series]) -> ConsumerResult<()> {
        let mut records: Vec<Record> = Vec::new();
        for series in series {
            let (timestamps, values) = points_to_avro(series.points.as_ref());

            let mut record = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create Avro record");

            record.put(
                "channel",
                series
                    .channel
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or("values".to_string()),
            );
            record.put("timestamps", Value::Array(timestamps));
            record.put("values", Value::Array(values));
            record.put("tags", series.tags.clone());

            records.push(record);
        }

        self.writer
            .lock()
            .extend(records)
            .map_err(|e| ConsumerError::AvroError(Box::new(e)))?;

        Ok(())
    }
}

fn points_to_avro(points: Option<&Points>) -> (Vec<Value>, Vec<Value>) {
    let Some(Points {
        points_type: Some(points),
    }) = points
    else {
        return (Vec::new(), Vec::new());
    };

    match points {
        PointsType::DoublePoints(DoublePoints { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(0, Box::new(Value::Double(point.value))),
                )
            })
            .collect(),
        PointsType::StringPoints(StringPoints { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(1, Box::new(Value::String(point.value.clone()))),
                )
            })
            .collect(),
        PointsType::IntegerPoints(IntegerPoints { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(2, Box::new(Value::Long(point.value))),
                )
            })
            .collect(),
        PointsType::ArrayPoints(ArrayPoints { array_type }) => match array_type {
            Some(ArrayType::DoubleArrayPoints(points)) => points
                .points
                .iter()
                .map(|point| {
                    let array_values: Vec<Value> =
                        point.value.iter().map(|v| Value::Double(*v)).collect();
                    let record =
                        Value::Record(vec![("items".to_string(), Value::Array(array_values))]);
                    (
                        convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                        Value::Union(3, Box::new(record)),
                    )
                })
                .collect(),
            Some(ArrayType::StringArrayPoints(points)) => points
                .points
                .iter()
                .map(|point| {
                    let array_values: Vec<Value> = point
                        .value
                        .iter()
                        .map(|v| Value::String(v.clone()))
                        .collect();
                    let record =
                        Value::Record(vec![("items".to_string(), Value::Array(array_values))]);
                    (
                        convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                        Value::Union(4, Box::new(record)),
                    )
                })
                .collect(),
            None => (Vec::new(), Vec::new()),
        },
        PointsType::StructPoints(StructPoints { points }) => points
            .iter()
            .map(|point| {
                let record = Value::Record(vec![(
                    "json".to_string(),
                    Value::String(point.json_string.clone()),
                )]);
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(5, Box::new(record)),
                )
            })
            .collect(),
        PointsType::Uint64Points(Uint64Points { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(2, Box::new(Value::Long(point.value as i64))),
                )
            })
            .collect(),
    }
}

fn convert_timestamp_to_nanoseconds(timestamp: Timestamp) -> Value {
    Value::Long(timestamp.seconds * 1_000_000_000 + timestamp.nanos as i64)
}

impl WriteRequestConsumer for AvroFileConsumer {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        self.append_series(&request.series)?;
        Ok(())
    }
}

impl Drop for AvroFileConsumer {
    /// Defensive flush-on-drop. In normal operation, records reach disk via
    /// `append_series` → `apache_avro::Writer::extend`, which flushes at the
    /// end of every call. But `apache_avro::Writer` itself does not flush on
    /// drop, so any code path that bypasses `extend` (e.g. a direct
    /// `Writer::append`, or a future writer call that forgets to flush) would
    /// silently lose buffered records when the consumer goes out of scope.
    /// This impl makes that failure mode impossible regardless of how the
    /// inner writer is driven.
    fn drop(&mut self) {
        if let Err(e) = self.writer.lock().flush() {
            warn!(
                "failed to flush avro writer for {:?} on drop: {e:?}",
                self.path
            );
        }
    }
}

#[derive(Clone)]
pub struct RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer,
    F: WriteRequestConsumer,
{
    primary: P,
    fallback: F,
}

impl<P, F> RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer,
    F: WriteRequestConsumer,
{
    pub fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

impl<P, F> Debug for RequestConsumerWithFallback<P, F>
where
    F: Send + Sync + WriteRequestConsumer,
    P: Send + Sync + WriteRequestConsumer,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestConsumerWithFallback")
            .field("primary", &self.primary)
            .field("fallback", &self.fallback)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct DualWriteRequestConsumer<P, S>
where
    P: WriteRequestConsumer,
    S: WriteRequestConsumer,
{
    primary: P,
    secondary: S,
}

impl<P, S> DualWriteRequestConsumer<P, S>
where
    P: WriteRequestConsumer,
    S: WriteRequestConsumer,
{
    pub fn new(primary: P, secondary: S) -> Self {
        Self { primary, secondary }
    }
}

impl<P, S> WriteRequestConsumer for DualWriteRequestConsumer<P, S>
where
    P: WriteRequestConsumer + Send + Sync,
    S: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let primary_result = self.primary.consume(request);
        let secondary_result = self.secondary.consume(request);
        if let Err(e) = &primary_result {
            warn!("Sending request to primary consumer failed: {:?}", e);
        }
        if let Err(e) = &secondary_result {
            warn!("Sending request to secondary consumer failed: {:?}", e);
        }

        // If either failed, return the error
        primary_result.and(secondary_result)
    }
}

impl<P, F> WriteRequestConsumer for RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer + Send + Sync,
    F: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        if let Err(e) = self.primary.consume(request) {
            warn!("Sending request to primary consumer failed. Attempting fallback.");
            let fallback_result = self.fallback.consume(request);
            // we want to notify the caller about the missing token error as it is a user error
            // todo: get rid of this once we figure out why the auth handle blocks in connect
            if let ConsumerError::MissingTokenError = e {
                return Err(ConsumerError::MissingTokenError);
            }
            return fallback_result;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ListeningWriteRequestConsumer<C>
where
    C: WriteRequestConsumer,
{
    consumer: C,
    listeners: Vec<Arc<dyn NominalStreamListener>>,
}

impl<C> ListeningWriteRequestConsumer<C>
where
    C: WriteRequestConsumer,
{
    pub fn new(consumer: C, listeners: Vec<Arc<dyn NominalStreamListener>>) -> Self {
        Self {
            consumer,
            listeners,
        }
    }
}

impl<C> WriteRequestConsumer for ListeningWriteRequestConsumer<C>
where
    C: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        match self.consumer.consume(request) {
            Ok(_) => {
                self.listeners.on_success(request);
                Ok(())
            }
            Err(e) => {
                self.listeners.on_error(&e, request);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use apache_avro::Reader;
    use nominal_api::tonic::google::protobuf::Timestamp;
    use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
    use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
    use tempfile::NamedTempFile;

    use super::*;

    fn make_timestamp(secs: i64, nanos: i32) -> Option<Timestamp> {
        Some(Timestamp {
            seconds: secs,
            nanos,
        })
    }

    fn make_series(name: &str, points: Points) -> Series {
        Series {
            channel: Some(Channel {
                name: name.to_string(),
            }),
            tags: HashMap::new(),
            points: Some(points),
        }
    }

    #[test]
    fn test_avro_file_with_all_value_types() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        // Create consumer and write all types
        {
            let consumer = AvroFileConsumer::new_with_full_path(&path, true, None).unwrap();

            // Create series with each type
            let double_series = make_series(
                "doubles",
                Points {
                    points_type: Some(PointsType::DoublePoints(DoublePoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint {
                                timestamp: make_timestamp(1000, 0),
                                value: 1.5,
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint {
                                timestamp: make_timestamp(1001, 0),
                                value: 2.5,
                            },
                        ],
                    })),
                },
            );

            let long_series = make_series(
                "longs",
                Points {
                    points_type: Some(PointsType::IntegerPoints(IntegerPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                                timestamp: make_timestamp(1000, 0),
                                value: 42,
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                                timestamp: make_timestamp(1001, 0),
                                value: -100,
                            },
                        ],
                    })),
                },
            );

            let string_series = make_series(
                "strings",
                Points {
                    points_type: Some(PointsType::StringPoints(StringPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::StringPoint {
                                timestamp: make_timestamp(1000, 0),
                                value: "hello".to_string(),
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::StringPoint {
                                timestamp: make_timestamp(1001, 0),
                                value: "world".to_string(),
                            },
                        ],
                    })),
                },
            );

            let double_array_series = make_series(
                "double_arrays",
                Points {
                    points_type: Some(PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::DoubleArrayPoints(
                            nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoints {
                                points: vec![
                                    DoubleArrayPoint {
                                        timestamp: make_timestamp(1000, 0),
                                        value: vec![1.0, 2.0, 3.0],
                                    },
                                    DoubleArrayPoint {
                                        timestamp: make_timestamp(1001, 0),
                                        value: vec![4.0, 5.0],
                                    },
                                ],
                            },
                        )),
                    })),
                },
            );

            let string_array_series = make_series(
                "string_arrays",
                Points {
                    points_type: Some(PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::StringArrayPoints(
                            nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoints {
                                points: vec![
                                    StringArrayPoint {
                                        timestamp: make_timestamp(1000, 0),
                                        value: vec!["a".to_string(), "b".to_string()],
                                    },
                                    StringArrayPoint {
                                        timestamp: make_timestamp(1001, 0),
                                        value: vec![
                                            "c".to_string(),
                                            "d".to_string(),
                                            "e".to_string(),
                                        ],
                                    },
                                ],
                            },
                        )),
                    })),
                },
            );

            let struct_series = make_series(
                "structs",
                Points {
                    points_type: Some(PointsType::StructPoints(StructPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::StructPoint {
                                timestamp: make_timestamp(1000, 0),
                                json_string: r#"{"key": "value"}"#.to_string(),
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::StructPoint {
                                timestamp: make_timestamp(1001, 0),
                                json_string: r#"{"count": 42}"#.to_string(),
                            },
                        ],
                    })),
                },
            );

            let uint64_series = make_series(
                "uint64s",
                Points {
                    points_type: Some(PointsType::Uint64Points(Uint64Points {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point {
                                timestamp: make_timestamp(1000, 0),
                                value: u64::MAX,
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point {
                                timestamp: make_timestamp(1001, 0),
                                value: 12345678901234567890,
                            },
                        ],
                    })),
                },
            );

            let request = WriteRequestNominal {
                series: vec![
                    double_series,
                    long_series,
                    string_series,
                    double_array_series,
                    string_array_series,
                    struct_series,
                    uint64_series,
                ],
                session_name: None,
            };

            consumer.consume(&request).unwrap();

            // Flush the writer by dropping it
            drop(consumer);
        }

        // Read back the file and verify
        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();

        let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 7, "Expected 7 series records");

        // Verify each record has the expected channel name and value types
        let channels: Vec<String> = records
            .iter()
            .filter_map(|r| {
                if let Value::Record(fields) = r {
                    fields.iter().find_map(|(name, value)| {
                        if name == "channel" {
                            if let Value::String(s) = value {
                                Some(s.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
            .collect();

        assert!(channels.contains(&"doubles".to_string()));
        assert!(channels.contains(&"longs".to_string()));
        assert!(channels.contains(&"strings".to_string()));
        assert!(channels.contains(&"double_arrays".to_string()));
        assert!(channels.contains(&"string_arrays".to_string()));
        assert!(channels.contains(&"structs".to_string()));
        assert!(channels.contains(&"uint64s".to_string()));

        // Verify specific value types by checking the union discriminants
        for record in &records {
            if let Value::Record(fields) = record {
                let channel = fields.iter().find_map(|(name, value)| {
                    if name == "channel" {
                        if let Value::String(s) = value {
                            Some(s.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                let values =
                    fields.iter().find_map(
                        |(name, value)| {
                            if name == "values" {
                                Some(value)
                            } else {
                                None
                            }
                        },
                    );

                if let (Some(channel), Some(Value::Array(values))) = (channel, values) {
                    assert_eq!(values.len(), 2, "Channel {} should have 2 values", channel);

                    match channel.as_str() {
                        "doubles" => {
                            assert_eq!(values[0], Value::Union(0, Box::new(Value::Double(1.5))));
                            assert_eq!(values[1], Value::Union(0, Box::new(Value::Double(2.5))));
                        }
                        "strings" => {
                            assert_eq!(
                                values[0],
                                Value::Union(1, Box::new(Value::String("hello".to_string())))
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(1, Box::new(Value::String("world".to_string())))
                            );
                        }
                        "longs" => {
                            assert_eq!(values[0], Value::Union(2, Box::new(Value::Long(42))));
                            assert_eq!(values[1], Value::Union(2, Box::new(Value::Long(-100))));
                        }
                        "double_arrays" => {
                            assert_eq!(
                                values[0],
                                Value::Union(
                                    3,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![
                                            Value::Double(1.0),
                                            Value::Double(2.0),
                                            Value::Double(3.0)
                                        ])
                                    )]))
                                )
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    3,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![Value::Double(4.0), Value::Double(5.0)])
                                    )]))
                                )
                            );
                        }
                        "string_arrays" => {
                            assert_eq!(
                                values[0],
                                Value::Union(
                                    4,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![
                                            Value::String("a".to_string()),
                                            Value::String("b".to_string())
                                        ])
                                    )]))
                                )
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    4,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![
                                            Value::String("c".to_string()),
                                            Value::String("d".to_string()),
                                            Value::String("e".to_string())
                                        ])
                                    )]))
                                )
                            );
                        }
                        "structs" => {
                            assert_eq!(
                                values[0],
                                Value::Union(
                                    5,
                                    Box::new(Value::Record(vec![(
                                        "json".to_string(),
                                        Value::String(r#"{"key": "value"}"#.to_string())
                                    )]))
                                )
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    5,
                                    Box::new(Value::Record(vec![(
                                        "json".to_string(),
                                        Value::String(r#"{"count": 42}"#.to_string())
                                    )]))
                                )
                            );
                        }
                        "uint64s" => {
                            // u64::MAX as i64 is -1, 12345678901234567890u64 as i64 is negative
                            assert_eq!(
                                values[0],
                                Value::Union(2, Box::new(Value::Long(u64::MAX as i64)))
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    2,
                                    Box::new(Value::Long(12345678901234567890u64 as i64))
                                )
                            );
                        }
                        _ => panic!("Unexpected channel: {}", channel),
                    }
                }
            }
        }
    }

    #[test]
    fn reopening_path_with_overwrite_truncates_to_valid_avro_file() {
        // Write 500 points, then re-open the same path with overwrite=true
        // and write 5 points. Both passes must produce a file that reads back
        // cleanly with the expected point count, AND the second write must
        // shrink the file at the filesystem level — not just produce a
        // readable record count. Without truncate, the second pass would
        // overwrite from offset 0 and leave the tail of the longer first
        // file intact, corrupting the reader stream.
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        write_integer_points(&path, 500);
        assert_eq!(read_integer_point_count(&path), 500);
        let first_size = std::fs::metadata(&path).unwrap().len();

        write_integer_points(&path, 5);
        assert_eq!(read_integer_point_count(&path), 5);
        let second_size = std::fs::metadata(&path).unwrap().len();

        assert!(
            second_size < first_size,
            "second write should shrink the file (first: {first_size} bytes, second: {second_size} bytes)"
        );
    }

    #[test]
    fn dropping_consumer_flushes_buffered_records() {
        // Defensive test against future misuse of avro api (writing without flushing).
        // Current stream implementation uses .extend(), which flushes internally.
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        {
            let consumer = AvroFileConsumer::new_with_full_path(&path, true, None).unwrap();

            let mut record = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create Avro record");
            record.put("channel", "ch".to_string());
            record.put("timestamps", Value::Array(vec![Value::Long(0)]));
            record.put(
                "values",
                Value::Array(vec![Value::Union(2, Box::new(Value::Long(42)))]),
            );
            record.put("tags", HashMap::<String, String>::new());

            consumer.writer.lock().append(record).unwrap();
            // consumer drops here — the only thing that can land the buffered
            // record on disk is a flush from the Drop impl.
        }

        assert_eq!(
            read_integer_point_count(&path),
            1,
            "expected the buffered point to land on disk after the consumer dropped"
        );
    }

    #[test]
    fn new_with_full_path_errors_when_overwrite_false_and_path_exists() {
        // Pre-create a file at the target path; opening with overwrite=false
        // must fail rather than silently destroying the existing data.
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();
        std::fs::write(&path, b"prior content").unwrap();

        let err = AvroFileConsumer::new_with_full_path(&path, false, None)
            .expect_err("expected AlreadyExists when overwrite=false and file exists");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        // Pre-existing bytes must be untouched.
        assert_eq!(std::fs::read(&path).unwrap(), b"prior content");
    }

    #[test]
    fn new_with_full_path_succeeds_when_overwrite_false_and_path_missing() {
        // overwrite=false should still create a brand-new file; the guard is
        // only against clobbering existing content.
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join("fresh.avro");

        write_integer_points_with(&path, 3, false, None);
        assert_eq!(read_integer_point_count(&path), 3);
    }

    #[test]
    fn writes_dataset_rid_to_avro_user_metadata() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();
        let rid = ResourceIdentifier::new("ri.catalog.main.dataset.abc123").unwrap();

        write_integer_points_with(&path, 1, true, Some(rid.clone()));

        let stored = read_dataset_rid_metadata(&path).expect("dataset_rid metadata missing");
        assert_eq!(stored, rid.to_string());
    }

    #[test]
    fn omits_dataset_rid_metadata_when_none() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        write_integer_points_with(&path, 1, true, None);

        assert!(read_dataset_rid_metadata(&path).is_none());
    }

    fn write_integer_points(path: &PathBuf, count: i64) {
        write_integer_points_with(path, count, true, None);
    }

    fn write_integer_points_with(
        path: &PathBuf,
        count: i64,
        overwrite: bool,
        dataset_rid: Option<ResourceIdentifier>,
    ) {
        let points = (0..count)
            .map(
                |i| nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                    timestamp: make_timestamp(i, 0),
                    value: i,
                },
            )
            .collect();
        let consumer = AvroFileConsumer::new_with_full_path(path, overwrite, dataset_rid).unwrap();
        consumer
            .append_series(&[make_series(
                "ch",
                Points {
                    points_type: Some(PointsType::IntegerPoints(IntegerPoints { points })),
                },
            )])
            .unwrap();
        // Consumer drops at end of scope, flushing the avro writer to disk.
    }

    fn read_integer_point_count(path: &PathBuf) -> usize {
        let reader = Reader::new(std::fs::File::open(path).unwrap()).unwrap();
        let mut total = 0;
        for record in reader {
            let Value::Record(fields) = record.unwrap() else {
                panic!("expected Record");
            };
            let timestamps = fields
                .iter()
                .find(|(name, _)| name == "timestamps")
                .map(|(_, v)| v)
                .unwrap();
            if let Value::Array(arr) = timestamps {
                total += arr.len();
            }
        }
        total
    }

    fn read_dataset_rid_metadata(path: &PathBuf) -> Option<String> {
        let file = std::fs::File::open(path).unwrap();
        let reader = Reader::new(file).unwrap();
        reader
            .user_metadata()
            .get(DATASET_RID_METADATA_KEY)
            .map(|bytes| String::from_utf8(bytes.clone()).unwrap())
    }
}
