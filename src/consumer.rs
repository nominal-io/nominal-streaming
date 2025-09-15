use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use apache_avro::types::Record;
use apache_avro::types::Value;
use conjure_object::ResourceIdentifier;
use log::info;
use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use parking_lot::Mutex;
use prost::Message;
use rand::Rng;
use tracing::warn;

use crate::client::NominalApiClients;
use crate::client::StreamingClient;
use crate::client::{self};
use crate::monitor::StreamHealthMonitor;
use crate::notifier::NominalStreamListener;
use crate::stream::AuthProvider;
use crate::upload::UploadManager;
use crate::upload::UploaderOpts;

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("avro error: {0}")]
    AvroError(#[from] Box<apache_avro::Error>),
    #[error("no token provided")]
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

pub trait WriteRequestConsumerFactory: Send + Sync {
    type Consumer: WriteRequestConsumer;
    fn create_consumer(&self, id: usize) -> Result<Self::Consumer, Box<dyn Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct NominalCoreConsumer<A: AuthProvider> {
    client: StreamingClient,
    handle: tokio::runtime::Handle,
    auth_provider: A,
    data_source_rid: ResourceIdentifier,
}

impl<A: AuthProvider> NominalCoreConsumer<A> {
    pub fn new(
        client: StreamingClient,
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

impl<A: AuthProvider> Debug for NominalCoreConsumer<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalCoreConsumer")
            .field("client", &self.client)
            .field("data_source_rid", &self.data_source_rid)
            .finish()
    }
}

impl<A: AuthProvider + 'static> WriteRequestConsumer for NominalCoreConsumer<A> {
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

impl<P, F> WriteRequestConsumer for RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer + Send + Sync,
    F: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        if self.primary.consume(request).is_err() {
            warn!("Sending request to primary consumer failed, trying fallback consumer");
            return self.fallback.consume(request);
        }
        Ok(())
    }
}

const DEFAULT_FILE_PREFIX: &str = "nominal_stream";

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
          "type": {"type": "array", "items": ["double", "string"]},
          "doc": "Array of values. Can either be doubles or strings"
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

#[derive(Debug, Clone)]
pub enum AvroPathConfig {
    Template {
        directory: PathBuf,
        file_prefix: String,
    },
    Raw(PathBuf),
}

#[derive(Clone)]
pub struct AvroFileConsumer {
    writer: Arc<Mutex<apache_avro::Writer<'static, std::fs::File>>>,
    path_config: AvroPathConfig,
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
    ) -> std::io::Result<Self> {
        let prefix = file_prefix.unwrap_or_else(|| DEFAULT_FILE_PREFIX.to_string());
        let path_config = AvroPathConfig::Template {
            directory: directory.into(),
            file_prefix: prefix,
        };

        Self::new_with_path_config(path_config)
    }

    pub fn new_with_full_path(file_path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = file_path.into();
        let path_config = AvroPathConfig::Raw(path.clone());
        Self::new_with_path_config(path_config)
    }

    pub fn new_with_path_config(path_config: AvroPathConfig) -> std::io::Result<Self> {
        let path = match &path_config {
            AvroPathConfig::Template {
                directory,
                file_prefix,
            } => {
                let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
                let filename = format!("{file_prefix}_{datetime}.avro");
                directory.join(filename)
            }
            AvroPathConfig::Raw(path) => path.clone(),
        };

        std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;

        let writer = apache_avro::Writer::builder()
            .schema(&CORE_AVRO_SCHEMA)
            .writer(file)
            .codec(apache_avro::Codec::Snappy)
            .build();

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            path_config,
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

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    // rotates file the consumer points to. uses a timestamped filename
    // returns old file name
    pub fn rotate_file(&mut self) -> std::io::Result<PathBuf> {
        let new_path = self.generate_timestamped_path();
        self.rotate_file_with(new_path)
    }

    // rotates file the consumer points to. uses a custom path
    pub fn rotate_file_with(&mut self, new_path: impl Into<PathBuf>) -> std::io::Result<PathBuf> {
        let new_path = new_path.into();
        std::fs::create_dir_all(new_path.parent().unwrap_or(&new_path))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&new_path)?;

        let new_writer = apache_avro::Writer::builder()
            .schema(&CORE_AVRO_SCHEMA)
            .writer(file)
            .codec(apache_avro::Codec::Snappy)
            .build();

        *self.writer.lock() = new_writer;
        let old_path = self.path.clone();

        self.path = new_path;
        Ok(old_path)
    }

    fn generate_timestamped_path(&self) -> PathBuf {
        match &self.path_config {
            AvroPathConfig::Template {
                directory,
                file_prefix,
            } => {
                let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
                let filename = format!("{file_prefix}_{datetime}.avro");
                directory.join(filename)
            }
            AvroPathConfig::Raw(path) => {
                let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
                let filename = format!(
                    "{}_{}.avro",
                    path.file_stem().unwrap().to_str().unwrap(),
                    datetime
                );
                path.with_file_name(filename)
            }
        }
    }
}

impl WriteRequestConsumer for AvroFileConsumer {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        self.append_series(&request.series)?;
        Ok(())
    }
}

fn points_to_avro(points: Option<&Points>) -> (Vec<Value>, Vec<Value>) {
    match points {
        Some(Points {
            points_type: Some(PointsType::DoublePoints(DoublePoints { points })),
        }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(0, Box::new(Value::Double(point.value))),
                )
            })
            .collect(),
        Some(Points {
            points_type: Some(PointsType::StringPoints(StringPoints { points })),
        }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(1, Box::new(Value::String(point.value.clone()))),
                )
            })
            .collect(),
        _ => (Vec::new(), Vec::new()),
    }
}

fn convert_timestamp_to_nanoseconds(timestamp: Timestamp) -> Value {
    Value::Long(timestamp.seconds * 1_000_000_000 + timestamp.nanos as i64)
}

// factory for timestamped and id-ed AvroFileConsumer instances
#[derive(Clone, Debug)]
pub struct AvroFileConsumerFactory {
    directory: PathBuf,
    file_prefix: String,
}

impl AvroFileConsumerFactory {
    pub fn new(directory: impl Into<PathBuf>, file_prefix: Option<String>) -> Self {
        Self {
            directory: directory.into(),
            file_prefix: file_prefix.unwrap_or_else(|| DEFAULT_FILE_PREFIX.to_string()),
        }
    }
}

impl WriteRequestConsumerFactory for AvroFileConsumerFactory {
    type Consumer = AvroFileConsumer;

    fn create_consumer(&self, id: usize) -> Result<Self::Consumer, Box<dyn Error + Send + Sync>> {
        let file_prefix = format!("{}_{}", self.file_prefix, id);
        let consumer = AvroFileConsumer::new(self.directory.clone(), Some(file_prefix))?;
        Ok(consumer)
    }
}

#[derive(Debug, Clone)]
pub struct ReuploadOpts {
    pub idle_threshold: std::time::Duration,
    pub time_since_last_failure_threshold: std::time::Duration,
    pub reupload_interval: std::time::Duration,
    pub upload_opts: UploaderOpts,
}

impl Default for ReuploadOpts {
    fn default() -> Self {
        Self {
            idle_threshold: std::time::Duration::from_secs(1),
            time_since_last_failure_threshold: std::time::Duration::from_secs(2),
            reupload_interval: std::time::Duration::from_millis(500),
            upload_opts: UploaderOpts::default(),
        }
    }
}

// Consumer that first attempts to stream points to Nominal Core
// and falls back to writing them to files if the streaming fails.
// at stream idle times, it attempts reuploads of the files to Nominal Core,
// removing them from disk if successful.
#[derive(Clone)]
pub struct StoreAndForwardNominalCoreConsumer<A: AuthProvider> {
    core_consumer: NominalCoreConsumer<A>,
    fallback_consumer: AvroFileConsumer,
    stream_monitor: Arc<StreamHealthMonitor>,
    handle: tokio::runtime::Handle,
    simulated_success_rate: Option<f64>,
}

impl<A: AuthProvider + 'static> Debug for StoreAndForwardNominalCoreConsumer<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreAndForwardNominalCoreConsumer")
            .field("core_consumer", &self.core_consumer)
            .field("fallback_consumer", &self.fallback_consumer)
            .finish()
    }
}

// factory for ReuploadingNominalCoreConsumer instances
// creates unique AvroFileConsumer instances for each consumer
// for more optimal performance.
pub struct StoreAndForwardNominalCoreConsumerFactory<A: AuthProvider> {
    clients: NominalApiClients,
    core_consumer: NominalCoreConsumer<A>,
    fallback_consumer_factory: AvroFileConsumerFactory,
    auth_provider: A,
    handle: tokio::runtime::Handle,
    reupload_opts: ReuploadOpts,
    simulated_success_rate: Option<f64>,
}

impl<A: AuthProvider> StoreAndForwardNominalCoreConsumerFactory<A> {
    pub fn new(
        clients: NominalApiClients,
        handle: tokio::runtime::Handle,
        auth_provider: A,
        data_source_rid: ResourceIdentifier,
        fallback_directory: impl Into<PathBuf>,
        fallback_file_prefix: Option<String>,
        reupload_opts: ReuploadOpts,
    ) -> Self {
        Self::new_with_success_rate(
            clients,
            handle,
            auth_provider,
            data_source_rid,
            fallback_directory,
            fallback_file_prefix,
            reupload_opts,
            None,
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new_with_success_rate(
        clients: NominalApiClients,
        handle: tokio::runtime::Handle,
        auth_provider: A,
        data_source_rid: ResourceIdentifier,
        fallback_directory: impl Into<PathBuf>,
        fallback_file_prefix: Option<String>,
        reupload_opts: ReuploadOpts,
        simulated_success_rate: Option<f64>,
    ) -> Self {
        let core_consumer = NominalCoreConsumer::new(
            clients.streaming.clone(),
            handle.clone(),
            auth_provider.clone(),
            data_source_rid,
        );
        let fallback_consumer_factory =
            AvroFileConsumerFactory::new(fallback_directory, fallback_file_prefix);

        Self {
            clients,
            core_consumer,
            fallback_consumer_factory,
            auth_provider,
            handle,
            reupload_opts,
            simulated_success_rate,
        }
    }
}

impl<A: AuthProvider + 'static> WriteRequestConsumerFactory
    for StoreAndForwardNominalCoreConsumerFactory<A>
{
    type Consumer = StoreAndForwardNominalCoreConsumer<A>;

    fn create_consumer(&self, id: usize) -> Result<Self::Consumer, Box<dyn Error + Send + Sync>> {
        Ok(StoreAndForwardNominalCoreConsumer::new_with_success_rate(
            self.clients.clone(),
            self.core_consumer.clone(),
            self.fallback_consumer_factory.create_consumer(id)?,
            self.auth_provider.clone(),
            self.handle.clone(),
            self.reupload_opts.clone(),
            self.simulated_success_rate,
        ))
    }
}

impl<T: AuthProvider + 'static> WriteRequestConsumer for StoreAndForwardNominalCoreConsumer<T> {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        if let Some(simulated_success_rate) = self.simulated_success_rate {
            let mut rng = rand::thread_rng();
            let random_value: f64 = rng.gen_range(0.0..1.0);
            if random_value > simulated_success_rate {
                warn!("Simulating failure, falling back to file storage");
                self.stream_monitor.record_failure();
                return self.fallback_consumer.consume(request);
            }
        }
        match self.core_consumer.consume(request) {
            Ok(_) => {
                self.stream_monitor.record_success();
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Primary consumer failed: {}, falling back to file storage",
                    e
                );
                self.stream_monitor.record_failure();
                self.fallback_consumer.consume(request)
            }
        }
    }
}

impl<A: AuthProvider + 'static> StoreAndForwardNominalCoreConsumer<A> {
    pub fn new(
        clients: NominalApiClients,
        core_consumer: NominalCoreConsumer<A>,
        fallback_consumer: AvroFileConsumer,
        auth_provider: A,
        handle: tokio::runtime::Handle,
        reupload_opts: ReuploadOpts,
    ) -> Self {
        Self::new_with_success_rate(
            clients,
            core_consumer,
            fallback_consumer,
            auth_provider,
            handle,
            reupload_opts,
            None,
        )
    }

    pub fn new_with_success_rate(
        clients: NominalApiClients,
        core_consumer: NominalCoreConsumer<A>,
        fallback_consumer: AvroFileConsumer,
        auth_provider: A,
        handle: tokio::runtime::Handle,
        reupload_opts: ReuploadOpts,
        simulated_success_rate: Option<f64>,
    ) -> Self {
        let stream_monitor = Arc::new(StreamHealthMonitor::new());
        let (file_tx, file_rx) = async_channel::bounded::<PathBuf>(5);
        let upload_manager = UploadManager::new(
            clients.clone(),
            reqwest::Client::new(),
            handle.clone(),
            reupload_opts.upload_opts.clone(),
            file_rx,
            auth_provider,
            core_consumer.data_source_rid.clone(),
        );
        let consumer = Self {
            core_consumer,
            fallback_consumer,
            stream_monitor,
            handle,
            simulated_success_rate,
        };

        consumer.start_reupload_task(file_tx, upload_manager, reupload_opts);

        consumer
    }

    pub fn start_reupload_task(
        &self,
        file_tx: async_channel::Sender<PathBuf>,
        upload_manager: UploadManager,
        reupload_opts: ReuploadOpts,
    ) {
        let handle = self.handle.clone();
        let mut fallback_consumer = self.fallback_consumer.clone();
        let stream_monitor = self.stream_monitor.clone();

        handle.spawn(async move {
            loop {
                if stream_monitor.is_idle(reupload_opts.idle_threshold)
                    && stream_monitor
                        .requests_failed
                        .load(std::sync::atomic::Ordering::Relaxed)
                        > 0
                    && !stream_monitor
                        .has_recent_failure(reupload_opts.time_since_last_failure_threshold)
                {
                    warn!("Stream is idle, reuploading data from file storage");
                    let path = fallback_consumer.rotate_file().unwrap();
                    if !upload_manager.upload_queue.is_full() {
                        if let Err(e) = file_tx.send(path).await {
                            warn!("Failed to send file path for reupload: {e}");
                        } else {
                            stream_monitor.reset();
                            info!("File path queued for reupload");
                        }
                    }
                }
                tokio::time::sleep(reupload_opts.reupload_interval).await;
            }
        });
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
        let len = request.series.len();
        match self.consumer.consume(request) {
            Ok(_) => Ok(()),
            Err(e) => {
                let message = format!("Failed to consume request of {len} series");

                for listener in &self.listeners {
                    listener.on_error(&message, &e);
                }

                Err(e)
            }
        }
    }
}
