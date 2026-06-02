use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use conjure_object::SafeLong;
use nominal_api::clients::storage::writer::api::AsyncNominalChannelWriterService;
use nominal_api::objects::api::rids::NominalDataSourceOrDatasetRid;
use nominal_api::objects::api::Channel as CoreChannel;
use nominal_api::objects::api::Timestamp as CoreTimestamp;
use nominal_api::objects::storage::writer::api::LogPoint as CoreLogPoint;
use nominal_api::objects::storage::writer::api::LogValue as CoreLogValue;
use nominal_api::objects::storage::writer::api::WriteLogsRequest;
use nominal_api::tonic::google::protobuf::Timestamp;
use parking_lot::Mutex;
use tracing::debug;
use tracing::error;
use tracing::warn;

use crate::client::NominalApiClients;
use crate::client::PRODUCTION_API_URL;
use crate::consumer::ConsumerError;
use crate::consumer::ConsumerResult;
use crate::types::AuthProvider;
use crate::types::IntoTimestamp;

const DEFAULT_LOG_CHANNEL: &str = "logs";
const DEFAULT_LOG_FILE_PREFIX: &str = "nominal_logs";

#[derive(Debug, Clone)]
pub struct NominalLogStreamOpts {
    pub max_logs_per_request: usize,
    pub max_request_delay: Duration,
    pub max_buffered_entries: usize,
    pub max_buffered_requests: usize,
    pub request_dispatcher_tasks: usize,
    pub base_api_url: String,
}

impl Default for NominalLogStreamOpts {
    fn default() -> Self {
        Self {
            max_logs_per_request: 50_000,
            max_request_delay: Duration::from_millis(100),
            max_buffered_entries: 50_000,
            max_buffered_requests: 4,
            request_dispatcher_tasks: 2,
            base_api_url: PRODUCTION_API_URL.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub channel: String,
    pub timestamp: Timestamp,
    pub message: String,
    pub args: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
}

impl LogEntry {
    pub fn new(
        channel: impl Into<String>,
        timestamp: impl IntoTimestamp,
        message: impl Into<String>,
    ) -> Self {
        Self {
            channel: channel.into(),
            timestamp: timestamp.into_timestamp(),
            message: message.into(),
            args: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    pub fn with_default_channel(timestamp: impl IntoTimestamp, message: impl Into<String>) -> Self {
        Self::new(DEFAULT_LOG_CHANNEL, timestamp, message)
    }

    pub fn with_args(
        mut self,
        args: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.args = args
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }

    pub fn with_tags(
        mut self,
        tags: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.tags = tags
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }
}

pub trait LogConsumer: Send + Sync + Debug {
    fn consume(&self, entries: &[LogEntry]) -> ConsumerResult<()>;
}

#[derive(Clone)]
pub struct NominalCoreLogConsumer<A: AuthProvider> {
    client: NominalApiClients,
    handle: tokio::runtime::Handle,
    auth_provider: A,
    data_source_rid: ResourceIdentifier,
}

impl<A: AuthProvider> NominalCoreLogConsumer<A> {
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

impl<T: AuthProvider> Debug for NominalCoreLogConsumer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalCoreLogConsumer")
            .field("client", &self.client)
            .field("data_source_rid", &self.data_source_rid)
            .finish()
    }
}

impl<T: AuthProvider + 'static> LogConsumer for NominalCoreLogConsumer<T> {
    fn consume(&self, entries: &[LogEntry]) -> ConsumerResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let token = self
            .auth_provider
            .token()
            .ok_or(ConsumerError::MissingTokenError)?;
        let requests = entries_to_write_logs_requests(entries)?;
        let data_source_rid = NominalDataSourceOrDatasetRid(self.data_source_rid.clone());
        self.handle.block_on(async {
            for request in requests {
                self.client
                    .writer
                    .write_logs(&token, &data_source_rid, &request)
                    .await
                    .map_err(|e| ConsumerError::RequestError(format!("{e:?}")))?;
            }
            Ok::<(), ConsumerError>(())
        })?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct JsonlLogFileConsumer {
    writer: Arc<Mutex<BufWriter<std::fs::File>>>,
    path: PathBuf,
    data_source_rid: Option<ResourceIdentifier>,
}

impl Debug for JsonlLogFileConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonlLogFileConsumer")
            .field("path", &self.path)
            .field("data_source_rid", &self.data_source_rid)
            .finish()
    }
}

impl JsonlLogFileConsumer {
    pub fn new(
        directory: impl Into<PathBuf>,
        file_prefix: Option<String>,
        data_source_rid: Option<ResourceIdentifier>,
    ) -> std::io::Result<Self> {
        let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let prefix = file_prefix.unwrap_or_else(|| DEFAULT_LOG_FILE_PREFIX.to_string());
        let filename = format!("{prefix}_{datetime}.jsonl");
        let directory = directory.into();
        let full_path = directory.join(&filename);

        Self::new_with_full_path(full_path, true, data_source_rid)
    }

    pub fn new_with_full_path(
        file_path: impl Into<PathBuf>,
        overwrite: bool,
        data_source_rid: Option<ResourceIdentifier>,
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

        Ok(Self {
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
            path,
            data_source_rid,
        })
    }

    fn append_entries(&self, entries: &[LogEntry]) -> ConsumerResult<()> {
        let mut writer = self.writer.lock();
        for entry in entries {
            let mut json_entry = serde_json::json!({
                "channel": entry.channel,
                "timestamp": {
                    "seconds": entry.timestamp.seconds,
                    "nanos": entry.timestamp.nanos,
                },
                "message": entry.message,
                "args": entry.args,
                "tags": entry.tags,
            });

            if let Some(data_source_rid) = &self.data_source_rid {
                json_entry["dataSourceRid"] = serde_json::json!(data_source_rid.to_string());
            }

            serde_json::to_writer(&mut *writer, &json_entry)
                .map_err(|e| ConsumerError::IoError(std::io::Error::other(e)))?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }
}

impl LogConsumer for JsonlLogFileConsumer {
    fn consume(&self, entries: &[LogEntry]) -> ConsumerResult<()> {
        self.append_entries(entries)
    }
}

impl Drop for JsonlLogFileConsumer {
    fn drop(&mut self) {
        if let Err(e) = self.writer.lock().flush() {
            warn!(
                "failed to flush jsonl log writer for {:?} on drop: {e:?}",
                self.path
            );
        }
    }
}

#[derive(Clone)]
struct LogConsumerWithFallback<P, F>
where
    P: LogConsumer,
    F: LogConsumer,
{
    primary: P,
    fallback: F,
}

impl<P, F> LogConsumerWithFallback<P, F>
where
    P: LogConsumer,
    F: LogConsumer,
{
    fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

impl<P, F> Debug for LogConsumerWithFallback<P, F>
where
    P: LogConsumer,
    F: LogConsumer,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogConsumerWithFallback")
            .field("primary", &self.primary)
            .field("fallback", &self.fallback)
            .finish()
    }
}

impl<P, F> LogConsumer for LogConsumerWithFallback<P, F>
where
    P: LogConsumer,
    F: LogConsumer,
{
    fn consume(&self, entries: &[LogEntry]) -> ConsumerResult<()> {
        if let Err(e) = self.primary.consume(entries) {
            warn!("Sending logs to primary consumer failed. Attempting fallback.");
            let fallback_result = self.fallback.consume(entries);
            if let ConsumerError::MissingTokenError = e {
                return Err(ConsumerError::MissingTokenError);
            }
            return fallback_result;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct DualLogConsumer<P, S>
where
    P: LogConsumer,
    S: LogConsumer,
{
    primary: P,
    secondary: S,
}

impl<P, S> DualLogConsumer<P, S>
where
    P: LogConsumer,
    S: LogConsumer,
{
    fn new(primary: P, secondary: S) -> Self {
        Self { primary, secondary }
    }
}

impl<P, S> Debug for DualLogConsumer<P, S>
where
    P: LogConsumer,
    S: LogConsumer,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualLogConsumer")
            .field("primary", &self.primary)
            .field("secondary", &self.secondary)
            .finish()
    }
}

impl<P, S> LogConsumer for DualLogConsumer<P, S>
where
    P: LogConsumer,
    S: LogConsumer,
{
    fn consume(&self, entries: &[LogEntry]) -> ConsumerResult<()> {
        let primary_result = self.primary.consume(entries);
        let secondary_result = self.secondary.consume(entries);
        if let Err(e) = &primary_result {
            warn!("Sending logs to primary consumer failed: {:?}", e);
        }
        if let Err(e) = &secondary_result {
            warn!("Sending logs to secondary consumer failed: {:?}", e);
        }

        primary_result.and(secondary_result)
    }
}

#[derive(Default)]
pub struct NominalLogStreamBuilder {
    stream_to_core: Option<(BearerToken, ResourceIdentifier, tokio::runtime::Handle)>,
    stream_to_file: Option<PathBuf>,
    file_fallback: Option<PathBuf>,
    opts: NominalLogStreamOpts,
}

impl Debug for NominalLogStreamBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalLogStreamBuilder")
            .field("stream_to_core", &self.stream_to_core.is_some())
            .field("stream_to_file", &self.stream_to_file)
            .field("file_fallback", &self.file_fallback)
            .finish()
    }
}

impl NominalLogStreamBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stream_to_core(
        self,
        bearer_token: BearerToken,
        data_source_rid: ResourceIdentifier,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            stream_to_core: Some((bearer_token, data_source_rid, handle)),
            stream_to_file: self.stream_to_file,
            file_fallback: self.file_fallback,
            opts: self.opts,
        }
    }

    pub fn stream_to_file(mut self, file_path: impl Into<PathBuf>) -> Self {
        self.stream_to_file = Some(file_path.into());
        self
    }

    pub fn with_file_fallback(mut self, file_path: impl Into<PathBuf>) -> Self {
        self.file_fallback = Some(file_path.into());
        self
    }

    pub fn with_options(mut self, opts: NominalLogStreamOpts) -> Self {
        self.opts = opts;
        self
    }

    pub fn build(self) -> NominalLogStream {
        let core_consumer = self.core_consumer();
        let file_consumer = self.file_consumer();
        let fallback_consumer = self.fallback_consumer();

        match (core_consumer, file_consumer, fallback_consumer) {
            (None, None, _) => panic!("nominal log stream must either stream to file or core"),
            (Some(_), Some(_), Some(_)) => {
                panic!("must choose one of stream_to_file and file_fallback when streaming to core")
            }
            (Some(core), None, None) => self.into_stream(core),
            (Some(core), None, Some(fallback)) => {
                self.into_stream(LogConsumerWithFallback::new(core, fallback))
            }
            (None, Some(file), None) => self.into_stream(file),
            (None, Some(file), Some(fallback)) => {
                self.into_stream(LogConsumerWithFallback::new(file, fallback))
            }
            (Some(core), Some(file), None) => self.into_stream(DualLogConsumer::new(core, file)),
        }
    }

    fn core_consumer(&self) -> Option<NominalCoreLogConsumer<BearerToken>> {
        self.stream_to_core
            .as_ref()
            .map(|(auth_provider, data_source_rid, handle)| {
                NominalCoreLogConsumer::new(
                    NominalApiClients::from_uri(self.opts.base_api_url.as_str()),
                    handle.clone(),
                    auth_provider.clone(),
                    data_source_rid.clone(),
                )
            })
    }

    fn data_source_rid(&self) -> Option<ResourceIdentifier> {
        self.stream_to_core.as_ref().map(|(_, rid, _)| rid.clone())
    }

    fn file_consumer(&self) -> Option<JsonlLogFileConsumer> {
        self.stream_to_file.as_ref().map(|path| {
            JsonlLogFileConsumer::new_with_full_path(path, true, self.data_source_rid()).unwrap()
        })
    }

    fn fallback_consumer(&self) -> Option<JsonlLogFileConsumer> {
        self.file_fallback.as_ref().map(|path| {
            JsonlLogFileConsumer::new_with_full_path(path, true, self.data_source_rid()).unwrap()
        })
    }

    fn into_stream<C: LogConsumer + 'static>(self, consumer: C) -> NominalLogStream {
        NominalLogStream::new_with_consumer(consumer, self.opts)
    }
}

pub struct NominalLogStream {
    entry_tx: Option<crossbeam_channel::Sender<LogEntry>>,
    batcher_handle: Option<thread::JoinHandle<()>>,
    dispatcher_handles: Vec<thread::JoinHandle<()>>,
}

impl NominalLogStream {
    pub fn builder() -> NominalLogStreamBuilder {
        NominalLogStreamBuilder::new()
    }

    pub fn new_with_consumer<C: LogConsumer + 'static>(
        consumer: C,
        opts: NominalLogStreamOpts,
    ) -> Self {
        let (entry_tx, entry_rx) =
            crossbeam_channel::bounded::<LogEntry>(opts.max_buffered_entries);
        let (request_tx, request_rx) =
            crossbeam_channel::bounded::<Vec<LogEntry>>(opts.max_buffered_requests);

        let batcher_handle = thread::Builder::new()
            .name("nmlog_batcher".to_string())
            .spawn(move || {
                log_batcher(
                    entry_rx,
                    request_tx,
                    opts.max_logs_per_request,
                    opts.max_request_delay,
                );
            })
            .unwrap();

        let consumer = Arc::new(consumer);
        let mut dispatcher_handles = Vec::new();
        for i in 0..opts.request_dispatcher_tasks {
            dispatcher_handles.push(
                thread::Builder::new()
                    .name(format!("nmlog_dispatch_{i}"))
                    .spawn({
                        let rx = request_rx.clone();
                        let consumer = Arc::clone(&consumer);
                        move || log_request_dispatcher(rx, consumer)
                    })
                    .unwrap(),
            );
        }

        Self {
            entry_tx: Some(entry_tx),
            batcher_handle: Some(batcher_handle),
            dispatcher_handles,
        }
    }

    pub fn log(
        &self,
        channel: impl Into<String>,
        timestamp: impl IntoTimestamp,
        message: impl Into<String>,
        args: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        tags: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) {
        self.log_entry(
            LogEntry::new(channel, timestamp, message)
                .with_args(args)
                .with_tags(tags),
        );
    }

    pub fn log_entry(&self, entry: LogEntry) {
        self.entry_tx
            .as_ref()
            .expect("nominal log stream is closed")
            .send(entry)
            .expect("nominal log stream worker exited");
    }
}

impl Drop for NominalLogStream {
    fn drop(&mut self) {
        debug!("starting drop for NominalLogStream");
        drop(self.entry_tx.take());

        if let Some(handle) = self.batcher_handle.take() {
            if let Err(e) = handle.join() {
                warn!("nominal log batcher thread panicked: {e:?}");
            }
        }

        for handle in std::mem::take(&mut self.dispatcher_handles) {
            if let Err(e) = handle.join() {
                warn!("nominal log dispatcher thread panicked: {e:?}");
            }
        }
    }
}

fn log_batcher(
    entry_rx: crossbeam_channel::Receiver<LogEntry>,
    request_tx: crossbeam_channel::Sender<Vec<LogEntry>>,
    max_logs_per_request: usize,
    max_request_delay: Duration,
) {
    let mut batch = Vec::with_capacity(max_logs_per_request);
    loop {
        match entry_rx.recv_timeout(max_request_delay) {
            Ok(entry) => {
                batch.push(entry);
                if batch.len() >= max_logs_per_request {
                    flush_log_batch(&mut batch, &request_tx);
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                flush_log_batch(&mut batch, &request_tx);
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                flush_log_batch(&mut batch, &request_tx);
                break;
            }
        }
    }
    debug!("nominal log batcher thread exiting");
}

fn flush_log_batch(
    batch: &mut Vec<LogEntry>,
    request_tx: &crossbeam_channel::Sender<Vec<LogEntry>>,
) {
    if batch.is_empty() {
        return;
    }

    let entries = std::mem::take(batch);
    if let Err(e) = request_tx.send(entries) {
        error!("failed to send log batch to dispatcher: {e}");
    }
}

fn log_request_dispatcher<C: LogConsumer + 'static>(
    request_rx: crossbeam_channel::Receiver<Vec<LogEntry>>,
    consumer: Arc<C>,
) {
    loop {
        match request_rx.recv() {
            Ok(entries) => {
                debug!("received log batch from channel");
                if let Err(e) = consumer.consume(&entries) {
                    error!("failed to send log batch: {e:?}");
                }
            }
            Err(e) => {
                debug!("log request channel closed, exiting dispatcher thread. info: '{e}'");
                break;
            }
        }
    }
    debug!("log request dispatcher thread exiting");
}

fn entries_to_write_logs_requests(entries: &[LogEntry]) -> ConsumerResult<Vec<WriteLogsRequest>> {
    let mut groups: BTreeMap<String, Vec<&LogEntry>> = BTreeMap::new();
    for entry in entries {
        groups.entry(entry.channel.clone()).or_default().push(entry);
    }

    groups
        .into_iter()
        .map(|(channel, entries)| {
            let logs = entries
                .into_iter()
                .map(entry_to_log_point)
                .collect::<ConsumerResult<Vec<_>>>()?;
            Ok(WriteLogsRequest::builder()
                .logs(logs)
                .channel(CoreChannel::from(channel))
                .build())
        })
        .collect()
}

fn entry_to_log_point(entry: &LogEntry) -> ConsumerResult<CoreLogPoint> {
    let mut args = entry.args.clone();
    args.extend(entry.tags.clone());

    Ok(CoreLogPoint::new(
        timestamp_to_core(entry.timestamp)?,
        CoreLogValue::builder()
            .message(entry.message.clone())
            .args(args)
            .build(),
    ))
}

fn timestamp_to_core(timestamp: Timestamp) -> ConsumerResult<CoreTimestamp> {
    let seconds = SafeLong::new(timestamp.seconds).map_err(|_| {
        ConsumerError::RequestError(format!(
            "timestamp seconds value {} exceeds safe integer range",
            timestamp.seconds
        ))
    })?;
    let nanos = SafeLong::new(timestamp.nanos as i64).map_err(|_| {
        ConsumerError::RequestError(format!(
            "timestamp nanos value {} exceeds safe integer range",
            timestamp.nanos
        ))
    })?;
    Ok(CoreTimestamp::new(seconds, nanos))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Mutex as StdMutex;

    use tempfile::NamedTempFile;

    use super::*;

    #[derive(Debug)]
    struct FailingLogConsumer;

    impl LogConsumer for FailingLogConsumer {
        fn consume(&self, _entries: &[LogEntry]) -> ConsumerResult<()> {
            Err(ConsumerError::RequestError("boom".to_string()))
        }
    }

    #[derive(Debug)]
    struct CollectingLogConsumer {
        entries: StdMutex<Vec<LogEntry>>,
    }

    impl LogConsumer for Arc<CollectingLogConsumer> {
        fn consume(&self, entries: &[LogEntry]) -> ConsumerResult<()> {
            self.entries.lock().unwrap().extend_from_slice(entries);
            Ok(())
        }
    }

    fn timestamp() -> Timestamp {
        Timestamp {
            seconds: 123,
            nanos: 456,
        }
    }

    fn log_entry(message: &str) -> LogEntry {
        LogEntry::new("events", timestamp(), message)
            .with_args([("phase", "boost"), ("attempt", "2")])
            .with_tags([("vehicle", "alpha"), ("run", "17")])
    }

    #[test]
    fn builds_write_logs_requests_with_args_and_tags() {
        let requests =
            entries_to_write_logs_requests(&[log_entry("ignition"), log_entry("liftoff")]).unwrap();

        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.channel().unwrap().as_ref(), "events");
        assert_eq!(request.logs().len(), 2);

        let point = &request.logs()[0];
        assert_eq!(*point.timestamp().seconds(), 123);
        assert_eq!(*point.timestamp().nanos(), 456);

        let value = point.value();
        assert_eq!(value.message(), "ignition");
        assert_eq!(value.args().get("phase").unwrap(), "boost");
        assert_eq!(value.args().get("attempt").unwrap(), "2");
        assert_eq!(value.args().get("vehicle").unwrap(), "alpha");
        assert_eq!(value.args().get("run").unwrap(), "17");
    }

    #[test]
    fn writes_jsonl_fallback_with_args_tags_and_dataset_rid() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path = tmp_file.path().to_path_buf();
        let rid = ResourceIdentifier::new("ri.catalog.main.dataset.logs").unwrap();
        let consumer = JsonlLogFileConsumer::new_with_full_path(&path, true, Some(rid.clone()))
            .expect("create jsonl consumer");

        consumer.consume(&[log_entry("ignition")]).unwrap();
        drop(consumer);

        let contents = fs::read_to_string(&path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);

        let value: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(value["dataSourceRid"], rid.to_string());
        assert_eq!(value["channel"], "events");
        assert_eq!(value["timestamp"]["seconds"], 123);
        assert_eq!(value["timestamp"]["nanos"], 456);
        assert_eq!(value["message"], "ignition");
        assert_eq!(value["args"]["phase"], "boost");
        assert_eq!(value["tags"]["vehicle"], "alpha");
    }

    #[test]
    fn writes_jsonl_when_primary_consumer_fails() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path = tmp_file.path().to_path_buf();
        let fallback =
            JsonlLogFileConsumer::new_with_full_path(&path, true, None).expect("fallback");
        let consumer = LogConsumerWithFallback::new(FailingLogConsumer, fallback);

        consumer.consume(&[log_entry("fallback")]).unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        let value: serde_json::Value =
            serde_json::from_str(contents.lines().next().unwrap()).expect("jsonl line");
        assert_eq!(value["message"], "fallback");
    }

    #[test]
    fn stream_flushes_logs_on_drop() {
        let collector = Arc::new(CollectingLogConsumer {
            entries: StdMutex::new(Vec::new()),
        });
        let stream = NominalLogStream::new_with_consumer(
            Arc::clone(&collector),
            NominalLogStreamOpts {
                max_logs_per_request: 10,
                max_request_delay: Duration::from_millis(5),
                max_buffered_entries: 10,
                max_buffered_requests: 1,
                request_dispatcher_tasks: 1,
                base_api_url: PRODUCTION_API_URL.to_string(),
            },
        );

        stream.log(
            "events",
            timestamp(),
            "shutdown",
            [("reason", "nominal")],
            [("run", "17")],
        );
        drop(stream);

        let entries = collector.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "shutdown");
        assert_eq!(entries[0].args.get("reason").unwrap(), "nominal");
        assert_eq!(entries[0].tags.get("run").unwrap(), "17");
    }
}
