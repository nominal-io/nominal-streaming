mod batching;

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "instrument")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use batching::for_each_record;
use batching::points_len;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use parking_lot::Condvar;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use tracing::debug;
use tracing::error;

use crate::client::NominalApiClients;
use crate::client::PRODUCTION_API_URL;
use crate::consumer::checked_call;
use crate::consumer::AvroFileConsumer;
use crate::consumer::ConsumerDelivery;
use crate::consumer::DualWriteRequestConsumer;
use crate::consumer::ListeningWriteRequestConsumer;
use crate::consumer::NominalCoreConsumer;
use crate::consumer::RequestConsumerWithFallback;
use crate::consumer::WriteRequestConsumer;
use crate::listener::LoggingListener;
use crate::types::ChannelDescriptor;
use crate::types::IntoPoints;
use crate::types::IntoTimestamp;

/// Configuration for a [`NominalDatasetStream`].
///
/// Marked `#[non_exhaustive]` so new options can be added without a breaking change.
/// Construct with [`NominalStreamOpts::default`] and customise via the `with_*` methods.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NominalStreamOpts {
    /// Maximum submitted data points per output request. Must be greater than zero.
    /// Oversized buffers are split by the background processor.
    pub max_points_per_record: usize,
    pub max_request_delay: Duration,
    pub max_buffered_requests: usize,
    pub request_dispatcher_tasks: usize,
    pub base_api_url: String,
    /// Emit request runtime metrics from the builder's Core consumer. Disabled by default.
    ///
    /// Completed request metrics piggyback on later data requests to the same dataset.
    /// Pending metrics are bounded and discarded on shutdown; no extra requests are sent.
    /// File-only streams are unaffected.
    /// Configure manually supplied consumers separately.
    pub track_metrics: bool,
    /// Channels the caller emits through the stream that carry metrics rather than data,
    /// beyond the request metrics `track_metrics` emits itself. They are excluded from
    /// request latency measurements when `track_metrics` is enabled.
    pub additional_metric_channels: Vec<String>,
}

impl Default for NominalStreamOpts {
    fn default() -> Self {
        Self {
            max_points_per_record: 250_000,
            max_request_delay: Duration::from_millis(100),
            max_buffered_requests: 4,
            request_dispatcher_tasks: 8,
            base_api_url: PRODUCTION_API_URL.to_string(),
            track_metrics: false,
            additional_metric_channels: Vec::new(),
        }
    }
}

impl NominalStreamOpts {
    pub fn with_max_points_per_record(mut self, max_points_per_record: usize) -> Self {
        self.max_points_per_record = max_points_per_record;
        self
    }

    pub fn with_max_request_delay(mut self, max_request_delay: Duration) -> Self {
        self.max_request_delay = max_request_delay;
        self
    }

    pub fn with_max_buffered_requests(mut self, max_buffered_requests: usize) -> Self {
        self.max_buffered_requests = max_buffered_requests;
        self
    }

    pub fn with_request_dispatcher_tasks(mut self, request_dispatcher_tasks: usize) -> Self {
        self.request_dispatcher_tasks = request_dispatcher_tasks;
        self
    }

    pub fn with_base_api_url(mut self, base_api_url: impl Into<String>) -> Self {
        self.base_api_url = base_api_url.into();
        self
    }

    pub fn with_track_metrics(mut self, track_metrics: bool) -> Self {
        self.track_metrics = track_metrics;
        self
    }

    pub fn with_additional_metric_channels(
        mut self,
        channels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.additional_metric_channels = channels.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Default)]
pub struct NominalDatasetStreamBuilder {
    stream_to_core: Option<(BearerToken, ResourceIdentifier, tokio::runtime::Handle)>,
    stream_to_file: Option<PathBuf>,
    file_fallback: Option<PathBuf>,
    listeners: Vec<Arc<dyn crate::listener::NominalStreamListener>>,
    opts: NominalStreamOpts,
}

impl Debug for NominalDatasetStreamBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalDatasetStreamBuilder")
            .field("stream_to_core", &self.stream_to_core.is_some())
            .field("stream_to_file", &self.stream_to_file)
            .field("file_fallback", &self.file_fallback)
            .field("listeners", &self.listeners.len())
            .finish()
    }
}

impl NominalDatasetStreamBuilder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NominalDatasetStreamBuilder {
    pub fn stream_to_core(
        self,
        bearer_token: BearerToken,
        dataset: ResourceIdentifier,
        handle: tokio::runtime::Handle,
    ) -> NominalDatasetStreamBuilder {
        NominalDatasetStreamBuilder {
            stream_to_core: Some((bearer_token, dataset, handle)),
            stream_to_file: self.stream_to_file,
            file_fallback: self.file_fallback,
            listeners: self.listeners,
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

    pub fn add_listener(
        mut self,
        listener: Arc<dyn crate::listener::NominalStreamListener>,
    ) -> Self {
        self.listeners.push(listener);
        self
    }

    pub fn with_listeners(
        mut self,
        listeners: Vec<Arc<dyn crate::listener::NominalStreamListener>>,
    ) -> Self {
        self.listeners = listeners;
        self
    }

    pub fn with_options(mut self, opts: NominalStreamOpts) -> Self {
        self.opts = opts;
        self
    }

    #[cfg(feature = "logging")]
    fn init_logging(self, directive: Option<&str>) -> Self {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        // Build the filter, either from an explicit directive or the environment.
        let base = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing_subscriber::filter::LevelFilter::DEBUG.into());
        let env_filter = match directive {
            Some(d) => base.parse_lossy(d),
            None => base.from_env_lossy(),
        };

        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_thread_ids(true)
                    .with_thread_names(true)
                    .with_line_number(true),
            )
            .with(env_filter);

        if let Err(error) = subscriber.try_init() {
            eprintln!("nominal streaming failed to enable logging: {error}");
        }

        self
    }

    #[cfg(feature = "logging")]
    pub fn enable_logging(self) -> Self {
        self.init_logging(None)
    }

    #[cfg(feature = "logging")]
    pub fn enable_logging_with_directive(self, log_directive: &str) -> Self {
        self.init_logging(Some(log_directive))
    }

    /// Builds a stream, panicking on invalid configuration or an inaccessible destination.
    /// Prefer [`Self::try_build`] to handle initialization failures.
    pub fn build(self) -> NominalDatasetStream {
        self.try_build().expect("failed to build stream")
    }

    /// Opens destinations before starting workers. Existing fallback files are never overwritten.
    pub fn try_build(self) -> Result<NominalDatasetStream, crate::consumer::ConsumerError> {
        use crate::consumer::ConsumerError;
        if self.stream_to_core.is_none() && self.stream_to_file.is_none() {
            return Err(ConsumerError::Configuration(
                "a Core or file destination is required".into(),
            ));
        }
        if self.stream_to_core.is_some()
            && self.stream_to_file.is_some()
            && self.file_fallback.is_some()
        {
            return Err(ConsumerError::Configuration(
                "choose either a file mirror or file fallback".into(),
            ));
        }
        if self.file_fallback.is_some() && self.stream_to_core.is_none() {
            return Err(ConsumerError::Configuration(
                "file fallback requires a Core destination".into(),
            ));
        }
        if let Some((_, _, _)) = &self.stream_to_core {
            url::Url::parse(&self.opts.base_api_url)
                .map_err(|e| ConsumerError::Configuration(format!("invalid base_api_url: {e}")))?;
        }
        if self.opts.max_points_per_record == 0 || self.opts.request_dispatcher_tasks == 0 {
            return Err(ConsumerError::Configuration(
                "point limit and dispatcher count must be positive".into(),
            ));
        }
        // Open the non-destructive fallback first, before a file-only target can be truncated.
        let fallback_consumer = self.fallback_consumer()?;
        let file_consumer = self.file_consumer()?;
        let core_consumer = self.core_consumer();
        Ok(match (core_consumer, file_consumer, fallback_consumer) {
            (Some(core), None, None) => self.into_stream(core),
            (Some(core), None, Some(fallback)) => {
                self.into_stream(RequestConsumerWithFallback::new(core, fallback))
            }
            (None, Some(file), None) => self.into_stream(file),
            (None, Some(file), Some(fallback)) => {
                self.into_stream(RequestConsumerWithFallback::new(file, fallback))
            }
            (Some(core), Some(file), None) => {
                self.into_stream(DualWriteRequestConsumer::new(core, file))
            }
            _ => unreachable!("destinations validated above"),
        })
    }

    fn core_consumer(&self) -> Option<NominalCoreConsumer<BearerToken>> {
        self.stream_to_core
            .as_ref()
            .map(|(auth_provider, dataset, handle)| {
                NominalCoreConsumer::new(
                    NominalApiClients::from_uri(self.opts.base_api_url.as_str()),
                    handle.clone(),
                    auth_provider.clone(),
                    dataset.clone(),
                )
                .with_track_metrics(self.opts.track_metrics)
                .with_additional_metric_channels(
                    self.opts.additional_metric_channels.iter().cloned(),
                )
            })
    }

    fn dataset_rid(&self) -> Option<ResourceIdentifier> {
        self.stream_to_core.as_ref().map(|(_, rid, _)| rid.clone())
    }

    fn file_consumer(&self) -> crate::consumer::ConsumerResult<Option<AvroFileConsumer>> {
        self.stream_to_file
            .as_ref()
            .map(|path| {
                AvroFileConsumer::new_with_full_path(path, true, self.dataset_rid()).map_err(
                    |source| crate::consumer::ConsumerError::FileError {
                        path: path.clone(),
                        operation: "open",
                        source: Box::new(source),
                    },
                )
            })
            .transpose()
    }

    fn fallback_consumer(&self) -> crate::consumer::ConsumerResult<Option<AvroFileConsumer>> {
        self.file_fallback
            .as_ref()
            .map(|path| {
                AvroFileConsumer::new_with_full_path(path, false, self.dataset_rid()).map_err(
                    |source| crate::consumer::ConsumerError::FileError {
                        path: path.clone(),
                        operation: "open fallback",
                        source: Box::new(source),
                    },
                )
            })
            .transpose()
    }

    fn into_stream<C: WriteRequestConsumer + 'static>(self, consumer: C) -> NominalDatasetStream {
        let mut listeners = self.listeners;
        listeners.push(Arc::new(LoggingListener));
        let listening_consumer = ListeningWriteRequestConsumer::new(consumer, listeners);
        NominalDatasetStream::new_with_consumer(listening_consumer, self.opts)
    }
}

// for backcompat, new code should use NominalDatasetStream
#[deprecated]
pub type NominalDatasourceStream = NominalDatasetStream;

/// Delivery evidence for accepted user points. Backend and file counts can
/// overlap for dual writes. Custom completion makes no backend/disk guarantee.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliverySummary {
    pub accepted_points: usize,
    pub acknowledged_points: usize,
    /// File appends; provisional until close successfully finalizes the files.
    pub file_points: usize,
    pub custom_consumer_points: usize,
    /// Points without a completed destination, including pending points in a live snapshot.
    pub unpreserved_points: usize,
    pub file_paths: Vec<PathBuf>,
    /// At most sixteen diagnostics, each limited to 2048 characters.
    pub failures: Vec<String>,
}

/// A sticky stream failure, including the available delivery evidence.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct StreamError {
    pub summary: DeliverySummary,
    pub message: String,
}

struct DeliveryState {
    open: bool,
    failed: bool,
    worker_failed: bool,
    summary: DeliverySummary,
    // Counts by destination bitset: backend=1, file=2, custom=4.
    // Fixed-size accounting preserves unions without retaining requests.
    delivered: [usize; 8],
}

impl DeliveryState {
    fn record_failure(&mut self, message: &str) {
        self.failed = true;
        if self.summary.failures.len() == 16 {
            self.summary.failures.pop();
        }
        self.diagnostic(message);
    }

    fn diagnostic(&mut self, message: &str) {
        if self.summary.failures.len() < 16 {
            self.summary
                .failures
                .push(message.chars().take(2048).collect());
        }
    }

    fn snapshot(&self) -> DeliverySummary {
        let mut summary = self.summary.clone();
        let mut completed = 0;
        for (destinations, count) in self.delivered.iter().enumerate().skip(1) {
            completed += count;
            if destinations & 1 != 0 {
                summary.acknowledged_points += count;
            }
            if destinations & 2 != 0 {
                summary.file_points += count;
            }
            if destinations & 4 != 0 {
                summary.custom_consumer_points += count;
            }
        }
        summary.unpreserved_points = summary.accepted_points.saturating_sub(completed);
        summary
    }

    fn error(&self, message: &str) -> StreamError {
        StreamError {
            summary: self.snapshot(),
            message: message.to_owned(),
        }
    }
}

struct Progress {
    state: Mutex<DeliveryState>,
    capacity: Condvar,
}

impl Progress {
    fn new() -> Self {
        Self {
            state: Mutex::new(DeliveryState {
                open: true,
                failed: false,
                worker_failed: false,
                summary: DeliverySummary::default(),
                delivered: [0; 8],
            }),
            capacity: Condvar::new(),
        }
    }

    fn worker_failure(&self, message: &str) {
        let mut state = self.state.lock();
        state.worker_failed = true;
        state.record_failure(message);
        self.capacity.notify_all();
    }

    fn completed(&self, count: usize, delivery: ConsumerDelivery) {
        let mut state = self.state.lock();
        let destinations = usize::from(delivery.acknowledged)
            | (usize::from(!delivery.file_paths.is_empty()) << 1)
            | (usize::from(delivery.custom) << 2);
        state.delivered[destinations] += count;
        for path in delivery.file_paths {
            if !state.summary.file_paths.contains(&path) {
                state.summary.file_paths.push(path);
            }
        }
        for failure in delivery.failures {
            if delivery.failed {
                state.record_failure(&failure);
            } else {
                state.diagnostic(&failure);
            }
        }
        state.failed |= delivery.failed;
        self.capacity.notify_all();
    }
}

pub struct NominalDatasetStream {
    opts: NominalStreamOpts,
    running: Arc<AtomicBool>,
    progress: Arc<Progress>,
    consumer: Option<Arc<dyn WriteRequestConsumer>>,
    workers: Vec<thread::JoinHandle<()>>,
    close_result: Option<Result<DeliverySummary, StreamError>>,
    primary_buffer: Arc<SeriesBuffer>,
    secondary_buffer: Arc<SeriesBuffer>,
    primary_handle: thread::Thread,
    secondary_handle: thread::Thread,
    /// Records the total time spent processing batches on background threads.
    ///
    /// This field is only available when the `instrument` feature is enabled.
    /// This field is *not* SemVer-compliant -- it may be removed during a minor version bump.
    /// This is intended only for use in benchmarks.
    #[cfg(feature = "instrument")]
    pub batch_processor_ns: Arc<AtomicU64>,
    /// Records the total time dispatching batched points on background threads.
    ///
    /// This field is only available when the `instrument` feature is enabled.
    /// This field is *not* SemVer-compliant -- it may be removed during a minor version bump.
    /// This is intended only for use in benchmarks.
    #[cfg(feature = "instrument")]
    pub dispatcher_ns: Arc<AtomicU64>,
}

impl NominalDatasetStream {
    pub fn builder() -> NominalDatasetStreamBuilder {
        NominalDatasetStreamBuilder::new()
    }

    /// # Panics
    /// Panics if `opts.max_points_per_record` or `opts.request_dispatcher_tasks` is zero.
    pub fn new_with_consumer<C: WriteRequestConsumer + 'static>(
        consumer: C,
        opts: NominalStreamOpts,
    ) -> Self {
        assert!(
            opts.max_points_per_record > 0,
            "max_points_per_record must be greater than zero"
        );
        assert!(
            opts.request_dispatcher_tasks > 0,
            "request_dispatcher_tasks must be greater than zero"
        );
        let primary_buffer = Arc::new(SeriesBuffer::new(opts.max_points_per_record));
        let secondary_buffer = Arc::new(SeriesBuffer::new(opts.max_points_per_record));
        let (request_tx, request_rx) = crossbeam_channel::bounded(opts.max_buffered_requests);
        let running = Arc::new(AtomicBool::new(true));
        let progress = Arc::new(Progress::new());
        let consumer: Arc<dyn WriteRequestConsumer> = Arc::new(consumer);
        #[cfg(feature = "instrument")]
        let batch_processor_ns = Arc::new(AtomicU64::new(0));
        #[cfg(feature = "instrument")]
        let dispatcher_ns = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::new();
        for (name, buffer) in [
            ("primary", &primary_buffer),
            ("secondary", &secondary_buffer),
        ] {
            let buffer = buffer.clone();
            let running = running.clone();
            let progress = progress.clone();
            let tx = request_tx.clone();
            let delay = opts.max_request_delay;
            #[cfg(feature = "instrument")]
            let bp_ns = batch_processor_ns.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("nmstream_{name}"))
                    .spawn(move || {
                        let result = checked_call(|| {
                            batch_processor(
                                running,
                                buffer,
                                tx,
                                delay,
                                &progress,
                                #[cfg(feature = "instrument")]
                                bp_ns,
                            )
                        });
                        if let Err(message) = result {
                            progress.worker_failure(&format!("batch worker: {message}"));
                        }
                    })
                    .expect("failed to spawn batch worker"),
            );
        }
        drop(request_tx);
        let primary_handle = workers[0].thread().clone();
        let secondary_handle = workers[1].thread().clone();
        for i in 0..opts.request_dispatcher_tasks {
            let rx = request_rx.clone();
            let consumer = consumer.clone();
            let progress = progress.clone();
            #[cfg(feature = "instrument")]
            let disp_ns = dispatcher_ns.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("nmstream_dispatch_{i}"))
                    .spawn(move || {
                        let result = checked_call(|| {
                            request_dispatcher(
                                rx,
                                consumer,
                                &progress,
                                #[cfg(feature = "instrument")]
                                disp_ns,
                            )
                        });
                        if let Err(message) = result {
                            progress.worker_failure(&format!("dispatch worker: {message}"));
                        }
                    })
                    .expect("failed to spawn dispatch worker"),
            );
        }
        drop(request_rx);
        Self {
            opts,
            running,
            progress,
            consumer: Some(consumer),
            workers,
            close_result: None,
            primary_buffer,
            secondary_buffer,
            primary_handle,
            secondary_handle,
            #[cfg(feature = "instrument")]
            batch_processor_ns,
            #[cfg(feature = "instrument")]
            dispatcher_ns,
        }
    }

    /// Snapshot delivery evidence. While open, unpreserved points include pending
    /// work and file evidence remains provisional until checked finalization.
    pub fn delivery_summary(&self) -> DeliverySummary {
        match &self.close_result {
            Some(Ok(summary)) => summary.clone(),
            Some(Err(error)) => error.summary.clone(),
            None => self.progress.state.lock().snapshot(),
        }
    }

    /// Stop admission, drain accepted points, join workers and finalize destinations.
    /// Repeated calls return the same result. A successful custom consumer is opaque:
    /// only built-in destination evidence establishes backend or Avro preservation.
    pub fn close(&mut self) -> Result<DeliverySummary, StreamError> {
        if let Some(result) = &self.close_result {
            return result.clone();
        }
        self.progress.state.lock().open = false;
        self.running.store(false, Ordering::Release);
        self.primary_handle.unpark();
        self.secondary_handle.unpark();
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                self.progress
                    .worker_failure("worker panicked outside its recovery boundary");
            }
        }
        let consumer = self.consumer.take().expect("open stream owns its consumer");
        let mut failures =
            checked_call(|| consumer.finish_delivery()).unwrap_or_else(|message| vec![message]);
        if let Err(message) = checked_call(|| drop(consumer)) {
            failures.push(message);
        }
        let mut state = self.progress.state.lock();
        if !failures.is_empty() {
            // Finalization errors make non-backend evidence uncertain. Conservatively
            // retain only backend acknowledgements; do not claim unflushed files safe.
            let mut acknowledged = 0;
            for (destinations, count) in state.delivered.iter().enumerate() {
                if destinations & 1 != 0 {
                    acknowledged += count;
                }
            }
            state.delivered = [0; 8];
            state.delivered[1] = acknowledged;
            for failure in failures {
                state.record_failure(&format!("finish: {failure}"));
            }
        }
        let summary = state.snapshot();
        let result = if state.failed || summary.unpreserved_points != 0 {
            let message = format!(
                "stream close failed: {} unpreserved points; {}",
                summary.unpreserved_points,
                summary.failures.join("; ")
            );
            Err(StreamError { summary, message })
        } else {
            Ok(summary)
        };
        self.close_result = Some(result.clone());
        result
    }

    pub fn double_writer(&self, channel_descriptor: ChannelDescriptor) -> NominalDoubleWriter<'_> {
        NominalDoubleWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn string_writer(&self, channel_descriptor: ChannelDescriptor) -> NominalStringWriter<'_> {
        NominalStringWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn integer_writer(
        &self,
        channel_descriptor: ChannelDescriptor,
    ) -> NominalIntegerWriter<'_> {
        NominalIntegerWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn uint64_writer(&self, channel_descriptor: ChannelDescriptor) -> NominalUint64Writer<'_> {
        NominalUint64Writer {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn struct_writer(&self, channel_descriptor: ChannelDescriptor) -> NominalStructWriter<'_> {
        NominalStructWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn double_array_writer(
        &self,
        channel_descriptor: ChannelDescriptor,
    ) -> NominalDoubleArrayWriter<'_> {
        NominalDoubleArrayWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn string_array_writer(
        &self,
        channel_descriptor: ChannelDescriptor,
    ) -> NominalStringArrayWriter<'_> {
        NominalStringArrayWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn enqueue(&self, channel_descriptor: &ChannelDescriptor, new_points: impl IntoPoints) {
        self.try_enqueue(channel_descriptor, new_points)
            .expect("stream rejected points");
    }

    /// Accept the entire submission or reject it before admission. Accepted points
    /// continue draining if a destination subsequently fails.
    pub fn try_enqueue(
        &self,
        channel_descriptor: &ChannelDescriptor,
        new_points: impl IntoPoints,
    ) -> Result<(), StreamError> {
        let new_points = new_points.into_points();
        let count = points_len(&new_points);
        self.reserve(count)?;
        self.when_capacity(count, |mut buffer| {
            buffer.extend(channel_descriptor, new_points)
        })
    }

    pub fn enqueue_many(&self, entries: Vec<(ChannelDescriptor, PointsType)>) {
        self.try_enqueue_many(entries)
            .expect("stream rejected batch");
    }

    /// Reserve the whole batch once, then submit bounded chunks. Destination
    /// failures do not reject a suffix of an already accepted batch.
    pub fn try_enqueue_many(
        &self,
        entries: Vec<(ChannelDescriptor, PointsType)>,
    ) -> Result<(), StreamError> {
        let total = entries.iter().map(|(_, points)| points_len(points)).sum();
        self.reserve(total)?;
        let mut chunk = Vec::new();
        let mut count = 0;
        for (channel, points) in entries {
            let n = points_len(&points);
            if count > 0 && count + n > self.opts.max_points_per_record {
                self.enqueue_chunk(std::mem::take(&mut chunk), count)?;
                count = 0;
            }
            count += n;
            chunk.push((channel, points));
        }
        if !chunk.is_empty() {
            self.enqueue_chunk(chunk, count)?;
        }
        Ok(())
    }

    fn reserve(&self, count: usize) -> Result<(), StreamError> {
        let mut state = self.progress.state.lock();
        if !state.open || state.failed {
            return Err(state.error(if state.open {
                "stream rejected points after a delivery failure"
            } else {
                "stream is closed"
            }));
        }
        state.summary.accepted_points += count;
        Ok(())
    }

    fn enqueue_chunk(
        &self,
        entries: Vec<(ChannelDescriptor, PointsType)>,
        count: usize,
    ) -> Result<(), StreamError> {
        self.when_capacity(count, |mut buffer| {
            for (channel, points) in entries {
                buffer.extend(&channel, points);
            }
        })
    }

    // This path submits already accepted points, including writer-local buffers.
    fn when_capacity(
        &self,
        count: usize,
        callback: impl FnOnce(SeriesBufferGuard),
    ) -> Result<(), StreamError> {
        let mut state = self.progress.state.lock();
        loop {
            if state.worker_failed {
                return Err(state.error("stream worker failed while draining accepted points"));
            }
            if self.primary_buffer.has_capacity(count) {
                let result = checked_call(|| callback(self.primary_buffer.lock()));
                return result.map_err(|message| {
                    state.record_failure(&message);
                    state.error(&message)
                });
            }
            self.primary_handle.unpark();
            if self.secondary_buffer.has_capacity(count) {
                let result = checked_call(|| callback(self.secondary_buffer.lock()));
                return result.map_err(|message| {
                    state.record_failure(&message);
                    state.error(&message)
                });
            }
            self.secondary_handle.unpark();
            self.progress.capacity.wait(&mut state);
        }
    }
}

pub struct NominalChannelWriter<'ds, T>
where
    Vec<T>: IntoPoints,
{
    channel: ChannelDescriptor,
    stream: &'ds NominalDatasetStream,
    last_flushed_at: Instant,
    unflushed: Vec<T>,
}

impl<T> NominalChannelWriter<'_, T>
where
    Vec<T>: IntoPoints,
{
    fn new(
        stream: &NominalDatasetStream,
        channel: ChannelDescriptor,
    ) -> NominalChannelWriter<'_, T> {
        NominalChannelWriter {
            channel,
            stream,
            last_flushed_at: Instant::now(),
            unflushed: vec![],
        }
    }

    fn push_point(&mut self, point: T) -> Result<(), StreamError> {
        self.stream.reserve(1)?;
        self.unflushed.push(point);
        if self.unflushed.len() >= self.stream.opts.max_points_per_record
            || self.last_flushed_at.elapsed() > self.stream.opts.max_request_delay
        {
            debug!(
                "conditionally flushing {:?}, ({} points, {:?} since last)",
                self.channel,
                self.unflushed.len(),
                self.last_flushed_at.elapsed()
            );
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StreamError> {
        if self.unflushed.is_empty() {
            return Ok(());
        }
        debug!(
            "flushing writer for {:?} with {} points",
            self.channel,
            self.unflushed.len()
        );
        self.stream.when_capacity(self.unflushed.len(), |mut buf| {
            let to_flush = std::mem::take(&mut self.unflushed);
            buf.extend(&self.channel, to_flush);
            self.last_flushed_at = Instant::now();
        })
    }
}

impl<T> Drop for NominalChannelWriter<'_, T>
where
    Vec<T>: IntoPoints,
{
    fn drop(&mut self) {
        debug!("flushing then dropping writer for: {:?}", self.channel);
        if let Err(error) = self.flush() {
            error!("writer drop failed: {error}");
        }
    }
}

pub struct NominalDoubleWriter<'ds> {
    writer: NominalChannelWriter<'ds, DoublePoint>,
}

impl NominalDoubleWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: f64) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: f64,
    ) -> Result<(), StreamError> {
        self.writer.push_point(DoublePoint {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

pub struct NominalIntegerWriter<'ds> {
    writer: NominalChannelWriter<'ds, IntegerPoint>,
}

impl NominalIntegerWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: i64) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: i64,
    ) -> Result<(), StreamError> {
        self.writer.push_point(IntegerPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

pub struct NominalUint64Writer<'ds> {
    writer: NominalChannelWriter<'ds, Uint64Point>,
}

impl NominalUint64Writer<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: u64) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: u64,
    ) -> Result<(), StreamError> {
        self.writer.push_point(Uint64Point {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

pub struct NominalStringWriter<'ds> {
    writer: NominalChannelWriter<'ds, StringPoint>,
}

impl NominalStringWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: impl Into<String>) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: impl Into<String>,
    ) -> Result<(), StreamError> {
        self.writer.push_point(StringPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value: value.into(),
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

pub struct NominalStructWriter<'ds> {
    writer: NominalChannelWriter<'ds, StructPoint>,
}

impl NominalStructWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: impl Into<String>) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: impl Into<String>,
    ) -> Result<(), StreamError> {
        self.writer.push_point(StructPoint {
            timestamp: Some(timestamp.into_timestamp()),
            json_string: value.into(),
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

pub struct NominalDoubleArrayWriter<'ds> {
    writer: NominalChannelWriter<'ds, DoubleArrayPoint>,
}

impl NominalDoubleArrayWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: Vec<f64>) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: Vec<f64>,
    ) -> Result<(), StreamError> {
        self.writer.push_point(DoubleArrayPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

pub struct NominalStringArrayWriter<'ds> {
    writer: NominalChannelWriter<'ds, StringArrayPoint>,
}

impl NominalStringArrayWriter<'_> {
    pub fn push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: impl IntoIterator<Item = impl Into<String>>,
    ) {
        self.try_push(timestamp, value)
            .expect("stream rejected writer point");
    }

    pub fn try_push(
        &mut self,
        timestamp: impl IntoTimestamp,
        value: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), StreamError> {
        self.writer.push_point(StringArrayPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value: value.into_iter().map(Into::into).collect(),
        })
    }

    pub fn try_flush(&mut self) -> Result<(), StreamError> {
        self.writer.flush()
    }
}

struct SeriesBuffer {
    points: Mutex<HashMap<ChannelDescriptor, PointsType>>,
    /// The total number of data points in the buffer.
    ///
    /// To ensure that `count` stays in sync with the contents of the `HashMap`,
    /// only update `count` through the `SeriesBufferGuard`.
    count: AtomicUsize,
    max_capacity: usize,
}

struct SeriesBufferGuard<'sb> {
    sb: MutexGuard<'sb, HashMap<ChannelDescriptor, PointsType>>,
    count: &'sb AtomicUsize,
}

impl SeriesBufferGuard<'_> {
    fn extend(&mut self, channel_descriptor: &ChannelDescriptor, points: impl IntoPoints) {
        let points = points.into_points();
        let new_point_count = points_len(&points);

        if let Some(existing) = self.sb.get_mut(channel_descriptor) {
            match (existing, points) {
                (PointsType::DoublePoints(existing), PointsType::DoublePoints(new)) => {
                    existing.points.extend(new.points)
                }
                (PointsType::StringPoints(existing), PointsType::StringPoints(new)) => {
                    existing.points.extend(new.points)
                }
                (PointsType::IntegerPoints(existing), PointsType::IntegerPoints(new)) => {
                    existing.points.extend(new.points)
                }
                (
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::DoubleArrayPoints(existing)),
                    }),
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::DoubleArrayPoints(new)),
                    }),
                ) => existing.points.extend(new.points),
                (PointsType::Uint64Points(existing), PointsType::Uint64Points(new)) => {
                    existing.points.extend(new.points)
                }
                (
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::StringArrayPoints(existing)),
                    }),
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::StringArrayPoints(new)),
                    }),
                ) => existing.points.extend(new.points),
                (
                    PointsType::ArrayPoints(ArrayPoints { array_type: None }),
                    PointsType::ArrayPoints(ArrayPoints { array_type: None }),
                ) => {}
                (PointsType::StructPoints(existing), PointsType::StructPoints(new)) => {
                    existing.points.extend(new.points);
                }
                // this is hideous, but exhaustive matching is good to avoid future errors
                (
                    PointsType::DoublePoints(_),
                    PointsType::IntegerPoints(_)
                    | PointsType::Uint64Points(_)
                    | PointsType::StringPoints(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::StringPoints(_),
                    PointsType::DoublePoints(_)
                    | PointsType::IntegerPoints(_)
                    | PointsType::Uint64Points(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::IntegerPoints(_),
                    PointsType::DoublePoints(_)
                    | PointsType::Uint64Points(_)
                    | PointsType::StringPoints(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::ArrayPoints(_),
                    PointsType::DoublePoints(_)
                    | PointsType::Uint64Points(_)
                    | PointsType::StringPoints(_)
                    | PointsType::IntegerPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(_),
                    }),
                    PointsType::ArrayPoints(ArrayPoints { array_type: None }),
                )
                | (
                    PointsType::ArrayPoints(ArrayPoints { array_type: None }),
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(_),
                    }),
                )
                | (
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::DoubleArrayPoints(_)),
                    }),
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::StringArrayPoints(_)),
                    }),
                )
                | (
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::StringArrayPoints(_)),
                    }),
                    PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::DoubleArrayPoints(_)),
                    }),
                )
                | (
                    PointsType::Uint64Points(_),
                    PointsType::IntegerPoints(_)
                    | PointsType::StringPoints(_)
                    | PointsType::DoublePoints(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::StructPoints(_),
                    PointsType::DoublePoints(_)
                    | PointsType::Uint64Points(_)
                    | PointsType::StringPoints(_)
                    | PointsType::IntegerPoints(_)
                    | PointsType::ArrayPoints(_),
                ) => {
                    // todo: improve error
                    panic!("mismatched types");
                }
            }
        } else {
            self.sb.insert(channel_descriptor.clone(), points);
        }

        self.count.fetch_add(new_point_count, Ordering::Release);
    }
}

impl SeriesBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            points: Mutex::new(HashMap::new()),
            count: AtomicUsize::new(0),
            max_capacity: capacity,
        }
    }

    /// Checks if the buffer has enough capacity to add new points.
    /// Note that the buffer can be larger than MAX_POINTS_PER_RECORD if a single batch of points
    /// larger than MAX_POINTS_PER_RECORD is inserted while the buffer is empty. Output request
    /// splitting is handled separately by the background processor.
    fn has_capacity(&self, new_points_count: usize) -> bool {
        let count = self.count.load(Ordering::Acquire);
        count == 0 || count + new_points_count <= self.max_capacity
    }

    fn lock(&self) -> SeriesBufferGuard<'_> {
        SeriesBufferGuard {
            sb: self.points.lock(),
            count: &self.count,
        }
    }

    fn take(&self) -> (usize, Vec<Series>) {
        let mut points = self.lock();
        let result = points
            .sb
            .drain()
            .map(|(ChannelDescriptor { name, tags }, points)| {
                let channel = Channel { name };
                let points_obj = Points {
                    points_type: Some(points),
                };
                Series {
                    channel: Some(channel),
                    // the protobuf `Series` owns its tags, so the shared map is copied out here --
                    // once per channel per flush, rather than once per channel per write
                    tags: tags
                        .map(|tags| {
                            tags.iter()
                                .map(|(key, value)| (key.clone(), value.clone()))
                                .collect()
                        })
                        .unwrap_or_default(),
                    points: Some(points_obj),
                }
            })
            .collect();
        let result_count = points.count.swap(0, Ordering::AcqRel);
        (result_count, result)
    }

    fn is_empty(&self) -> bool {
        self.count() == 0
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

fn batch_processor(
    running: Arc<AtomicBool>,
    points_buffer: Arc<SeriesBuffer>,
    request_chan: crossbeam_channel::Sender<(WriteRequestNominal, usize)>,
    max_request_delay: Duration,
    progress: &Progress,
    #[cfg(feature = "instrument")] bp_ns: Arc<AtomicU64>,
) {
    loop {
        debug!("starting processor loop");
        if points_buffer.is_empty() {
            if !running.load(Ordering::Acquire) {
                debug!("batch processor thread exiting due to running flag");
                drop(request_chan);
                break;
            } else {
                debug!("empty points buffer, waiting");
                thread::park_timeout(max_request_delay);
            }
            continue;
        }

        #[cfg(feature = "instrument")]
        let t = Instant::now();

        let (point_count, series) = points_buffer.take();

        {
            let _state = progress.state.lock();
            progress.capacity.notify_all();
        }

        for_each_record(
            series,
            point_count,
            points_buffer.max_capacity,
            |series, count| {
                let request = WriteRequestNominal {
                    series,
                    session_name: None,
                };
                if let Err(error) = request_chan.send((request, count)) {
                    progress.worker_failure(&format!(
                        "failed to send {count} points to dispatcher: {error}"
                    ));
                }
            },
        );

        #[cfg(feature = "instrument")]
        bp_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Drain without the flush delay during shutdown.
        if running.load(Ordering::Acquire) {
            thread::park_timeout(max_request_delay);
        }
    }
    debug!("batch processor thread exiting");
}

impl Drop for NominalDatasetStream {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            error!("stream drop failed: {error}");
        }
    }
}

fn request_dispatcher(
    request_rx: crossbeam_channel::Receiver<(WriteRequestNominal, usize)>,
    consumer: Arc<dyn WriteRequestConsumer>,
    progress: &Progress,
    #[cfg(feature = "instrument")] disp_ns: Arc<AtomicU64>,
) {
    for (request, count) in request_rx {
        #[cfg(feature = "instrument")]
        let start = Instant::now();
        let delivery = checked_call(|| consumer.consume_delivery(&request))
            .unwrap_or_else(ConsumerDelivery::failure);
        progress.completed(count, delivery);
        #[cfg(feature = "instrument")]
        disp_ns.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod flow_control_tests;

#[cfg(test)]
mod close_tests;
