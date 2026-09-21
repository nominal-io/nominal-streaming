use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::UNIX_EPOCH;

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
use crate::consumer::AvroFileConsumer;
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

    pub fn build(self) -> NominalDatasetStream {
        let core_consumer = self.core_consumer();
        let file_consumer = self.file_consumer();
        let fallback_consumer = self.fallback_consumer();

        match (core_consumer, file_consumer, fallback_consumer) {
            (None, None, _) => panic!("nominal dataset stream must either stream to file or core"),
            (Some(_), Some(_), Some(_)) => {
                panic!("must choose one of stream_to_file and file_fallback when streaming to core")
            }
            (Some(core), None, None) => self.into_stream(core),
            (Some(core), None, Some(fallback)) => {
                self.into_stream(RequestConsumerWithFallback::new(core, fallback))
            }
            (None, Some(file), None) => self.into_stream(file),
            (None, Some(file), Some(fallback)) => {
                // todo: should this even be supported?
                self.into_stream(RequestConsumerWithFallback::new(file, fallback))
            }
            (Some(core), Some(file), None) => {
                self.into_stream(DualWriteRequestConsumer::new(core, file))
            }
        }
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

    fn file_consumer(&self) -> Option<AvroFileConsumer> {
        self.stream_to_file.as_ref().map(|path| {
            AvroFileConsumer::new_with_full_path(path, true, self.dataset_rid()).unwrap()
        })
    }

    fn fallback_consumer(&self) -> Option<AvroFileConsumer> {
        self.file_fallback.as_ref().map(|path| {
            AvroFileConsumer::new_with_full_path(path, true, self.dataset_rid()).unwrap()
        })
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

pub struct NominalDatasetStream {
    opts: NominalStreamOpts,
    running: Arc<AtomicBool>,
    unflushed_points: Arc<AtomicUsize>,
    primary_buffer: Arc<SeriesBuffer>,
    secondary_buffer: Arc<SeriesBuffer>,
    primary_handle: thread::JoinHandle<()>,
    secondary_handle: thread::JoinHandle<()>,
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
    /// Panics if `opts.max_points_per_record` is zero.
    pub fn new_with_consumer<C: WriteRequestConsumer + 'static>(
        consumer: C,
        opts: NominalStreamOpts,
    ) -> Self {
        assert!(
            opts.max_points_per_record > 0,
            "max_points_per_record must be greater than zero"
        );
        let primary_buffer = Arc::new(SeriesBuffer::new(opts.max_points_per_record));
        let secondary_buffer = Arc::new(SeriesBuffer::new(opts.max_points_per_record));

        let (request_tx, request_rx) =
            crossbeam_channel::bounded::<(WriteRequestNominal, usize)>(opts.max_buffered_requests);

        let running = Arc::new(AtomicBool::new(true));
        let unflushed_points = Arc::new(AtomicUsize::new(0));

        #[cfg(feature = "instrument")]
        let batch_processor_ns = Arc::new(AtomicU64::new(0));
        #[cfg(feature = "instrument")]
        let dispatcher_ns = Arc::new(AtomicU64::new(0));

        let primary_handle = thread::Builder::new()
            .name("nmstream_primary".to_string())
            .spawn({
                let points_buffer = Arc::clone(&primary_buffer);
                let running = running.clone();
                let tx = request_tx.clone();
                #[cfg(feature = "instrument")]
                let bp_ns = Arc::clone(&batch_processor_ns);
                move || {
                    batch_processor(
                        running,
                        points_buffer,
                        tx,
                        opts.max_request_delay,
                        #[cfg(feature = "instrument")]
                        bp_ns,
                    );
                }
            })
            .unwrap();

        let secondary_handle = thread::Builder::new()
            .name("nmstream_secondary".to_string())
            .spawn({
                let secondary_buffer = Arc::clone(&secondary_buffer);
                let running = running.clone();
                #[cfg(feature = "instrument")]
                let bp_ns = Arc::clone(&batch_processor_ns);
                move || {
                    batch_processor(
                        running,
                        secondary_buffer,
                        request_tx,
                        opts.max_request_delay,
                        #[cfg(feature = "instrument")]
                        bp_ns,
                    );
                }
            })
            .unwrap();

        let consumer = Arc::new(consumer);

        for i in 0..opts.request_dispatcher_tasks {
            thread::Builder::new()
                .name(format!("nmstream_dispatch_{i}"))
                .spawn({
                    let running = Arc::clone(&running);
                    let unflushed_points = Arc::clone(&unflushed_points);
                    let rx = request_rx.clone();
                    let consumer = consumer.clone();
                    #[cfg(feature = "instrument")]
                    let disp_ns = Arc::clone(&dispatcher_ns);
                    move || {
                        debug!("starting request dispatcher #{}", i);
                        request_dispatcher(
                            running,
                            unflushed_points,
                            rx,
                            consumer,
                            #[cfg(feature = "instrument")]
                            disp_ns,
                        );
                    }
                })
                .unwrap();
        }

        NominalDatasetStream {
            opts,
            running,
            unflushed_points,
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
        let new_points = new_points.into_points();
        let new_count = points_len(&new_points);

        self.when_capacity(new_count, |mut sb| {
            sb.extend(channel_descriptor, new_points)
        });
    }

    /// Enqueues points for many channels, blocking while both buffers are full.
    ///
    /// This reserves capacity and takes the buffer lock once per batch, where the equivalent run of
    /// [`enqueue`](Self::enqueue) calls would do both once per channel. For a wide record --
    /// thousands of channels sharing a timestamp -- that is the difference between one buffer
    /// insertion and thousands of them.
    ///
    /// A batch larger than `max_points_per_record` is admitted in groups of channel entries.
    /// An individual oversized entry is admitted whole; the background processor splits output
    /// requests to the configured limit. Fitting buffered records are sent unchanged. Concurrent
    /// producers can overfill a buffer, in which case even a fitting batch may span requests.
    pub fn enqueue_many(&self, entries: Vec<(ChannelDescriptor, PointsType)>) {
        let total: usize = entries.iter().map(|(_, points)| points_len(points)).sum();

        if total <= self.opts.max_points_per_record {
            self.enqueue_chunk(entries, total);
            return;
        }

        let mut chunk: Vec<(ChannelDescriptor, PointsType)> = Vec::new();
        let mut chunk_count = 0;

        for (channel_descriptor, points) in entries {
            let count = points_len(&points);

            if chunk_count > 0 && chunk_count + count > self.opts.max_points_per_record {
                self.enqueue_chunk(std::mem::take(&mut chunk), chunk_count);
                chunk_count = 0;
            }

            // A single entry over the limit still goes through whole: the buffer admits an oversized
            // batch into an empty buffer. The background processor splits output requests.
            chunk_count += count;
            chunk.push((channel_descriptor, points));
        }

        if !chunk.is_empty() {
            self.enqueue_chunk(chunk, chunk_count);
        }
    }

    fn enqueue_chunk(&self, entries: Vec<(ChannelDescriptor, PointsType)>, new_count: usize) {
        self.when_capacity(new_count, move |mut sb| {
            for (channel_descriptor, points) in entries {
                sb.extend(&channel_descriptor, points)
            }
        });
    }

    fn when_capacity(&self, new_count: usize, callback: impl FnOnce(SeriesBufferGuard)) {
        self.unflushed_points
            .fetch_add(new_count, Ordering::Release);

        if self.primary_buffer.has_capacity(new_count) {
            debug!("adding {} points to primary buffer", new_count);
            callback(self.primary_buffer.lock());
        } else if self.secondary_buffer.has_capacity(new_count) {
            // primary buffer is definitely full
            self.primary_handle.thread().unpark();
            debug!("adding {} points to secondary buffer", new_count);
            callback(self.secondary_buffer.lock());
        } else {
            let buf = if self.primary_buffer < self.secondary_buffer {
                debug!("waiting for primary buffer to flush to append {new_count} points...");
                self.primary_handle.thread().unpark();
                &self.primary_buffer
            } else {
                debug!("waiting for secondary buffer to flush to append {new_count} points...");
                self.secondary_handle.thread().unpark();
                &self.secondary_buffer
            };

            buf.on_notify(callback);
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

    fn push_point(&mut self, point: T) {
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
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.unflushed.is_empty() {
            return;
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
        self.flush();
    }
}

pub struct NominalDoubleWriter<'ds> {
    writer: NominalChannelWriter<'ds, DoublePoint>,
}

impl NominalDoubleWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: f64) {
        self.writer.push_point(DoublePoint {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        });
    }
}

pub struct NominalIntegerWriter<'ds> {
    writer: NominalChannelWriter<'ds, IntegerPoint>,
}

impl NominalIntegerWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: i64) {
        self.writer.push_point(IntegerPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        });
    }
}

pub struct NominalUint64Writer<'ds> {
    writer: NominalChannelWriter<'ds, Uint64Point>,
}

impl NominalUint64Writer<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: u64) {
        self.writer.push_point(Uint64Point {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        });
    }
}

pub struct NominalStringWriter<'ds> {
    writer: NominalChannelWriter<'ds, StringPoint>,
}

impl NominalStringWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: impl Into<String>) {
        self.writer.push_point(StringPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value: value.into(),
        });
    }
}

pub struct NominalStructWriter<'ds> {
    writer: NominalChannelWriter<'ds, StructPoint>,
}

impl NominalStructWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: impl Into<String>) {
        self.writer.push_point(StructPoint {
            timestamp: Some(timestamp.into_timestamp()),
            json_string: value.into(),
        });
    }
}

pub struct NominalDoubleArrayWriter<'ds> {
    writer: NominalChannelWriter<'ds, DoubleArrayPoint>,
}

impl NominalDoubleArrayWriter<'_> {
    pub fn push(&mut self, timestamp: impl IntoTimestamp, value: Vec<f64>) {
        self.writer.push_point(DoubleArrayPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value,
        });
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
        self.writer.push_point(StringArrayPoint {
            timestamp: Some(timestamp.into_timestamp()),
            value: value.into_iter().map(Into::into).collect(),
        });
    }
}

struct SeriesBuffer {
    points: Mutex<HashMap<ChannelDescriptor, PointsType>>,
    /// The total number of data points in the buffer.
    ///
    /// To ensure that `count` stays in sync with the contents of the `HashMap`,
    /// only update `count` through the `SeriesBufferGuard`.
    count: AtomicUsize,
    flush_time: AtomicU64,
    condvar: Condvar,
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

impl PartialEq for SeriesBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.flush_time.load(Ordering::Acquire) == other.flush_time.load(Ordering::Acquire)
    }
}

impl PartialOrd for SeriesBuffer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let flush_time = self.flush_time.load(Ordering::Acquire);
        let other_flush_time = other.flush_time.load(Ordering::Acquire);
        flush_time.partial_cmp(&other_flush_time)
    }
}

impl SeriesBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            points: Mutex::new(HashMap::new()),
            count: AtomicUsize::new(0),
            flush_time: AtomicU64::new(0),
            condvar: Condvar::new(),
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
        self.flush_time.store(
            UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64,
            Ordering::Release,
        );
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

    fn on_notify(&self, on_notify: impl FnOnce(SeriesBufferGuard)) {
        let mut points_lock = self.points.lock();
        // concurrency bug without this - the buffer could have been emptied since we
        // checked the count, so this will wait forever & block any new points from entering
        if !points_lock.is_empty() {
            self.condvar.wait(&mut points_lock);
        } else {
            debug!("buffer emptied since last check, skipping condvar wait");
        }
        on_notify(SeriesBufferGuard {
            sb: points_lock,
            count: &self.count,
        });
    }

    fn notify(&self) -> bool {
        self.condvar.notify_one()
    }
}

fn batch_processor(
    running: Arc<AtomicBool>,
    points_buffer: Arc<SeriesBuffer>,
    request_chan: crossbeam_channel::Sender<(WriteRequestNominal, usize)>,
    max_request_delay: Duration,
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

        if points_buffer.notify() {
            debug!("notified one waiting thread after clearing points buffer");
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
                    error!("failed to send request to dispatcher: {error}");
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
        debug!("starting drop for NominalDatasetStream");
        self.running.store(false, Ordering::Release);
        // Wake sleeping workers to flush pending points and exit.
        self.primary_handle.thread().unpark();
        self.secondary_handle.thread().unpark();
        loop {
            let count = self.unflushed_points.load(Ordering::Acquire);
            if count == 0 {
                break;
            }
            debug!(
                "waiting for all points to be flushed before dropping stream, {count} points remaining",
            );
            // todo: reduce this + give up after some maximum timeout is reached
            thread::sleep(Duration::from_millis(50));
        }
    }
}

fn request_dispatcher<C: WriteRequestConsumer + 'static>(
    running: Arc<AtomicBool>,
    unflushed_points: Arc<AtomicUsize>,
    request_rx: crossbeam_channel::Receiver<(WriteRequestNominal, usize)>,
    consumer: Arc<C>,
    #[cfg(feature = "instrument")] disp_ns: Arc<AtomicU64>,
) {
    let mut total_request_time = 0;
    loop {
        match request_rx.recv() {
            Ok((request, point_count)) => {
                debug!("received writerequest from channel");
                let req_start = Instant::now();
                match consumer.consume(&request) {
                    Ok(_) => {
                        let time = req_start.elapsed().as_millis();
                        debug!("request of {} points sent in {} ms", point_count, time);
                        total_request_time += time as u64;
                    }
                    Err(e) => {
                        error!("Failed to send request: {e:?}");
                    }
                }
                #[cfg(feature = "instrument")]
                disp_ns.fetch_add(req_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                unflushed_points.fetch_sub(point_count, Ordering::Release);

                if unflushed_points.load(Ordering::Acquire) == 0 && !running.load(Ordering::Acquire)
                {
                    debug!("all points flushed, closing dispatcher thread");
                    // notify the processor thread that all points have been flushed
                    drop(request_rx);
                    break;
                }
            }
            Err(e) => {
                debug!("request channel closed, exiting dispatcher thread. info: '{e}'");
                break;
            }
        }
    }
    debug!(
        "request dispatcher thread exiting. total request time: {}",
        total_request_time
    );
}

/// Split detached data without holding the buffer lock; sending applies queue backpressure.
fn for_each_record(
    series: Vec<Series>,
    count: usize,
    cap: usize,
    mut send: impl FnMut(Vec<Series>, usize),
) {
    if count <= cap {
        send(series, count);
        return;
    }
    let mut record = Vec::new();
    let mut count = 0;
    for mut series in series {
        let points = series.points.take().unwrap().points_type.unwrap();
        for_each_points_chunk(points, cap, |points| {
            let n = points_len(&points);
            if count > 0 && count + n > cap {
                send(std::mem::take(&mut record), count);
                count = 0;
            }
            record.push(Series {
                channel: series.channel.clone(),
                tags: series.tags.clone(),
                points: Some(Points {
                    points_type: Some(points),
                }),
            });
            count += n;
        });
    }
    if !record.is_empty() {
        send(record, count);
    }
}

/// Move oversized inputs into one chunk at a time, leaving fitting inputs untouched.
fn for_each_points_chunk(points: PointsType, cap: usize, mut submit: impl FnMut(PointsType)) {
    if points_len(&points) <= cap {
        submit(points);
        return;
    }

    fn submit_chunks<T>(points: Vec<T>, cap: usize, mut submit: impl FnMut(PointsType))
    where
        Vec<T>: IntoPoints,
    {
        let mut points = points.into_iter();
        while points.len() > 0 {
            submit(points.by_ref().take(cap).collect::<Vec<_>>().into_points());
        }
    }

    match points {
        PointsType::DoublePoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::IntegerPoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::Uint64Points(p) => submit_chunks(p.points, cap, submit),
        PointsType::StringPoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::StructPoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::ArrayPoints(p) => match p.array_type {
            Some(ArrayType::DoubleArrayPoints(p)) => submit_chunks(p.points, cap, submit),
            Some(ArrayType::StringArrayPoints(p)) => submit_chunks(p.points, cap, submit),
            None => unreachable!("empty array points fit in one chunk"),
        },
    }
}

fn points_len(points_type: &PointsType) -> usize {
    match points_type {
        PointsType::DoublePoints(points) => points.points.len(),
        PointsType::StringPoints(points) => points.points.len(),
        PointsType::IntegerPoints(points) => points.points.len(),
        PointsType::Uint64Points(points) => points.points.len(),
        PointsType::ArrayPoints(points) => match &points.array_type {
            Some(ArrayType::DoubleArrayPoints(points)) => points.points.len(),
            Some(ArrayType::StringArrayPoints(points)) => points.points.len(),
            None => 0,
        },
        PointsType::StructPoints(points) => points.points.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_chunks_preserve_contents_and_order() {
        for count in [0, 1, 3, 8] {
            let points = (0..count)
                .map(|i| DoublePoint {
                    timestamp: Some(i.into_timestamp()),
                    value: i as f64,
                })
                .collect::<Vec<_>>()
                .into_points();
            let buffer = SeriesBuffer::new(usize::MAX);
            let channel = ChannelDescriptor::new("value");
            for_each_points_chunk(points.clone(), 3, |chunk| {
                assert!(points_len(&chunk) <= 3);
                buffer.lock().extend(&channel, chunk);
            });
            assert_eq!(buffer.lock().sb.get(&channel), Some(&points));
        }
    }

    #[test]
    #[should_panic(expected = "mismatched types")]
    fn test_mismatched_array_types_panics() {
        // Protects the exhaustive match in SeriesBufferGuard::extend from being
        // silently simplified to a catch-all: pushing a DoubleArray and then a
        // StringArray to the same channel must panic at buffer merge time.
        //
        // Exercise the buffer directly under one lock. Between public enqueue
        // calls, a worker could flush the first array and prevent the mismatch.
        // This also avoids the stream's shutdown hang during panic without using
        // ManuallyDrop, which would leave its workers running.
        let buffer = SeriesBuffer::new(100);
        let mut guard = buffer.lock();
        let descriptor = ChannelDescriptor::new("mixed_array");
        guard.extend(
            &descriptor,
            vec![DoubleArrayPoint {
                timestamp: None,
                value: vec![1.0, 2.0],
            }],
        );
        guard.extend(
            &descriptor,
            vec![StringArrayPoint {
                timestamp: None,
                value: vec!["a".into()],
            }],
        );
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[derive(Debug, Default)]
    struct StepGate {
        permits: Mutex<usize>,
        ready: Condvar,
    }

    impl StepGate {
        fn release(&self, count: usize) {
            *self.permits.lock() += count;
            self.ready.notify_all();
        }

        fn wait(&self) {
            let mut permits = self.permits.lock();
            while *permits == 0 {
                self.ready.wait(&mut permits);
            }
            *permits -= 1;
        }
    }

    #[derive(Debug)]
    struct StepConsumer {
        entered: std::sync::mpsc::Sender<()>,
        gate: Arc<StepGate>,
        requests: Mutex<Vec<WriteRequestNominal>>,
    }

    impl WriteRequestConsumer for Arc<StepConsumer> {
        fn consume(&self, request: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
            self.requests.lock().push(request.clone());
            self.entered.send(()).unwrap();
            self.gate.wait();
            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum BatchShape {
        SingleChannel,
        ManyChannels,
        MixedChannels,
    }

    fn enqueue_batch(
        stream: &NominalDatasetStream,
        shape: BatchShape,
        batch: usize,
        points_per_batch: usize,
    ) {
        let point = |index| DoublePoint {
            timestamp: None,
            value: (batch * points_per_batch + index) as f64,
        };
        match shape {
            BatchShape::SingleChannel => stream.enqueue(
                &ChannelDescriptor::new(format!("batch-{batch}")),
                (0..points_per_batch).map(point).collect::<Vec<_>>(),
            ),
            BatchShape::ManyChannels => stream.enqueue_many(
                (0..points_per_batch)
                    .map(|index| {
                        (
                            ChannelDescriptor::new(format!("batch-{batch}-point-{index}")),
                            vec![point(index)].into_points(),
                        )
                    })
                    .collect(),
            ),
            BatchShape::MixedChannels => {
                let split = points_per_batch / 2;
                let mut entries = vec![(
                    ChannelDescriptor::new(format!("batch-{batch}-grouped")),
                    (0..split).map(point).collect::<Vec<_>>().into_points(),
                )];
                entries.extend((split..points_per_batch).map(|index| {
                    (
                        ChannelDescriptor::new(format!("batch-{batch}-point-{index}")),
                        vec![point(index)].into_points(),
                    )
                }));
                stream.enqueue_many(entries);
            }
        }
    }

    #[derive(Debug)]
    struct ReleaseControlledRecordingConsumer {
        entered: std::sync::mpsc::SyncSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        requests: Mutex<Vec<WriteRequestNominal>>,
    }

    impl WriteRequestConsumer for Arc<ReleaseControlledRecordingConsumer> {
        fn consume(&self, request: &WriteRequestNominal) -> crate::consumer::ConsumerResult<()> {
            self.requests.lock().push(request.clone());
            let _ = self.entered.try_send(());
            self.release.lock().recv().unwrap();
            Ok(())
        }
    }

    #[test]
    fn drop_wakes_partial_batches_before_flush_deadline() {
        #[derive(Debug)]
        struct CountingConsumer(Arc<AtomicUsize>);
        impl WriteRequestConsumer for CountingConsumer {
            fn consume(
                &self,
                request: &WriteRequestNominal,
            ) -> crate::consumer::ConsumerResult<()> {
                let count: usize = request
                    .series
                    .iter()
                    .map(|s| points_len(s.points.as_ref().unwrap().points_type.as_ref().unwrap()))
                    .sum();
                self.0.fetch_add(count, Ordering::Relaxed);
                Ok(())
            }
        }
        let accepted = Arc::new(AtomicUsize::new(0));
        let stream = NominalDatasetStream::new_with_consumer(
            CountingConsumer(accepted.clone()),
            NominalStreamOpts {
                max_request_delay: Duration::from_secs(60),
                ..Default::default()
            },
        );
        // Let empty processors enter their long idle wait.
        thread::sleep(Duration::from_millis(100));
        stream.enqueue(
            &ChannelDescriptor::new("value"),
            vec![DoublePoint {
                timestamp: None,
                value: 1.0,
            }],
        );
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            drop(stream);
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("drop waited for the flush deadline");
        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Arc::strong_count(&accepted) != 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            Arc::strong_count(&accepted),
            1,
            "idle workers retained the consumer"
        );
    }

    #[test]
    fn drop_drains_oversized_detached_and_buffered_batches() {
        const POINTS_PER_BATCH: usize = 4;
        const BATCHES: usize = 3;
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let consumer = Arc::new(ReleaseControlledRecordingConsumer {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            requests: Mutex::new(Vec::new()),
        });
        let stream = NominalDatasetStream::new_with_consumer(
            consumer.clone(),
            NominalStreamOpts::default()
                .with_max_points_per_record(1)
                .with_max_buffered_requests(0)
                .with_request_dispatcher_tasks(1)
                .with_max_request_delay(Duration::from_millis(1)),
        );

        for batch in 0..BATCHES {
            stream.enqueue(
                &ChannelDescriptor::new(format!("batch-{batch}")),
                (0..POINTS_PER_BATCH)
                    .map(|point| DoublePoint {
                        timestamp: None,
                        value: (batch * POINTS_PER_BATCH + point) as f64,
                    })
                    .collect::<Vec<_>>(),
            );
            if batch == 0 {
                entered_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("oversized batch did not reach the consumer");
            }
        }

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            drop(stream);
            done_tx.send(()).unwrap();
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "drop completed while the consumer was blocked"
        );

        for _ in 0..POINTS_PER_BATCH * BATCHES {
            release_tx.send(()).unwrap();
        }
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("drop did not drain every split request");

        let requests = consumer.requests.lock();
        assert_eq!(requests.len(), POINTS_PER_BATCH * BATCHES);
        let mut values = requests
            .iter()
            .flat_map(|request| {
                let count: usize = request
                    .series
                    .iter()
                    .map(|series| {
                        points_len(
                            series
                                .points
                                .as_ref()
                                .unwrap()
                                .points_type
                                .as_ref()
                                .unwrap(),
                        )
                    })
                    .sum();
                assert_eq!(count, 1);
                request.series.iter().flat_map(|series| {
                    let PointsType::DoublePoints(points) = series
                        .points
                        .as_ref()
                        .unwrap()
                        .points_type
                        .as_ref()
                        .unwrap()
                    else {
                        panic!("expected double points");
                    };
                    points.points.iter().map(|point| point.value as usize)
                })
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, (0..POINTS_PER_BATCH * BATCHES).collect::<Vec<_>>());
    }

    #[rstest::rstest]
    #[case::rendezvous_oversized_narrow(0, 1, 1, 8, 12, BatchShape::SingleChannel)]
    #[case::single_slot_exact_capacity(1, 1, 2, 2, 24, BatchShape::SingleChannel)]
    #[case::multi_dispatcher_remainder(3, 2, 3, 8, 24, BatchShape::SingleChannel)]
    #[case::wide_fitting(1, 2, 4, 4, 24, BatchShape::ManyChannels)]
    #[case::wide_underfilled(0, 1, 4, 1, 32, BatchShape::ManyChannels)]
    #[case::mixed_oversized(2, 3, 5, 12, 24, BatchShape::MixedChannels)]
    fn saturated_dispatchers_apply_backpressure_and_resume_incrementally(
        #[case] queue_capacity: usize,
        #[case] dispatcher_tasks: usize,
        #[case] record_capacity: usize,
        #[case] points_per_batch: usize,
        #[case] batches: usize,
        #[case] shape: BatchShape,
    ) {
        let gate = Arc::new(StepGate::default());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let consumer = Arc::new(StepConsumer {
            entered: entered_tx,
            gate: gate.clone(),
            requests: Mutex::new(Vec::new()),
        });
        let stream = Arc::new(NominalDatasetStream::new_with_consumer(
            consumer.clone(),
            NominalStreamOpts::default()
                .with_max_points_per_record(record_capacity)
                .with_max_buffered_requests(queue_capacity)
                .with_request_dispatcher_tasks(dispatcher_tasks)
                .with_max_request_delay(Duration::from_millis(1)),
        ));
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let producer_stream = stream.clone();
        let producer = thread::spawn(move || {
            for batch in 0..batches {
                enqueue_batch(&producer_stream, shape, batch, points_per_batch);
                admitted_tx.send(()).unwrap();
            }
        });

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("no request reached the controlled consumer");
        thread::sleep(Duration::from_millis(50));
        let initially_admitted = admitted_rx.try_iter().count();
        assert!(initially_admitted > 0);
        assert!(
            initially_admitted < batches,
            "all batches bypassed backpressure with queue={queue_capacity}, dispatchers={dispatcher_tasks}, cap={record_capacity}, shape={shape:?}"
        );

        gate.release(1);
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("releasing one request did not resume dispatch");
        thread::sleep(Duration::from_millis(50));
        let admitted_after_release = initially_admitted + admitted_rx.try_iter().count();
        assert!(
            admitted_after_release < batches,
            "one release drained an unbounded amount of producer work"
        );

        let total_points = points_per_batch * batches;
        gate.release(total_points);
        producer.join().unwrap();
        drop(stream);

        let requests = consumer.requests.lock();
        let mut values = requests
            .iter()
            .flat_map(|request| {
                let count: usize = request
                    .series
                    .iter()
                    .map(|series| {
                        points_len(
                            series
                                .points
                                .as_ref()
                                .unwrap()
                                .points_type
                                .as_ref()
                                .unwrap(),
                        )
                    })
                    .sum();
                assert!(count > 0);
                assert!(count <= record_capacity);
                request.series.iter().flat_map(|series| {
                    let PointsType::DoublePoints(points) = series
                        .points
                        .as_ref()
                        .unwrap()
                        .points_type
                        .as_ref()
                        .unwrap()
                    else {
                        panic!("expected double points");
                    };
                    points.points.iter().map(|point| point.value as usize)
                })
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, (0..total_points).collect::<Vec<_>>());
    }
}
