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
use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point;
use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Points;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use parking_lot::Condvar;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use tracing::debug;
use tracing::error;
use tracing::info;

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

#[derive(Debug, Clone)]
pub struct NominalStreamOpts {
    /// Upper bound on the number of points in any single downstream
    /// `WriteRequest`. Producer submissions exceeding this are split internally
    /// (see `NominalDatasetStream::enqueue` and `enqueue_batch`); the buffer
    /// `SeriesBuffer::lock_if_capacity` enforces the bound atomically so the
    /// invariant holds under concurrent producers.
    pub max_points_per_record: usize,
    pub max_request_delay: Duration,
    pub max_buffered_requests: usize,
    pub request_dispatcher_tasks: usize,
    pub base_api_url: String,
}

impl Default for NominalStreamOpts {
    fn default() -> Self {
        Self {
            max_points_per_record: 250_000,
            max_request_delay: Duration::from_millis(100),
            max_buffered_requests: 4,
            request_dispatcher_tasks: 8,
            base_api_url: PRODUCTION_API_URL.to_string(),
        }
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

    pub fn new_with_consumer<C: WriteRequestConsumer + 'static>(
        consumer: C,
        opts: NominalStreamOpts,
    ) -> Self {
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

    /// Enqueue points for a single channel. Submissions of any size are accepted;
    /// inputs larger than `max_points_per_record` are split internally and produce
    /// `ceil(N / max_points_per_record)` downstream `WriteRequest`s. Each chunk
    /// blocks for buffer capacity independently, so backpressure under load
    /// applies per-chunk rather than per-call.
    pub fn enqueue(&self, channel_descriptor: &ChannelDescriptor, new_points: impl IntoPoints) {
        let new_points = new_points.into_points();
        let total = points_len(&new_points);
        let cap = self.opts.max_points_per_record;

        let chunks = chunk_points(new_points, cap);
        if chunks.len() > 1 {
            debug!(
                "chunking {total} points for {channel_descriptor:?} into {} requests of ≤{cap} points",
                chunks.len()
            );
        }
        for chunk in chunks {
            let n = points_len(&chunk);
            self.when_capacity(n, |mut sb| sb.extend(channel_descriptor, chunk));
        }
    }

    /// Enqueue points for multiple channels in one or more critical sections.
    ///
    /// Greedy-packs the input into rounds whose total ≤ `max_points_per_record`
    /// and extends the buffer once per round (each round is a single critical
    /// section across all of its channels). Single-channel oversize entries are
    /// split across multiple rounds — the channel descriptor is preserved on
    /// every resulting `WriteRequest`.
    pub fn enqueue_batch(&self, batch: Vec<(ChannelDescriptor, PointsType)>) {
        let cap = self.opts.max_points_per_record;
        let batch_total: usize = batch.iter().map(|(_, p)| points_len(p)).sum();
        let batch_channels = batch.len();

        let rounds = chunk_by_capacity(batch, cap);
        if rounds.len() > 1 {
            debug!(
                "chunking {batch_total} points across {batch_channels} channels into {} rounds of ≤{cap} points",
                rounds.len()
            );
        }
        for round in rounds {
            let total: usize = round.iter().map(|(_, p)| points_len(p)).sum();
            if total == 0 {
                continue;
            }
            self.when_capacity(total, move |mut sb| {
                for (desc, pts) in round {
                    sb.extend(&desc, pts);
                }
            });
        }
    }

    fn when_capacity(&self, new_count: usize, callback: impl FnOnce(SeriesBufferGuard)) {
        self.unflushed_points
            .fetch_add(new_count, Ordering::Release);

        // Atomic check-and-acquire on each buffer in turn. `lock_if_capacity`
        // verifies capacity under the lock, so two concurrent producers cannot
        // both pass the check and overflow the buffer.
        if let Some(guard) = self.primary_buffer.lock_if_capacity(new_count) {
            debug!("adding {} points to primary buffer", new_count);
            callback(guard);
            return;
        }
        if let Some(guard) = self.secondary_buffer.lock_if_capacity(new_count) {
            // primary buffer is full; nudge its processor so it drains
            self.primary_handle.thread().unpark();
            debug!("adding {} points to secondary buffer", new_count);
            callback(guard);
            return;
        }

        // Both buffers full: wait on the older one, which has had longer to
        // drain. `wait_for_capacity` re-checks the invariant on each wakeup,
        // so spurious or partial drains don't let an oversize submission slip
        // through.
        let buf = if self.primary_buffer < self.secondary_buffer {
            info!("waiting for primary buffer to flush to append {new_count} points...");
            self.primary_handle.thread().unpark();
            &self.primary_buffer
        } else {
            info!("waiting for secondary buffer to flush to append {new_count} points...");
            self.secondary_handle.thread().unpark();
            &self.secondary_buffer
        };
        buf.wait_for_capacity(new_count, callback);
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
        info!(
            "flushing writer for {:?} with {} points",
            self.channel,
            self.unflushed.len()
        );
        self.stream.when_capacity(self.unflushed.len(), |mut buf| {
            let to_flush: Vec<T> = self.unflushed.drain(..).collect();
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
        info!("flushing then dropping writer for: {:?}", self.channel);
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

        if !self.sb.contains_key(channel_descriptor) {
            self.sb.insert(channel_descriptor.clone(), points);
        } else {
            match (self.sb.get_mut(channel_descriptor).unwrap(), points) {
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

    /// Acquire the buffer lock and verify capacity for `n` additional points
    /// atomically. Returns the guard if `count + n <= max_capacity`, or `None`
    /// (releasing the lock) otherwise.
    ///
    /// Use this instead of an external capacity check followed by `lock()` —
    /// the two-step pattern is racy: two concurrent producers can both observe
    /// available capacity, both acquire the lock sequentially, and overflow it.
    fn lock_if_capacity(&self, n: usize) -> Option<SeriesBufferGuard<'_>> {
        // Fast path: skip the lock when an atomic load already proves there's
        // no room. False negatives (count drains between this load and the
        // caller's next attempt) just cost a missed opportunity, not safety.
        if self.count.load(Ordering::Acquire) + n > self.max_capacity {
            return None;
        }
        // Slow path: take the lock and re-check. This in-lock check closes
        // the time-of-check-to-time-of-use race: between the fast-path load
        // and now, another producer could have raced ahead and filled the
        // buffer, so the earlier read may be stale.
        let sb = self.points.lock();
        if self.count.load(Ordering::Acquire) + n <= self.max_capacity {
            Some(SeriesBufferGuard {
                sb,
                count: &self.count,
            })
        } else {
            None
        }
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
                    tags: tags
                        .map(|tags| tags.into_iter().collect())
                        .unwrap_or_default(),
                    points: Some(points_obj),
                }
            })
            .collect();
        let result_count = points
            .count
            .fetch_update(Ordering::Release, Ordering::Acquire, |_| Some(0))
            .unwrap();
        (result_count, result)
    }

    fn is_empty(&self) -> bool {
        self.count() == 0
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Wait under the buffer lock until there is capacity for `n` additional
    /// points, then run `callback` while still holding the lock. Re-checks the
    /// capacity invariant on every wakeup so multiple waiters with varying `n`
    /// can all eventually proceed as space frees up — the `notify_all` paired
    /// with this loop is the standard "broadcast and re-check" condvar
    /// pattern.
    fn wait_for_capacity(&self, n: usize, callback: impl FnOnce(SeriesBufferGuard)) {
        let mut points_lock = self.points.lock();
        let initial_count = self.count.load(Ordering::Acquire);
        if initial_count + n <= self.max_capacity {
            debug!("buffer drained between caller's check and lock acquisition, skipping condvar wait for {n} points");
        } else {
            debug!(
                "waiting for capacity for {n} points (current: {initial_count} / {})",
                self.max_capacity
            );
        }
        while self.count.load(Ordering::Acquire) + n > self.max_capacity {
            self.condvar.wait(&mut points_lock);
        }
        callback(SeriesBufferGuard {
            sb: points_lock,
            count: &self.count,
        });
    }

    /// Wake every thread waiting in `wait_for_capacity` so each can re-check
    /// the capacity invariant under the lock. Returns true if any thread was
    /// woken.
    fn notify(&self) -> bool {
        self.condvar.notify_all() > 0
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

        let write_request = WriteRequestNominal {
            series,
            session_name: None,
        };

        if request_chan.is_full() {
            debug!("ready to queue request but request channel is full");
        }
        let rep = request_chan.send((write_request, point_count));
        debug!("queued request for processing");
        if rep.is_err() {
            error!("failed to send request to dispatcher");
        } else {
            debug!("finished submitting request");
        }

        #[cfg(feature = "instrument")]
        bp_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);

        thread::park_timeout(max_request_delay);
    }
    debug!("batch processor thread exiting");
}

impl Drop for NominalDatasetStream {
    fn drop(&mut self) {
        debug!("starting drop for NominalDatasetStream");
        self.running.store(false, Ordering::Release);
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
                    info!("all points flushed, closing dispatcher thread");
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

/// Split a `PointsType` into a sequence of `PointsType` values, each with
/// at most `cap` points. Empty input → empty Vec. `ArrayPoints` with
/// `array_type: None` is treated as empty (zero points).
///
/// Caller invariant: `cap > 0` (default `max_points_per_record` is 250k).
fn chunk_points(points: PointsType, cap: usize) -> Vec<PointsType> {
    assert!(cap > 0, "chunk_points: cap must be > 0");
    let n = points_len(&points);
    if n == 0 {
        return vec![];
    }
    if n <= cap {
        return vec![points];
    }
    match points {
        PointsType::DoublePoints(p) => p
            .points
            .chunks(cap)
            .map(|c| PointsType::DoublePoints(DoublePoints { points: c.to_vec() }))
            .collect(),
        PointsType::IntegerPoints(p) => p
            .points
            .chunks(cap)
            .map(|c| PointsType::IntegerPoints(IntegerPoints { points: c.to_vec() }))
            .collect(),
        PointsType::Uint64Points(p) => p
            .points
            .chunks(cap)
            .map(|c| PointsType::Uint64Points(Uint64Points { points: c.to_vec() }))
            .collect(),
        PointsType::StringPoints(p) => p
            .points
            .chunks(cap)
            .map(|c| PointsType::StringPoints(StringPoints { points: c.to_vec() }))
            .collect(),
        PointsType::StructPoints(p) => p
            .points
            .chunks(cap)
            .map(|c| PointsType::StructPoints(StructPoints { points: c.to_vec() }))
            .collect(),
        PointsType::ArrayPoints(ArrayPoints {
            array_type: Some(ArrayType::DoubleArrayPoints(p)),
        }) => p
            .points
            .chunks(cap)
            .map(|c| {
                PointsType::ArrayPoints(ArrayPoints {
                    array_type: Some(ArrayType::DoubleArrayPoints(DoubleArrayPoints {
                        points: c.to_vec(),
                    })),
                })
            })
            .collect(),
        PointsType::ArrayPoints(ArrayPoints {
            array_type: Some(ArrayType::StringArrayPoints(p)),
        }) => p
            .points
            .chunks(cap)
            .map(|c| {
                PointsType::ArrayPoints(ArrayPoints {
                    array_type: Some(ArrayType::StringArrayPoints(StringArrayPoints {
                        points: c.to_vec(),
                    })),
                })
            })
            .collect(),
        // Unreachable: n == 0 path returned earlier.
        PointsType::ArrayPoints(ArrayPoints { array_type: None }) => vec![],
    }
}

/// Group a multi-channel batch into rounds where each round's total points ≤ `cap`
/// and each entry within a round individually has ≤ `cap` points.
///
/// Two-phase: (1) pre-split any oversize entry via `chunk_points` (preserving
/// channel descriptor), (2) greedy-pack the pre-split entries into rounds,
/// starting a new round whenever the next entry would overflow the current.
///
/// Entries with 0 points are dropped (they would produce no `WriteRequest` anyway).
///
/// Caller invariant: `cap > 0`.
fn chunk_by_capacity(
    batch: Vec<(ChannelDescriptor, PointsType)>,
    cap: usize,
) -> Vec<Vec<(ChannelDescriptor, PointsType)>> {
    assert!(cap > 0, "chunk_by_capacity: cap must be > 0");

    // Phase 1: pre-split.
    // Each pre-split entry clones the ChannelDescriptor (name String + tags BTreeMap).
    // For an N-point single-channel batch with cap C, that's ceil(N/C) clones.
    // If profiling later shows this is hot, switch to Arc<ChannelDescriptor> in the
    // round tuples to share the descriptor across pre-split entries.
    let pre_split: Vec<(ChannelDescriptor, PointsType)> = batch
        .into_iter()
        .flat_map(|(cd, pts)| {
            chunk_points(pts, cap)
                .into_iter()
                .map(move |p| (cd.clone(), p))
        })
        .collect();

    // Phase 2: greedy-pack.
    let mut rounds: Vec<Vec<(ChannelDescriptor, PointsType)>> = Vec::new();
    let mut current: Vec<(ChannelDescriptor, PointsType)> = Vec::new();
    let mut current_total = 0usize;

    for (cd, pts) in pre_split {
        let n = points_len(&pts);
        if current_total + n > cap && current_total > 0 {
            rounds.push(std::mem::take(&mut current));
            current_total = 0;
        }
        current.push((cd, pts));
        current_total += n;
    }
    if !current.is_empty() {
        rounds.push(current);
    }
    rounds
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use nominal_api::tonic::google::protobuf::Timestamp;
    use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
    use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
    use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StructPoints;
    use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point;
    use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Points;
    use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;

    use crate::client::PRODUCTION_API_URL;
    use crate::consumer::ConsumerResult;
    use crate::consumer::WriteRequestConsumer;
    use crate::stream::NominalDatasetStream;
    use crate::stream::NominalStreamOpts;
    use crate::types::ChannelDescriptor;

    // ---- Construction helpers (English-named, single-purpose) ----

    fn timestamp_at(seconds: i64) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos: 0 })
    }

    fn channel(name: &str) -> ChannelDescriptor {
        ChannelDescriptor::new(name)
    }

    fn integer_points(start: i64, count: usize) -> Vec<IntegerPoint> {
        (start..start + count as i64)
            .map(|value| IntegerPoint {
                timestamp: timestamp_at(value),
                value,
            })
            .collect()
    }

    fn integer_pointstype(start: i64, count: usize) -> PointsType {
        PointsType::IntegerPoints(IntegerPoints {
            points: integer_points(start, count),
        })
    }

    // ---- Recording consumer + stream factory ----

    #[derive(Debug, Default)]
    struct RecordingConsumer {
        requests: Mutex<Vec<WriteRequestNominal>>,
    }

    impl WriteRequestConsumer for Arc<RecordingConsumer> {
        fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(())
        }
    }

    /// Create a stream with the given per-record cap, paired with a consumer
    /// that records every emitted `WriteRequest` for inspection. The buffer +
    /// dispatcher are sized generously since these tests exercise chunking,
    /// not backpressure.
    fn create_test_stream(
        max_points_per_record: usize,
    ) -> (Arc<RecordingConsumer>, NominalDatasetStream) {
        let consumer = Arc::new(RecordingConsumer::default());
        let stream = NominalDatasetStream::new_with_consumer(
            consumer.clone(),
            NominalStreamOpts {
                max_points_per_record,
                max_request_delay: Duration::from_millis(100),
                max_buffered_requests: 8,
                request_dispatcher_tasks: 1,
                base_api_url: PRODUCTION_API_URL.to_string(),
            },
        );
        (consumer, stream)
    }

    // ---- Inspection helpers (work across PointsType variants) ----

    /// Per-request point counts (across all variants) in order recorded.
    fn recorded_request_sizes(consumer: &Arc<RecordingConsumer>) -> Vec<usize> {
        consumer
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|req| {
                req.series
                    .iter()
                    .map(|s| {
                        s.points
                            .as_ref()
                            .and_then(|pts| pts.points_type.as_ref())
                            .map(super::points_len)
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .collect()
    }

    /// Flatten every recorded `IntegerPoint`'s value, in the order the
    /// dispatcher saw them. (Each `WriteRequest`'s points are in submission
    /// order; cross-request ordering is non-deterministic under buffer
    /// fan-out, so callers that care about ordering should look at one
    /// request at a time via `recorded_integer_values_per_request`.)
    fn recorded_integer_values(consumer: &Arc<RecordingConsumer>) -> Vec<i64> {
        consumer
            .requests
            .lock()
            .unwrap()
            .iter()
            .flat_map(|req| {
                req.series.iter().flat_map(|s| {
                    s.points
                        .as_ref()
                        .and_then(|pts| pts.points_type.as_ref())
                        .map(integer_values_in)
                        .unwrap_or_default()
                })
            })
            .collect()
    }

    /// `IntegerPoint` values grouped by recorded request, then by series.
    /// Use this when intra-request ordering matters.
    fn recorded_integer_values_per_request(consumer: &Arc<RecordingConsumer>) -> Vec<Vec<i64>> {
        consumer
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|req| {
                req.series
                    .iter()
                    .flat_map(|s| {
                        s.points
                            .as_ref()
                            .and_then(|pts| pts.points_type.as_ref())
                            .map(integer_values_in)
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .collect()
    }

    fn integer_values_in(pts: &PointsType) -> Vec<i64> {
        match pts {
            PointsType::IntegerPoints(ip) => ip.points.iter().map(|p| p.value).collect(),
            _ => vec![],
        }
    }

    // ============================================================
    // enqueue / enqueue_batch behaviors
    // ============================================================

    #[test_log::test]
    fn enqueue_chunks_oversize_input() {
        let cap = 250;
        let count = 1000;
        let (consumer, stream) = create_test_stream(cap);

        stream.enqueue(&channel("ch"), integer_points(0, count));
        drop(stream);

        let sizes = recorded_request_sizes(&consumer);
        for size in &sizes {
            assert!(*size <= cap, "request of {size} points exceeds cap {cap}");
        }
        assert_eq!(sizes.iter().sum::<usize>(), count, "all data lands");

        // Per-request ordering: each request's values are monotonic. Cross-request
        // ordering is racy under buffer fan-out, so we don't assert it.
        for values in recorded_integer_values_per_request(&consumer) {
            for w in values.windows(2) {
                assert!(w[0] < w[1], "intra-request order broken: {values:?}");
            }
        }

        // Multiset completeness: every submitted value appears exactly once.
        let mut all = recorded_integer_values(&consumer);
        all.sort();
        let expected: Vec<i64> = (0..count as i64).collect();
        assert_eq!(all, expected);
    }

    #[test_log::test]
    fn enqueue_passes_through_undersize_input() {
        let cap = 1000;
        let (consumer, stream) = create_test_stream(cap);

        stream.enqueue(&channel("ch"), integer_points(0, 50));
        drop(stream);

        assert_eq!(recorded_request_sizes(&consumer), vec![50]);
    }

    #[test_log::test]
    fn enqueue_batch_chunks_when_oversize() {
        // Combines two scenarios: (1) sub-cap channels whose total exceeds cap,
        // and (2) a single oversize entry that splits across rounds. Both must
        // produce only ≤cap requests with all data preserved.
        let cap = 100;
        let (consumer, stream) = create_test_stream(cap);

        // Total = 60 + 60 + 60 = 180 > cap; plus one oversize 250-point entry.
        let batch = vec![
            (channel("a"), integer_pointstype(0, 60)),
            (channel("b"), integer_pointstype(100, 60)),
            (channel("c"), integer_pointstype(200, 60)),
            (channel("big"), integer_pointstype(1000, 250)),
        ];
        stream.enqueue_batch(batch);
        drop(stream);

        let sizes = recorded_request_sizes(&consumer);
        for size in &sizes {
            assert!(*size <= cap, "request {size} > cap {cap}");
        }
        assert_eq!(sizes.iter().sum::<usize>(), 60 * 3 + 250);
    }

    #[test_log::test]
    fn enqueue_batch_preserves_descriptor_across_split() {
        // Tagged descriptor on an oversize entry — every resulting request must
        // carry the same name + tags, even though the submission was split.
        let cap = 100;
        let (consumer, stream) = create_test_stream(cap);

        let tagged =
            ChannelDescriptor::with_tags("tagged", [("env", "prod"), ("region", "us-east")]);
        stream.enqueue_batch(vec![(tagged.clone(), integer_pointstype(0, 250))]);
        drop(stream);

        let requests = consumer.requests.lock().unwrap();
        for req in requests.iter() {
            for series in &req.series {
                let chan = series.channel.as_ref().unwrap();
                assert_eq!(chan.name, tagged.name);
                let want: std::collections::HashMap<_, _> = tagged
                    .tags
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                assert_eq!(series.tags, want, "tags preserved across split");
            }
        }
    }

    #[test_log::test]
    fn enqueue_batch_passes_through_undersize_input() {
        let cap = 1000;
        let (consumer, stream) = create_test_stream(cap);

        stream.enqueue_batch(vec![
            (channel("a"), integer_pointstype(0, 50)),
            (channel("b"), integer_pointstype(0, 50)),
        ]);
        drop(stream);

        // Sub-cap multi-channel batch lands as exactly one request of 100 points.
        assert_eq!(recorded_request_sizes(&consumer), vec![100]);
    }

    #[test_log::test]
    fn concurrent_producers_never_overflow_buffer() {
        // 4 producers × 8 chunks of cap-many points each, racing into the same
        // stream. The capacity invariant must hold: no `WriteRequest` exceeds
        // `cap`, and no data is lost.
        use std::thread;

        let cap = 100;
        let producers = 4usize;
        let chunks_per_producer = 8usize;
        let (consumer, stream) = create_test_stream(cap);
        let stream = Arc::new(stream);

        thread::scope(|s| {
            for p in 0..producers {
                let stream = Arc::clone(&stream);
                s.spawn(move || {
                    let cd = channel(&format!("p{p}"));
                    for c in 0..chunks_per_producer {
                        let start = (p as i64) * 1_000_000 + (c as i64) * 1_000;
                        stream.enqueue(&cd, integer_points(start, cap));
                    }
                });
            }
        });
        drop(stream);

        let sizes = recorded_request_sizes(&consumer);
        for size in &sizes {
            assert!(
                *size <= cap,
                "concurrent producers produced an oversize request: {size} > {cap}"
            );
        }
        assert_eq!(
            sizes.iter().sum::<usize>(),
            producers * chunks_per_producer * cap
        );
    }

    // ============================================================
    // chunk_points / chunk_by_capacity helper invariants
    // ============================================================

    /// Build one instance of every `PointsType` variant, paired with a label
    /// for failure messages. Each instance has `count` points starting at 0.
    fn pointstype_variants(count: usize) -> Vec<(&'static str, PointsType)> {
        let n = count as i64;
        vec![
            (
                "DoublePoints",
                PointsType::DoublePoints(DoublePoints {
                    points: (0..n)
                        .map(|i| DoublePoint {
                            timestamp: timestamp_at(i),
                            value: i as f64,
                        })
                        .collect(),
                }),
            ),
            (
                "IntegerPoints",
                PointsType::IntegerPoints(IntegerPoints {
                    points: (0..n)
                        .map(|i| IntegerPoint {
                            timestamp: timestamp_at(i),
                            value: i,
                        })
                        .collect(),
                }),
            ),
            (
                "Uint64Points",
                PointsType::Uint64Points(Uint64Points {
                    points: (0..n)
                        .map(|i| Uint64Point {
                            timestamp: timestamp_at(i),
                            value: i as u64,
                        })
                        .collect(),
                }),
            ),
            (
                "StringPoints",
                PointsType::StringPoints(StringPoints {
                    points: (0..n)
                        .map(|i| StringPoint {
                            timestamp: timestamp_at(i),
                            value: format!("v{i}"),
                        })
                        .collect(),
                }),
            ),
            (
                "StructPoints",
                PointsType::StructPoints(StructPoints {
                    points: (0..n)
                        .map(|i| StructPoint {
                            timestamp: timestamp_at(i),
                            json_string: format!("{{\"i\":{i}}}"),
                        })
                        .collect(),
                }),
            ),
            (
                "ArrayPoints/Double",
                PointsType::ArrayPoints(ArrayPoints {
                    array_type: Some(ArrayType::DoubleArrayPoints(DoubleArrayPoints {
                        points: (0..n)
                            .map(|i| DoubleArrayPoint {
                                timestamp: timestamp_at(i),
                                value: vec![i as f64],
                            })
                            .collect(),
                    })),
                }),
            ),
            (
                "ArrayPoints/String",
                PointsType::ArrayPoints(ArrayPoints {
                    array_type: Some(ArrayType::StringArrayPoints(StringArrayPoints {
                        points: (0..n)
                            .map(|i| StringArrayPoint {
                                timestamp: timestamp_at(i),
                                value: vec![format!("s{i}")],
                            })
                            .collect(),
                    })),
                }),
            ),
        ]
    }

    #[test_log::test]
    fn chunk_points_handles_all_pointstype_variants() {
        let cap = 3;
        let count = 7; // not a multiple of cap → tests the trailing-partial chunk

        for (label, original) in pointstype_variants(count) {
            assert_eq!(super::points_len(&original), count, "{label}: setup");
            let chunks = super::chunk_points(original, cap);
            assert_eq!(chunks.len(), count.div_ceil(cap), "{label}: chunk count");
            assert_eq!(
                chunks.iter().map(super::points_len).sum::<usize>(),
                count,
                "{label}: total preserved",
            );
            for chunk in &chunks {
                assert!(super::points_len(chunk) <= cap, "{label}: chunk > cap");
            }
        }
    }

    #[test_log::test]
    fn chunk_points_handles_empty_and_undersize_input() {
        let cap = 3;

        // Empty input → empty Vec.
        let empty = PointsType::DoublePoints(DoublePoints { points: vec![] });
        assert!(super::chunk_points(empty, cap).is_empty());

        // ArrayPoints with array_type: None counts as zero points → empty Vec.
        let no_array = PointsType::ArrayPoints(ArrayPoints { array_type: None });
        assert!(super::chunk_points(no_array, cap).is_empty());

        // Under-cap input → single-element Vec containing the original.
        let small = integer_pointstype(0, 1);
        let result = super::chunk_points(small, cap);
        assert_eq!(result.len(), 1);
        assert_eq!(super::points_len(&result[0]), 1);
    }

    #[test_log::test]
    fn chunk_by_capacity_packs_under_cap_into_single_round() {
        // 50 + 30 = 80 ≤ cap=100 → exactly one round containing both entries.
        let rounds = super::chunk_by_capacity(
            vec![
                (channel("a"), integer_pointstype(0, 50)),
                (channel("b"), integer_pointstype(0, 30)),
            ],
            100,
        );
        assert_eq!(rounds.len(), 1);
        assert_eq!(rounds[0].len(), 2);
    }

    #[test_log::test]
    fn chunk_by_capacity_starts_new_round_when_next_entry_overflows() {
        // 40 + 40 = 80 ≤ cap=100 in round 1; +40 would overflow → round 2.
        // Tests the strict `>` boundary check (a `>=` mutation would split
        // earlier and produce the wrong round structure).
        let rounds = super::chunk_by_capacity(
            vec![
                (channel("a"), integer_pointstype(0, 40)),
                (channel("b"), integer_pointstype(0, 40)),
                (channel("c"), integer_pointstype(0, 40)),
            ],
            100,
        );
        let totals: Vec<usize> = rounds
            .iter()
            .map(|r| r.iter().map(|(_, p)| super::points_len(p)).sum())
            .collect();
        assert_eq!(totals, vec![80, 40]);
    }

    #[test_log::test]
    fn chunk_by_capacity_splits_oversize_entry_across_rounds() {
        // A single 250-point entry with cap=100 must split into 100/100/50.
        let rounds =
            super::chunk_by_capacity(vec![(channel("big"), integer_pointstype(0, 250))], 100);
        let totals: Vec<usize> = rounds
            .iter()
            .map(|r| r.iter().map(|(_, p)| super::points_len(p)).sum())
            .collect();
        assert_eq!(totals, vec![100, 100, 50]);
    }

    #[test_log::test]
    fn chunk_by_capacity_drops_empty_entries() {
        // Entries with zero points produce no `WriteRequest` and are dropped.
        let rounds = super::chunk_by_capacity(
            vec![
                (channel("empty"), integer_pointstype(0, 0)),
                (channel("real"), integer_pointstype(0, 50)),
            ],
            100,
        );
        assert_eq!(rounds.len(), 1);
        assert_eq!(rounds[0].len(), 1);
        assert_eq!(rounds[0][0].0.name, "real");
    }

    #[test_log::test]
    fn chunk_by_capacity_handles_empty_input() {
        let empty: Vec<(ChannelDescriptor, PointsType)> = vec![];
        assert!(super::chunk_by_capacity(empty, 100).is_empty());
    }

    // ============================================================
    // Buffer capacity primitive
    // ============================================================

    #[test_log::test]
    fn lock_if_capacity_rejects_when_full() {
        let buf = super::SeriesBuffer::new(100);

        // Empty buffer rejects oversize, accepts undersize / exactly-cap / zero.
        // Each `is_some()` / `is_none()` drops the temporary guard at the
        // statement boundary, so the next call can re-acquire the lock.
        assert!(buf.lock_if_capacity(150).is_none());
        assert!(buf.lock_if_capacity(50).is_some());
        assert!(buf.lock_if_capacity(100).is_some());
        assert!(buf.lock_if_capacity(0).is_some());
    }
}
