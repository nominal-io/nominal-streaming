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
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
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

impl Default for NominalDatasetStreamBuilder {
    fn default() -> Self {
        Self {
            stream_to_core: None,
            stream_to_file: None,
            file_fallback: None,
            listeners: Vec::new(),
            opts: NominalStreamOpts::default(),
        }
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

    fn file_consumer(&self) -> Option<AvroFileConsumer> {
        self.stream_to_file
            .as_ref()
            .map(|path| AvroFileConsumer::new_with_full_path(path).unwrap())
    }

    fn fallback_consumer(&self) -> Option<AvroFileConsumer> {
        self.file_fallback
            .as_ref()
            .map(|path| AvroFileConsumer::new_with_full_path(path).unwrap())
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

        let primary_handle = thread::Builder::new()
            .name("nmstream_primary".to_string())
            .spawn({
                let points_buffer = Arc::clone(&primary_buffer);
                let running = running.clone();
                let tx = request_tx.clone();
                move || {
                    batch_processor(running, points_buffer, tx, opts.max_request_delay);
                }
            })
            .unwrap();

        let secondary_handle = thread::Builder::new()
            .name("nmstream_secondary".to_string())
            .spawn({
                let secondary_buffer = Arc::clone(&secondary_buffer);
                let running = running.clone();
                move || {
                    batch_processor(
                        running,
                        secondary_buffer,
                        request_tx,
                        opts.max_request_delay,
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
                    move || {
                        debug!("starting request dispatcher");
                        request_dispatcher(running, unflushed_points, rx, consumer);
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
        }
    }

    pub fn double_writer<'a>(
        &'a self,
        channel_descriptor: &'a ChannelDescriptor,
    ) -> NominalDoubleWriter<'a> {
        NominalDoubleWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn string_writer<'a>(
        &'a self,
        channel_descriptor: &'a ChannelDescriptor,
    ) -> NominalStringWriter<'a> {
        NominalStringWriter {
            writer: NominalChannelWriter::new(self, channel_descriptor),
        }
    }

    pub fn integer_writer<'a>(
        &'a self,
        channel_descriptor: &'a ChannelDescriptor,
    ) -> NominalIntegerWriter<'a> {
        NominalIntegerWriter {
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
                info!("waiting for primary buffer to flush to append {new_count} points...");
                self.primary_handle.thread().unpark();
                &self.primary_buffer
            } else {
                info!("waiting for secondary buffer to flush to append {new_count} points...");
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
    channel: &'ds ChannelDescriptor,
    stream: &'ds NominalDatasetStream,
    last_flushed_at: Instant,
    unflushed: Vec<T>,
}

impl<T> NominalChannelWriter<'_, T>
where
    Vec<T>: IntoPoints,
{
    fn new<'ds>(
        stream: &'ds NominalDatasetStream,
        channel: &'ds ChannelDescriptor,
    ) -> NominalChannelWriter<'ds, T> {
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
            buf.extend(self.channel, to_flush);
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

struct SeriesBuffer {
    points: Mutex<HashMap<ChannelDescriptor, PointsType>>,
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
                (_, _) => {
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

    /// Checks if the buffer has enough capacity to add new points.
    /// Note that the buffer can be larger than MAX_POINTS_PER_RECORD if a single batch of points
    /// larger than MAX_POINTS_PER_RECORD is inserted while the buffer is empty. This avoids needing
    /// to handle splitting batches of points across multiple requests.
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
                    tags: tags
                        .map(|tags| tags.into_iter().collect())
                        .unwrap_or_default(),
                    points: Some(points_obj),
                }
            })
            .collect();
        let result_count = self
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
        let (point_count, series) = points_buffer.take();

        if points_buffer.notify() {
            debug!("notified one waiting thread after clearing points buffer");
        }

        let write_request = WriteRequestNominal { series };

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

        thread::park_timeout(max_request_delay);
    }
    debug!("batch processor thread exiting");
}

impl Drop for NominalDatasetStream {
    fn drop(&mut self) {
        debug!("starting drop for NominalDatasetStream");
        self.running.store(false, Ordering::Release);
        while self.unflushed_points.load(Ordering::Acquire) > 0 {
            debug!(
                "waiting for all points to be flushed before dropping stream, {} points remaining",
                self.unflushed_points.load(Ordering::Acquire)
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
    }
}
