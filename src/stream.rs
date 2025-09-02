use crate::client::{StreamingClient, PRODUCTION_STREAMING_CLIENT};
use crate::consumer::{NominalCoreConsumer, WriteRequestConsumer};
use conjure_object::{BearerToken, ResourceIdentifier};
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::{
    Channel, DoublePoint, DoublePoints, IntegerPoint, IntegerPoints, Points, Series, StringPoint,
    StringPoints, WriteRequestNominal,
};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct ChannelName(String);

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
struct ChannelKey(ChannelName, Vec<(String, String)>);

impl From<&str> for ChannelName {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

pub trait AuthProvider: Clone + Send + Sync {
    fn token(&self) -> Option<BearerToken>;
}

pub trait IntoPoints {
    fn into_points(self) -> PointsType;
}

impl IntoPoints for PointsType {
    fn into_points(self) -> PointsType {
        self
    }
}

impl IntoPoints for Vec<DoublePoint> {
    fn into_points(self) -> PointsType {
        PointsType::DoublePoints(DoublePoints { points: self })
    }
}

impl IntoPoints for Vec<StringPoint> {
    fn into_points(self) -> PointsType {
        PointsType::StringPoints(StringPoints { points: self })
    }
}

impl IntoPoints for Vec<IntegerPoint> {
    fn into_points(self) -> PointsType {
        PointsType::IntegerPoints(IntegerPoints { points: self })
    }
}

#[derive(Debug, Clone)]
pub struct NominalStreamOpts {
    pub max_points_per_record: usize,
    pub max_request_delay: Duration,
    pub max_buffered_requests: usize,
    pub request_dispatcher_tasks: usize,
}

impl Default for NominalStreamOpts {
    fn default() -> Self {
        Self {
            max_points_per_record: 250_000,
            max_request_delay: Duration::from_millis(100),
            max_buffered_requests: 4,
            request_dispatcher_tasks: 8,
        }
    }
}

pub struct NominalDatasourceStream {
    running: Arc<AtomicBool>,
    unflushed_points: Arc<AtomicUsize>,
    primary_buffer: Arc<SeriesBuffer>,
    secondary_buffer: Arc<SeriesBuffer>,
    primary_handle: thread::JoinHandle<()>,
    secondary_handle: thread::JoinHandle<()>,
}

impl NominalDatasourceStream {
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

        NominalDatasourceStream {
            running,
            unflushed_points,
            primary_buffer,
            secondary_buffer,
            primary_handle,
            secondary_handle,
        }
    }

    pub fn enqueue(
        &self,
        channel: ChannelName,
        tags: HashMap<String, String>,
        new_points: impl IntoPoints,
    ) {
        let new_points = new_points.into_points();
        let new_count = points_len(&new_points);

        self.unflushed_points
            .fetch_add(new_count, Ordering::Release);

        if self.primary_buffer.has_capacity(new_count) {
            debug!("adding {} points to primary buffer", new_count);
            self.primary_buffer.add_points(channel, tags, new_points);
        } else if self.secondary_buffer.has_capacity(new_count) {
            // primary buffer is definitely full
            self.primary_handle.thread().unpark();
            debug!("adding {} points to secondary buffer", new_count);
            self.secondary_buffer.add_points(channel, tags, new_points);
        } else {
            warn!("both buffers are full, picking least recently flushed buffer to add to");
            // both buffers are full - wait on the buffer that flushed least recently (i.e more
            // likely that it's nearly done)
            let buf = if self.primary_buffer < self.secondary_buffer {
                debug!("waiting for primary buffer to flush...");
                self.primary_handle.thread().unpark();
                &self.primary_buffer
            } else {
                debug!("waiting for secondary buffer to flush...");
                self.secondary_handle.thread().unpark();
                &self.secondary_buffer
            };
            buf.add_on_notify(channel, tags, new_points);
            debug!("added points after wait to chosen buffer")
        }
    }
}

struct SeriesBuffer {
    points: Mutex<HashMap<ChannelKey, PointsType>>,
    count: AtomicUsize,
    flush_time: AtomicU64,
    condvar: Condvar,
    max_capacity: usize,
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

    fn add_points(
        &self,
        channel: ChannelName,
        tags: HashMap<String, String>,
        new_points: PointsType,
    ) {
        self.inner_add_points(channel, tags, new_points, self.points.lock());
    }

    fn inner_add_points(
        &self,
        channel: ChannelName,
        tags: HashMap<String, String>,
        new_points: PointsType,
        mut points_guard: MutexGuard<HashMap<ChannelKey, PointsType>>,
    ) {
        let mut tags = tags.into_iter().collect::<Vec<_>>();
        tags.sort();
        self.count
            .fetch_add(points_len(&new_points), Ordering::Release);
        let key = ChannelKey(channel, tags);
        match (points_guard.get_mut(&key), new_points) {
            (None, new_points) => {
                points_guard.insert(key, new_points);
            }
            (Some(PointsType::DoublePoints(points)), PointsType::DoublePoints(new_points)) => {
                points
                    .points
                    .extend_from_slice(new_points.points.as_slice());
            }
            (Some(PointsType::StringPoints(points)), PointsType::StringPoints(new_points)) => {
                points
                    .points
                    .extend_from_slice(new_points.points.as_slice());
            }
            (Some(PointsType::IntegerPoints(points)), PointsType::IntegerPoints(new_points)) => {
                points
                    .points
                    .extend_from_slice(new_points.points.as_slice());
            }
            (Some(PointsType::DoublePoints(_)), PointsType::StringPoints(_)) => {
                // todo: return an error instead of panicking
                panic!(
                    "attempting to add points of the wrong type to an existing channel. expected: double. provided: string"
                )
            }
            (Some(PointsType::DoublePoints(_)), PointsType::IntegerPoints(_)) => {
                // todo: return an error instead of panicking
                panic!(
                    "attempting to add points of the wrong type to an existing channel. expected: double. provided: string"
                )
            }
            (Some(PointsType::StringPoints(_)), PointsType::DoublePoints(_)) => {
                // todo: return an error instead of panicking
                panic!(
                    "attempting to add points of the wrong type to an existing channel. expected: string. provided: double"
                )
            }
            (Some(PointsType::StringPoints(_)), PointsType::IntegerPoints(_)) => {
                // todo: return an error instead of panicking
                panic!(
                    "attempting to add points of the wrong type to an existing channel. expected: string. provided: double"
                )
            }
            (Some(PointsType::IntegerPoints(_)), PointsType::DoublePoints(_)) => {
                // todo: return an error instead of panicking
                panic!(
                    "attempting to add points of the wrong type to an existing channel. expected: string. provided: double"
                )
            }
            (Some(PointsType::IntegerPoints(_)), PointsType::StringPoints(_)) => {
                // todo: return an error instead of panicking
                panic!(
                    "attempting to add points of the wrong type to an existing channel. expected: string. provided: double"
                )
            }
        }
    }

    fn take(&self) -> (usize, Vec<Series>) {
        let mut points = self.points.lock();
        self.flush_time.store(
            UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64,
            Ordering::Release,
        );
        let result = points
            .drain()
            .map(|(ChannelKey(channel_name, tags), points)| {
                let channel = Channel {
                    name: channel_name.0,
                };
                let points_obj = Points {
                    points_type: Some(points),
                };
                Series {
                    channel: Some(channel),
                    tags: tags.into_iter().collect(),
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

    fn add_on_notify(
        &self,
        channel: ChannelName,
        tags: HashMap<String, String>,
        new_points: PointsType,
    ) {
        let mut points_lock = self.points.lock();
        // concurrency bug without this - the buffer could have been emptied since we
        // checked the count, so this will wait forever & block any new points from entering
        if !points_lock.is_empty() {
            self.condvar.wait(&mut points_lock);
        } else {
            debug!("buffer emptied since last check, skipping condvar wait");
        }
        self.inner_add_points(channel, tags, new_points, points_lock);
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
            warn!("request channel is full");
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

impl Drop for NominalDatasourceStream {
    fn drop(&mut self) {
        debug!("starting drop for NominalDatasourceStream");
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
