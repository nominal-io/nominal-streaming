use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::UNIX_EPOCH;

use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use parking_lot::Condvar;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use tracing::debug;

use crate::prelude::ChannelDescriptor;
use crate::types::IntoPoints;
use crate::types::PointsTypeExt;

pub(crate) struct SeriesBuffer {
    points: Mutex<HashMap<ChannelDescriptor, PointsType>>,
    count: AtomicUsize,
    flush_time: AtomicU64,
    condvar: Condvar,
    max_capacity: usize,
}

pub(crate) struct SeriesBufferGuard<'sb> {
    sb: MutexGuard<'sb, HashMap<ChannelDescriptor, PointsType>>,
    count: &'sb AtomicUsize,
}

impl SeriesBufferGuard<'_> {
    pub(crate) fn extend(
        &mut self,
        channel_descriptor: &ChannelDescriptor,
        points: impl IntoPoints,
    ) {
        let points = points.into_points();
        let new_point_count = points.len();

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
                    | PointsType::StringPoints(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::StringPoints(_),
                    PointsType::DoublePoints(_)
                    | PointsType::IntegerPoints(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::IntegerPoints(_),
                    PointsType::DoublePoints(_)
                    | PointsType::StringPoints(_)
                    | PointsType::ArrayPoints(_)
                    | PointsType::StructPoints(_),
                )
                | (
                    PointsType::ArrayPoints(_),
                    PointsType::DoublePoints(_)
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
                    PointsType::StructPoints(_),
                    PointsType::DoublePoints(_)
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
    pub(crate) fn new(capacity: usize) -> Self {
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
    pub(crate) fn has_capacity(&self, new_points_count: usize) -> bool {
        let count = self.count.load(Ordering::Acquire);
        count == 0 || count + new_points_count <= self.max_capacity
    }

    pub(crate) fn lock(&self) -> SeriesBufferGuard<'_> {
        SeriesBufferGuard {
            sb: self.points.lock(),
            count: &self.count,
        }
    }

    pub(crate) fn take(&self) -> (usize, Vec<Series>) {
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

    pub(crate) fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    pub(crate) fn on_notify(&self, on_notify: impl FnOnce(SeriesBufferGuard)) {
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

    pub(crate) fn notify(&self) -> bool {
        self.condvar.notify_one()
    }
}
