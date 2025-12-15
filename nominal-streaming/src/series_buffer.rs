use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::UNIX_EPOCH;

use inner::SeriesBufferInner;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use parking_lot::Condvar;
use parking_lot::MutexGuard;
use tracing::debug;

use crate::prelude::ChannelDescriptor;
use crate::types::IntoPoints;
use crate::types::PointsTypeExt;

pub(crate) struct SeriesBuffer {
    inner: SeriesBufferInner,
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

    fn take(&mut self) -> (usize, Vec<Series>) {
        let result = self
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
            inner: SeriesBufferInner::new(),
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
        let count = self.count();
        count == 0 || count + new_points_count <= self.max_capacity
    }

    pub(crate) fn with_lock<R>(&self, f: impl FnOnce(SeriesBufferGuard<'_>) -> R) -> R {
        self.inner.with_lock(f)
    }

    pub(crate) fn _lock(&self) -> SeriesBufferGuard<'_> {
        self.inner.lock()
    }

    pub(crate) fn take(&self) -> (usize, Vec<Series>) {
        self.with_lock(|mut guard| {
            self.flush_time.store(
                UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64,
                Ordering::Release,
            );
            guard.take()
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn count(&self) -> usize {
        self.inner.count()
    }

    pub(crate) fn on_notify(&self, on_notify: impl FnOnce(SeriesBufferGuard)) {
        self.with_lock(|mut guard| {
            // concurrency bug without this - the buffer could have been emptied since we
            // checked the count, so this will wait forever & block any new points from entering
            if !guard.sb.is_empty() {
                self.condvar.wait(&mut guard.sb);
            } else {
                debug!("buffer emptied since last check, skipping condvar wait");
            }
            on_notify(guard);
        })
    }

    pub(crate) fn notify(&self) -> bool {
        self.condvar.notify_one()
    }
}

mod inner {
    use parking_lot::Mutex;

    use super::*;

    pub(super) struct SeriesBufferInner {
        points: Mutex<HashMap<ChannelDescriptor, PointsType>>,
        count: AtomicUsize,
    }

    impl SeriesBufferInner {
        pub(super) fn new() -> Self {
            Self {
                points: Mutex::new(HashMap::new()),
                count: AtomicUsize::new(0),
            }
        }

        pub(super) fn count(&self) -> usize {
            self.count.load(Ordering::Acquire)
        }

        pub(super) fn is_empty(&self) -> bool {
            self.count() == 0
        }

        pub(super) fn lock(&self) -> SeriesBufferGuard<'_> {
            SeriesBufferGuard {
                sb: self.points.lock(),
                count: &self.count,
            }
        }

        pub(super) fn with_lock<R>(&self, f: impl FnOnce(SeriesBufferGuard<'_>) -> R) -> R {
            f(self.lock())
        }
    }
}
