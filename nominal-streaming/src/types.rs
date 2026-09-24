use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use conjure_object::BearerToken;
use nominal_api::objects::api::rids::WorkspaceRid;
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

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// A descriptor for a channel.
///
/// Note that this is used internally to compare channels.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct ChannelDescriptor {
    /// The name of the channel.
    pub name: String,
    /// The tags associated with the channel, if any.
    ///
    /// Shared rather than owned: a wide record is thousands of channels written at one timestamp
    /// all carrying the same tags, so a descriptor per channel would otherwise mean copying the
    /// same map thousands of times per write.
    pub tags: Option<Arc<BTreeMap<String, String>>>,
}

impl ChannelDescriptor {
    /// Creates a new channel descriptor from the given `name`.
    ///
    /// If you would like to include tags, see also [`Self::with_tags`].
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tags: None,
        }
    }

    /// Creates a new channel descriptor from the given `name` and `tags`.
    pub fn with_tags(
        name: impl Into<String>,
        tags: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self {
            name: name.into(),
            tags: Some(Arc::new(
                tags.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            )),
        }
    }
}

pub trait AuthProvider: Clone + Send + Sync {
    fn token(&self) -> Option<BearerToken>;

    fn workspace_rid(&self) -> Option<WorkspaceRid> {
        None
    }
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

impl IntoPoints for Vec<StructPoint> {
    fn into_points(self) -> PointsType {
        PointsType::StructPoints(StructPoints { points: self })
    }
}

impl IntoPoints for Vec<Uint64Point> {
    fn into_points(self) -> PointsType {
        PointsType::Uint64Points(Uint64Points { points: self })
    }
}

impl IntoPoints for Vec<DoubleArrayPoint> {
    fn into_points(self) -> PointsType {
        PointsType::ArrayPoints(ArrayPoints {
            array_type: Some(ArrayType::DoubleArrayPoints(DoubleArrayPoints {
                points: self,
            })),
        })
    }
}

impl IntoPoints for Vec<StringArrayPoint> {
    fn into_points(self) -> PointsType {
        PointsType::ArrayPoints(ArrayPoints {
            array_type: Some(ArrayType::StringArrayPoints(StringArrayPoints {
                points: self,
            })),
        })
    }
}

/// A submission rejected before any of its points were buffered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EnqueueError {
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(#[from] TimestampError),
}

/// A timestamp that cannot be represented as canonical signed nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TimestampError {
    #[error("missing timestamp")]
    Missing,
    #[error("timestamp nanos must be in 0..1_000_000_000")]
    InvalidNanos,
    #[error("timestamp exceeds the signed 64-bit nanosecond range")]
    OutOfRange,
}

pub(crate) fn timestamp_nanos(timestamp: Option<Timestamp>) -> Result<i64, TimestampError> {
    let timestamp = timestamp.ok_or(TimestampError::Missing)?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(TimestampError::InvalidNanos);
    }
    let nanos =
        i128::from(timestamp.seconds) * i128::from(NANOS_PER_SECOND) + i128::from(timestamp.nanos);
    i64::try_from(nanos).map_err(|_| TimestampError::OutOfRange)
}

pub(crate) fn validate_points(points: &PointsType) -> Result<(), TimestampError> {
    match points {
        PointsType::DoublePoints(points) => points
            .points
            .iter()
            .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
        PointsType::StringPoints(points) => points
            .points
            .iter()
            .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
        PointsType::IntegerPoints(points) => points
            .points
            .iter()
            .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
        PointsType::StructPoints(points) => points
            .points
            .iter()
            .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
        PointsType::Uint64Points(points) => points
            .points
            .iter()
            .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
        PointsType::ArrayPoints(points) => match &points.array_type {
            Some(ArrayType::DoubleArrayPoints(points)) => points
                .points
                .iter()
                .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
            Some(ArrayType::StringArrayPoints(points)) => points
                .points
                .iter()
                .try_for_each(|point| timestamp_nanos(point.timestamp).map(|_| ())),
            None => Ok(()),
        },
    }
}

pub trait IntoTimestamp {
    fn into_timestamp(self) -> Timestamp;

    /// Converts and validates a timestamp before it enters a writer buffer.
    fn try_into_timestamp(self) -> Result<Timestamp, TimestampError>
    where
        Self: Sized,
    {
        let timestamp = self.into_timestamp();
        timestamp_nanos(Some(timestamp))?;
        Ok(timestamp)
    }
}

impl IntoTimestamp for Duration {
    fn try_into_timestamp(self) -> Result<Timestamp, TimestampError> {
        let nanos = i64::try_from(self.as_nanos()).map_err(|_| TimestampError::OutOfRange)?;
        Ok(nanos.into_timestamp())
    }

    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: self.as_secs() as i64,
            nanos: self.subsec_nanos() as i32,
        }
    }
}

impl<T: chrono::TimeZone> IntoTimestamp for chrono::DateTime<T> {
    fn try_into_timestamp(self) -> Result<Timestamp, TimestampError> {
        self.timestamp_nanos_opt()
            .map(IntoTimestamp::into_timestamp)
            .ok_or(TimestampError::OutOfRange)
    }

    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: self.timestamp(),
            nanos: self.timestamp_subsec_nanos() as i32,
        }
    }
}

impl IntoTimestamp for i64 {
    fn try_into_timestamp(self) -> Result<Timestamp, TimestampError> {
        // Every i64 is already a valid signed nanosecond count.
        Ok(self.into_timestamp())
    }

    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: self.div_euclid(NANOS_PER_SECOND),
            nanos: self.rem_euclid(NANOS_PER_SECOND) as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;

    use super::*;

    #[test]
    fn signed_nanoseconds_normalize_and_roundtrip() {
        for nanos in [i64::MIN, -1_000_000_001, -1, 0, 1, i64::MAX] {
            let timestamp = nanos.try_into_timestamp().unwrap();
            assert!((0..1_000_000_000).contains(&timestamp.nanos));
            assert_eq!(timestamp_nanos(Some(timestamp)), Ok(nanos));
            let datetime =
                chrono::DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
                    .unwrap();
            assert_eq!(datetime.try_into_timestamp(), Ok(timestamp));
        }
    }

    #[test]
    fn every_point_type_rejects_missing_timestamps() {
        let cases = [
            vec![DoublePoint::default()].into_points(),
            vec![IntegerPoint::default()].into_points(),
            vec![Uint64Point::default()].into_points(),
            vec![StringPoint::default()].into_points(),
            vec![StructPoint::default()].into_points(),
            vec![DoubleArrayPoint::default()].into_points(),
            vec![StringArrayPoint::default()].into_points(),
        ];
        for points in cases {
            assert_eq!(validate_points(&points), Err(TimestampError::Missing));
        }
    }

    #[test]
    fn vec_double_array_point_converts_to_points_type() {
        let points = vec![DoubleArrayPoint {
            timestamp: None,
            value: vec![1.0, 2.0, 3.0],
        }];
        let PointsType::ArrayPoints(arr) = points.into_points() else {
            panic!("expected ArrayPoints");
        };
        let Some(ArrayType::DoubleArrayPoints(dp)) = arr.array_type else {
            panic!("expected DoubleArrayPoints");
        };
        assert_eq!(dp.points.len(), 1);
        assert_eq!(dp.points[0].value, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn vec_string_array_point_converts_to_points_type() {
        let points = vec![StringArrayPoint {
            timestamp: None,
            value: vec!["a".into(), "b".into()],
        }];
        let PointsType::ArrayPoints(arr) = points.into_points() else {
            panic!("expected ArrayPoints");
        };
        let Some(ArrayType::StringArrayPoints(sp)) = arr.array_type else {
            panic!("expected StringArrayPoints");
        };
        assert_eq!(sp.points.len(), 1);
        assert_eq!(sp.points[0].value, vec!["a", "b"]);
    }
}
