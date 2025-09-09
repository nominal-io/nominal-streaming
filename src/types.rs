use std::collections::BTreeMap;
use std::time::Duration;

use conjure_object::BearerToken;
use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// A descriptor for a channel.
///
/// Note that this is used internally to compare channels.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct ChannelDescriptor {
    /// The name of the channel.
    pub name: String,
    /// The tags associated with the channel.
    pub tags: Option<BTreeMap<String, String>>,
}

impl ChannelDescriptor {
    pub fn new(
        name: impl Into<String>,
        tags: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self {
            name: name.into(),
            tags: Some(
                tags.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
        }
    }

    pub fn channel(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tags: None,
        }
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

pub trait IntoTimestamp {
    fn into_timestamp(self) -> Timestamp;
}

impl IntoTimestamp for Duration {
    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: self.as_secs() as i64,
            nanos: self.subsec_nanos() as i32,
        }
    }
}

impl<T: chrono::TimeZone> IntoTimestamp for chrono::DateTime<T> {
    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: self.timestamp(),
            nanos: self.timestamp_subsec_nanos() as i32,
        }
    }
}

impl IntoTimestamp for u64 {
    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: (self / NANOS_PER_SECOND) as i64,
            nanos: (self % NANOS_PER_SECOND) as i32,
        }
    }
}
