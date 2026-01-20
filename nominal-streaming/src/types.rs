use std::collections::BTreeMap;
use std::time::Duration;

use conjure_object::BearerToken;
use nominal_api::api::rids::WorkspaceRid;
use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
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
    pub tags: Option<BTreeMap<String, String>>,
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
            tags: Some(
                tags.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
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

impl IntoTimestamp for i64 {
    fn into_timestamp(self) -> Timestamp {
        Timestamp {
            seconds: (self / NANOS_PER_SECOND),
            nanos: (self % NANOS_PER_SECOND) as i32,
        }
    }
}
