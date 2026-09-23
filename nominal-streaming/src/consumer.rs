mod avro_writer;

use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use apache_avro::types::Record;
use apache_avro::types::Value;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::StructPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Points;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;
use parking_lot::Mutex;
use prost::Message;
use tracing::warn;

use self::avro_writer::AvroWriter;
use self::avro_writer::CompleteWrite;
use crate::client::NominalApiClients;
use crate::client::WriteRequest;
use crate::client::{self};
use crate::listener::NominalStreamListener;
use crate::metrics::RequestMetrics;
use crate::types::AuthProvider;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConsumerError {
    #[error("failed to {operation} file {path:?}: {source}")]
    FileError {
        path: PathBuf,
        operation: &'static str,
        source: std::io::Error,
    },
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("avro error: {0}")]
    AvroError(#[from] Box<apache_avro::Error>),
    #[error("No auth token provided. Please make sure you're authenticated.")]
    MissingTokenError,
    #[error("request error: {0}")]
    RequestError(String),
    #[error("consumer error occurred: {0}")]
    GenericConsumerError(#[from] Box<dyn Error + Send + Sync>),
}

pub type ConsumerResult<T> = Result<T, ConsumerError>;

/// Compact, single-line summary of a failed request: the conjure error kind, the
/// HTTP status when the failure came from a response, and the cause chain's
/// Display output.
///
/// Deliberately NOT the conjure `Error`'s Debug form, which embeds captured
/// backtraces and parameter maps — several KB per line in the per-request warn
/// logs that high-volume callers (e.g. Lambda ingest) run with.
fn describe_request_error(e: &conjure_error::Error) -> String {
    let mut out = match e.kind() {
        conjure_error::ErrorKind::Service(s) => format!("service error {}", s.error_code()),
        conjure_error::ErrorKind::Throttle(_) => "throttled (429)".to_owned(),
        conjure_error::ErrorKind::Unavailable(_) => "unavailable (503)".to_owned(),
        _ => "unknown error kind".to_owned(),
    };
    let mut cause: Option<&(dyn Error + 'static)> = Some(e.cause());
    while let Some(c) = cause {
        if let Some(remote) = c.downcast_ref::<client::conjure::runtime::errors::RemoteError>() {
            let _ = write!(out, ": {c} (http {})", remote.status());
        } else {
            let _ = write!(out, ": {c}");
        }
        cause = c.source();
    }
    out
}

pub trait WriteRequestConsumer: Send + Sync + Debug {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()>;
}

#[derive(Clone)]
pub struct NominalCoreConsumer<A: AuthProvider> {
    client: NominalApiClients,
    handle: tokio::runtime::Handle,
    auth_provider: A,
    data_source_rid: ResourceIdentifier,
    metrics: RequestMetrics,
}

impl<A: AuthProvider> NominalCoreConsumer<A> {
    pub fn new(
        client: NominalApiClients,
        handle: tokio::runtime::Handle,
        auth_provider: A,
        data_source_rid: ResourceIdentifier,
    ) -> Self {
        Self {
            client,
            handle,
            auth_provider,
            data_source_rid,
            metrics: RequestMetrics::default(),
        }
    }

    /// Piggyback completed request metrics on later data requests to the same dataset.
    /// Pending metrics are bounded and best-effort; no extra requests are sent.
    pub fn with_track_metrics(mut self, enabled: bool) -> Self {
        self.metrics.set_enabled(enabled);
        self
    }

    /// Name channels the caller emits through the stream as metrics rather than data, so
    /// they are excluded from request latency measurements. The request metrics this
    /// consumer emits itself are always excluded. Only relevant when metrics tracking is
    /// enabled.
    pub fn with_additional_metric_channels(
        mut self,
        channels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.metrics.set_additional_metric_channels(channels);
        self
    }

    fn encode(
        &self,
        request: &WriteRequestNominal,
        token: &BearerToken,
    ) -> ConsumerResult<WriteRequest<'static>> {
        Ok(client::encode_request(
            &request.encode_to_vec(),
            token,
            &self.data_source_rid,
        )?)
    }

    fn send(&self, request: WriteRequest<'static>) -> ConsumerResult<()> {
        self.handle.block_on(async {
            self.client
                .send(request)
                .await
                .map_err(|e| ConsumerError::RequestError(describe_request_error(&e)))
        })?;
        Ok(())
    }
}

impl<T: AuthProvider> Debug for NominalCoreConsumer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NominalCoreConsumer")
            .field("client", &self.client)
            .field("data_source_rid", &self.data_source_rid)
            .finish()
    }
}

impl<T: AuthProvider + 'static> WriteRequestConsumer for NominalCoreConsumer<T> {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let token = self
            .auth_provider
            .token()
            .ok_or(ConsumerError::MissingTokenError)?;
        let (request, measurement) = self.metrics.prepare(request);
        let encoded = self.encode(&request, &token)?;
        let in_flight = measurement.start();
        self.send(encoded)?;
        in_flight.complete();
        Ok(())
    }
}

const DEFAULT_FILE_PREFIX: &str = "nominal_stream";

pub const DATASET_RID_METADATA_KEY: &str = "nominal.dataset_rid";

pub static CORE_SCHEMA_STR: &str = r#"{
  "type": "record",
  "name": "AvroStream",
  "namespace": "io.nominal.ingest",
  "fields": [
      {
          "name": "channel",
          "type": "string",
          "doc": "Channel/series name (e.g., 'vehicle_id', 'col_1', 'temperature')"
      },
      {
          "name": "timestamps",
          "type": {"type": "array", "items": "long"},
          "doc": "Array of Unix timestamps in nanoseconds"
      },
      {
          "name": "values",
          "type": {"type": "array", "items": [
              "double",
              "string",
              "long",
              {"type": "record", "name": "DoubleArray", "fields": [{"name": "items", "type": {"type": "array", "items": "double"}}]},
              {"type": "record", "name": "StringArray", "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}]},
              {"type": "record", "name": "JsonStruct", "fields": [{"name": "json", "type": "string"}]}
          ]},
          "doc": "Array of values. Can be doubles, longs, strings, arrays, or JSON structs"
      },
      {
          "name": "tags",
          "type": {"type": "map", "values": "string"},
          "default": {},
          "doc": "Key-value metadata tags"
      }
  ]
}
"#;

pub static CORE_AVRO_SCHEMA: LazyLock<apache_avro::Schema> = LazyLock::new(|| {
    let json = serde_json::from_str(CORE_SCHEMA_STR).expect("Failed to parse JSON schema");
    apache_avro::Schema::parse(&json).expect("Failed to parse Avro schema")
});

#[derive(Clone)]
pub struct AvroFileConsumer {
    writer: Arc<Mutex<AvroWriter<std::fs::File>>>,
    path: PathBuf,
}

impl Debug for AvroFileConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroFileConsumer")
            .field("path", &self.path)
            .finish()
    }
}

impl AvroFileConsumer {
    pub fn new(
        directory: impl Into<PathBuf>,
        file_prefix: Option<String>,
        dataset_rid: Option<ResourceIdentifier>,
    ) -> std::io::Result<Self> {
        let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let prefix = file_prefix.unwrap_or_else(|| DEFAULT_FILE_PREFIX.to_string());
        let filename = format!("{prefix}_{datetime}.avro");
        let directory = directory.into();
        let full_path = directory.join(&filename);

        Self::new_with_full_path(full_path, true, dataset_rid)
    }

    /// Opens `file_path` for writing and wraps it in an avro `Writer`.
    ///
    /// If `overwrite` is true and the path already exists, its prior contents
    /// are discarded. Truncation is required when reusing a path: the avro
    /// container format is single-header-and-blocks, so opening a longer
    /// existing file without truncating would leave leftover bytes from the
    /// previous run past the new content's end and produce a corrupt reader
    /// stream.
    ///
    /// If `overwrite` is false and the path already exists, an
    /// `io::ErrorKind::AlreadyExists` error is returned and no file is
    /// touched. This is the safe choice when the caller does not want to
    /// silently destroy prior data.
    ///
    /// If `dataset_rid` is provided, it is written to the avro file's user
    /// metadata under the `nominal.dataset_rid` key so downstream readers
    /// can identify the dataset the file belongs to.
    pub fn new_with_full_path(
        file_path: impl Into<PathBuf>,
        overwrite: bool,
        dataset_rid: Option<ResourceIdentifier>,
    ) -> std::io::Result<Self> {
        let path = file_path.into();
        std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true);
        if overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        }
        let file = options.open(&path)?;

        let mut writer = apache_avro::Writer::builder()
            .schema(&CORE_AVRO_SCHEMA)
            .writer(CompleteWrite(file))
            .codec(apache_avro::Codec::Snappy)
            .build();

        if let Some(rid) = dataset_rid {
            writer
                .add_user_metadata(DATASET_RID_METADATA_KEY.to_string(), rid.to_string())
                .map_err(|e| {
                    std::io::Error::other(format!("failed to write avro metadata: {e}"))
                })?;
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(AvroWriter::new(writer, path.clone()))),
            path,
        })
    }

    fn append_series(&self, series: &[Series]) -> ConsumerResult<()> {
        let mut records: Vec<Record> = Vec::new();
        for series in series {
            let (timestamps, values) = points_to_avro(series.points.as_ref())
                .map_err(|e| self.file_error("convert timestamps for", std::io::Error::other(e)))?;

            let mut record = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create Avro record");

            record.put(
                "channel",
                series
                    .channel
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or("values".to_string()),
            );
            record.put("timestamps", Value::Array(timestamps));
            record.put("values", Value::Array(values));
            record.put("tags", series.tags.clone());

            records.push(record);
        }

        self.writer
            .lock()
            .append(records)
            .map_err(|e| self.file_error("append", e))
    }

    /// Finalize the Avro container and synchronize its contents to disk.
    ///
    /// Repeated calls return the same outcome. All clones share this state;
    /// consuming further requests after finalization returns an error.
    pub fn finish(&self) -> ConsumerResult<()> {
        self.writer
            .lock()
            .finish()
            .map_err(|e| self.file_error("finalize", e))
    }

    fn file_error(&self, operation: &'static str, source: std::io::Error) -> ConsumerError {
        ConsumerError::FileError {
            path: self.path.clone(),
            operation,
            source,
        }
    }
}

fn points_to_avro(points: Option<&Points>) -> ConsumerResult<(Vec<Value>, Vec<Value>)> {
    let Some(Points {
        points_type: Some(points),
    }) = points
    else {
        return Ok((Vec::new(), Vec::new()));
    };

    match points {
        PointsType::DoublePoints(DoublePoints { points }) => points
            .iter()
            .map(|point| {
                Ok((
                    convert_timestamp_to_nanoseconds(point.timestamp)?,
                    Value::Union(0, Box::new(Value::Double(point.value))),
                ))
            })
            .collect(),
        PointsType::StringPoints(StringPoints { points }) => points
            .iter()
            .map(|point| {
                Ok((
                    convert_timestamp_to_nanoseconds(point.timestamp)?,
                    Value::Union(1, Box::new(Value::String(point.value.clone()))),
                ))
            })
            .collect(),
        PointsType::IntegerPoints(IntegerPoints { points }) => points
            .iter()
            .map(|point| {
                Ok((
                    convert_timestamp_to_nanoseconds(point.timestamp)?,
                    Value::Union(2, Box::new(Value::Long(point.value))),
                ))
            })
            .collect(),
        PointsType::ArrayPoints(ArrayPoints { array_type }) => match array_type {
            Some(ArrayType::DoubleArrayPoints(points)) => points
                .points
                .iter()
                .map(|point| {
                    let array_values: Vec<Value> =
                        point.value.iter().map(|v| Value::Double(*v)).collect();
                    let record =
                        Value::Record(vec![("items".to_string(), Value::Array(array_values))]);
                    Ok((
                        convert_timestamp_to_nanoseconds(point.timestamp)?,
                        Value::Union(3, Box::new(record)),
                    ))
                })
                .collect(),
            Some(ArrayType::StringArrayPoints(points)) => points
                .points
                .iter()
                .map(|point| {
                    let array_values: Vec<Value> = point
                        .value
                        .iter()
                        .map(|v| Value::String(v.clone()))
                        .collect();
                    let record =
                        Value::Record(vec![("items".to_string(), Value::Array(array_values))]);
                    Ok((
                        convert_timestamp_to_nanoseconds(point.timestamp)?,
                        Value::Union(4, Box::new(record)),
                    ))
                })
                .collect(),
            None => Ok((Vec::new(), Vec::new())),
        },
        PointsType::StructPoints(StructPoints { points }) => points
            .iter()
            .map(|point| {
                let record = Value::Record(vec![(
                    "json".to_string(),
                    Value::String(point.json_string.clone()),
                )]);
                Ok((
                    convert_timestamp_to_nanoseconds(point.timestamp)?,
                    Value::Union(5, Box::new(record)),
                ))
            })
            .collect(),
        PointsType::Uint64Points(Uint64Points { points }) => points
            .iter()
            .map(|point| {
                Ok((
                    convert_timestamp_to_nanoseconds(point.timestamp)?,
                    Value::Union(2, Box::new(Value::Long(point.value as i64))),
                ))
            })
            .collect(),
    }
}

fn convert_timestamp_to_nanoseconds(timestamp: Option<Timestamp>) -> ConsumerResult<Value> {
    let timestamp =
        timestamp.ok_or_else(|| ConsumerError::RequestError("missing timestamp".into()))?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(ConsumerError::RequestError(
            "timestamp nanos must be in 0..1_000_000_000".into(),
        ));
    }
    let nanos = i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos);
    let nanos = i64::try_from(nanos).map_err(|_| {
        ConsumerError::RequestError("timestamp exceeds Avro i64 nanosecond range".into())
    })?;
    Ok(Value::Long(nanos))
}

impl WriteRequestConsumer for AvroFileConsumer {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        self.append_series(&request.series)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer,
    F: WriteRequestConsumer,
{
    primary: P,
    fallback: F,
}

impl<P, F> RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer,
    F: WriteRequestConsumer,
{
    pub fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

impl<P, F> Debug for RequestConsumerWithFallback<P, F>
where
    F: Send + Sync + WriteRequestConsumer,
    P: Send + Sync + WriteRequestConsumer,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestConsumerWithFallback")
            .field("primary", &self.primary)
            .field("fallback", &self.fallback)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct DualWriteRequestConsumer<P, S>
where
    P: WriteRequestConsumer,
    S: WriteRequestConsumer,
{
    primary: P,
    secondary: S,
}

impl<P, S> DualWriteRequestConsumer<P, S>
where
    P: WriteRequestConsumer,
    S: WriteRequestConsumer,
{
    pub fn new(primary: P, secondary: S) -> Self {
        Self { primary, secondary }
    }
}

impl<P, S> WriteRequestConsumer for DualWriteRequestConsumer<P, S>
where
    P: WriteRequestConsumer + Send + Sync,
    S: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        let primary_result = self.primary.consume(request);
        let secondary_result = self.secondary.consume(request);
        if let Err(e) = &primary_result {
            warn!("Sending request to primary consumer failed: {:?}", e);
        }
        if let Err(e) = &secondary_result {
            warn!("Sending request to secondary consumer failed: {:?}", e);
        }

        // If either failed, return the error
        primary_result.and(secondary_result)
    }
}

impl<P, F> WriteRequestConsumer for RequestConsumerWithFallback<P, F>
where
    P: WriteRequestConsumer + Send + Sync,
    F: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        if let Err(e) = self.primary.consume(request) {
            warn!("Sending request to primary consumer failed: {e}. Attempting fallback.");
            let fallback_result = self.fallback.consume(request);
            // we want to notify the caller about the missing token error as it is a user error
            // todo: get rid of this once we figure out why the auth handle blocks in connect
            if let ConsumerError::MissingTokenError = e {
                return Err(ConsumerError::MissingTokenError);
            }
            return fallback_result;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ListeningWriteRequestConsumer<C>
where
    C: WriteRequestConsumer,
{
    consumer: C,
    listeners: Vec<Arc<dyn NominalStreamListener>>,
}

impl<C> ListeningWriteRequestConsumer<C>
where
    C: WriteRequestConsumer,
{
    pub fn new(consumer: C, listeners: Vec<Arc<dyn NominalStreamListener>>) -> Self {
        Self {
            consumer,
            listeners,
        }
    }
}

impl<C> WriteRequestConsumer for ListeningWriteRequestConsumer<C>
where
    C: WriteRequestConsumer + Send + Sync,
{
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
        match self.consumer.consume(request) {
            Ok(_) => {
                self.listeners.on_success(request);
                Ok(())
            }
            Err(e) => {
                self.listeners.on_error(&e, request);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use apache_avro::Reader;
    use nominal_api::tonic::google::protobuf::Timestamp;
    use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
    use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn malformed_request_does_not_append_earlier_series() {
        let file = NamedTempFile::new().unwrap();
        let consumer = AvroFileConsumer::new_with_full_path(file.path(), true, None).unwrap();
        let series = |timestamp| {
            make_series(
                "ch",
                Points {
                    points_type: Some(PointsType::IntegerPoints(IntegerPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                                timestamp,
                                value: 42,
                            },
                        ],
                    })),
                },
            )
        };
        consumer
            .append_series(&[series(make_timestamp(0, 0))])
            .unwrap();
        for timestamp in [
            None,
            make_timestamp(0, -1),
            make_timestamp(0, 1_000_000_000),
            make_timestamp(i64::MAX, 0),
        ] {
            assert!(consumer
                .append_series(&[series(make_timestamp(1, 0)), series(timestamp)])
                .is_err());
        }
        drop(consumer);
        assert_eq!(read_integer_point_count(&file.path().to_path_buf()), 1);
    }

    #[test]
    fn finish_is_shared_idempotent_and_writes_empty_container() {
        let file = NamedTempFile::new().unwrap();
        let consumer = AvroFileConsumer::new_with_full_path(file.path(), true, None).unwrap();
        let clone = consumer.clone();
        drop(consumer);
        clone.consume(&WriteRequestNominal::default()).unwrap();
        clone.finish().unwrap();
        clone.finish().unwrap();
        assert_eq!(
            Reader::new(std::fs::File::open(file.path()).unwrap())
                .unwrap()
                .count(),
            0
        );
        let error = clone.consume(&WriteRequestNominal::default()).unwrap_err();
        assert!(error.to_string().contains(file.path().to_str().unwrap()));
    }

    #[test]
    fn timestamp_boundaries_roundtrip() {
        let file = NamedTempFile::new().unwrap();
        let consumer = AvroFileConsumer::new_with_full_path(file.path(), true, None).unwrap();
        let expected = [i64::MIN, -1, 0, i64::MAX];
        let points = expected
            .iter()
            .map(
                |value| nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                    timestamp: make_timestamp(
                        value.div_euclid(1_000_000_000),
                        value.rem_euclid(1_000_000_000) as i32,
                    ),
                    value: 42,
                },
            )
            .collect();
        consumer
            .append_series(&[make_series(
                "ch",
                Points {
                    points_type: Some(PointsType::IntegerPoints(IntegerPoints { points })),
                },
            )])
            .unwrap();
        drop(consumer);
        let records = Reader::new(std::fs::File::open(file.path()).unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let Value::Record(fields) = &records[0] else {
            panic!("record")
        };
        assert_eq!(
            fields
                .iter()
                .find(|(name, _)| name == "timestamps")
                .unwrap()
                .1,
            Value::Array(expected.map(Value::Long).to_vec())
        );
    }

    #[test]
    fn describe_request_error_is_compact_and_names_the_cause() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let described = describe_request_error(&conjure_error::Error::internal_safe(io));
        assert!(described.contains("connection refused"), "{described}");
        assert!(!described.contains("Backtrace"), "{described}");
        assert!(described.len() < 200, "{described}");

        let throttled =
            describe_request_error(&conjure_error::Error::throttle_safe("too many requests"));
        assert!(throttled.contains("429"), "{throttled}");

        let unavailable =
            describe_request_error(&conjure_error::Error::unavailable_safe("service rolling"));
        assert!(unavailable.contains("503"), "{unavailable}");
    }

    fn make_timestamp(secs: i64, nanos: i32) -> Option<Timestamp> {
        Some(Timestamp {
            seconds: secs,
            nanos,
        })
    }

    fn make_series(name: &str, points: Points) -> Series {
        Series {
            channel: Some(Channel {
                name: name.to_string(),
            }),
            tags: HashMap::new(),
            points: Some(points),
        }
    }

    #[test]
    fn test_avro_file_with_all_value_types() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        // Create consumer and write all types
        {
            let consumer = AvroFileConsumer::new_with_full_path(&path, true, None).unwrap();

            // Create series with each type
            let double_series = make_series(
                "doubles",
                Points {
                    points_type: Some(PointsType::DoublePoints(DoublePoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint {
                                timestamp: make_timestamp(1000, 0),
                                value: 1.5,
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint {
                                timestamp: make_timestamp(1001, 0),
                                value: 2.5,
                            },
                        ],
                    })),
                },
            );

            let long_series = make_series(
                "longs",
                Points {
                    points_type: Some(PointsType::IntegerPoints(IntegerPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                                timestamp: make_timestamp(1000, 0),
                                value: 42,
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                                timestamp: make_timestamp(1001, 0),
                                value: -100,
                            },
                        ],
                    })),
                },
            );

            let string_series = make_series(
                "strings",
                Points {
                    points_type: Some(PointsType::StringPoints(StringPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::StringPoint {
                                timestamp: make_timestamp(1000, 0),
                                value: "hello".to_string(),
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::StringPoint {
                                timestamp: make_timestamp(1001, 0),
                                value: "world".to_string(),
                            },
                        ],
                    })),
                },
            );

            let double_array_series = make_series(
                "double_arrays",
                Points {
                    points_type: Some(PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::DoubleArrayPoints(
                            nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoints {
                                points: vec![
                                    DoubleArrayPoint {
                                        timestamp: make_timestamp(1000, 0),
                                        value: vec![1.0, 2.0, 3.0],
                                    },
                                    DoubleArrayPoint {
                                        timestamp: make_timestamp(1001, 0),
                                        value: vec![4.0, 5.0],
                                    },
                                ],
                            },
                        )),
                    })),
                },
            );

            let string_array_series = make_series(
                "string_arrays",
                Points {
                    points_type: Some(PointsType::ArrayPoints(ArrayPoints {
                        array_type: Some(ArrayType::StringArrayPoints(
                            nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoints {
                                points: vec![
                                    StringArrayPoint {
                                        timestamp: make_timestamp(1000, 0),
                                        value: vec!["a".to_string(), "b".to_string()],
                                    },
                                    StringArrayPoint {
                                        timestamp: make_timestamp(1001, 0),
                                        value: vec![
                                            "c".to_string(),
                                            "d".to_string(),
                                            "e".to_string(),
                                        ],
                                    },
                                ],
                            },
                        )),
                    })),
                },
            );

            let struct_series = make_series(
                "structs",
                Points {
                    points_type: Some(PointsType::StructPoints(StructPoints {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::StructPoint {
                                timestamp: make_timestamp(1000, 0),
                                json_string: r#"{"key": "value"}"#.to_string(),
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::StructPoint {
                                timestamp: make_timestamp(1001, 0),
                                json_string: r#"{"count": 42}"#.to_string(),
                            },
                        ],
                    })),
                },
            );

            let uint64_series = make_series(
                "uint64s",
                Points {
                    points_type: Some(PointsType::Uint64Points(Uint64Points {
                        points: vec![
                            nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point {
                                timestamp: make_timestamp(1000, 0),
                                value: u64::MAX,
                            },
                            nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point {
                                timestamp: make_timestamp(1001, 0),
                                value: 12345678901234567890,
                            },
                        ],
                    })),
                },
            );

            let request = WriteRequestNominal {
                series: vec![
                    double_series,
                    long_series,
                    string_series,
                    double_array_series,
                    string_array_series,
                    struct_series,
                    uint64_series,
                ],
                session_name: None,
            };

            consumer.consume(&request).unwrap();

            // Flush the writer by dropping it
            drop(consumer);
        }

        // Read back the file and verify
        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();

        let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 7, "Expected 7 series records");

        // Verify each record has the expected channel name and value types
        let channels: Vec<String> = records
            .iter()
            .filter_map(|r| {
                if let Value::Record(fields) = r {
                    fields.iter().find_map(|(name, value)| {
                        if name == "channel" {
                            if let Value::String(s) = value {
                                Some(s.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
            .collect();

        assert!(channels.contains(&"doubles".to_string()));
        assert!(channels.contains(&"longs".to_string()));
        assert!(channels.contains(&"strings".to_string()));
        assert!(channels.contains(&"double_arrays".to_string()));
        assert!(channels.contains(&"string_arrays".to_string()));
        assert!(channels.contains(&"structs".to_string()));
        assert!(channels.contains(&"uint64s".to_string()));

        // Verify specific value types by checking the union discriminants
        for record in &records {
            if let Value::Record(fields) = record {
                let channel = fields.iter().find_map(|(name, value)| {
                    if name == "channel" {
                        if let Value::String(s) = value {
                            Some(s.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                let values =
                    fields.iter().find_map(
                        |(name, value)| {
                            if name == "values" {
                                Some(value)
                            } else {
                                None
                            }
                        },
                    );

                if let (Some(channel), Some(Value::Array(values))) = (channel, values) {
                    assert_eq!(values.len(), 2, "Channel {} should have 2 values", channel);

                    match channel.as_str() {
                        "doubles" => {
                            assert_eq!(values[0], Value::Union(0, Box::new(Value::Double(1.5))));
                            assert_eq!(values[1], Value::Union(0, Box::new(Value::Double(2.5))));
                        }
                        "strings" => {
                            assert_eq!(
                                values[0],
                                Value::Union(1, Box::new(Value::String("hello".to_string())))
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(1, Box::new(Value::String("world".to_string())))
                            );
                        }
                        "longs" => {
                            assert_eq!(values[0], Value::Union(2, Box::new(Value::Long(42))));
                            assert_eq!(values[1], Value::Union(2, Box::new(Value::Long(-100))));
                        }
                        "double_arrays" => {
                            assert_eq!(
                                values[0],
                                Value::Union(
                                    3,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![
                                            Value::Double(1.0),
                                            Value::Double(2.0),
                                            Value::Double(3.0)
                                        ])
                                    )]))
                                )
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    3,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![Value::Double(4.0), Value::Double(5.0)])
                                    )]))
                                )
                            );
                        }
                        "string_arrays" => {
                            assert_eq!(
                                values[0],
                                Value::Union(
                                    4,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![
                                            Value::String("a".to_string()),
                                            Value::String("b".to_string())
                                        ])
                                    )]))
                                )
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    4,
                                    Box::new(Value::Record(vec![(
                                        "items".to_string(),
                                        Value::Array(vec![
                                            Value::String("c".to_string()),
                                            Value::String("d".to_string()),
                                            Value::String("e".to_string())
                                        ])
                                    )]))
                                )
                            );
                        }
                        "structs" => {
                            assert_eq!(
                                values[0],
                                Value::Union(
                                    5,
                                    Box::new(Value::Record(vec![(
                                        "json".to_string(),
                                        Value::String(r#"{"key": "value"}"#.to_string())
                                    )]))
                                )
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    5,
                                    Box::new(Value::Record(vec![(
                                        "json".to_string(),
                                        Value::String(r#"{"count": 42}"#.to_string())
                                    )]))
                                )
                            );
                        }
                        "uint64s" => {
                            // u64::MAX as i64 is -1, 12345678901234567890u64 as i64 is negative
                            assert_eq!(
                                values[0],
                                Value::Union(2, Box::new(Value::Long(u64::MAX as i64)))
                            );
                            assert_eq!(
                                values[1],
                                Value::Union(
                                    2,
                                    Box::new(Value::Long(12345678901234567890u64 as i64))
                                )
                            );
                        }
                        _ => panic!("Unexpected channel: {}", channel),
                    }
                }
            }
        }
    }

    #[test]
    fn reopening_path_with_overwrite_truncates_to_valid_avro_file() {
        // Write 500 points, then re-open the same path with overwrite=true
        // and write 5 points. Both passes must produce a file that reads back
        // cleanly with the expected point count, AND the second write must
        // shrink the file at the filesystem level — not just produce a
        // readable record count. Without truncate, the second pass would
        // overwrite from offset 0 and leave the tail of the longer first
        // file intact, corrupting the reader stream.
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        write_integer_points(&path, 500);
        assert_eq!(read_integer_point_count(&path), 500);
        let first_size = std::fs::metadata(&path).unwrap().len();

        write_integer_points(&path, 5);
        assert_eq!(read_integer_point_count(&path), 5);
        let second_size = std::fs::metadata(&path).unwrap().len();

        assert!(
            second_size < first_size,
            "second write should shrink the file (first: {first_size} bytes, second: {second_size} bytes)"
        );
    }

    #[test]
    fn dropping_consumer_flushes_buffered_records() {
        // Defensive test against future misuse of avro api (writing without flushing).
        // Current stream implementation uses .extend(), which flushes internally.
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        {
            let consumer = AvroFileConsumer::new_with_full_path(&path, true, None).unwrap();

            let mut record = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create Avro record");
            record.put("channel", "ch".to_string());
            record.put("timestamps", Value::Array(vec![Value::Long(0)]));
            record.put(
                "values",
                Value::Array(vec![Value::Union(2, Box::new(Value::Long(42)))]),
            );
            record.put("tags", HashMap::<String, String>::new());

            consumer
                .writer
                .lock()
                .writer
                .as_mut()
                .unwrap()
                .append(record)
                .unwrap();
            // consumer drops here — the only thing that can land the buffered
            // record on disk is a flush from the Drop impl.
        }

        assert_eq!(
            read_integer_point_count(&path),
            1,
            "expected the buffered point to land on disk after the consumer dropped"
        );
    }

    #[test]
    fn new_with_full_path_errors_when_overwrite_false_and_path_exists() {
        // Pre-create a file at the target path; opening with overwrite=false
        // must fail rather than silently destroying the existing data.
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();
        std::fs::write(&path, b"prior content").unwrap();

        let err = AvroFileConsumer::new_with_full_path(&path, false, None)
            .expect_err("expected AlreadyExists when overwrite=false and file exists");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        // Pre-existing bytes must be untouched.
        assert_eq!(std::fs::read(&path).unwrap(), b"prior content");
    }

    #[test]
    fn new_with_full_path_succeeds_when_overwrite_false_and_path_missing() {
        // overwrite=false should still create a brand-new file; the guard is
        // only against clobbering existing content.
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join("fresh.avro");

        write_integer_points_with(&path, 3, false, None);
        assert_eq!(read_integer_point_count(&path), 3);
    }

    #[test]
    fn writes_dataset_rid_to_avro_user_metadata() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();
        let rid = ResourceIdentifier::new("ri.catalog.main.dataset.abc123").unwrap();

        write_integer_points_with(&path, 1, true, Some(rid.clone()));

        let stored = read_dataset_rid_metadata(&path).expect("dataset_rid metadata missing");
        assert_eq!(stored, rid.to_string());
    }

    #[test]
    fn omits_dataset_rid_metadata_when_none() {
        let tmp_file = NamedTempFile::new().unwrap();
        let path: PathBuf = tmp_file.path().to_path_buf();

        write_integer_points_with(&path, 1, true, None);

        assert!(read_dataset_rid_metadata(&path).is_none());
    }

    fn write_integer_points(path: &PathBuf, count: i64) {
        write_integer_points_with(path, count, true, None);
    }

    fn write_integer_points_with(
        path: &PathBuf,
        count: i64,
        overwrite: bool,
        dataset_rid: Option<ResourceIdentifier>,
    ) {
        let points = (0..count)
            .map(
                |i| nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint {
                    timestamp: make_timestamp(i, 0),
                    value: i,
                },
            )
            .collect();
        let consumer = AvroFileConsumer::new_with_full_path(path, overwrite, dataset_rid).unwrap();
        consumer
            .append_series(&[make_series(
                "ch",
                Points {
                    points_type: Some(PointsType::IntegerPoints(IntegerPoints { points })),
                },
            )])
            .unwrap();
        // Consumer drops at end of scope, flushing the avro writer to disk.
    }

    fn read_integer_point_count(path: &PathBuf) -> usize {
        let reader = Reader::new(std::fs::File::open(path).unwrap()).unwrap();
        let mut total = 0;
        for record in reader {
            let Value::Record(fields) = record.unwrap() else {
                panic!("expected Record");
            };
            let timestamps = fields
                .iter()
                .find(|(name, _)| name == "timestamps")
                .map(|(_, v)| v)
                .unwrap();
            if let Value::Array(arr) = timestamps {
                total += arr.len();
            }
        }
        total
    }

    fn read_dataset_rid_metadata(path: &PathBuf) -> Option<String> {
        let file = std::fs::File::open(path).unwrap();
        let reader = Reader::new(file).unwrap();
        reader
            .user_metadata()
            .get(DATASET_RID_METADATA_KEY)
            .map(|bytes| String::from_utf8(bytes.clone()).unwrap())
    }
}
