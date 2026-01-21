use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use apache_avro::types::Record;
use apache_avro::types::Value;
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

use crate::client::NominalApiClients;
use crate::client::{self};
use crate::listener::NominalStreamListener;
use crate::types::AuthProvider;

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
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

pub trait WriteRequestConsumer: Send + Sync + Debug {
    fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()>;
}

#[derive(Clone)]
pub struct NominalCoreConsumer<A: AuthProvider> {
    client: NominalApiClients,
    handle: tokio::runtime::Handle,
    auth_provider: A,
    data_source_rid: ResourceIdentifier,
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
        }
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
        let write_request =
            client::encode_request(request.encode_to_vec(), &token, &self.data_source_rid)?;
        self.handle.block_on(async {
            self.client
                .send(write_request)
                .await
                .map_err(|e| ConsumerError::RequestError(format!("{e:?}")))
        })?;
        Ok(())
    }
}

const DEFAULT_FILE_PREFIX: &str = "nominal_stream";

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
    writer: Arc<Mutex<apache_avro::Writer<'static, std::fs::File>>>,
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
    ) -> std::io::Result<Self> {
        let datetime = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let prefix = file_prefix.unwrap_or_else(|| DEFAULT_FILE_PREFIX.to_string());
        let filename = format!("{prefix}_{datetime}.avro");
        let directory = directory.into();
        let full_path = directory.join(&filename);

        Self::new_with_full_path(full_path)
    }

    pub fn new_with_full_path(file_path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = file_path.into();
        std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;

        let writer = apache_avro::Writer::builder()
            .schema(&CORE_AVRO_SCHEMA)
            .writer(file)
            .codec(apache_avro::Codec::Snappy)
            .build();

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            path,
        })
    }

    fn append_series(&self, series: &[Series]) -> ConsumerResult<()> {
        let mut records: Vec<Record> = Vec::new();
        for series in series {
            let (timestamps, values) = points_to_avro(series.points.as_ref());

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
            .extend(records)
            .map_err(|e| ConsumerError::AvroError(Box::new(e)))?;

        Ok(())
    }
}

fn points_to_avro(points: Option<&Points>) -> (Vec<Value>, Vec<Value>) {
    let Some(Points {
        points_type: Some(points),
    }) = points
    else {
        return (Vec::new(), Vec::new());
    };

    match points {
        PointsType::DoublePoints(DoublePoints { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(0, Box::new(Value::Double(point.value))),
                )
            })
            .collect(),
        PointsType::StringPoints(StringPoints { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(1, Box::new(Value::String(point.value.clone()))),
                )
            })
            .collect(),
        PointsType::IntegerPoints(IntegerPoints { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(2, Box::new(Value::Long(point.value))),
                )
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
                    (
                        convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                        Value::Union(3, Box::new(record)),
                    )
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
                    (
                        convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                        Value::Union(4, Box::new(record)),
                    )
                })
                .collect(),
            None => (Vec::new(), Vec::new()),
        },
        PointsType::StructPoints(StructPoints { points }) => points
            .iter()
            .map(|point| {
                let record = Value::Record(vec![(
                    "json".to_string(),
                    Value::String(point.json_string.clone()),
                )]);
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(5, Box::new(record)),
                )
            })
            .collect(),
        PointsType::Uint64Points(Uint64Points { points }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(2, Box::new(Value::Long(point.value as i64))),
                )
            })
            .collect(),
    }
}

fn convert_timestamp_to_nanoseconds(timestamp: Timestamp) -> Value {
    Value::Long(timestamp.seconds * 1_000_000_000 + timestamp.nanos as i64)
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
            warn!("Sending request to primary consumer failed. Attempting fallback.");
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
            let consumer = AvroFileConsumer::new_with_full_path(&path).unwrap();

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
                    assert!(!values.is_empty(), "Channel {} should have values", channel);

                    // Check the first value's union type
                    if let Some(Value::Union(idx, inner)) = values.first() {
                        match channel.as_str() {
                            "doubles" => {
                                assert_eq!(*idx, 0, "doubles should use union index 0");
                                assert!(matches!(inner.as_ref(), Value::Double(_)));
                            }
                            "strings" => {
                                assert_eq!(*idx, 1, "strings should use union index 1");
                                assert!(matches!(inner.as_ref(), Value::String(_)));
                            }
                            "longs" => {
                                assert_eq!(*idx, 2, "longs should use union index 2");
                                assert!(matches!(inner.as_ref(), Value::Long(_)));
                            }
                            "double_arrays" => {
                                assert_eq!(*idx, 3, "double_arrays should use union index 3");
                                assert!(matches!(inner.as_ref(), Value::Record(_)));
                            }
                            "string_arrays" => {
                                assert_eq!(*idx, 4, "string_arrays should use union index 4");
                                assert!(matches!(inner.as_ref(), Value::Record(_)));
                            }
                            "structs" => {
                                assert_eq!(*idx, 5, "structs should use union index 5");
                                assert!(matches!(inner.as_ref(), Value::Record(_)));
                            }
                            "uint64s" => {
                                assert_eq!(*idx, 2, "uint64s should use union index 2 (long)");
                                assert!(matches!(inner.as_ref(), Value::Long(_)));
                            }
                            _ => panic!("Unexpected channel: {}", channel),
                        }
                    }
                }
            }
        }
    }
}
