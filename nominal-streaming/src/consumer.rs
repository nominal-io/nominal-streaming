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
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoints;
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
          "type": {"type": "array", "items": ["double", "string", "long"]},
          "doc": "Array of values. Can either be doubles, strings, or longs"
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
    match points {
        Some(Points {
            points_type: Some(PointsType::DoublePoints(DoublePoints { points })),
        }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(0, Box::new(Value::Double(point.value))),
                )
            })
            .collect(),
        Some(Points {
            points_type: Some(PointsType::StringPoints(StringPoints { points })),
        }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(1, Box::new(Value::String(point.value.clone()))),
                )
            })
            .collect(),
        Some(Points {
            points_type: Some(PointsType::IntegerPoints(IntegerPoints { points })),
        }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(2, Box::new(Value::Long(point.value.clone()))),
                )
            })
            .collect(),
        Some(Points {
            points_type: Some(PointsType::Uint64Points(Uint64Points { points })),
        }) => points
            .iter()
            .map(|point| {
                (
                    convert_timestamp_to_nanoseconds(point.timestamp.unwrap()),
                    Value::Union(2, Box::new(Value::Long(point.value.clone() as i64))),
                )
            })
            .collect(),
        _ => (Vec::new(), Vec::new()),
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
    use super::*;
    use nominal_api::tonic::io::nominal::scout::api::proto::{
        Channel, DoublePoint, IntegerPoint, StringPoint, Uint64Point,
    };

    use apache_avro::types::Value as AvroValue;


    /// A helper struct for the parsed contents of a single avro record from an avro file.
    struct ParsedRecord {
        pub channel: String,
        pub values: Vec<AvroValue>,
    }

    impl ParsedRecord {
        /// Given a avro record, parse out the channel name and values.
        /// 
        /// Note that their are other fields in the record per the schema, but they are not 
        /// parsed out for now.
        fn new(record: &Record) -> Self {
            Self {
                channel: Self::get_channel(&record),
                values: Self::get_values(&record)
            }
        }

        /// Get the channel field from an avro record.
        /// 
        /// The channel field is keyed by the "channel" field in the record.
        fn get_channel(record: &Record) -> String {
            record.fields.iter().find(|(k,_)| k == "channel").map(|(_,v)| {
                let AvroValue::String(s) = v else {
                    panic!("Channel for record not of type string: {:?}", v);
                };
                s.to_owned()
            }).expect("Could not extract channel from record")
        }

        /// Get the vector of contained values from the avro record.
        /// 
        /// The inner values are keyed by a "values" field in the record.
        fn get_values(record: &Record) -> Vec<AvroValue> {
            record.fields
                .iter()
                .find(|(key, _)| key == "values")
                .map(|(_, v)| {
                    let AvroValue::Array(a) = v else {
                        panic!(
                            "Values for record not not of type array {:?}: {:?}",
                            record, v
                        );
                    };

                    let vals: Vec<AvroValue> = a.iter().map(|v| {
                        let AvroValue::Union(_, boxed_val) = v else {
                            panic!("Value type not Union: {:?}", v);
                        };

                        *boxed_val.to_owned()
                    }).collect();
                    
                    vals
                })
                .expect("Could not find channel in record")
        }
    }

    #[test]
    fn test_avro_consumer_with_all_types() {
        // Create a temporary file
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let temp_file = temp_dir.path().join("test.avro");

        // Create an AvroFileConsumer
        let consumer =
            AvroFileConsumer::new_with_full_path(&temp_file).expect("Failed to create consumer");

        // Create test data with all supported types
        let write_request = WriteRequestNominal {
            series: vec![
                // Double points
                Series {
                    channel: Some(Channel {
                        name: "temperature".to_string(),
                    }),
                    points: Some(Points {
                        points_type: Some(PointsType::DoublePoints(DoublePoints {
                            points: vec![
                                DoublePoint {
                                    timestamp: Some(Timestamp {
                                        seconds: 1000,
                                        nanos: 500,
                                    }),
                                    value: 23.5,
                                },
                                DoublePoint {
                                    timestamp: Some(Timestamp {
                                        seconds: 2000,
                                        nanos: 1000,
                                    }),
                                    value: 25.7,
                                },
                            ],
                        })),
                    }),
                    tags: Default::default(),
                },
                // String points
                Series {
                    channel: Some(Channel {
                        name: "status".to_string(),
                    }),
                    points: Some(Points {
                        points_type: Some(PointsType::StringPoints(StringPoints {
                            points: vec![
                                StringPoint {
                                    timestamp: Some(Timestamp {
                                        seconds: 1000,
                                        nanos: 500,
                                    }),
                                    value: "OK".to_string(),
                                },
                                StringPoint {
                                    timestamp: Some(Timestamp {
                                        seconds: 2000,
                                        nanos: 1000,
                                    }),
                                    value: "WARNING".to_string(),
                                },
                            ],
                        })),
                    }),
                    tags: Default::default(),
                },
                // Integer points
                Series {
                    channel: Some(Channel {
                        name: "count".to_string(),
                    }),
                    points: Some(Points {
                        points_type: Some(PointsType::IntegerPoints(IntegerPoints {
                            points: vec![
                                IntegerPoint {
                                    timestamp: Some(Timestamp {
                                        seconds: 1000,
                                        nanos: 500,
                                    }),
                                    value: 42,
                                },
                                IntegerPoint {
                                    timestamp: Some(Timestamp {
                                        seconds: 2000,
                                        nanos: 1000,
                                    }),
                                    value: -17,
                                },
                            ],
                        })),
                    }),
                    tags: Default::default(),
                },
                // Uint64 points
                Series {
                    channel: Some(Channel {
                        name: "uptime".to_string(),
                    }),
                    points: Some(Points {
                        points_type: Some(PointsType::Uint64Points(Uint64Points {
                            points: vec![
                                Uint64Point {
                                    timestamp: Some(Timestamp {
                                        seconds: 1000,
                                        nanos: 500,
                                    }),
                                    value: 12345678901234,
                                },
                                Uint64Point {
                                    timestamp: Some(Timestamp {
                                        seconds: 2000,
                                        nanos: 1000,
                                    }),
                                    value: 98765432109876,
                                },
                            ],
                        })),
                    }),
                    tags: Default::default(),
                },
            ],
        };

        // Consume the write request
        consumer
            .consume(&write_request)
            .expect("Failed to consume write request");

        // Flush the writer to ensure data is written
        drop(consumer);

        // Read the avro file back and verify the data
        let file = std::fs::File::open(&temp_file).expect("Failed to open avro file");
        let reader = apache_avro::Reader::new(file).expect("Failed to create avro reader");

        let mut records: Vec<Record> = Vec::new();
        for record_result in reader {
            let record = record_result.expect("Failed to read record");
            if let Value::Record(fields) = record {
                let mut rec = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create record");
                for (name, value) in fields {
                    rec.put(&name, value);
                }
                records.push(rec);
            }
        }


        // Verify temperature (double) series
        let temp_record = ParsedRecord::new(&records[0]);
        assert_eq!(
            temp_record.channel,
            "temperature"
        );
        assert_eq!(temp_record.values[0], AvroValue::Double(23.5));
        assert_eq!(temp_record.values[1], AvroValue::Double(25.7));

        // Verify status (string) series
        let status_record = ParsedRecord::new(&records[1]);
        assert_eq!(status_record.channel, "status");
        assert_eq!(status_record.values[0], AvroValue::String("OK".into()));
        assert_eq!(status_record.values[1], AvroValue::String("WARNING".into()));


        // Verify count (integer) series
        let count_record = ParsedRecord::new(&records[2]);
        assert_eq!(count_record.channel, "count");
        assert_eq!(count_record.values[0], AvroValue::Long(42));
        assert_eq!(count_record.values[1], AvroValue::Long(-17));


        // Verify uptime (uint64) series
        let uptime_record = ParsedRecord::new(&records[3]);
        assert_eq!(uptime_record.channel, "uptime");
        assert_eq!(uptime_record.values[0], AvroValue::Long(12345678901234));
        assert_eq!(uptime_record.values[1], AvroValue::Long(98765432109876));
    }
}
