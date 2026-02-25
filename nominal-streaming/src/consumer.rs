use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use apache_avro::types::Record;
use apache_avro::types::Value;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::nominal::direct_channel_writer::v2::WriteBatchesRequest;
use nominal_api::tonic::nominal::direct_channel_writer::v2::{
    self as columnar, DoublePoints, IntPoints, LogPoints, StringPoints, StructPoints,
    Uint64Points,
};
use nominal_api::tonic::nominal::direct_channel_writer::v2::array_points::ArrayType as ColumnarArrayType;
use nominal_api::tonic::nominal::types::time::Timestamp as NominalTimestamp;
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
    fn consume(&self, request: &WriteBatchesRequest) -> ConsumerResult<()>;
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
    fn consume(&self, request: &WriteBatchesRequest) -> ConsumerResult<()> {
        let token = self
            .auth_provider
            .token()
            .ok_or(ConsumerError::MissingTokenError)?;
        // Set the data_source_rid in the proto body before encoding
        let mut request_with_rid = request.clone();
        request_with_rid.data_source_rid = self.data_source_rid.to_string();
        let write_request = client::encode_request(request_with_rid.encode_to_vec(), &token)?;
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

    fn append_batches(&self, batches: &[columnar::RecordsBatch]) -> ConsumerResult<()> {
        let mut records: Vec<Record> = Vec::new();
        for batch in batches {
            let (timestamps, values) = columnar_points_to_avro(batch.points.as_ref());

            let mut record = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create Avro record");

            record.put("channel", batch.channel.clone());
            record.put("timestamps", Value::Array(timestamps));
            record.put("values", Value::Array(values));
            record.put("tags", batch.tags.clone());

            records.push(record);
        }

        self.writer
            .lock()
            .extend(records)
            .map_err(|e| ConsumerError::AvroError(Box::new(e)))?;

        Ok(())
    }
}

fn convert_nominal_timestamp_to_nanoseconds(timestamp: &NominalTimestamp) -> Value {
    let seconds = timestamp.seconds.unwrap_or(0);
    let nanos = timestamp.nanos.unwrap_or(0);
    Value::Long(seconds * 1_000_000_000 + nanos)
}

fn columnar_points_to_avro(
    points: Option<&columnar::Points>,
) -> (Vec<Value>, Vec<Value>) {
    let Some(points) = points else {
        return (Vec::new(), Vec::new());
    };

    let timestamps: Vec<Value> = points
        .timestamps
        .iter()
        .map(convert_nominal_timestamp_to_nanoseconds)
        .collect();

    let values: Vec<Value> = match &points.points {
        Some(columnar::points::Points::DoublePoints(DoublePoints { points })) => points
            .iter()
            .map(|v| Value::Union(0, Box::new(Value::Double(*v))))
            .collect(),
        Some(columnar::points::Points::StringPoints(StringPoints { points })) => points
            .iter()
            .map(|v| Value::Union(1, Box::new(Value::String(v.clone()))))
            .collect(),
        Some(columnar::points::Points::IntPoints(IntPoints { points })) => points
            .iter()
            .map(|v| Value::Union(2, Box::new(Value::Long(*v))))
            .collect(),
        Some(columnar::points::Points::ArrayPoints(columnar::ArrayPoints { array_type })) => {
            match array_type {
                Some(ColumnarArrayType::DoubleArrayPoints(dap)) => dap
                    .points
                    .iter()
                    .map(|point| {
                        let array_values: Vec<Value> =
                            point.value.iter().map(|v| Value::Double(*v)).collect();
                        let record = Value::Record(vec![(
                            "items".to_string(),
                            Value::Array(array_values),
                        )]);
                        Value::Union(3, Box::new(record))
                    })
                    .collect(),
                Some(ColumnarArrayType::StringArrayPoints(sap)) => sap
                    .points
                    .iter()
                    .map(|point| {
                        let array_values: Vec<Value> = point
                            .value
                            .iter()
                            .map(|v: &String| Value::String(v.clone()))
                            .collect();
                        let record = Value::Record(vec![(
                            "items".to_string(),
                            Value::Array(array_values),
                        )]);
                        Value::Union(4, Box::new(record))
                    })
                    .collect(),
                None => Vec::new(),
            }
        }
        Some(columnar::points::Points::StructPoints(StructPoints { points })) => points
            .iter()
            .map(|v| {
                let record =
                    Value::Record(vec![("json".to_string(), Value::String(v.clone()))]);
                Value::Union(5, Box::new(record))
            })
            .collect(),
        Some(columnar::points::Points::Uint64Points(Uint64Points { points })) => points
            .iter()
            .map(|v| Value::Union(2, Box::new(Value::Long(*v as i64))))
            .collect(),
        Some(columnar::points::Points::LogPoints(LogPoints { points })) => points
            .iter()
            .map(|p| {
                let msg = p
                    .value
                    .as_ref()
                    .map(|v| v.message.clone())
                    .unwrap_or_default();
                Value::Union(1, Box::new(Value::String(msg)))
            })
            .collect(),
        None => Vec::new(),
    };

    (timestamps, values)
}

impl WriteRequestConsumer for AvroFileConsumer {
    fn consume(&self, request: &WriteBatchesRequest) -> ConsumerResult<()> {
        self.append_batches(&request.batches)?;
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
    fn consume(&self, request: &WriteBatchesRequest) -> ConsumerResult<()> {
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
    fn consume(&self, request: &WriteBatchesRequest) -> ConsumerResult<()> {
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
    fn consume(&self, request: &WriteBatchesRequest) -> ConsumerResult<()> {
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

    use nominal_api::tonic::nominal::direct_channel_writer::v2::{
        self as columnar, DoubleArrayPoint, DoubleArrayPoints, StringArrayPoint,
        StringArrayPoints,
    };
    use nominal_api::tonic::nominal::types::time::Timestamp as NominalTimestamp;
    use tempfile::NamedTempFile;

    use apache_avro::Reader;

    use super::*;

    fn make_timestamp(secs: i64, nanos: i64) -> NominalTimestamp {
        NominalTimestamp {
            seconds: Some(secs),
            nanos: Some(nanos),
            picos: None,
        }
    }

    fn make_batch(name: &str, points: columnar::Points) -> columnar::RecordsBatch {
        columnar::RecordsBatch {
            channel: name.to_string(),
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

            let double_batch = make_batch(
                "doubles",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::DoublePoints(DoublePoints {
                        points: vec![1.5, 2.5],
                    })),
                },
            );

            let int_batch = make_batch(
                "longs",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::IntPoints(IntPoints {
                        points: vec![42, -100],
                    })),
                },
            );

            let string_batch = make_batch(
                "strings",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::StringPoints(StringPoints {
                        points: vec!["hello".to_string(), "world".to_string()],
                    })),
                },
            );

            let double_array_batch = make_batch(
                "double_arrays",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::ArrayPoints(columnar::ArrayPoints {
                        array_type: Some(ColumnarArrayType::DoubleArrayPoints(DoubleArrayPoints {
                            points: vec![
                                DoubleArrayPoint {
                                    value: vec![1.0, 2.0, 3.0],
                                },
                                DoubleArrayPoint {
                                    value: vec![4.0, 5.0],
                                },
                            ],
                        })),
                    })),
                },
            );

            let string_array_batch = make_batch(
                "string_arrays",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::ArrayPoints(columnar::ArrayPoints {
                        array_type: Some(ColumnarArrayType::StringArrayPoints(StringArrayPoints {
                            points: vec![
                                StringArrayPoint {
                                    value: vec!["a".to_string(), "b".to_string()],
                                },
                                StringArrayPoint {
                                    value: vec![
                                        "c".to_string(),
                                        "d".to_string(),
                                        "e".to_string(),
                                    ],
                                },
                            ],
                        })),
                    })),
                },
            );

            let struct_batch = make_batch(
                "structs",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::StructPoints(StructPoints {
                        points: vec![
                            r#"{"key": "value"}"#.to_string(),
                            r#"{"count": 42}"#.to_string(),
                        ],
                    })),
                },
            );

            let uint64_batch = make_batch(
                "uint64s",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::Uint64Points(Uint64Points {
                        points: vec![u64::MAX, 12345678901234567890],
                    })),
                },
            );

            let request = WriteBatchesRequest {
                batches: vec![
                    double_batch,
                    int_batch,
                    string_batch,
                    double_array_batch,
                    string_array_batch,
                    struct_batch,
                    uint64_batch,
                ],
                data_source_rid: String::new(),
            };

            consumer.consume(&request).unwrap();

            // Flush the writer by dropping it
            drop(consumer);
        }

        // Read back the file and verify
        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();

        let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 7, "Expected 7 batch records");

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
}
