//! Columnar format — targets the `writeNominalColumnarBatches` endpoint, which
//! takes a single dataset rid in the proto body and packs timestamps and values
//! into parallel arrays per channel.
//!
//! Only compiled when the `columnar` feature is enabled; the row-oriented
//! sibling module (`crate::row`) replaces this when that feature is off.

use std::io::Write;

use apache_avro::types::Record;
use apache_avro::types::Value;
use conjure_http::client::AsyncRequestBody;
use conjure_http::private::header::CONTENT_ENCODING;
use conjure_http::private::header::CONTENT_TYPE;
use conjure_http::private::Request;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::nominal::direct_channel_writer::v2 as columnar;
use nominal_api::tonic::nominal::direct_channel_writer::v2::array_points::ArrayType as ColumnarArrayType;
use nominal_api::tonic::nominal::direct_channel_writer::v2::WriteBatchesRequest;
use nominal_api::tonic::nominal::types::time::Timestamp as NominalTimestamp;
use parking_lot::Mutex;
use prost::Message;
use snap::write::FrameEncoder;

use crate::client::StreamWriteRequest;
use crate::client::WriteRequest;
use crate::consumer::ConsumerError;
use crate::consumer::ConsumerResult;
use crate::consumer::CORE_AVRO_SCHEMA;
use crate::types::ChannelDescriptor;

/// Build a `StreamWriteRequest` (here: `WriteBatchesRequest`) from the drained
/// contents of a `SeriesBuffer`. Row-oriented point batches are repacked into
/// columnar `RecordsBatch`es. The `data_source_rid` is left blank; the core
/// consumer fills it in before encoding.
pub(crate) fn build_write_request(
    drained: Vec<(ChannelDescriptor, PointsType)>,
) -> StreamWriteRequest {
    let batches = drained
        .into_iter()
        .map(|(ChannelDescriptor { name, tags }, points_type)| {
            let columnar_points = points_type_to_columnar(points_type);
            columnar::RecordsBatch {
                channel: name,
                tags: tags
                    .map(|tags| tags.into_iter().collect())
                    .unwrap_or_default(),
                points: Some(columnar_points),
            }
        })
        .collect();
    WriteBatchesRequest {
        batches,
        data_source_rid: String::new(),
    }
}

/// Serialize a `StreamWriteRequest` and wrap it in a `WriteRequest` destined for
/// the Nominal core writer endpoint. Sets `data_source_rid` in the proto body
/// first, since the columnar endpoint expects it there rather than in the URL.
pub(crate) fn encode_for_core<'b>(
    request: &StreamWriteRequest,
    api_key: &BearerToken,
    data_source_rid: &ResourceIdentifier,
) -> std::io::Result<WriteRequest<'b>> {
    let mut request_with_rid = request.clone();
    request_with_rid.data_source_rid = data_source_rid.to_string();
    encode_request(request_with_rid.encode_to_vec(), api_key)
}

fn encode_request<'b>(
    write_request_bytes: Vec<u8>,
    api_key: &BearerToken,
) -> std::io::Result<WriteRequest<'b>> {
    let mut encoder = FrameEncoder::new(Vec::with_capacity(write_request_bytes.len()));

    encoder.write_all(&write_request_bytes)?;

    let mut request = Request::new(AsyncRequestBody::Fixed(
        encoder.into_inner().unwrap().into(),
    ));

    let headers = request.headers_mut();
    headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
    headers.insert(CONTENT_ENCODING, "x-snappy-framed".parse().unwrap());

    *request.method_mut() = conjure_http::private::http::Method::POST;
    let mut path = conjure_http::private::UriBuilder::new();
    path.push_literal("/storage/writer/v1/nominal-columnar");

    *request.uri_mut() = path.build();
    conjure_http::private::encode_header_auth(&mut request, api_key);
    request
        .extensions_mut()
        .insert(conjure_http::client::Endpoint::new(
            "NominalChannelWriterService",
            None,
            "writeNominalColumnarBatches",
            "/storage/writer/v1/nominal-columnar",
        ));
    Ok(request)
}

/// Append the batches in a `StreamWriteRequest` as avro records to the writer.
pub(crate) fn avro_append(
    writer: &Mutex<apache_avro::Writer<'static, std::fs::File>>,
    request: &StreamWriteRequest,
) -> ConsumerResult<()> {
    let mut records: Vec<Record> = Vec::new();
    for batch in &request.batches {
        let (timestamps, values) = columnar_points_to_avro(batch.points.as_ref());

        let mut record = Record::new(&CORE_AVRO_SCHEMA).expect("Failed to create Avro record");

        record.put("channel", batch.channel.clone());
        record.put("timestamps", Value::Array(timestamps));
        record.put("values", Value::Array(values));
        record.put("tags", batch.tags.clone());

        records.push(record);
    }

    writer
        .lock()
        .extend(records)
        .map_err(|e| ConsumerError::AvroError(Box::new(e)))?;

    Ok(())
}

/// Short summary of the request's payload shape, for log messages.
pub(crate) fn request_summary(request: &StreamWriteRequest) -> String {
    format!("{} batches", request.batches.len())
}

/// Convert a `google.protobuf.Timestamp` to the `nominal.types.time.Timestamp`
/// used by the columnar writer protocol.
fn to_nominal_timestamp(ts: &nominal_api::tonic::google::protobuf::Timestamp) -> NominalTimestamp {
    NominalTimestamp {
        seconds: Some(ts.seconds),
        nanos: Some(ts.nanos as i64),
        picos: None,
    }
}

/// Convert row-oriented `PointsType` (per-point timestamps) to columnar `Points`
/// (a parallel timestamp array plus a packed value array).
fn points_type_to_columnar(points_type: PointsType) -> columnar::Points {
    match points_type {
        PointsType::DoublePoints(dp) => {
            let mut timestamps = Vec::with_capacity(dp.points.len());
            let mut values = Vec::with_capacity(dp.points.len());
            for point in dp.points {
                if let Some(ts) = &point.timestamp {
                    timestamps.push(to_nominal_timestamp(ts));
                }
                values.push(point.value);
            }
            columnar::Points {
                timestamps,
                points: Some(columnar::points::Points::DoublePoints(
                    columnar::DoublePoints { points: values },
                )),
            }
        }
        PointsType::StringPoints(sp) => {
            let mut timestamps = Vec::with_capacity(sp.points.len());
            let mut values = Vec::with_capacity(sp.points.len());
            for point in sp.points {
                if let Some(ts) = &point.timestamp {
                    timestamps.push(to_nominal_timestamp(ts));
                }
                values.push(point.value);
            }
            columnar::Points {
                timestamps,
                points: Some(columnar::points::Points::StringPoints(
                    columnar::StringPoints { points: values },
                )),
            }
        }
        PointsType::IntegerPoints(ip) => {
            let mut timestamps = Vec::with_capacity(ip.points.len());
            let mut values = Vec::with_capacity(ip.points.len());
            for point in ip.points {
                if let Some(ts) = &point.timestamp {
                    timestamps.push(to_nominal_timestamp(ts));
                }
                values.push(point.value);
            }
            columnar::Points {
                timestamps,
                points: Some(columnar::points::Points::IntPoints(columnar::IntPoints {
                    points: values,
                })),
            }
        }
        PointsType::Uint64Points(up) => {
            let mut timestamps = Vec::with_capacity(up.points.len());
            let mut values = Vec::with_capacity(up.points.len());
            for point in up.points {
                if let Some(ts) = &point.timestamp {
                    timestamps.push(to_nominal_timestamp(ts));
                }
                values.push(point.value);
            }
            columnar::Points {
                timestamps,
                points: Some(columnar::points::Points::Uint64Points(
                    columnar::Uint64Points { points: values },
                )),
            }
        }
        PointsType::StructPoints(stp) => {
            let mut timestamps = Vec::with_capacity(stp.points.len());
            let mut values = Vec::with_capacity(stp.points.len());
            for point in stp.points {
                if let Some(ts) = &point.timestamp {
                    timestamps.push(to_nominal_timestamp(ts));
                }
                values.push(point.json_string);
            }
            columnar::Points {
                timestamps,
                points: Some(columnar::points::Points::StructPoints(
                    columnar::StructPoints { points: values },
                )),
            }
        }
        PointsType::ArrayPoints(ArrayPoints { array_type }) => match array_type {
            Some(ArrayType::DoubleArrayPoints(dap)) => {
                let mut timestamps = Vec::with_capacity(dap.points.len());
                let mut values = Vec::with_capacity(dap.points.len());
                for point in dap.points {
                    if let Some(ts) = &point.timestamp {
                        timestamps.push(to_nominal_timestamp(ts));
                    }
                    values.push(columnar::DoubleArrayPoint { value: point.value });
                }
                columnar::Points {
                    timestamps,
                    points: Some(columnar::points::Points::ArrayPoints(
                        columnar::ArrayPoints {
                            array_type: Some(columnar::array_points::ArrayType::DoubleArrayPoints(
                                columnar::DoubleArrayPoints { points: values },
                            )),
                        },
                    )),
                }
            }
            Some(ArrayType::StringArrayPoints(sap)) => {
                let mut timestamps = Vec::with_capacity(sap.points.len());
                let mut values = Vec::with_capacity(sap.points.len());
                for point in sap.points {
                    if let Some(ts) = &point.timestamp {
                        timestamps.push(to_nominal_timestamp(ts));
                    }
                    values.push(columnar::StringArrayPoint { value: point.value });
                }
                columnar::Points {
                    timestamps,
                    points: Some(columnar::points::Points::ArrayPoints(
                        columnar::ArrayPoints {
                            array_type: Some(columnar::array_points::ArrayType::StringArrayPoints(
                                columnar::StringArrayPoints { points: values },
                            )),
                        },
                    )),
                }
            }
            None => columnar::Points {
                timestamps: Vec::new(),
                points: None,
            },
        },
    }
}

fn columnar_points_to_avro(points: Option<&columnar::Points>) -> (Vec<Value>, Vec<Value>) {
    let Some(points) = points else {
        return (Vec::new(), Vec::new());
    };

    let timestamps: Vec<Value> = points
        .timestamps
        .iter()
        .map(convert_nominal_timestamp_to_nanoseconds)
        .collect();

    let values: Vec<Value> = match &points.points {
        Some(columnar::points::Points::DoublePoints(columnar::DoublePoints { points })) => points
            .iter()
            .map(|v| Value::Union(0, Box::new(Value::Double(*v))))
            .collect(),
        Some(columnar::points::Points::StringPoints(columnar::StringPoints { points })) => points
            .iter()
            .map(|v| Value::Union(1, Box::new(Value::String(v.clone()))))
            .collect(),
        Some(columnar::points::Points::IntPoints(columnar::IntPoints { points })) => points
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
                        let record =
                            Value::Record(vec![("items".to_string(), Value::Array(array_values))]);
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
                        let record =
                            Value::Record(vec![("items".to_string(), Value::Array(array_values))]);
                        Value::Union(4, Box::new(record))
                    })
                    .collect(),
                None => Vec::new(),
            }
        }
        Some(columnar::points::Points::StructPoints(columnar::StructPoints { points })) => points
            .iter()
            .map(|v| {
                let record = Value::Record(vec![("json".to_string(), Value::String(v.clone()))]);
                Value::Union(5, Box::new(record))
            })
            .collect(),
        Some(columnar::points::Points::Uint64Points(columnar::Uint64Points { points })) => points
            .iter()
            .map(|v| Value::Union(2, Box::new(Value::Long(*v as i64))))
            .collect(),
        Some(columnar::points::Points::LogPoints(columnar::LogPoints { points })) => points
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

fn convert_nominal_timestamp_to_nanoseconds(timestamp: &NominalTimestamp) -> Value {
    let seconds = timestamp.seconds.unwrap_or(0);
    let nanos = timestamp.nanos.unwrap_or(0);
    Value::Long(seconds * 1_000_000_000 + nanos)
}

/// Counts of points per value-kind within a single channel, used by stream tests
/// to assert on captured request contents without exposing the underlying proto
/// shape.
#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub(crate) struct ChannelCounts {
    pub double: usize,
    pub int: usize,
    pub uint64: usize,
    pub string: usize,
    pub struct_: usize,
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;

    use super::*;

    /// Length of the first double-points batch in the first request, if any.
    pub(crate) fn first_request_first_double_count(
        requests: &[StreamWriteRequest],
    ) -> Option<usize> {
        let batch = requests.first()?.batches.first()?;
        let inner = batch.points.as_ref()?;
        if let Some(columnar::points::Points::DoublePoints(dp)) = inner.points.as_ref() {
            // Sanity-check the parallel timestamp array matches.
            debug_assert_eq!(inner.timestamps.len(), dp.points.len());
            Some(dp.points.len())
        } else {
            None
        }
    }

    /// Sum the number of points found under each channel, bucketed by value-kind.
    pub(crate) fn count_points_by_channel(
        requests: &[StreamWriteRequest],
    ) -> HashMap<String, ChannelCounts> {
        let mut out: HashMap<String, ChannelCounts> = HashMap::new();
        for request in requests {
            for batch in &request.batches {
                let entry = out.entry(batch.channel.clone()).or_default();
                let Some(points) = batch.points.as_ref() else {
                    continue;
                };
                match points.points.as_ref() {
                    Some(columnar::points::Points::DoublePoints(dp)) => {
                        entry.double += dp.points.len();
                    }
                    Some(columnar::points::Points::IntPoints(ip)) => {
                        entry.int += ip.points.len();
                    }
                    Some(columnar::points::Points::Uint64Points(up)) => {
                        entry.uint64 += up.points.len();
                    }
                    Some(columnar::points::Points::StringPoints(sp)) => {
                        entry.string += sp.points.len();
                    }
                    Some(columnar::points::Points::StructPoints(stp)) => {
                        entry.struct_ += stp.points.len();
                    }
                    _ => {}
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod avro_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use apache_avro::Reader;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::DoubleArrayPoint;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::DoubleArrayPoints;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::DoublePoints;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::IntPoints;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::StringArrayPoint;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::StringArrayPoints;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::StringPoints;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::StructPoints;
    use nominal_api::tonic::nominal::direct_channel_writer::v2::Uint64Points;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::consumer::AvroFileConsumer;
    use crate::consumer::WriteRequestConsumer;

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
                    points: Some(columnar::points::Points::ArrayPoints(
                        columnar::ArrayPoints {
                            array_type: Some(ColumnarArrayType::DoubleArrayPoints(
                                DoubleArrayPoints {
                                    points: vec![
                                        DoubleArrayPoint {
                                            value: vec![1.0, 2.0, 3.0],
                                        },
                                        DoubleArrayPoint {
                                            value: vec![4.0, 5.0],
                                        },
                                    ],
                                },
                            )),
                        },
                    )),
                },
            );

            let string_array_batch = make_batch(
                "string_arrays",
                columnar::Points {
                    timestamps: vec![make_timestamp(1000, 0), make_timestamp(1001, 0)],
                    points: Some(columnar::points::Points::ArrayPoints(
                        columnar::ArrayPoints {
                            array_type: Some(ColumnarArrayType::StringArrayPoints(
                                StringArrayPoints {
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
                                },
                            )),
                        },
                    )),
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

            drop(consumer);
        }

        let file = std::fs::File::open(&path).unwrap();
        let reader = Reader::new(file).unwrap();

        let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 7, "Expected 7 batch records");

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
