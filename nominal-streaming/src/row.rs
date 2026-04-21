//! Row-oriented format — targets the `writeNominalBatches` endpoint, which takes
//! a per-dataset-rid URL path and carries one timestamp per point in the body.
//!
//! Only compiled when the `columnar` feature is disabled; the columnar
//! sibling module (`crate::columnar`) replaces this when that feature is on.

use std::io::Write;

use apache_avro::types::Record;
use apache_avro::types::Value;
use conjure_http::client::AsyncRequestBody;
use conjure_http::private::header::CONTENT_ENCODING;
use conjure_http::private::header::CONTENT_TYPE;
use conjure_http::private::Request;
use conjure_object::BearerToken;
use conjure_object::ResourceIdentifier;
use nominal_api::objects::api::rids::NominalDataSourceOrDatasetRid;
use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::ArrayPoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
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
use snap::write::FrameEncoder;

use crate::client::StreamWriteRequest;
use crate::client::WriteRequest;
use crate::consumer::ConsumerError;
use crate::consumer::ConsumerResult;
use crate::consumer::CORE_AVRO_SCHEMA;
use crate::types::ChannelDescriptor;

/// Build a `StreamWriteRequest` (here: `WriteRequestNominal`) from the drained
/// contents of a `SeriesBuffer`.
pub(crate) fn build_write_request(
    drained: Vec<(ChannelDescriptor, PointsType)>,
) -> StreamWriteRequest {
    let series = drained
        .into_iter()
        .map(|(ChannelDescriptor { name, tags }, points_type)| Series {
            channel: Some(Channel { name }),
            tags: tags
                .map(|tags| tags.into_iter().collect())
                .unwrap_or_default(),
            points: Some(Points {
                points_type: Some(points_type),
            }),
        })
        .collect();
    WriteRequestNominal { series }
}

/// Serialize a `StreamWriteRequest` and wrap it in a `WriteRequest` destined for
/// the Nominal core writer endpoint.
pub(crate) fn encode_for_core<'b>(
    request: &StreamWriteRequest,
    api_key: &BearerToken,
    data_source_rid: &ResourceIdentifier,
) -> std::io::Result<WriteRequest<'b>> {
    encode_request(request.encode_to_vec(), api_key, data_source_rid)
}

fn encode_request<'a, 'b>(
    write_request_bytes: Vec<u8>,
    api_key: &'a BearerToken,
    data_source_rid: &'a ResourceIdentifier,
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
    path.push_literal("/storage/writer/v1/nominal");

    let nominal_data_source_or_dataset_rid = NominalDataSourceOrDatasetRid(data_source_rid.clone());
    path.push_path_parameter(&nominal_data_source_or_dataset_rid);

    *request.uri_mut() = path.build();
    conjure_http::private::encode_header_auth(&mut request, api_key);
    request
        .extensions_mut()
        .insert(conjure_http::client::Endpoint::new(
            "NominalChannelWriterService",
            None,
            "writeNominalBatches",
            "/storage/writer/v1/nominal/{dataSourceRid}",
        ));
    Ok(request)
}

/// Append the series in a `StreamWriteRequest` as avro records to the writer.
pub(crate) fn avro_append(
    writer: &Mutex<apache_avro::Writer<'static, std::fs::File>>,
    request: &StreamWriteRequest,
) -> ConsumerResult<()> {
    let mut records: Vec<Record> = Vec::new();
    for series in &request.series {
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

    writer
        .lock()
        .extend(records)
        .map_err(|e| ConsumerError::AvroError(Box::new(e)))?;

    Ok(())
}

/// Short summary of the request's payload shape, for log messages.
pub(crate) fn request_summary(request: &StreamWriteRequest) -> String {
    format!("{} series", request.series.len())
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

    /// Length of the first double-points series in the first request, if any.
    pub(crate) fn first_request_first_double_count(
        requests: &[StreamWriteRequest],
    ) -> Option<usize> {
        let series = requests.first()?.series.first()?;
        if let Some(PointsType::DoublePoints(points)) = series.points.as_ref()?.points_type.as_ref()
        {
            Some(points.points.len())
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
            for series in &request.series {
                let name = series
                    .channel
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or_default();
                let entry = out.entry(name).or_default();
                match series.points.as_ref().and_then(|p| p.points_type.as_ref()) {
                    Some(PointsType::DoublePoints(dp)) => entry.double += dp.points.len(),
                    Some(PointsType::IntegerPoints(ip)) => entry.int += ip.points.len(),
                    Some(PointsType::Uint64Points(up)) => entry.uint64 += up.points.len(),
                    Some(PointsType::StringPoints(sp)) => entry.string += sp.points.len(),
                    Some(PointsType::StructPoints(stp)) => entry.struct_ += stp.points.len(),
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
    use nominal_api::tonic::io::nominal::scout::api::proto::DoubleArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::IntegerPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringArrayPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::StructPoint;
    use nominal_api::tonic::io::nominal::scout::api::proto::Uint64Point;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::consumer::AvroFileConsumer;
    use crate::consumer::WriteRequestConsumer;

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

            let double_series = make_series(
                "doubles",
                Points {
                    points_type: Some(PointsType::DoublePoints(DoublePoints {
                        points: vec![
                            DoublePoint {
                                timestamp: make_timestamp(1000, 0),
                                value: 1.5,
                            },
                            DoublePoint {
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
                            IntegerPoint {
                                timestamp: make_timestamp(1000, 0),
                                value: 42,
                            },
                            IntegerPoint {
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
                            StringPoint {
                                timestamp: make_timestamp(1000, 0),
                                value: "hello".to_string(),
                            },
                            StringPoint {
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
                            StructPoint {
                                timestamp: make_timestamp(1000, 0),
                                json_string: r#"{"key": "value"}"#.to_string(),
                            },
                            StructPoint {
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
                            Uint64Point {
                                timestamp: make_timestamp(1000, 0),
                                value: u64::MAX,
                            },
                            Uint64Point {
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
