//! Runtime metric channels compatible with nominal-client's experimental backend.

use std::time::Instant;
use std::time::UNIX_EPOCH;

use nominal_api::tonic::google::protobuf::Timestamp;
use nominal_api::tonic::io::nominal::scout::api::proto::array_points::ArrayType;
use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
use nominal_api::tonic::io::nominal::scout::api::proto::Channel;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
use nominal_api::tonic::io::nominal::scout::api::proto::Points;
use nominal_api::tonic::io::nominal::scout::api::proto::Series;
use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;

use crate::consumer::ConsumerResult;

const REQUEST_METRICS: [&str; 5] = [
    "__nominal.metric.largest_latency_before_request",
    "__nominal.metric.smallest_latency_before_request",
    "__nominal.metric.request_rtt",
    "__nominal.metric.largest_latency_after_request",
    "__nominal.metric.smallest_latency_after_request",
];

fn is_metric(name: &str) -> bool {
    REQUEST_METRICS.contains(&name)
        || matches!(
            name,
            "enque_dict_start_staleness" | "enque_dict_end_staleness"
        )
}

fn timestamp_ns(timestamp: &Timestamp) -> i128 {
    i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos)
}

fn timestamp_bounds(request: &WriteRequestNominal) -> Option<(i128, i128)> {
    let mut bounds: Option<(i128, i128)> = None;
    let mut add = |timestamp: &Option<Timestamp>| {
        if let Some(timestamp) = timestamp {
            let ns = timestamp_ns(timestamp);
            bounds = Some(match bounds {
                Some((oldest, newest)) => (oldest.min(ns), newest.max(ns)),
                None => (ns, ns),
            });
        }
    };
    for series in &request.series {
        if series
            .channel
            .as_ref()
            .is_some_and(|channel| is_metric(&channel.name))
        {
            continue;
        }
        let Some(points) = series
            .points
            .as_ref()
            .and_then(|points| points.points_type.as_ref())
        else {
            continue;
        };
        macro_rules! visit {
            ($points:expr) => {
                for point in &$points.points {
                    add(&point.timestamp);
                }
            };
        }
        match points {
            PointsType::DoublePoints(points) => visit!(points),
            PointsType::StringPoints(points) => visit!(points),
            PointsType::IntegerPoints(points) => visit!(points),
            PointsType::Uint64Points(points) => visit!(points),
            PointsType::StructPoints(points) => visit!(points),
            PointsType::ArrayPoints(points) => match &points.array_type {
                Some(ArrayType::DoubleArrayPoints(points)) => visit!(points),
                Some(ArrayType::StringArrayPoints(points)) => visit!(points),
                None => {}
            },
        }
    }
    bounds
}

fn metric_request(
    oldest: i128,
    newest: i128,
    before: i128,
    after: i128,
    rtt: f64,
) -> WriteRequestNominal {
    let timestamp = Timestamp {
        seconds: after.div_euclid(1_000_000_000) as i64,
        nanos: after.rem_euclid(1_000_000_000) as i32,
    };
    let values = [
        (before - oldest) as f64 / 1e9,
        (before - newest) as f64 / 1e9,
        rtt,
        (after - oldest) as f64 / 1e9,
        (after - newest) as f64 / 1e9,
    ];
    WriteRequestNominal {
        session_name: None,
        series: REQUEST_METRICS
            .into_iter()
            .zip(values)
            .map(|(name, value)| Series {
                channel: Some(Channel {
                    name: name.to_string(),
                }),
                tags: Default::default(),
                points: Some(Points {
                    points_type: Some(PointsType::DoublePoints(DoublePoints {
                        points: vec![DoublePoint {
                            timestamp: Some(timestamp),
                            value,
                        }],
                    })),
                }),
            })
            .collect(),
    }
}

/// Called after serialization, immediately around the HTTP send (including retries).
/// A failed metrics upload must not turn a successful data write into a fallback write.
pub(crate) fn consume_with_metrics(
    request: &WriteRequestNominal,
    send: impl FnOnce() -> ConsumerResult<()>,
    upload_metrics: impl FnOnce(&WriteRequestNominal) -> ConsumerResult<()>,
) -> ConsumerResult<()> {
    let Some((oldest, newest)) = timestamp_bounds(request) else {
        return send();
    };
    let before = UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128;
    let start = Instant::now();
    send()?;
    let rtt = start.elapsed().as_secs_f64();
    let after = UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128;
    let mut metrics = metric_request(oldest, newest, before, after, rtt);
    metrics.session_name = request.session_name.clone();
    if let Err(error) = upload_metrics(&metrics) {
        tracing::warn!("Failed to upload runtime metrics: {error}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerError;
    use crate::types::IntoPoints;

    fn request(points: PointsType) -> WriteRequestNominal {
        WriteRequestNominal {
            session_name: None,
            series: vec![Series {
                channel: Some(Channel {
                    name: "data".into(),
                }),
                tags: Default::default(),
                points: Some(Points {
                    points_type: Some(points),
                }),
            }],
        }
    }

    fn data_request() -> WriteRequestNominal {
        request(
            vec![DoublePoint {
                timestamp: Some(Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                value: 42.0,
            }]
            .into_points(),
        )
    }

    #[test]
    fn metrics_have_compatible_names_units_and_timestamps() {
        let request = metric_request(
            1_000_000_000,
            4_000_000_000,
            3_000_000_000,
            3_500_000_001,
            0.5,
        );
        let expected = [2.0, -1.0, 0.5, 2.500000001, -0.499999999];
        for ((series, name), value) in request.series.iter().zip(REQUEST_METRICS).zip(expected) {
            assert_eq!(series.channel.as_ref().unwrap().name, name);
            assert!(series.tags.is_empty());
            let Some(PointsType::DoublePoints(points)) =
                &series.points.as_ref().unwrap().points_type
            else {
                panic!()
            };
            assert_eq!(points.points.len(), 1);
            assert!((points.points[0].value - value).abs() < 1e-12);
            assert_eq!(
                points.points[0].timestamp,
                Some(Timestamp {
                    seconds: 3,
                    nanos: 500_000_001
                })
            );
        }
        // Metrics never contribute to latency bounds or generate more metrics.
        assert_eq!(timestamp_bounds(&request), None);
    }

    #[test]
    fn finds_bounds_across_every_point_type_and_unsorted_points() {
        use nominal_api::tonic::io::nominal::scout::api::proto::*;
        let low = Some(Timestamp {
            seconds: -1,
            nanos: 999_999_999,
        });
        let high = Some(Timestamp {
            seconds: 2,
            nanos: 1,
        });
        macro_rules! check {
            ($ty:ident, $field:ident, $value:expr) => {{
                let points = vec![
                    $ty {
                        timestamp: high,
                        $field: $value,
                    },
                    $ty {
                        timestamp: None,
                        $field: $value,
                    },
                    $ty {
                        timestamp: low,
                        $field: $value,
                    },
                ]
                .into_points();
                assert_eq!(
                    timestamp_bounds(&request(points)),
                    Some((-1, 2_000_000_001))
                );
            }};
        }
        check!(DoublePoint, value, 0.0);
        check!(IntegerPoint, value, 0);
        check!(Uint64Point, value, 0);
        check!(StringPoint, value, String::new());
        check!(StructPoint, json_string, String::new());
        check!(DoubleArrayPoint, value, vec![0.0]);
        check!(StringArrayPoint, value, vec![String::new()]);
    }

    #[test]
    fn successful_write_emits_once_and_metric_failure_preserves_success() {
        let mut sends = 0;
        let mut uploads = 0;
        consume_with_metrics(
            &data_request(),
            || {
                sends += 1;
                Ok(())
            },
            |metrics| {
                uploads += 1;
                assert_eq!(metrics.series.len(), 5);
                assert_eq!(timestamp_bounds(metrics), None);
                Err(ConsumerError::RequestError("metrics unavailable".into()))
            },
        )
        .unwrap();
        assert_eq!((sends, uploads), (1, 1));
    }

    #[test]
    fn failed_write_does_not_emit_metrics() {
        let result = consume_with_metrics(
            &data_request(),
            || Err(ConsumerError::MissingTokenError),
            |_| panic!("must not upload"),
        );
        assert!(matches!(result, Err(ConsumerError::MissingTokenError)));
    }

    #[test]
    fn empty_and_metric_only_requests_do_not_emit_metrics() {
        for request in [
            WriteRequestNominal::default(),
            metric_request(0, 0, 0, 0, 0.0),
        ] {
            let mut sent = false;
            consume_with_metrics(
                &request,
                || {
                    sent = true;
                    Ok(())
                },
                |_| panic!("must not upload"),
            )
            .unwrap();
            assert!(sent);
        }
    }
}
