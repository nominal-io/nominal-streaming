//! Runtime metric channels compatible with nominal-client's experimental backend.

use std::collections::VecDeque;
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
use parking_lot::Mutex;

use crate::consumer::ConsumerResult;

const REQUEST_METRICS: [&str; 5] = [
    "__nominal.metric.largest_latency_before_request",
    "__nominal.metric.smallest_latency_before_request",
    "__nominal.metric.request_rtt",
    "__nominal.metric.largest_latency_after_request",
    "__nominal.metric.smallest_latency_after_request",
];

// At most 320 metric points are retained, regardless of dispatcher concurrency.
const MAX_PENDING_REQUESTS: usize = 64;

/// Measurements waiting to be piggybacked onto a later data request, shared
/// across dispatcher threads.
#[derive(Debug, Default)]
pub(crate) struct PendingMetrics {
    pending: Mutex<VecDeque<WriteRequestNominal>>,
}

impl PendingMetrics {
    /// Sends a data request while measuring its latency, attaching any earlier
    /// measurements for the same session so metrics never cost an extra request.
    /// Requests with nothing to measure are sent unchanged. A failed send drops
    /// the attached measurements and records none of its own.
    pub(crate) fn consume<T>(
        &self,
        request: &WriteRequestNominal,
        encode: impl FnOnce(&WriteRequestNominal) -> ConsumerResult<T>,
        send: impl FnOnce(T) -> ConsumerResult<()>,
    ) -> ConsumerResult<()> {
        let Some((oldest, newest)) = timestamp_bounds(request) else {
            return send(encode(request)?);
        };
        let attached = {
            let mut pending = self.pending.lock();
            let mut attached = Vec::new();
            pending.retain(|metrics| {
                if metrics.session_name == request.session_name {
                    attached.extend(metrics.series.iter().cloned());
                    false
                } else {
                    true
                }
            });
            attached
        };
        let encoded = if attached.is_empty() {
            encode(request)?
        } else {
            let mut combined = request.clone();
            combined.series.extend(attached);
            encode(&combined)?
        };
        let before = UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128;
        let start = Instant::now();
        send(encoded)?;
        let rtt = start.elapsed().as_secs_f64();
        let after = UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128;
        let mut metrics = metric_request(oldest, newest, before, after, rtt);
        metrics.session_name = request.session_name.clone();
        let mut pending = self.pending.lock();
        if pending.len() == MAX_PENDING_REQUESTS {
            pending.pop_front();
        }
        pending.push_back(metrics);
        Ok(())
    }
}

/// Finds the oldest and newest data timestamps in a request, which anchor the
/// staleness metrics for that send. Returns `None` when the request has no
/// timestamped data points, including empty and metric-only requests, since
/// metric channels are ignored.
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

/// Builds the five request-latency series for one completed send, using the
/// channel names and units (seconds) that nominal-client dashboards expect.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerError;
    use crate::types::IntoPoints;

    /// Builds a minimal non-metric request around the given points.
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

    /// A one-point request with well-defined timestamp bounds.
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
    fn metrics_piggyback_on_next_data_request_without_extra_sends() {
        let metrics = PendingMetrics::default();
        let original = data_request();
        let mut sent = Vec::new();
        metrics
            .consume(
                &original,
                |r| Ok(r.clone()),
                |r| {
                    sent.push(r);
                    Ok(())
                },
            )
            .unwrap();
        let expected = metrics.pending.lock()[0].series.clone();
        assert_eq!(sent, vec![original.clone()]);
        metrics
            .consume(
                &original,
                |r| Ok(r.clone()),
                |r| {
                    sent.push(r);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(&sent[1].series[1..], expected.as_slice());
        assert_eq!(timestamp_bounds(&sent[1]), timestamp_bounds(&original));
        assert_eq!(original.series.len(), 1);
        assert_eq!(metrics.pending.lock().len(), 1);
        // Dropping the buffer performs no send, including the final measurement.
        drop(metrics);
        assert_eq!(sent.len(), 2);
    }

    #[test]
    fn failures_propagate_without_metrics_or_replaying_attached_samples() {
        let metrics = PendingMetrics::default();
        metrics
            .consume(&data_request(), |_| Ok(()), |_| Ok(()))
            .unwrap();
        let result = metrics.consume(
            &data_request(),
            |r| {
                assert_eq!(r.series.len(), 6);
                Ok(())
            },
            |_| Err(ConsumerError::MissingTokenError),
        );
        assert!(matches!(result, Err(ConsumerError::MissingTokenError)));
        assert!(metrics.pending.lock().is_empty());
        metrics
            .consume(&data_request(), |_| Ok(()), |_| Ok(()))
            .unwrap();
        let result = metrics.consume(
            &data_request(),
            |r| {
                assert_eq!(r.series.len(), 6);
                Err::<(), _>(ConsumerError::MissingTokenError)
            },
            |_| panic!("must not send"),
        );
        assert!(result.is_err());
        assert!(metrics.pending.lock().is_empty());
    }

    #[test]
    fn empty_and_metric_only_requests_leave_pending_metrics_untouched() {
        let metrics = PendingMetrics::default();
        metrics
            .consume(&data_request(), |_| Ok(()), |_| Ok(()))
            .unwrap();
        for request in [
            WriteRequestNominal::default(),
            metric_request(0, 0, 0, 0, 0.0),
        ] {
            metrics
                .consume(
                    &request,
                    |r| {
                        assert_eq!(r, &request);
                        Ok(())
                    },
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(metrics.pending.lock().len(), 1);
        }
    }

    #[test]
    fn pending_metrics_are_bounded_and_keep_sessions_separate() {
        let metrics = PendingMetrics::default();
        for index in 0..MAX_PENDING_REQUESTS + 1 {
            let mut request = data_request();
            request.session_name = Some(index.to_string());
            metrics
                .consume(
                    &request,
                    |r| {
                        assert_eq!(r.series.len(), 1);
                        Ok(())
                    },
                    |_| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(metrics.pending.lock().len(), MAX_PENDING_REQUESTS);
        assert_eq!(metrics.pending.lock()[0].session_name.as_deref(), Some("1"));
        let mut request = data_request();
        request.session_name = Some("1".into());
        metrics
            .consume(
                &request,
                |r| {
                    assert_eq!(r.series.len(), 6);
                    assert_eq!(r.session_name, request.session_name);
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(metrics.pending.lock().len(), MAX_PENDING_REQUESTS);
    }

    #[test]
    fn concurrent_sends_share_pending_metrics_without_holding_the_lock() {
        let metrics = PendingMetrics::default();
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let metrics = &metrics;
                let barrier = &barrier;
                scope.spawn(move || {
                    metrics
                        .consume(
                            &data_request(),
                            |_| Ok(()),
                            |_| {
                                barrier.wait();
                                Ok(())
                            },
                        )
                        .unwrap()
                });
            }
        });
        assert_eq!(metrics.pending.lock().len(), 8);
        metrics
            .consume(
                &data_request(),
                |r| {
                    assert_eq!(r.series.len(), 41);
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(metrics.pending.lock().len(), 1);
    }
}
