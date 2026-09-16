//! Runtime metric channels compatible with nominal-client's experimental backend.

use std::borrow::Cow;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
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

const REQUEST_METRICS: [&str; 5] = [
    "__nominal.metric.largest_latency_before_request",
    "__nominal.metric.smallest_latency_before_request",
    "__nominal.metric.request_rtt",
    "__nominal.metric.largest_latency_after_request",
    "__nominal.metric.smallest_latency_after_request",
];

// At most 320 metric points are retained, regardless of dispatcher concurrency.
const MAX_PENDING_REQUESTS: usize = 64;

/// Request metrics for one consumer. Every step is a no-op while disabled, so
/// the consumer runs the same send path whether or not metrics are tracked.
#[derive(Debug, Clone, Default)]
pub(crate) struct RequestMetrics {
    pending: Option<Arc<PendingMetrics>>,
    additional_metric_channels: HashSet<String>,
}

impl RequestMetrics {
    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        if enabled {
            self.pending.get_or_insert_with(Default::default);
        } else {
            self.pending = None;
        }
    }

    /// Channels the caller emits through the stream as metrics rather than data.
    /// They are excluded from latency bounds alongside the request metrics
    /// emitted here, so metrics never measure themselves.
    pub(crate) fn set_additional_metric_channels(
        &mut self,
        channels: impl IntoIterator<Item = impl Into<String>>,
    ) {
        self.additional_metric_channels = channels.into_iter().map(Into::into).collect();
    }

    /// Prepares a request for a measured send. Earlier measurements for the same
    /// session are attached to a copy of the request, so metrics never cost an
    /// extra request. While disabled, or when the request has nothing to measure,
    /// the request is returned unchanged with a measurement that records nothing.
    pub(crate) fn prepare<'a>(
        &'a self,
        request: &'a WriteRequestNominal,
    ) -> (Cow<'a, WriteRequestNominal>, RequestMeasurement<'a>) {
        match self.measure(request) {
            Some((request, bounds)) => (request, RequestMeasurement(Some(bounds))),
            None => (Cow::Borrowed(request), RequestMeasurement(None)),
        }
    }

    fn measure<'a>(
        &'a self,
        request: &'a WriteRequestNominal,
    ) -> Option<(Cow<'a, WriteRequestNominal>, Bounds<'a>)> {
        let pending = self.pending.as_deref()?;
        let (oldest, newest) = timestamp_bounds(request, &self.additional_metric_channels)?;
        let attached = pending.take(&request.session_name);
        let to_send = if attached.is_empty() {
            Cow::Borrowed(request)
        } else {
            let mut combined = request.clone();
            combined.series.extend(attached);
            Cow::Owned(combined)
        };
        let bounds = Bounds {
            pending,
            session_name: request.session_name.clone(),
            oldest,
            newest,
        };
        Some((to_send, bounds))
    }
}

/// Measurements waiting to be piggybacked onto a later data request, shared
/// across dispatcher threads.
#[derive(Debug, Default)]
struct PendingMetrics {
    pending: Mutex<VecDeque<WriteRequestNominal>>,
}

impl PendingMetrics {
    fn take(&self, session_name: &Option<String>) -> Vec<Series> {
        let mut attached = Vec::new();
        self.pending.lock().retain(|metrics| {
            if metrics.session_name == *session_name {
                attached.extend(metrics.series.iter().cloned());
                false
            } else {
                true
            }
        });
        attached
    }

    fn push(&self, metrics: WriteRequestNominal) {
        let mut pending = self.pending.lock();
        if pending.len() == MAX_PENDING_REQUESTS {
            pending.pop_front();
        }
        pending.push_back(metrics);
    }
}

/// A request about to be sent. Records nothing when there is nothing to measure.
#[derive(Debug)]
pub(crate) struct RequestMeasurement<'a>(Option<Bounds<'a>>);

/// The data timestamp bounds of a request that is about to be sent.
#[derive(Debug)]
struct Bounds<'a> {
    pending: &'a PendingMetrics,
    session_name: Option<String>,
    oldest: i128,
    newest: i128,
}

impl<'a> RequestMeasurement<'a> {
    /// Starts the clock. Call immediately before the HTTP send so encoding and
    /// compression stay outside the measured interval.
    pub(crate) fn start(self) -> InFlightRequest<'a> {
        InFlightRequest(self.0.map(|bounds| InFlight {
            bounds,
            before: UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128,
            start: Instant::now(),
        }))
    }
}

/// A send in progress.
#[derive(Debug)]
pub(crate) struct InFlightRequest<'a>(Option<InFlight<'a>>);

#[derive(Debug)]
struct InFlight<'a> {
    bounds: Bounds<'a>,
    before: i128,
    start: Instant,
}

impl InFlightRequest<'_> {
    /// Records the completed send so it piggybacks onto a later request in the
    /// same session. Call only after a successful send; dropping an in-flight
    /// request after a failure records nothing.
    pub(crate) fn complete(self) {
        let Some(InFlight {
            bounds,
            before,
            start,
        }) = self.0
        else {
            return;
        };
        let rtt = start.elapsed().as_secs_f64();
        let after = UNIX_EPOCH.elapsed().unwrap().as_nanos() as i128;
        let mut metrics = metric_request(bounds.oldest, bounds.newest, before, after, rtt);
        metrics.session_name = bounds.session_name;
        bounds.pending.push(metrics);
    }
}

/// Finds the oldest and newest data timestamps in a request, which anchor the
/// staleness metrics for that send. Returns `None` when no non-metric series
/// carries a timestamp. That happens when a flush separates caller-emitted
/// metric channels from the data they describe, or when raw points are
/// enqueued without timestamps.
fn timestamp_bounds(
    request: &WriteRequestNominal,
    additional_metric_channels: &HashSet<String>,
) -> Option<(i128, i128)> {
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
            .is_some_and(|channel| is_metric(&channel.name, additional_metric_channels))
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

/// Metric channels are excluded from latency bounds so metrics never measure
/// themselves. Beyond the request metrics emitted here, the caller names any
/// additional metric channels it sends through the stream.
fn is_metric(name: &str, additional_metric_channels: &HashSet<String>) -> bool {
    REQUEST_METRICS.contains(&name) || additional_metric_channels.contains(name)
}

fn timestamp_ns(timestamp: &Timestamp) -> i128 {
    i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IntoPoints;

    fn enabled() -> RequestMetrics {
        let mut metrics = RequestMetrics::default();
        metrics.set_enabled(true);
        metrics
    }

    fn queue(metrics: &RequestMetrics) -> VecDeque<WriteRequestNominal> {
        metrics.pending.as_ref().unwrap().pending.lock().clone()
    }

    fn none() -> HashSet<String> {
        HashSet::new()
    }

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
        assert_eq!(timestamp_bounds(&request, &none()), None);
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
                    timestamp_bounds(&request(points), &none()),
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
    fn disabled_metrics_leave_requests_untouched_and_record_nothing() {
        let metrics = RequestMetrics::default();
        let request = data_request();
        let (sent, measurement) = metrics.prepare(&request);
        assert!(matches!(sent, Cow::Borrowed(_)));
        assert!(measurement.0.is_none());
        let in_flight = measurement.start();
        assert!(in_flight.0.is_none());
        in_flight.complete();
        assert!(metrics.pending.is_none());
    }

    #[test]
    fn metrics_piggyback_on_next_data_request_without_extra_sends() {
        let metrics = enabled();
        let original = data_request();
        let (sent, measurement) = metrics.prepare(&original);
        assert!(matches!(sent, Cow::Borrowed(_)));
        assert_eq!(*sent, original);
        measurement.start().complete();
        let expected = queue(&metrics)[0].series.clone();
        let (sent, measurement) = metrics.prepare(&original);
        assert_eq!(sent.series[0], original.series[0]);
        assert_eq!(&sent.series[1..], expected.as_slice());
        assert_eq!(
            timestamp_bounds(&sent, &none()),
            timestamp_bounds(&original, &none())
        );
        assert_eq!(original.series.len(), 1);
        assert!(queue(&metrics).is_empty());
        measurement.start().complete();
        assert_eq!(queue(&metrics).len(), 1);
    }

    #[test]
    fn failed_sends_record_nothing_and_do_not_replay_attached_samples() {
        let metrics = enabled();
        let request = data_request();
        let (_, measurement) = metrics.prepare(&request);
        measurement.start().complete();
        let (sent, measurement) = metrics.prepare(&request);
        assert_eq!(sent.series.len(), 6);
        assert!(queue(&metrics).is_empty());
        // Encoding failed before the clock started.
        drop(measurement);
        assert!(queue(&metrics).is_empty());
        let (_, measurement) = metrics.prepare(&request);
        measurement.start().complete();
        let (sent, measurement) = metrics.prepare(&request);
        assert_eq!(sent.series.len(), 6);
        let in_flight = measurement.start();
        // The send failed.
        drop(in_flight);
        assert!(queue(&metrics).is_empty());
    }

    #[test]
    fn empty_and_metric_only_requests_leave_pending_metrics_untouched() {
        let mut metrics = enabled();
        let mut custom = data_request();
        custom.series[0].channel = Some(Channel {
            name: "custom_metric".into(),
        });
        // Caller-named metric channels are only excluded when configured.
        assert!(metrics.prepare(&custom).1 .0.is_some());
        metrics.set_additional_metric_channels(["custom_metric"]);
        let request = data_request();
        let (_, measurement) = metrics.prepare(&request);
        measurement.start().complete();
        for request in [
            WriteRequestNominal::default(),
            metric_request(0, 0, 0, 0, 0.0),
            custom,
        ] {
            let (sent, measurement) = metrics.prepare(&request);
            assert!(matches!(sent, Cow::Borrowed(_)));
            assert!(measurement.0.is_none());
            measurement.start().complete();
            assert_eq!(queue(&metrics).len(), 1);
        }
    }

    #[test]
    fn pending_metrics_are_bounded_and_keep_sessions_separate() {
        let metrics = enabled();
        for index in 0..MAX_PENDING_REQUESTS + 1 {
            let mut request = data_request();
            request.session_name = Some(index.to_string());
            let (sent, measurement) = metrics.prepare(&request);
            assert_eq!(sent.series.len(), 1);
            measurement.start().complete();
        }
        assert_eq!(queue(&metrics).len(), MAX_PENDING_REQUESTS);
        assert_eq!(queue(&metrics)[0].session_name.as_deref(), Some("1"));
        let mut request = data_request();
        request.session_name = Some("1".into());
        let (sent, measurement) = metrics.prepare(&request);
        assert_eq!(sent.series.len(), 6);
        assert_eq!(sent.session_name, request.session_name);
        measurement.start().complete();
        assert_eq!(queue(&metrics).len(), MAX_PENDING_REQUESTS);
    }

    #[test]
    fn concurrent_sends_share_pending_metrics_without_holding_the_lock() {
        let metrics = enabled();
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let metrics = &metrics;
                let barrier = &barrier;
                scope.spawn(move || {
                    let request = data_request();
                    let (_, measurement) = metrics.prepare(&request);
                    let in_flight = measurement.start();
                    barrier.wait();
                    in_flight.complete();
                });
            }
        });
        assert_eq!(queue(&metrics).len(), 8);
        let request = data_request();
        let (sent, measurement) = metrics.prepare(&request);
        assert_eq!(sent.series.len(), 41);
        measurement.start().complete();
        assert_eq!(queue(&metrics).len(), 1);
    }
}
