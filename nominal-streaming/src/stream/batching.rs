use super::ArrayType;
use super::IntoPoints;
use super::Points;
use super::PointsType;
use super::Series;

/// Split detached data without holding the buffer lock; sending applies queue backpressure.
pub(super) fn for_each_record(
    series: Vec<Series>,
    count: usize,
    cap: usize,
    mut send: impl FnMut(Vec<Series>, usize),
) {
    if count <= cap {
        send(series, count);
        return;
    }
    let mut record = Vec::new();
    let mut count = 0;
    for mut series in series {
        let points = series.points.take().unwrap().points_type.unwrap();
        for_each_points_chunk(points, cap, |points, n| {
            if count > 0 && count + n > cap {
                send(std::mem::take(&mut record), count);
                count = 0;
            }
            record.push(Series {
                channel: series.channel.clone(),
                tags: series.tags.clone(),
                points: Some(Points {
                    points_type: Some(points),
                }),
            });
            count += n;
        });
    }
    if !record.is_empty() {
        send(record, count);
    }
}

/// Move oversized inputs into one chunk at a time, leaving fitting inputs untouched.
fn for_each_points_chunk(
    points: PointsType,
    cap: usize,
    mut submit: impl FnMut(PointsType, usize),
) {
    let count = points_len(&points);
    if count <= cap {
        submit(points, count);
        return;
    }

    fn submit_chunks<T>(points: Vec<T>, cap: usize, mut submit: impl FnMut(PointsType, usize))
    where
        Vec<T>: IntoPoints,
    {
        let mut points = points.into_iter();
        while points.len() > 0 {
            let count = points.len().min(cap);
            submit(
                points
                    .by_ref()
                    .take(count)
                    .collect::<Vec<_>>()
                    .into_points(),
                count,
            );
        }
    }

    match points {
        PointsType::DoublePoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::IntegerPoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::Uint64Points(p) => submit_chunks(p.points, cap, submit),
        PointsType::StringPoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::StructPoints(p) => submit_chunks(p.points, cap, submit),
        PointsType::ArrayPoints(p) => match p.array_type {
            Some(ArrayType::DoubleArrayPoints(p)) => submit_chunks(p.points, cap, submit),
            Some(ArrayType::StringArrayPoints(p)) => submit_chunks(p.points, cap, submit),
            None => unreachable!("empty array points fit in one chunk"),
        },
    }
}

pub(super) fn points_len(points_type: &PointsType) -> usize {
    match points_type {
        PointsType::DoublePoints(points) => points.points.len(),
        PointsType::StringPoints(points) => points.points.len(),
        PointsType::IntegerPoints(points) => points.points.len(),
        PointsType::Uint64Points(points) => points.points.len(),
        PointsType::ArrayPoints(points) => match &points.array_type {
            Some(ArrayType::DoubleArrayPoints(points)) => points.points.len(),
            Some(ArrayType::StringArrayPoints(points)) => points.points.len(),
            None => 0,
        },
        PointsType::StructPoints(points) => points.points.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::ChannelDescriptor;
    use super::super::DoublePoint;
    use super::super::IntoTimestamp;
    use super::super::SeriesBuffer;
    use super::super::StringPoint;
    use super::*;

    #[test]
    fn split_records_preserve_tagged_series_contents_and_order() {
        let input = SeriesBuffer::new(usize::MAX);
        for (tag, offset) in [("left", 0), ("right", 100)] {
            input.lock().extend(
                &ChannelDescriptor::with_tags("shared", [("sensor", tag)]),
                (0..8)
                    .map(|i| DoublePoint {
                        timestamp: Some((offset + i).into_timestamp()),
                        value: (offset + i) as f64,
                    })
                    .collect::<Vec<_>>(),
            );
        }
        input.lock().extend(
            &ChannelDescriptor::with_tags("shared", [("sensor", "text")]),
            (0..8)
                .map(|i| StringPoint {
                    timestamp: Some(i.into_timestamp()),
                    value: format!("sample-{i}"),
                })
                .collect::<Vec<_>>(),
        );
        let expected = input.lock().sb.clone();
        let (total, series) = input.take();
        for cap in [1, 3, 8, 24] {
            let output = SeriesBuffer::new(usize::MAX);
            let mut emitted = 0;
            for_each_record(series.clone(), total, cap, |record, count| {
                let actual: usize = record
                    .iter()
                    .map(|s| points_len(s.points.as_ref().unwrap().points_type.as_ref().unwrap()))
                    .sum();
                assert_eq!(count, actual);
                assert!(count > 0 && count <= cap);
                emitted += count;
                for series in record {
                    let channel =
                        ChannelDescriptor::with_tags(series.channel.unwrap().name, series.tags);
                    output
                        .lock()
                        .extend(&channel, series.points.unwrap().points_type.unwrap());
                }
            });
            assert_eq!(emitted, total);
            assert_eq!(*output.lock().sb, expected, "record cap {cap}");
        }
    }

    #[test]
    fn point_chunks_preserve_contents_and_order() {
        for count in [0, 1, 3, 8] {
            let points = (0..count)
                .map(|i| DoublePoint {
                    timestamp: Some(i.into_timestamp()),
                    value: i as f64,
                })
                .collect::<Vec<_>>()
                .into_points();
            let buffer = SeriesBuffer::new(usize::MAX);
            let channel = ChannelDescriptor::new("value");
            for_each_points_chunk(points.clone(), 3, |chunk, count| {
                assert_eq!(points_len(&chunk), count);
                assert!(count <= 3);
                buffer.lock().extend(&channel, chunk);
            });
            assert_eq!(buffer.lock().sb.get(&channel), Some(&points));
        }
    }
}
