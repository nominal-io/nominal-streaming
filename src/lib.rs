pub mod client;
pub mod consumer;
pub mod notifier;
pub mod stream;
mod types;

/// This includes the most common types in this crate, re-exported for your convenience.
pub mod prelude {
    pub use conjure_object::BearerToken;
    pub use conjure_object::ResourceIdentifier;
    pub use nominal_api::tonic::google::protobuf::Timestamp;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::points::PointsType;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::DoublePoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringPoint;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::StringPoints;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequest;
    pub use nominal_api::tonic::io::nominal::scout::api::proto::WriteRequestNominal;

    pub use crate::client::PRODUCTION_STREAMING_CLIENT;
    pub use crate::client::STAGING_STREAMING_CLIENT;
    pub use crate::consumer::NominalCoreConsumer;
    pub use crate::stream::NominalDatasourceStream;
    pub use crate::stream::NominalStreamOpts;
    pub use crate::types::ChannelDescriptor;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::UNIX_EPOCH;

    use crate::consumer::ConsumerResult;
    use crate::consumer::WriteRequestConsumer;
    use crate::prelude::*;

    #[derive(Debug)]
    struct TestDatasourceStream {
        requests: Mutex<Vec<WriteRequestNominal>>,
    }

    impl WriteRequestConsumer for Arc<TestDatasourceStream> {
        fn consume(&self, request: &WriteRequestNominal) -> ConsumerResult<()> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(())
        }
    }

    fn create_test_stream() -> (Arc<TestDatasourceStream>, NominalDatasourceStream) {
        let test_consumer = Arc::new(TestDatasourceStream {
            requests: Mutex::new(vec![]),
        });
        let stream = NominalDatasourceStream::new_with_consumer(
            test_consumer.clone(),
            NominalStreamOpts {
                max_points_per_record: 1000,
                max_request_delay: Default::default(),
                max_buffered_requests: 2,
                request_dispatcher_tasks: 4,
            },
        );

        (test_consumer, stream)
    }

    #[test]
    fn test_stream() {
        let (test_consumer, stream) = create_test_stream();

        for batch in 0..5 {
            let mut points = Vec::new();
            for i in 0..1000 {
                let start_time = UNIX_EPOCH.elapsed().unwrap();
                points.push(DoublePoint {
                    timestamp: Some(Timestamp {
                        seconds: start_time.as_secs() as i64,
                        nanos: start_time.subsec_nanos() as i32 + i,
                    }),
                    value: (i % 50) as f64,
                });
            }

            stream.enqueue(
                &ChannelDescriptor::new("channel_1", [("batch_id", batch.to_string())]),
                points,
            );
        }

        drop(stream); // wait for points to flush

        let requests = test_consumer.requests.lock().unwrap();

        // validate that the requests were flushed based on the max_records value, not the
        // max request delay
        assert_eq!(requests.len(), 5);
        let series = requests.first().unwrap().series.first().unwrap();
        if let Some(PointsType::DoublePoints(points)) =
            series.points.as_ref().unwrap().points_type.as_ref()
        {
            assert_eq!(points.points.len(), 1000);
        } else {
            panic!("unexpected data type");
        }
    }

    #[test_log::test]
    fn test_writer() {
        let (test_consumer, stream) = create_test_stream();

        let cd = ChannelDescriptor::channel("channel_1");
        let mut writer = stream.double_writer(&cd);

        for i in 0..5000 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            let value = i % 50;
            writer.push(start_time, value as f64);
        }

        drop(writer); // flush points to stream
        drop(stream); // flush stream to nominal

        let requests = test_consumer.requests.lock().unwrap();

        assert_eq!(requests.len(), 5);
        let series = requests.first().unwrap().series.first().unwrap();
        if let Some(PointsType::DoublePoints(points)) =
            series.points.as_ref().unwrap().points_type.as_ref()
        {
            assert_eq!(points.points.len(), 1000);
        } else {
            panic!("unexpected data type");
        }
    }

    #[test_log::test]
    fn test_dual_writers() {
        let (test_consumer, stream) = create_test_stream();

        let cd1 = ChannelDescriptor::channel("channel_1");
        let cd2 = ChannelDescriptor::channel("channel_2");
        let mut writer1 = stream.double_writer(&cd1);
        let mut writer2 = stream.double_writer(&cd2);

        for i in 0..5000 {
            let start_time = UNIX_EPOCH.elapsed().unwrap();
            let value = i % 50;
            writer1.push(start_time, value as f64);
            writer2.push(start_time, value as f64);
        }

        drop(writer1);
        drop(writer2);
        drop(stream);

        let requests = test_consumer.requests.lock().unwrap();

        assert_eq!(requests.len(), 10);
        let series = requests.first().unwrap().series.first().unwrap();
        if let Some(PointsType::DoublePoints(points)) =
            series.points.as_ref().unwrap().points_type.as_ref()
        {
            assert_eq!(points.points.len(), 1000);
        } else {
            panic!("unexpected data type");
        }
    }
}
