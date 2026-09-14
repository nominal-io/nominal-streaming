//! Diagnostic boundaries, not TCP acknowledgements. No headers or payloads are logged.
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use parking_lot::Mutex;

#[derive(Default)]
pub(super) struct BodyTiming {
    pub first_poll_micros: Option<u64>,
    pub last_chunk_micros: Option<u64>,
    pub supplied_bytes: usize,
}

pub(super) struct TimedBody {
    remaining: Bytes,
    timing: Arc<Mutex<BodyTiming>>,
    started: Instant,
}
impl TimedBody {
    pub fn new(remaining: Bytes, timing: Arc<Mutex<BodyTiming>>, started: Instant) -> Self {
        Self {
            remaining,
            timing,
            started,
        }
    }
}
impl Body for TimedBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if self.remaining.is_empty() {
            return Poll::Ready(None);
        }
        let length = self.remaining.len().min(64 * 1024);
        let chunk = self.remaining.split_to(length);
        let mut timing = self.timing.lock();
        let elapsed = self.started.elapsed().as_micros() as u64;
        timing.first_poll_micros.get_or_insert(elapsed);
        timing.supplied_bytes += length;
        if self.remaining.is_empty() {
            timing.last_chunk_micros = Some(elapsed);
        }
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }
    fn is_end_stream(&self) -> bool {
        self.remaining.is_empty()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining.len() as u64)
    }
}

#[derive(Clone)]
pub(super) struct ConnectionTiming<S>(pub S);
impl<S, Request> tower::Service<Request> for ConnectionTiming<S>
where
    S: tower::Service<Request>,
    S::Future: Send + 'static,
    S::Response: 'static,
    S::Error: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }
    fn call(&mut self, request: Request) -> Self::Future {
        let started = Instant::now();
        let utc = chrono::Utc::now();
        let future = self.0.call(request);
        Box::pin(async move {
            let result = future.await;
            tracing::info!(target: "nominal_streaming::log::attempt", "{}", serde_json::json!({
                "event": "connection_completed", "started_utc": utc.to_rfc3339(),
                "completed_utc": chrono::Utc::now().to_rfc3339(),
                "elapsed_micros": started.elapsed().as_micros() as u64, "success": result.is_ok(),
            }));
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use http_body::Body;

    use super::*;

    #[test]
    fn timed_body_preserves_bytes_and_exact_length_across_chunks() {
        let input = bytes::Bytes::from(vec![42; 150_000]);
        let timing = std::sync::Arc::new(parking_lot::Mutex::new(BodyTiming::default()));
        let mut body = TimedBody::new(input.clone(), timing.clone(), std::time::Instant::now());
        let mut output = Vec::new();
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        while !body.is_end_stream() {
            assert_eq!(
                body.size_hint().exact(),
                Some((input.len() - output.len()) as u64)
            );
            let frame = std::pin::Pin::new(&mut body).poll_frame(&mut cx);
            let std::task::Poll::Ready(Some(Ok(frame))) = frame else {
                panic!("expected data");
            };
            let chunk = frame.into_data().unwrap();
            assert!(chunk.len() <= 64 * 1024);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, input);
        assert_eq!(body.size_hint().exact(), Some(0));
        let recorded = timing.lock();
        assert_eq!(recorded.supplied_bytes, input.len());
        assert!(recorded.first_poll_micros <= recorded.last_chunk_micros);
        assert!(recorded.last_chunk_micros.is_some());
    }
}
