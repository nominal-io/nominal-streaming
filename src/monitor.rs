use std::sync::atomic::AtomicU64;

use parking_lot::Mutex;

#[derive(Debug)]
pub struct StreamHealthMonitor {
    pub requests_sent: AtomicU64,
    pub requests_failed: AtomicU64,
    pub last_active: Mutex<Option<std::time::Instant>>,
    pub last_failure: Mutex<Option<std::time::Instant>>,
    pub last_success: Mutex<Option<std::time::Instant>>,
}

impl Default for StreamHealthMonitor {
    fn default() -> Self {
        Self {
            requests_sent: AtomicU64::new(0),
            requests_failed: AtomicU64::new(0),
            last_active: Mutex::new(None),
            last_failure: Mutex::new(None),
            last_success: Mutex::new(None),
        }
    }
}

impl StreamHealthMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&self) {
        self.requests_sent
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.requests_failed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        *self.last_active.lock() = None;
    }

    pub fn is_idle(&self, window: std::time::Duration) -> bool {
        let last_active = self.last_active.lock();
        if let Some(last) = *last_active {
            last.elapsed() > window
        } else {
            true
        }
    }

    pub fn has_recent_failure(&self, window: std::time::Duration) -> bool {
        let last_failure = self.last_failure.lock();
        if let Some(last) = *last_failure {
            last.elapsed() <= window
        } else {
            false
        }
    }

    pub fn record_success(&self) {
        self.requests_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *self.last_success.lock() = Some(std::time::Instant::now());
        *self.last_active.lock() = Some(std::time::Instant::now());
    }

    pub fn record_failure(&self) {
        self.requests_failed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.requests_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *self.last_failure.lock() = Some(std::time::Instant::now());
        *self.last_active.lock() = Some(std::time::Instant::now());
    }
}
