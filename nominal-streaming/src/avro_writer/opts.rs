use std::time::Duration;

/// Configuration for [`super::AvroWriter`].
///
/// All fields have sensible defaults via [`Self::default()`]. Use
/// [`Self::new`] for the default and the `with_*` builders to tune
/// individual fields, or construct by field directly.
#[derive(Debug, Clone)]
pub struct AvroWriterOpts {
    /// Maximum number of points packed into a single avro record before the
    /// underlying stream's batch-processor emits it. Larger values produce fewer,
    /// bigger records; smaller values produce more frequent, smaller records.
    /// Default: 250,000.
    pub max_points_per_batch: usize,

    /// Maximum time the underlying stream buffers a partial batch before forcing
    /// a flush. Tune up for throughput-biased workloads, down for
    /// latency-biased workloads. Default: 100ms.
    pub max_batch_delay: Duration,

    /// Whether [`super::AvroWriter::close`] and `Drop` call `File::sync_all()`
    /// before returning. Default: `true`.
    pub fsync_on_close: bool,

    /// Maximum number of points to write to a single file before rotating to a
    /// new numbered file. `0` means no rotation (write everything to the path
    /// given to the constructor). Default: `0`.
    ///
    /// When > 0, filenames follow `<stem>_<index:03d><suffix>` starting at
    /// `_000`. E.g. `out.avro` → `out_000.avro`, `out_001.avro`, ...
    pub max_points_per_file: usize,
}

impl AvroWriterOpts {
    /// Construct a new opts struct with default values. Equivalent to
    /// [`Self::default`]; provided for discoverability alongside the
    /// `with_*` builder methods.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_points_per_batch(mut self, n: usize) -> Self {
        self.max_points_per_batch = n;
        self
    }

    pub fn with_max_batch_delay(mut self, d: Duration) -> Self {
        self.max_batch_delay = d;
        self
    }

    pub fn with_fsync_on_close(mut self, b: bool) -> Self {
        self.fsync_on_close = b;
        self
    }

    pub fn with_max_points_per_file(mut self, n: usize) -> Self {
        self.max_points_per_file = n;
        self
    }
}

impl Default for AvroWriterOpts {
    fn default() -> Self {
        Self {
            max_points_per_batch: 250_000,
            max_batch_delay: Duration::from_millis(100),
            fsync_on_close: true,
            max_points_per_file: 0,
        }
    }
}
