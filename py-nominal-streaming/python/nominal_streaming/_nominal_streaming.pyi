from __future__ import annotations

import pathlib
from types import TracebackType
from typing import TYPE_CHECKING, Any, Mapping, Sequence, Type

from typing_extensions import Self

from nominal_streaming.nominal_dataset_stream import DataType

if TYPE_CHECKING:
    import polars as pl

class PyNominalStreamOpts:
    """Configuration options for Nominal data streaming.

    This class configures how data points are batched, buffered, and dispatched
    to the Nominal backend. It mirrors the Rust `NominalStreamOpts` structure,
    providing Pythonic accessors and fluent builder-style methods.
    """

    def __init__(
        self,
        *,
        max_points_per_batch: int = 250_000,
        max_request_delay_secs: float = 0.1,
        max_buffered_requests: int = 4,
        num_upload_workers: int = 8,
        num_runtime_workers: int = 8,
        base_api_url: str = "https://api.gov.nominal.io/api",
    ) -> None:
        """Initialize a PyNominalStreamOpts instance.

        Args:
            max_points_per_batch: Maximum number of points per record before dispatching a request.
            max_request_delay_secs: Maximum delay before a request is sent, even if it results in a partial request.
            max_buffered_requests: Maximum number of buffered requests before applying backpressure.
            num_upload_workers: Number of concurrent network dispatches to perform.
                NOTE: should be less than the number of `num_runtime_workers`
            num_runtime_workers: Number of runtime worker threads for concurrent processing.
            base_api_url: Base URL of the Nominal API endpoint to stream data to.
        """

    @property
    def max_points_per_batch(self) -> int:
        """Maximum number of data points per record before dispatch.

        Returns:
            The configured upper bound on points per record.

        Example:
            >>> PyNominalStreamOpts.default().max_points_per_batch
            50000
        """

    @property
    def max_request_delay_secs(self) -> float:
        """Maximum delay before forcing a request flush.

        Returns:
            The maximum time to wait before sending pending data, in seconds.

        Example:
            >>> PyNominalStreamOpts.default().max_request_delay > 0
            True
        """

    @property
    def max_buffered_requests(self) -> int:
        """Maximum number of requests that may be buffered concurrently.

        Returns:
            The maximum number of buffered requests before backpressure is applied.

        Example:
            >>> PyNominalStreamOpts.default().max_buffered_requests >= 0
            True
        """

    @property
    def num_upload_workers(self) -> int:
        """Number of concurrent dispatcher tasks used for network transmission.

        Returns:
            The number of dispatcher tasks.

        Example:
            >>> PyNominalStreamOpts.default().num_upload_workers >= 1
            True
        """

    @property
    def num_runtime_workers(self) -> int:
        """Number of runtime worker threads for internal processing.

        Returns:
            The configured number of runtime workers.

        Example:
            >>> PyNominalStreamOpts.default().num_runtime_workers
            8
        """

    @property
    def base_api_url(self) -> str:
        """Base URL for the Nominal API endpoint.

        Returns:
            The fully-qualified base API URL used for streaming requests.

        Example:
            >>> isinstance(PyNominalStreamOpts.default().base_api_url, str)
            True
        """

    def with_max_points_per_batch(self, n: int) -> Self:
        """Set the maximum number of points per record.

        Args:
            n: Maximum number of data points to include in a single record.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts.default().with_max_points_per_batch(1000)
        """

    def with_max_request_delay_secs(self, delay_secs: float) -> Self:
        """Set the maximum delay before forcing a request flush.

        Args:
            delay_secs: Maximum time in seconds to wait before sending pending data.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts.default().with_max_request_delay_secs(1.0)
        """

    def with_max_buffered_requests(self, n: int) -> Self:
        """Set the maximum number of requests that can be buffered concurrently.

        Args:
            n: Maximum number of buffered requests.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts.default().with_max_buffered_requests(200)
        """

    def with_num_upload_workers(self, n: int) -> Self:
        """Set the number of asynchronous dispatcher tasks.

        Args:
            n: Number of dispatcher tasks responsible for request transmission.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts.default().with_num_upload_workers(8)
        """

    def with_num_runtime_workers(self, n: int) -> Self:
        """Set the number of runtime worker threads.

        Args:
            n: Number of background worker threads used for internal processing.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts.default().with_num_runtime_workers(16)
        """

    def with_api_base_url(self, url: str) -> Self:
        """Set the base URL for the Nominal API.

        Args:
            url: Fully-qualified base API URL for streaming requests.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts.default().with_api_base_url("https://staging.nominal.io")
        """

    def __repr__(self) -> str:
        """Return a developer-friendly string representation of this configuration."""

    def __str__(self) -> str:
        """Return a human-readable summary of this configuration."""

class PyNominalDatasetStream:
    """High-throughput client for enqueueing dataset points to Nominal.

    This is the Python-facing streaming client. It supports a fluent builder
    API for configuration, lifecycle controls (`open`, `close`, `cancel`), and
    multiple enqueue modes (single point, long series, and wide records).
    """

    def __init__(self, /, opts: PyNominalStreamOpts | None = None) -> None:
        """Create a new stream builder.

        Args:
            opts: Optional stream options. If omitted, sensible defaults are used.

        Example:
            >>> from nominal_streaming import PyNominalStreamOpts
            >>> stream = PyNominalDatasetStream(PyNominalStreamOpts())
        """

    def enable_logging(self, log_directive: str | None = None) -> Self:
        """Enable client-side logging for diagnostics.

        NOTE: must be applied before calling open()

        Args:
            log_directive: If provided, log directive (e.g. "trace" or "info") to configure logging with.
                If not provided, searches for a `RUST_LOG` environment variable, or if not found,
                defaults to debug level logging.

        Returns:
            The updated instance for fluent chaining.
        """

    def with_options(self, opts: PyNominalStreamOpts) -> Self:
        """Attach or replace stream options.

        NOTE: must be applied before calling open()

        Args:
            opts: Options for the underlying stream.

        Returns:
            The updated instance for fluent chaining.
        """

    def with_core_consumer(
        self,
        dataset_rid: str,
        token: str | None = None,
    ) -> Self:
        """Send data to a Dataset in Nominal.

        NOTE: Must be applied before calling open()

        NOTE: Mutually exclusive with `to_file`.

        Args:
            dataset_rid: Resource identifier of the dataset.
            token: Optional bearer token. If omitted, uses `NOMINAL_TOKEN` environment variable.

        Returns:
            The updated instance for fluent chaining.

        Raises:
            RuntimeError: If called after `to_file`.
        """

    def to_file(self, path: pathlib.Path) -> Self:
        """Write points to a local file (newline-delimited records).

        Mutually exclusive with `with_core_consumer`.

        Args:
            path: Destination file path.

        Returns:
            The updated instance for fluent chaining.

        Raises:
            RuntimeError: If already configured for core consumption.
        """

    def with_file_fallback(self, path: pathlib.Path) -> Self:
        """If sending to core fails, fall back to writing to `path`.

        NOTE: Requires that `with_core_consumer` has been configured.

        NOTE: Not allowed with `to_file`.

        Args:
            path: Fallback file path.

        Returns:
            The updated instance for fluent chaining.

        Raises:
            RuntimeError: If core consumer is not configured.
        """

    def open(self) -> None:
        """Start the runtime and accept enqueues.

        NOTE: Safe to call multiple times; subsequent calls are no-ops.

        NOTE: May raise if the builder is not fully configured.
        """

    def close(self) -> None:
        """Gracefully drain pending data and stop the worker runtime.

        NOTE: Blocks while joining internal threads. Safe to call multiple times.
        """

    def cancel(self) -> None:
        """Fast cancellation of work without guaranteeing a full drain.

        NOTE: Intended for signal handlers or rapid shutdown paths.
        """

    def enqueue(
        self,
        channel_name: str,
        timestamp: int,
        value: DataType,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a single point.

        Args:
            channel_name: Channel name to stream to
            timestamp: Timestamp for the enqueued value.
                Accepts either integral nanoseconds since unix epoch or a datetime, which is presumed to be in UTC.
            value: Data value to stream
            tags: Optional tags to attach to the data.

        Raises:
            RuntimeError: If the stream is not open or has been cancelled.
            TypeError: If `value` is not an `int`, `float`, or `str`.
        """

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[int],
        values: Sequence[DataType],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a series for a single channel.

        Args:
            channel_name: Channel name.
            timestamps: Sequence of timestamps (same accepted forms as in `enqueue`).
            values: Sequence of values (must be homogeneous: all must be float, int, or strings).
            tags: Optional tags to attach to the values.

        Raises:
            RuntimeError: If the stream is not open or has been cancelled.
            TypeError: If value types are heterogeneous or unsupported.
            ValueError: If lengths of `timestamps` and `values` differ.
        """

    def enqueue_from_dict(
        self,
        timestamp: int,
        channel_values: dict[str, DataType],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a wide record: many channels at a single timestamp.

        Args:
            timestamp: Record timestamp (see `enqueue`).
            channel_values: Mapping from channel name to value.
            tags: Optional tags attach to all values in the record.

        Raises:
            RuntimeError: If the stream is not open or has been cancelled.
            TypeError: If any value is not an `int`, `float`, or `str`.
        """

    def enqueue_struct(
        self,
        channel_name: str,
        timestamp: int,
        value: Mapping[str, Any],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a single struct value.

        The value is JSON-serialized inside Rust using Python's json.dumps
        (with allow_nan=False). Raises TypeError at enqueue time if the
        value contains non-JSON-native elements.

        Args:
            channel_name: Channel name to stream to
            timestamp: Integral nanoseconds since unix epoch.
            value: Struct value, must be JSON-encodable.
            tags: Optional tags to attach to the data.

        Raises:
            RuntimeError: If the stream is not open or has been cancelled.
            TypeError: If `value` contains a non-JSON-native element.
        """

    def enqueue_float_array(
        self,
        channel_name: str,
        timestamp: int,
        value: Sequence[float],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a single array-of-doubles value.

        Args:
            channel_name: Channel name to stream to
            timestamp: Integral nanoseconds since unix epoch.
            value: Sequence of doubles forming the array value at this timestamp.
                Integer elements are coerced to float; pass an explicit float
                sequence if implicit int-to-float promotion is undesired.
            tags: Optional tags to attach to the data.

        Raises:
            RuntimeError: If the stream is not open or has been cancelled.
        """

    def enqueue_string_array(
        self,
        channel_name: str,
        timestamp: int,
        value: Sequence[str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a single array-of-strings value.

        Args:
            channel_name: Channel name to stream to
            timestamp: Integral nanoseconds since unix epoch.
            value: Sequence of strings forming the array value at this timestamp.
            tags: Optional tags to attach to the data.

        Raises:
            RuntimeError: If the stream is not open or has been cancelled.
        """

    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None: ...

class NominalAvroWriterOpts:
    """Options for :class:`NominalAvroWriter`.

    Configures how points are batched before being handed to the underlying
    avro writer, plus whether close() forces an fsync, and optional per-file
    rotation.
    """

    def __init__(
        self,
        *,
        max_points_per_batch: int = 250_000,
        max_batch_delay_secs: float = 0.1,
        fsync_on_close: bool = True,
        max_points_per_file: int = 0,
    ) -> None:
        """Initialize options.

        Args:
            max_points_per_batch: Max points packed into one avro record before
                the underlying stream's batch-processor emits it. Default: 250,000.
            max_batch_delay_secs: Maximum time the underlying stream buffers a
                partial batch before forcing a flush. Default: 0.1 (100 ms).
            fsync_on_close: Whether close() calls ``File::sync_all()`` before returning.
            max_points_per_file: Maximum points per file before rotating to a new
                numbered file. ``0`` (default) means no rotation — all data goes
                to the single path given to :class:`NominalAvroWriter`.
                When > 0, filenames follow ``<stem>_<index:03d><suffix>``
                (e.g. ``out.avro`` → ``out_000.avro``, ``out_001.avro``, ...).
        """

    @property
    def max_points_per_batch(self) -> int:
        """Maximum number of points packed into one avro record before the underlying stream emits it."""
    @property
    def max_batch_delay_secs(self) -> float:
        """Maximum time the underlying stream buffers a partial batch before forcing a flush, in seconds."""
    @property
    def fsync_on_close(self) -> bool:
        """Whether :meth:`NominalAvroWriter.close` calls ``File::sync_all()`` before returning."""
    @property
    def max_points_per_file(self) -> int:
        """Maximum points per file before rotation. ``0`` means no rotation."""

    def with_max_points_per_batch(self, n: int) -> Self:
        """Return a copy with ``max_points_per_batch`` set to *n*."""
    def with_max_batch_delay_secs(self, secs: float) -> Self:
        """Return a copy with ``max_batch_delay_secs`` set to *secs*."""
    def with_fsync_on_close(self, b: bool) -> Self:
        """Return a copy with ``fsync_on_close`` set to *b*."""
    def with_max_points_per_file(self, n: int) -> Self:
        """Return a copy with ``max_points_per_file`` set to *n*."""

    def __repr__(self) -> str: ...

class NominalAvroWriter:
    """Purpose-built avro file writer for the Nominal schema.

    Writes snappy-compressed avro files in the format consumed by the Nominal
    Core ingest API. Use this when you want a local file on disk (optionally
    rotated into numbered shards); if you want to upload to Core directly
    instead, use :class:`PyNominalDatasetStream`.

    Timestamps throughout the API are ``int`` nanoseconds since the Unix
    epoch. No automatic conversion from ``datetime`` / ``pl.Datetime`` is
    performed — convert up front.

    Example::

        from nominal_streaming import NominalAvroWriter, NominalAvroWriterOpts

        with NominalAvroWriter("out.avro") as w:
            w.write("temperature", ts_ns=1_700_000_000_000_000_000, value=21.3)
            w.write_batch(
                "speed",
                ts_ns=[1_700_000_000_000_000_000, 1_700_000_000_001_000_000],
                values=[0.0, 1.2],
            )
        # The file is fully flushed + fsync'd on context-manager exit.

    Choosing a write method:

        * :meth:`write` — one scalar point for one channel. Per-call FFI
          overhead; fine for low rates.
        * :meth:`write_batch` — many points for one channel. Preferred over
          a loop of :meth:`write`.
        * :meth:`write_from_dict` — one timestamp, many channels (a "row").
          One FFI crossing per row regardless of channel count.
        * :meth:`write_dataframe` — many channels × many rows from a polars
          ``DataFrame``. Highest-throughput path — prefer when you have the
          data in a frame. See its own docstring for full dtype / null /
          NaN semantics.
        * :meth:`write_struct` / :meth:`write_float_array` /
          :meth:`write_string_array` — non-scalar point types.

    Concurrency:

        * Multi-producer safe — any number of threads may call any
          ``write_*`` method concurrently.
        * Ordering across threads is unspecified; within one producer, order
          is preserved.
        * The GIL is released for the duration of the Rust-side enqueue
          on every ``write_*`` method.

    Errors:

        * ``RuntimeError`` is raised on background encoder failures (disk
          I/O, avro encoding, fsync). The first such error is **latched**:
          every subsequent ``write_*`` / :meth:`flush` / :meth:`sync` /
          :meth:`close` call returns the same error until the writer is
          discarded. There is no reset.
        * ``RuntimeError`` with message "send after close" is raised from
          any ``write_*`` call made after :meth:`close` completed. This is
          always caller misuse, not encoder failure.
        * ``ValueError`` / ``TypeError`` are raised for malformed input
          (unsupported dtype, null in the timestamp column, non-JSON-native
          values to :meth:`write_struct`, etc.) — these do not latch.
        * :meth:`__exit__` always calls :meth:`close` — even when the body
          raised — and will propagate a latched encoder error if the user's
          exception did not already trigger one.
    """

    def __init__(
        self,
        path: pathlib.Path,
        opts: NominalAvroWriterOpts | None = None,
    ) -> None:
        """Open a new writer at ``path``.

        If a file already exists at ``path``, it is **truncated**. When
        rotation is configured (``opts.max_points_per_file > 0``), ``path``
        is the template: actual output goes to ``<stem>_000<suffix>``,
        ``<stem>_001<suffix>``, ...

        The parent directory is created if it does not exist.

        Raises:
            OSError: Parent directory or file cannot be created / truncated.
        """

    def write(
        self,
        channel: str,
        ts_ns: int,
        value: int | float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single scalar point (int, float, or str).

        The Python type of ``value`` determines the avro variant:
        ``int`` → ``IntegerPoint``, ``float`` → ``DoublePoint``,
        ``str`` → ``StringPoint``. To avoid confusion in mixed-int-and-float
        streams, prefer the explicit typed methods below for non-scalars.
        """

    def write_batch(
        self,
        channel: str,
        ts_ns: Sequence[int],
        values: Sequence[int | float | str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write many points of one channel. Values must be homogeneously typed.

        Note: In a batch, integer values are emitted as ``DoublePoint`` (not
        ``IntegerPoint``) — the value-kind classifier checks float first for
        compatibility with the dataset-stream path. If you need integer
        emission, use :meth:`write` (single points), :meth:`write_from_dict`
        (per-timestamp dicts), or :meth:`write_dataframe` with an ``Int64``
        column (which always emits ``IntegerPoint``).
        """

    def write_from_dict(
        self,
        ts_ns: int,
        channel_values: dict[str, int | float | str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write many channels at one timestamp from a dict. Values are
        emitted per their Python type (int → IntegerPoint, etc.).
        """

    def write_struct(
        self,
        channel: str,
        ts_ns: int,
        value: Mapping[str, Any],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a struct value (JSON-encoded internally).

        Raises:
            TypeError: ``value`` contains a non-JSON-native object.
        """

    def write_float_array(
        self,
        channel: str,
        ts_ns: int,
        value: Sequence[float],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write an array-of-doubles value. NaN / ±Inf elements are preserved."""

    def write_string_array(
        self,
        channel: str,
        ts_ns: int,
        value: Sequence[str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write an array-of-strings value."""

    def write_dataframe(
        self,
        df: "pl.DataFrame",
        timestamp_column: str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write each non-timestamp column of ``df`` as a separate channel.

        Highest-throughput entry point. The column loop runs Rust-side with
        the GIL released; on Apple-Silicon + CPython 3.12 throughput is
        roughly 24–30 M points/second for typical shapes (see the branch's
        benchmarks/ directory).

        The timestamp column must be ``Int64`` (nanoseconds since epoch).
        Other timestamp dtypes (e.g. ``pl.Datetime``) are **not** auto-cast —
        convert first with ``df.with_columns(pl.col("ts").cast(pl.Int64))``
        if your frame uses a datetime representation.

        Per-column dispatch:

            * ``Float64`` → ``DoublePoint`` per row.
            * ``Int64``   → ``IntegerPoint`` per row.
            * ``String``  → ``StringPoint`` per row.
            * ``List(*)`` / ``Array(*, _)`` with ``String`` inner →
              ``StringArrayPoint`` per row.
            * ``List(*)`` / ``Array(*, _)`` with numeric inner →
              ``DoubleArrayPoint`` per row (cast to f64).
            * ``Struct(…)`` → ``StructPoint`` per row (JSON-serialized).

        Null / NaN / Infinity handling (deliberate — matches the Nominal
        Core ingest backend's filtering semantics):

            * **Polars null → the row is skipped.** No avro record is
              emitted for that ``(channel, timestamp)`` — the timestamp is
              not paired with a sentinel value on any dtype. Skipping
              client-side aligns with the backend's row-level
              ``Tuple IS NOT NULL`` filter; a null-valued record would be
              dropped at insert anyway.
            * **Float64 NaN / ±Infinity → preserved as-is.** These are real
              IEEE-754 values, not nulls. They round-trip unchanged through
              the avro ``double`` branch and ClickHouse's
              ``Variant(Float64, …)`` column.
            * **Inside a struct or list:** polars-null *elements* become
              JSON ``null`` so the containing object's / array's shape
              survives. Non-finite floats inside a struct field **also**
              encode as JSON ``null`` (not as literal ``NaN`` / ``Infinity``
              tokens) — emitting literals would corrupt downstream queries
              that parse the JSON, because ClickHouse's JSON column accepts
              them silently at insert but fails on read.

        Raises:
            ValueError: The timestamp column is missing, contains nulls, or
                has the wrong dtype.
            RuntimeError: An unsupported dtype appears (anything not in the
                dispatch list above) — or the writer has a latched error.
        """

    def flush(self) -> None:
        """Best-effort error surface. Does NOT force a drain of the underlying stream.

        Returns immediately (aside from the latched-error check). For
        durability guarantees, call :meth:`close`.

        Raises:
            RuntimeError: A background encoder error has been latched.
        """

    def sync(self) -> None:
        """Fsync the file path, durably persisting data the stream has already dispatched.

        In-flight buffered data is NOT forced to disk by this call — the
        underlying dataset stream has no synchronous drain hook. For strong
        durability guarantees, call :meth:`close`.

        Raises:
            RuntimeError: fsync failed, or a background encoder error has
                been latched.
        """

    def close(self) -> list[pathlib.Path]:
        """Gracefully shut down the encoder. Idempotent; blocks until every
        already-enqueued point has reached disk.

        When ``max_points_per_file == 0`` (no rotation), returns a
        1-element list containing the single output path. When rotating,
        returns all files written, in order.

        After :meth:`close` returns, any ``write_*`` call raises
        ``RuntimeError("send after close")``.

        Raises:
            RuntimeError: A background encoder error was latched during
                shutdown (final flush or fsync failure). The same error is
                cached and re-raised on every subsequent :meth:`close` call.
        """

    def points_accepted(self) -> int:
        """Total points successfully accepted by ``write_*`` so far, across all files.

        Counts successful enqueues — not on-disk bytes. Points that are
        still in the stream's in-memory buffer are included. For on-disk
        durability, call :meth:`close`.
        """

    def path(self) -> pathlib.Path:
        """The file path currently being written to.

        Without rotation, this is the path given to ``__init__``. With
        rotation, it is the currently-open shard. After :meth:`close`,
        this returns the path of the last shard that was open.
        """

    def finalized_paths(self) -> list[pathlib.Path]:
        """All fully-closed (finalized) file paths, in order.

        Empty until at least one rotation has occurred (or :meth:`close`
        is called). The current in-progress file is NOT included — use
        :meth:`written_files` if you want that.
        """

    def written_files(self) -> list[pathlib.Path]:
        """Every file path this writer has opened, in order.

        Includes the currently-open file while the writer is live; after
        :meth:`close`, identical to :meth:`finalized_paths`.
        """

    def stats(self) -> dict[str, int]:
        """Snapshot of pipeline timing counters.

        All ``*_ns`` values are nanoseconds accumulated across the writer's
        lifetime; ``*_calls`` are counts.

        Keys:

            * ``df_handoff_ns`` — time inside the Arrow C Data Interface
              FFI hop (python polars → rust polars) per :meth:`write_dataframe`.
            * ``extract_ts_ns`` — time extracting the timestamp column.
            * ``column_build_ns`` — time iterating non-timestamp columns
              and building Point structs (the producer's main CPU cost).
            * ``enqueue_batch_ns`` — wall time the producer spent handing
              the batch to the underlying stream. Includes time blocked
              waiting for buffer capacity when the consumer can't drain
              fast enough — a large value here means the consumer is the
              bottleneck.
            * ``consumer_consume_ns`` — wall time the background dispatcher
              thread spent inside ``consume`` (avro encode + snappy + file
              write). Runs in parallel with the producer.
            * ``consumer_consume_calls`` — number of ``WriteRequest``s the
              dispatcher dispatched.
            * ``write_dataframe_calls`` — number of :meth:`write_dataframe`
              invocations.
            * ``write_dataframe_total_ns`` — total wall time across all
              :meth:`write_dataframe` calls (producer-side only).

        Diagnosis rule of thumb: if ``consumer_consume_ns`` is close to the
        total wall clock, the avro encoder is the bottleneck; if
        ``column_build_ns + df_handoff_ns`` dominates instead, the producer
        is. Includes all dtypes the writer has processed over its lifetime —
        the counters are cumulative, not per-call.
        """

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the writer (always, even on exception in the body).

        A latched encoder error raised during the final close call will
        propagate unless the body already raised. If you want to suppress
        close-time errors, wrap the ``with`` in a ``try``/``except``.
        """
