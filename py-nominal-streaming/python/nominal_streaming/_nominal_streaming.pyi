from __future__ import annotations

import pathlib
from types import TracebackType
from typing import Sequence, Type

from typing_extensions import Self

from nominal_streaming.nominal_dataset_stream import DataType

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

    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None: ...
