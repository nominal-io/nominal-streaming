"""Type declarations for the native streaming extension.

Application code should use ``nominal_streaming.NominalDatasetStream``, whose
Python wrapper also normalizes datetime and string timestamps. Native methods
accept integer nanoseconds since the Unix epoch, from -(2**63) through 2**63 - 1.
NumPy imports here describe optional array inputs; this stub is not executed
at runtime and does not make NumPy a runtime requirement.
"""

from __future__ import annotations

import pathlib
from types import TracebackType
from typing import Any, Mapping, Sequence, Type

import numpy as np
from numpy.typing import NDArray
from typing_extensions import Self

from nominal_streaming.nominal_dataset_stream import DataType

# Private build capability read by the Python wrapper, not a runtime switch.
# True for the Python 3.11+ ABI build; false for the Python 3.10 ABI build,
# even when that fallback wheel is loaded on a newer interpreter.
_BUFFER_FAST_PATH: bool

class _TimestampTypeError(TypeError):
    """Private signal for the wrapper to normalize batch timestamps and retry.

    Marks timestamp extraction TypeErrors only; value errors and timestamp
    overflows propagate separately. Applications should catch TypeError.
    """

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
        track_metrics: bool = False,
    ) -> None:
        """Initialize a PyNominalStreamOpts instance.

        Args:
            max_points_per_batch: Maximum number of points per record before dispatching a request.
            max_request_delay_secs: Maximum delay before a request is sent, even if it results in a partial request.
            max_buffered_requests: Maximum number of buffered requests before applying backpressure.
            num_upload_workers: Number of concurrent network dispatches to perform.
                Must be at most `num_runtime_workers`.
            num_runtime_workers: Number of runtime worker threads for concurrent processing.
            track_metrics: Emit runtime metric channels; disabled by default.
            base_api_url: Base URL of the Nominal API endpoint to stream data to.
        """

    @property
    def track_metrics(self) -> bool:
        """Whether runtime metric channels are enabled."""

    def with_track_metrics(self, enabled: bool) -> Self:
        """Enable or disable runtime metric channels.

        Args:
            enabled: Whether to emit runtime metrics when this configuration is used.

        Returns:
            The same configuration instance, updated for fluent chaining.
        """

    @property
    def max_points_per_batch(self) -> int:
        """Maximum number of data points per record before dispatch.

        Returns:
            The configured upper bound on points per record.

        Example:
            >>> PyNominalStreamOpts().max_points_per_batch
            250000
        """

    @property
    def max_request_delay_secs(self) -> float:
        """Maximum delay before forcing a request flush.

        Returns:
            The maximum time to wait before sending pending data, in seconds.

        Example:
            >>> PyNominalStreamOpts().max_request_delay_secs > 0
            True
        """

    @property
    def max_buffered_requests(self) -> int:
        """Maximum number of requests that may be buffered concurrently.

        Returns:
            The maximum number of buffered requests before backpressure is applied.

        Example:
            >>> PyNominalStreamOpts().max_buffered_requests >= 0
            True
        """

    @property
    def num_upload_workers(self) -> int:
        """Number of concurrent dispatcher tasks used for network transmission.

        Returns:
            The number of dispatcher tasks.

        Example:
            >>> PyNominalStreamOpts().num_upload_workers >= 1
            True
        """

    @property
    def num_runtime_workers(self) -> int:
        """Number of runtime worker threads for internal processing.

        Returns:
            The configured number of runtime workers.

        Example:
            >>> PyNominalStreamOpts().num_runtime_workers
            8
        """

    @property
    def base_api_url(self) -> str:
        """Base URL for the Nominal API endpoint.

        Returns:
            The fully-qualified base API URL used for streaming requests.

        Example:
            >>> isinstance(PyNominalStreamOpts().base_api_url, str)
            True
        """

    def with_max_points_per_batch(self, n: int) -> Self:
        """Set the maximum number of points per record.

        Args:
            n: Maximum number of data points to include in a single record.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts().with_max_points_per_batch(1000)
        """

    def with_max_request_delay_secs(self, delay_secs: float) -> Self:
        """Set the maximum delay before forcing a request flush.

        Args:
            delay_secs: Maximum time in seconds to wait before sending pending data.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts().with_max_request_delay_secs(1.0)
        """

    def with_max_buffered_requests(self, n: int) -> Self:
        """Set the maximum number of requests that can be buffered concurrently.

        Args:
            n: Maximum number of buffered requests.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts().with_max_buffered_requests(200)
        """

    def with_num_upload_workers(self, n: int) -> Self:
        """Set the number of asynchronous dispatcher tasks.

        Args:
            n: Number of dispatcher tasks responsible for request transmission.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts().with_num_upload_workers(8)
        """

    def with_num_runtime_workers(self, n: int) -> Self:
        """Set the number of runtime worker threads.

        Args:
            n: Number of background worker threads used for internal processing.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts().with_num_runtime_workers(16)
        """

    def with_api_base_url(self, url: str) -> Self:
        """Set the base URL for the Nominal API.

        Args:
            url: Fully-qualified base API URL for streaming requests.

        Returns:
            The updated instance for fluent chaining.

        Example:
            >>> opts = PyNominalStreamOpts().with_api_base_url("https://staging.nominal.io")
        """

    def __repr__(self) -> str:
        """Return a developer-friendly string representation of this configuration."""

    def __str__(self) -> str:
        """Return a human-readable summary of this configuration."""

class PyNominalDatasetStream:
    """High-throughput client for enqueueing dataset points to Nominal.

    This is the native client used by the public Python wrapper. It supports a fluent builder
    API for configuration, lifecycle controls (`open`, `close`, `cancel`), and
    multiple enqueue modes (single point, long series, and wide records).
    """

    def __init__(self, /, opts: PyNominalStreamOpts | None = None) -> None:
        """Create a new stream builder.

        Args:
            opts: Stream options, or None to use the Rust defaults.

        Example:
            >>> from nominal_streaming import PyNominalStreamOpts
            >>> stream = PyNominalDatasetStream(PyNominalStreamOpts())
        """

    def enable_logging(self, log_directive: str | None = None) -> Self:
        """Enable client-side logging for diagnostics.

        NOTE: must be applied before calling open()

        Args:
            log_directive: If provided, log directive (e.g. "trace" or "info") to configure logging with.
                If not provided, defaults to debug level logging.

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

        Can be combined with `to_file` to write to both destinations.

        Args:
            dataset_rid: Resource identifier of the dataset.
            token: Optional bearer token. If omitted, uses `NOMINAL_TOKEN` environment variable.

        Returns:
            The updated instance for fluent chaining.

        Raises:
            RuntimeError: If the token is missing or the token or dataset identifier is invalid.
        """

    def to_file(self, path: pathlib.Path) -> Self:
        """Write points to a local Avro file.

        Configure before `open()`. Can be combined with `with_core_consumer`
        to write to both destinations, but not with both a core consumer and
        `with_file_fallback`. Invalid target combinations fail at `open()`.

        Args:
            path: Destination file path.

        Returns:
            The updated instance for fluent chaining.
        """

    def with_file_fallback(self, path: pathlib.Path) -> Self:
        """If sending to core fails, fall back to writing to `path`.

        Configure before `open()`, normally alongside `with_core_consumer`.
        Failed requests are written as Avro records. Configuring all three of
        core, file, and fallback destinations fails at `open()`.

        Args:
            path: Fallback file path.

        Returns:
            The updated instance for fluent chaining.
        """

    def open(self) -> None:
        """Start the runtime and accept enqueues.

        NOTE: Safe to call multiple times; subsequent calls are no-ops.

        Raises:
            RuntimeError: If targets or worker counts are invalid, or runtime startup fails.
        """

    def close(self) -> None:
        """Gracefully drain pending data and stop the worker runtime.

        NOTE: Blocks while joining internal threads. Safe to call multiple times.
        """

    def cancel(self) -> None:
        """Close the stream, draining buffered points.

        Currently delegates to `close()` and can block; it does not abort uploads.
        """

    def stop_accepting_writes(self) -> None:
        """Refuse further writes without tearing the stream down.

        Data already enqueued is still flushed by a subsequent `close()`. Used by the SIGINT
        handler so that the drain is not racing a producer that has not stopped yet.
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
            timestamp: Integer nanoseconds since the Unix epoch, from -(2**63) through 2**63 - 1.
            value: Data value to stream
            tags: Optional tags to attach to the data.

        Raises:
            RuntimeError: If the stream is not open or is shutting down.
            OverflowError: If a timestamp is outside the signed 64-bit nanosecond range.
            TypeError: If `value` is not an `int`, `float`, or `str`.
        """

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[int] | NDArray[np.integer[Any]],
        values: Sequence[DataType] | NDArray[np.integer[Any] | np.floating[Any] | np.bool_ | np.str_],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a series for a single channel.

        Both inputs must be one-dimensional, nonempty, and have equal lengths.
        Numeric values use float-first conversion: mixed integers and floats are
        accepted, and large integers can lose precision when converted to doubles.
        Strings must not be mixed with numeric values.

        The Python 3.11+ ABI wheel copies eligible exact NumPy arrays through the
        buffer protocol: int64/uint64 timestamps and float32/float64/int64/uint64
        values with native byte order and aligned storage. Strided, reversed,
        and read-only arrays are supported. Other supported dtypes, layouts,
        and subclasses use element-by-element conversion, as does the Python
        3.10 ABI wheel. NumPy is optional for list and tuple inputs.

        Inputs are converted to owned storage before enqueueing; callers may
        mutate or release them after this call returns. Conversion failures
        enqueue no points. Returning means queued, not uploaded; backpressure
        can block the call.

        Args:
            channel_name: Channel name.
            timestamps: Integer nanoseconds since the Unix epoch, each from
                -(2**63) through 2**63 - 1. Datetimes and strings require the public wrapper.
            values: Numeric or string sequence, or a NumPy array of integer,
                floating, boolean, or Unicode string values. Arrays with masked
                elements or datetime64/timedelta64 value dtypes are rejected.
            tags: Optional tags shared by every point in the batch.

        Raises:
            RuntimeError: If the stream is not open or is shutting down.
            TypeError: If timestamps or values cannot be converted, including
                unsupported mixtures of strings and numbers.
            OverflowError: If a timestamp is outside the signed 64-bit nanosecond range.
            ValueError: If values are empty or the input lengths differ.
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
            RuntimeError: If the stream is not open or is shutting down.
            OverflowError: If a timestamp is outside the signed 64-bit nanosecond range.
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
            RuntimeError: If the stream is not open or is shutting down.
            OverflowError: If a timestamp is outside the signed 64-bit nanosecond range.
            TypeError: If `value` contains a non-JSON-native element.
            ValueError: If `value` contains NaN, infinity, or a circular reference.
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
                Integer elements are converted to doubles, which can lose precision
                for large integers.
            tags: Optional tags to attach to the data.

        Raises:
            RuntimeError: If the stream is not open or is shutting down.
            TypeError: If an element cannot be converted to a double.
            OverflowError: If a timestamp is outside the signed 64-bit nanosecond range.
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
            RuntimeError: If the stream is not open or is shutting down.
            TypeError: If an element is not a string.
            OverflowError: If a timestamp is outside the signed 64-bit nanosecond range.
        """

    def __enter__(self) -> Self:
        """Open the stream and return this instance; propagate errors from `open()`."""
    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Drain and close the stream without suppressing the context's exception.

        Args:
            exc_type: Exception type raised in the context, or None on normal exit.
            exc_value: Exception raised in the context, or None on normal exit.
            traceback: Exception traceback, or None on normal exit.
        """
