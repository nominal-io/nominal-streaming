"""Python-facing API for the streaming client.

Example:
-------
import pathlib
from datetime import datetime, timedelta, timezone

from nominal_streaming import PyNominalStreamOpts, NominalDatasetStream

# NOTE: may also use PyNominalStreamOpts() for sensible defaults that may be customized
opts = PyNominalStreamOpts(
    max_points_per_batch=250_000,
    max_request_delay=timedelta(seconds=0.1),
    max_buffered_requests=4,
    num_upload_workers=8,
    base_api_url="https://api.gov.nominal.io/api",
    runtime_workers=8,
)

with (
    NominalDatasetStream("api_key", opts)
    .with_core_consumer("ri.catalog.dataset...")
    .with_file_fallback(pathlib.Path("/tmp/fallback.avro")) as stream
):
    stream.enqueue("chanA", datetime.now(timezone.utc), 1.23, tags={"site": "a1"})
    stream.enqueue_batch("chanB", [0, 1_000_000_000], [5, 6], tags={"phase": "prod"})
    stream.enqueue_from_dict(0, {"chanC": "ok", "chanD": 7}, tags={"who": "tester"})

"""

from __future__ import annotations

import datetime
import logging
import pathlib
import signal
import threading
from types import TracebackType
from typing import Mapping, Sequence, Type

import dateutil
from typing_extensions import Self

from nominal_streaming._nominal_streaming import (
    PyNominalDatasetStream,
    PyNominalStreamOpts,
)

logger = logging.getLogger(__name__)

TimestampLike = str | int | datetime.datetime
DataType = int | float | str


def _parse_timestamp(ts: str | int | datetime.datetime) -> int:
    if isinstance(ts, int):
        return ts
    elif isinstance(ts, datetime.datetime):
        secs = ts.astimezone(datetime.timezone.utc).timestamp()
        return int(secs * 1e9)
    else:
        # TODO(drake): by involving dateutil, this chops off any nano level precision provided
        #              in the timestamp. Update to not lose precision when converting to absolute nanos.
        secs = dateutil.parser.parse(ts).astimezone(datetime.timezone.utc).timestamp()
        return int(secs * 1e9)


class NominalDatasetStream:
    """Top-level python wrapper for the Rust streaming client to Nominal."""

    def __init__(self, auth_header: str, opts: PyNominalStreamOpts):
        """Initializer for dataset stream.

        Args:
            auth_header: API key or access token to the Nominal API
            opts: Optional options for the underlying stream
        """
        self._auth_header = auth_header
        self._opts = opts
        self._impl = PyNominalDatasetStream(self._opts)
        self._old_sigint = None
        self._opened = False

    @classmethod
    def create(
        cls,
        auth_header: str,
        base_api_url: str,
        max_points_per_batch: int = 250_000,
        max_request_delay_secs: float = 0.1,
        max_buffered_requests: int = 4,
        num_upload_workers: int = 8,
        num_runtime_workers: int = 8,
    ) -> Self:
        """Factory constructor to build a NominalDatasetStream using optional overrides for configuration options

        Args:
            auth_header: API Key or Personal Access Token for accessing the Nominal API
            base_api_url: Base API URL for hitting the Nominal API with.
            max_points_per_batch: Overrides the default number of points that may be sent in a single batch
            max_request_delay_secs: Overrides the default maximum buffering time for data between flushes.
                NOTE: if the amount of data being streamed is greater than available bandwidth, data may be
                      buffered longer than the configured duration.
            max_buffered_requests: Overrides the default number of requests that may be buffered between encoding
                threads and upload threads. Increasing this may prevent blocking threads in situations with spotty
                internet, but increase teardown time (e.g. when pressing ctrl + c)
            num_upload_workers: Overrides the default number of upload worker threads
                NOTE: must be set as low as the number of runtime workers.
            num_runtime_workers: Overrides the default number of runtime worker threads
                NOTE: must be set as high as the number of upload workers.

        """
        opts = PyNominalStreamOpts(
            max_points_per_batch=max_points_per_batch,
            max_request_delay_secs=max_request_delay_secs,
            max_buffered_requests=max_buffered_requests,
            num_upload_workers=num_upload_workers,
            num_runtime_workers=num_runtime_workers,
            base_api_url=base_api_url,
        )
        return cls(auth_header, opts)

    def enable_logging(self, log_directive: str = "debug") -> Self:
        """Enable logging with the given verbosity level

        Args:
            log_directive: Log verbosity level to expose from Rust code. Defaults to verbose debug logging.
                See the following for valid values: https://docs.rs/env_logger/latest/env_logger/#enabling-logging
        """
        logger.info("Setting rust log verbosity to '%s'", log_directive)
        self._impl = self._impl.enable_logging(log_directive)
        return self

    def with_core_consumer(self, dataset_rid: str) -> Self:
        """Enables streaming to a Dataset in Core

        Args:
            dataset_rid: RID of the Dataset in Nominal to stream to
        """
        self._impl = self._impl.with_core_consumer(dataset_rid, self._auth_header)
        return self

    def to_file(self, path: pathlib.Path) -> Self:
        """Target streaming towards a local `.avro` file

        The written file will contain snappy-compressed avro data. This can be read as follows:

            ```python
            from fastavro import reader

            with open("test.avro", "rb") as f:
                for record in reader(f):
                    channel_name = record["channel"]
                    tags = record["tags"]
                    timestamps = record["timestamps"]
                    values = record["values"]
            ```
        """
        self._impl = self._impl.to_file(path)
        return self

    def with_file_fallback(self, path: pathlib.Path) -> Self:
        """Setup file fallback for streaming to core

        The written file will contain snappy-compressed avro data for any batches of data that were unable to make
        it to the backend successfully. This can be read as follows:

            ```python
            from fastavro import reader

            with open("test.avro", "rb") as f:
                for record in reader(f):
                    channel_name = record["channel"]
                    tags = record["tags"]
                    timestamps = record["timestamps"]
                    values = record["values"]
            ```
        """
        self._impl = self._impl.with_file_fallback(path)
        return self

    def open(self) -> Self:
        """Create the stream as a context manager.

        NOTE: installs a sigint handler to enable more graceful shutdown.
              This is restored upon exit. The handler is only installed when
              running in the main thread; in worker threads, the stream still
              functions but Ctrl+C will not trigger graceful cancellation.
        """
        if self._opened:
            raise RuntimeError("Stream already opened!")

        logger.info("Opening underlying stream")
        self._impl.open()
        self._opened = True

        if threading.current_thread() is threading.main_thread():
            # Map Ctrl+C → fast cancel; keep handler tiny and re-raise KeyboardInterrupt.
            def _on_sigint(signum, frame):  # type: ignore[no-untyped-def]
                logger.debug("Cancelling underlying stream")
                try:
                    self._impl.cancel()
                finally:
                    raise KeyboardInterrupt

            logger.info("Installing sigint handler")
            self._old_sigint = signal.getsignal(signal.SIGINT)  # type: ignore[assignment]
            signal.signal(signal.SIGINT, _on_sigint)
        else:
            logger.info(
                "Stream opened from worker thread; Ctrl+C will not trigger fast cancellation. "
                "Stream will close gracefully when the context manager exits."
            )

        return self

    def __enter__(self) -> Self:
        """Create the stream as a context manager.

        NOTE: installs a sigint handler to enable more graceful shutdown.
              This is restored upon exit.
        """
        return self.open()

    def close(self, wait: bool = True) -> None:
        """Exit the stream and close out any used system resources.

        NOTE: uninstalls the installed sigint handler and restores any pre-existing sigint handlers
        """
        try:
            if wait:
                logger.info("Awaiting graceful shutdown")
                self._impl.close()
            else:
                logger.info("Quickly shutting down")
                self._impl.cancel()
        finally:
            if self._old_sigint is not None:
                logger.info("Restoring original sigint handler")
                signal.signal(signal.SIGINT, self._old_sigint)
                self._old_sigint = None
            self._opened = False

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the stream and close out any used system resources.

        NOTE: uninstalls the installed sigint handler and restores any pre-existing sigint handlers
        """
        self.close()

    def enqueue(
        self,
        channel_name: str,
        timestamp: TimestampLike,
        value: DataType,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single value to the stream

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute UTC timestamp of the data being uploaded.
            value: Value to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
        """
        self._impl.enqueue(channel_name, _parse_timestamp(timestamp), value, {**tags} if tags else None)

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[TimestampLike],
        values: Sequence[DataType],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add a sequence of messages to the queue to upload to Nominal.

        Messages are added one-by-one (with timestamp normalization) and flushed
        based on the batch conditions.

        NOTE: assumes that all values have the same type as the first value in the batch--
              ensure that any provided value arrays are homogenously typed

        Args:
            channel_name: Name of the channel to upload data for.
            timestamps: Absolute UTC timestamps of the data being uploaded.
            values: Values to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
        """
        self._impl.enqueue_batch(
            channel_name, [_parse_timestamp(ts) for ts in timestamps], values, {**tags} if tags else None
        )

    def enqueue_from_dict(
        self,
        timestamp: TimestampLike,
        channel_values: Mapping[str, DataType],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write multiple channel values at a given timestamp using a flattened dictionary.

        Each key in the dictionary is treated as a channel name and the corresponding value
        is enqueued with the given timestamp.

        Args:
            timestamp: The shared absolute UTC timestamp to use for all items to enqueue.
            channel_values: A dictionary mapping channel names to their respective values.
            tags: Key-value tags associated with the data being uploaded.
        """
        self._impl.enqueue_from_dict(_parse_timestamp(timestamp), {**channel_values}, {**tags} if tags else None)
