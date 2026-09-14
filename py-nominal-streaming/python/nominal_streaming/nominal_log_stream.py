"""Bounded native streaming for timestamped log messages."""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from types import TracebackType
from typing import Mapping, Sequence

from typing_extensions import Self, TypeGuard

from nominal_streaming._nominal_streaming import LogStreamStats, PyNominalLogStream, PyNominalLogStreamOpts

TimestampLike = int | str | dt.datetime
_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_ISO = re.compile(r"(\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?([Zz]|[+-]\d{2}:\d{2})")


def _timestamp_ns(value: TimestampLike) -> int:
    if type(value) is int:
        return value
    fraction = 0
    if isinstance(value, str):
        match = _ISO.fullmatch(value)
        if match is None:
            raise ValueError("timestamp must be ISO 8601 with timezone and at most nine fractional digits")
        base, digits, offset = match.groups()
        value = dt.datetime.fromisoformat(base.upper() + ("+00:00" if offset.upper() == "Z" else offset))
        fraction = int((digits or "").ljust(9, "0"))
    if not isinstance(value, dt.datetime):
        raise TypeError("timestamp must be integer nanoseconds, an aware datetime, or an ISO 8601 string")
    if value.utcoffset() is None:
        raise ValueError("datetime timestamp must include a timezone")
    delta = value.astimezone(dt.timezone.utc) - _EPOCH
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000 + fraction


def _is_nanoseconds(values: Sequence[TimestampLike]) -> TypeGuard[Sequence[int]]:
    return all(type(value) is int for value in values)


def _args(tags: Mapping[str, str] | None, args: Mapping[str, str] | None) -> dict[str, str] | None:
    if tags is not None and args is not None:
        raise ValueError("tags is an alias for args; provide only one")
    value = args if args is not None else tags
    return dict(value) if value is not None else None


class NominalLogStream:
    """Log records are acknowledged by Core or preserved in a local journal.

    Integer timestamps are signed Unix nanoseconds. Datetimes must be timezone aware.
    Configure a target before opening. Instances cannot be reopened after close.
    No process-wide signal handlers are installed.
    """

    def __init__(self, auth_header: str | None = None, opts: PyNominalLogStreamOpts | None = None) -> None:
        """Configure credentials and limits; credentials are optional for file-only streams."""
        self._auth_header = auth_header
        self._impl = PyNominalLogStream(opts)

    def enable_logging(self, log_directive: str = "debug") -> Self:
        """Enable Rust stream logging when the stream opens.

        Args:
            log_directive: Tracing filter directive, such as "info" or "nominal_streaming=debug".
        """
        self._impl = self._impl.enable_logging(log_directive)
        return self

    def with_options(self, opts: PyNominalLogStreamOpts) -> Self:
        """Set stream options before opening.

        Args:
            opts: Batching, buffering, retry and runtime configuration.
        """
        self._impl = self._impl.with_options(opts)
        return self

    def with_core_consumer(self, dataset_rid: str) -> Self:
        self._impl = self._impl.with_core_consumer(dataset_rid, self._auth_header)
        return self

    def with_file_fallback(self, directory: str | Path) -> Self:
        self._impl = self._impl.with_file_fallback(Path(directory))
        return self

    def to_file(self, directory: str | Path) -> Self:
        self._impl = self._impl.to_file(Path(directory))
        return self

    def open(self) -> Self:
        self._impl.open()
        return self

    def __enter__(self) -> Self:
        """Open this stream."""
        return self.open()

    def close(self, wait: bool = True) -> LogStreamStats | None:
        """Stop writes and drain. With wait=False, drain in the background.

        A later close(wait=True) waits for that drain and reports delivery errors.
        """
        return self._impl.close(wait)

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Drain the stream before leaving the context."""
        self.close()

    def stop_accepting_writes(self) -> None:
        self._impl.stop_accepting_writes()

    def flush(self) -> LogStreamStats:
        return self._impl.flush()

    def save_failed(self, directory: str | Path) -> LogStreamStats:
        """Rescue retained failed batches to a usable journal directory after close."""
        return self._impl.save_failed(Path(directory))

    def stats(self) -> LogStreamStats:
        return self._impl.stats()

    def enqueue(
        self,
        channel: str,
        timestamp: TimestampLike,
        message: str,
        tags: Mapping[str, str] | None = None,
        *,
        args: Mapping[str, str] | None = None,
    ) -> None:
        self._impl.enqueue(
            channel, timestamp if type(timestamp) is int else _timestamp_ns(timestamp), message, _args(tags, args)
        )

    def enqueue_batch(
        self,
        channel: str,
        timestamps: Sequence[TimestampLike],
        messages: Sequence[str],
        tags: Mapping[str, str] | None = None,
        *,
        args: Mapping[str, str] | None = None,
        per_record_args: Sequence[Mapping[str, str]] | None = None,
    ) -> None:
        """Enqueue a batch in one native call; record args override common args."""
        common = _args(tags, args)
        if len(timestamps) != len(messages):
            raise ValueError("timestamps and messages must have equal lengths")
        if per_record_args is not None and len(per_record_args) != len(messages):
            raise ValueError("per_record_args and messages must have equal lengths")
        normalized = timestamps if _is_nanoseconds(timestamps) else [_timestamp_ns(ts) for ts in timestamps]
        self._impl.enqueue_batch(
            channel,
            normalized,
            messages,
            common,
            [dict(item) for item in per_record_args] if per_record_args is not None else None,
        )

    def enqueue_from_dict(
        self,
        timestamp: TimestampLike,
        channel_values: Mapping[str, str],
        tags: Mapping[str, str] | None = None,
        *,
        args: Mapping[str, str] | None = None,
    ) -> None:
        normalized = timestamp if type(timestamp) is int else _timestamp_ns(timestamp)
        common = _args(tags, args)
        for channel, message in channel_values.items():
            self._impl.enqueue(channel, normalized, message, common)
