"""Shared timestamp parsing used by NominalDatasetStream and NominalAvroWriter."""

from __future__ import annotations

import datetime

import dateutil.parser

TimestampLike = str | int | datetime.datetime
ScalarValue = int | float | str


def _parse_timestamp(ts: str | int | datetime.datetime) -> int:
    """Convert a TimestampLike into nanoseconds since the Unix epoch."""
    if isinstance(ts, int):
        return ts
    elif isinstance(ts, datetime.datetime):
        secs = ts.astimezone(datetime.timezone.utc).timestamp()
        return int(secs * 1e9)
    else:
        # TODO(drake): dateutil loses sub-microsecond precision.
        secs = dateutil.parser.parse(ts).astimezone(datetime.timezone.utc).timestamp()
        return int(secs * 1e9)
