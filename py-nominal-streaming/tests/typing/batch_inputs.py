"""Consumer examples checked by mypy; this module is not executed."""

import datetime

import numpy as np
from nominal_streaming import NominalDatasetStream
from nominal_streaming._nominal_streaming import PyNominalDatasetStream
from numpy.typing import NDArray


def accepts_batches(stream: NominalDatasetStream, native: PyNominalDatasetStream) -> None:
    """Lists, tuples, numeric arrays, and normalized timestamps are valid inputs."""
    timestamps = np.array([1, 2], dtype=np.uint64)
    values = np.array([1.0, 2.0], dtype=np.float64)
    stream.enqueue_batch("arrays", timestamps, values)
    native.enqueue_batch("arrays", timestamps, values)
    stream.enqueue_batch("lists", [1, 2], [1.0, 2.0])
    native.enqueue_batch("tuples", (1, 2), ("one", "two"))
    stream.enqueue_batch("mixed", timestamps, [1.0, 2.0])
    stream.enqueue_batch("dates", [datetime.datetime.now(), "2026-01-01"], values)
    stream.enqueue_batch("float32", timestamps, values.astype(np.float32))
    stream.enqueue_batch("integers", timestamps.astype(np.int64), values.astype(np.int64))
    stream.enqueue_batch("unsigned", timestamps, values.astype(np.uint64))
    stream.enqueue_batch("strings", timestamps, np.array(["one", "two"]))
    # These ignores are checked too: accepting these inputs makes them unused errors.
    stream.enqueue_batch("generator", [1, 2], iter([1.0, 2.0]))  # type: ignore[arg-type]
    stream.enqueue_batch("objects", [1, 2], [object(), object()])  # type: ignore[list-item]


def rejects_unsupported_inputs(stream: NominalDatasetStream, native: PyNominalDatasetStream) -> None:
    """Do not widen sequence/scalar APIs into arbitrary conversion protocols."""
    stream.enqueue_batch("mapping", {0: 1}, {0: 1.0})  # type: ignore[arg-type]
    native.enqueue_batch("mapping", {0: 1}, {0: 1.0})  # type: ignore[arg-type]
    stream.enqueue_batch("float-ts", [1.5], [1.0])  # type: ignore[list-item]
    float_timestamps: NDArray[np.float64] = np.array([1.5])
    complex_values: NDArray[np.complex128] = np.array([1j])
    stream.enqueue_batch("float-array-ts", float_timestamps, [1.0])  # type: ignore[arg-type]
    stream.enqueue_batch("complex", [1], complex_values)  # type: ignore[arg-type]
    stream.enqueue("scalar", IndexLike(), 1.0)  # type: ignore[arg-type]
    stream.enqueue_struct("struct", IndexLike(), {})  # type: ignore[arg-type]
    stream.enqueue_batch("custom-ts", [IndexLike()], [1.0])  # type: ignore[list-item]
    stream.enqueue_batch("custom-values", [1], [FloatLike()])  # type: ignore[list-item]


class IndexLike:
    def __index__(self) -> int:
        """Supply an integer conversion without being a supported scalar type."""
        return 1


class FloatLike:
    def __float__(self) -> float:
        """Supply a float conversion without being a supported scalar type."""
        return 1.0
