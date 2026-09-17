"""Batch conversion contracts, tested against both installed ABI wheels."""

import datetime
import importlib
import math
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest
import warnings
from unittest import mock

import numpy as np
from fastavro import reader
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

wrapper = importlib.import_module("nominal_streaming.nominal_dataset_stream")


class ScaledArray(np.ndarray):
    """An ndarray subclass whose scalar access differs from its storage."""

    def __getitem__(self, index):
        """Scale scalar reads to exercise subclass fallback."""
        return super().__getitem__(index) * 2


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = pathlib.Path(self.directory.name) / "points.avro"
        opts = PyNominalStreamOpts(max_request_delay_secs=0.001, num_upload_workers=1, num_runtime_workers=1)
        self.stream = NominalDatasetStream(opts=opts).to_file(self.path)
        self.stream.open()
        self.addCleanup(self.stream.close)

    def records(self):
        self.stream.close()
        # The Avro writer creates its header lazily, on the first accepted batch.
        if self.path.stat().st_size == 0:
            return {}
        with self.path.open("rb") as file:
            return {record["channel"]: record for record in reader(file)}

    def test_numeric_types_and_layouts(self):
        timestamps = np.arange(6, dtype="uint64") + 1_700_000_000_000_000_000
        values = np.arange(6, dtype="float64") / 8
        cases = {
            "native": (timestamps, values),
            "signed": (timestamps.astype("int64"), values),
            "stride": (timestamps[::2], values[::2]),
            "reverse": (timestamps[::-1], values[::-1]),
            "broadcast": (np.broadcast_to(timestamps[:1], (6,)), np.broadcast_to(values[1:2], (6,))),
            "big_endian": (timestamps.astype(">u8"), values.astype(">f8")),
            "lists": (timestamps.tolist(), values.tolist()),
            "tuples": (tuple(timestamps.tolist()), tuple(values.tolist())),
            "mixed_timestamps": (timestamps, values.tolist()),
            "mixed_values": (timestamps.tolist(), values),
            "subclass": (np.arange(6, dtype="int64").view(ScaledArray), values.view(ScaledArray)),
            "unmasked": (timestamps, np.ma.array(values, mask=False)),
        }
        for dtype in (
            "float16",
            "float32",
            "int8",
            "uint8",
            "int16",
            "uint16",
            "int32",
            "uint32",
            "int64",
            "uint64",
            "bool",
            "object",
        ):
            cases[dtype] = (timestamps, values.astype(dtype))
        unaligned_ts = np.ndarray((6,), dtype="uint64", buffer=bytearray(49), offset=1)
        unaligned_vs = np.ndarray((6,), dtype="float64", buffer=bytearray(49), offset=1)
        unaligned_ts[:] = timestamps
        unaligned_vs[:] = values
        cases["unaligned"] = (unaligned_ts, unaligned_vs)
        readonly = values.copy()
        readonly.flags.writeable = False
        cases["readonly"] = (timestamps, readonly)
        for name, (ts, vs) in cases.items():
            with self.subTest(name=name):
                self.stream.enqueue_batch(name, ts, vs, tags={"site": "test"})
        records = self.records()
        self.assertEqual(set(records), set(cases))
        for name, (ts, vs) in cases.items():
            with self.subTest(name=name):
                self.assertEqual(records[name]["timestamps"], list(ts))
                self.assertEqual(records[name]["values"], [float(v) for v in vs])
                self.assertEqual(records[name]["tags"], {"site": "test"})

    def test_string_values_and_timestamp_normalization(self):
        self.stream.enqueue_batch("strings", np.array([0, 1], dtype="int64"), np.array(["one", "two"]))
        self.stream.enqueue_batch(
            "dates",
            [datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc), "2020-01-02T00:00:00Z"],
            np.array([1.5, 2.5]),
        )
        records = self.records()
        self.assertEqual(records["strings"]["values"], ["one", "two"])
        self.assertEqual(records["dates"]["timestamps"], [1_577_836_800_000_000_000, 1_577_923_200_000_000_000])
        self.assertEqual(records["dates"]["values"], [1.5, 2.5])

    def test_numpy_scalars_in_other_enqueue_methods(self):
        self.stream.enqueue("scalar", np.int64(1), np.float32(1.25))
        self.stream.enqueue_from_dict(np.uint64(2), {"dictionary": np.int64(42)})
        records = self.records()
        self.assertEqual(records["scalar"]["timestamps"], [1])
        self.assertEqual(records["scalar"]["values"], [1.25])
        self.assertEqual(records["dictionary"]["timestamps"], [2])
        self.assertEqual(records["dictionary"]["values"], [42.0])

    def test_owns_input_before_returning(self):
        ts = np.array([1, 2, 3], dtype="uint64")
        vs = np.array([4.0, 5.0, 6.0])
        self.stream.enqueue_batch("owned", ts, vs)
        ts[:] = 99
        vs[:] = 99
        del ts, vs
        record = self.records()["owned"]
        self.assertEqual(record["values"], [4.0, 5.0, 6.0])
        self.assertEqual(record["timestamps"], [1, 2, 3])

    def test_numeric_edge_values(self):
        cases = {
            "signed": np.array([-(2**63), 2**53 + 1, 2**63 - 1], dtype="int64"),
            "unsigned": np.array([0, 2**63, 2**64 - 1], dtype="uint64"),
            "floating": np.array([float("nan"), float("inf"), -0.0]),
        }
        for name, values in cases.items():
            self.stream.enqueue_batch(name, np.array([0, 1, 2], dtype="int64"), values)
        records = self.records()
        for name in ("signed", "unsigned"):
            self.assertEqual(records[name]["values"], [float(v) for v in cases[name]])
        self.assertTrue(math.isnan(records["floating"]["values"][0]))
        self.assertEqual(records["floating"]["values"][1], float("inf"))
        self.assertEqual(math.copysign(1, records["floating"]["values"][2]), -1)

    def test_unsigned_timestamp_range_is_not_narrowed(self):
        # Avro uses signed nanoseconds, so check extraction on an unopened stream:
        # accepted timestamps reach the lifecycle check instead of overflowing.
        unopened = NominalDatasetStream()
        for timestamps in (
            np.array([0, 2**63, 2**64 - 1], dtype="uint64"),
            [0, 2**63, 2**64 - 1],
        ):
            with self.subTest(timestamps=timestamps), self.assertRaisesRegex(RuntimeError, "stream"):
                unopened.enqueue_batch("range", timestamps, [1.0, 2.0, 3.0])

    def test_invalid_batch_never_writes_partial_points(self):
        cases = {
            "negative": (np.array([0, -1], dtype="int64"), [1.0, 2.0], OverflowError),
            "overflow": ([0, 2**64], [1.0, 2.0], OverflowError),
            "length": (np.array([0, 1], dtype="uint64"), np.array([1.0]), ValueError),
            "empty": (np.array([], dtype="uint64"), np.array([], dtype="float64"), ValueError),
            "bad_last_value": ([0, 1], [1.0, object()], TypeError),
            "masked": ([0, 1], np.ma.array([1.0, 2.0], mask=[False, True]), TypeError),
            "datetime": ([0, 1], np.array([0, 1], dtype="datetime64[ns]"), TypeError),
            "timedelta": ([0, 1], np.array([0, 1], dtype="timedelta64[ns]"), TypeError),
            "matrix": ([0, 1], np.zeros((2, 2)), TypeError),
            "scalar_array": ([0], np.array(1.0), TypeError),
            "generator": ([0, 1], iter([1.0, 2.0]), TypeError),
        }
        for name, (ts, vs, error) in cases.items():
            with self.subTest(name=name), self.assertRaises(error):
                self.stream.enqueue_batch(name, ts, vs)
        self.stream.enqueue_batch("valid", [1], [1.0])
        self.assertEqual(set(self.records()), {"valid"})

    def test_sequence_registration_cannot_bypass_array_validation(self):
        # ABC registration is global and irreversible: isolate it from other tests.
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent("""\
                    from collections.abc import Sequence
                    import unittest
                    import numpy as np
                    from nominal_streaming import NominalDatasetStream

                    Sequence.register(np.ndarray)
                    stream = NominalDatasetStream()
                    for values in (
                        np.array([0, 1], dtype="datetime64[ns]"),
                        np.array([0, 1], dtype="timedelta64[ns]"),
                        np.ma.array([1.0, 2.0], mask=[False, True]),
                    ):
                        # Validation must reject before reaching the unopened stream.
                        with unittest.TestCase().assertRaises(TypeError):
                            stream.enqueue_batch("invalid", [0, 1], values)
                    """),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_values_error_does_not_retry_timestamp_normalization(self):
        with mock.patch.object(wrapper, "_parse_timestamp", side_effect=AssertionError("timestamps retried")):
            with self.assertRaisesRegex(TypeError, "values|Values"):
                self.stream.enqueue_batch("bad", np.array([0, 1], dtype="uint64"), [1.0, object()])
        self.assertEqual(self.records(), {})

    def test_optional_acceleration_warning_is_once_and_only_for_arrays(self):
        with (
            mock.patch.object(wrapper, "_BUFFER_FAST_PATH", False),
            mock.patch.object(wrapper, "_warned_slow_batch", False),
            warnings.catch_warnings(record=True) as captured,
        ):
            warnings.simplefilter("always")
            self.stream.enqueue_batch("list", [0], [1.0])
            self.assertEqual(len(captured), 0)
            self.stream.enqueue_batch("array", np.array([0]), np.array([1.0]))
            self.stream.enqueue_batch("again", np.array([0]), np.array([1.0]))
            self.assertEqual(len(captured), 1)
            self.assertIn("3.11", str(captured[0].message))
            self.assertIs(captured[0].category, RuntimeWarning)

    def test_accelerated_build_does_not_warn(self):
        with (
            mock.patch.object(wrapper, "_BUFFER_FAST_PATH", True),
            mock.patch.object(wrapper, "_warned_slow_batch", False),
            warnings.catch_warnings(record=True) as captured,
        ):
            warnings.simplefilter("always")
            self.stream.enqueue_batch("array", np.array([0]), np.array([1.0]))
            self.assertEqual(len(captured), 0)


if __name__ == "__main__":
    unittest.main()
