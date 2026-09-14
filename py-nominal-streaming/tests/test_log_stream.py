"""File-only tests; never calls Nominal services."""

import datetime as dt
import json
import signal
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

from nominal_streaming import NominalLogStream, PyNominalLogStreamOpts
from nominal_streaming.nominal_log_stream import _timestamp_ns


class LogStreamTests(unittest.TestCase):
    def test_exact_timestamps(self):
        self.assertEqual(_timestamp_ns("2026-09-14T00:00:00.123456789Z") % 1_000_000_000, 123456789)
        self.assertEqual(_timestamp_ns("1970-01-01T01:00:00.000000001+01:00"), 1)
        self.assertEqual(_timestamp_ns(dt.datetime(1969, 12, 31, 23, 59, 59, 999999, dt.timezone.utc)), -1000)
        for invalid in ["2026-09-14", "2026-09-14T00:00:00.1234567890Z", dt.datetime(2026, 1, 1), 1.5, True]:
            with self.assertRaises((TypeError, ValueError)):
                _timestamp_ns(invalid)

    def test_file_batch_and_close(self):
        handler = signal.getsignal(signal.SIGINT)
        with tempfile.TemporaryDirectory() as directory:
            stream = NominalLogStream().to_file(Path(directory)).open()
            stream.enqueue_batch(
                "engine",
                [1, 2],
                ["start", "stop"],
                args={"shared": "yes"},
                per_record_args=[{"shared": "override"}, {"other": "value"}],
            )
            stream.close(wait=False)
            with ThreadPoolExecutor(4) as pool:
                list(pool.map(lambda _: stream.close(), range(4)))
            stats = stream.stats()
            self.assertEqual(stats.accepted_records, 2)
            self.assertEqual(stats.backed_up_records, 2)
            self.assertEqual(stats.buffered_bytes, 0)
            records = [
                json.loads(line) for p in Path(directory).rglob("*.jsonl") for line in p.read_text().splitlines()
            ]
            self.assertEqual(len(records), 2)
            records.sort(key=lambda row: int(row["__REALTIME_TIMESTAMP"]))
            self.assertEqual(records[0], {"__REALTIME_TIMESTAMP": "1", "MESSAGE": "start", "shared": "override"})
            self.assertEqual(
                records[1], {"__REALTIME_TIMESTAMP": "2", "MESSAGE": "stop", "shared": "yes", "other": "value"}
            )
            with self.assertRaises(RuntimeError):
                stream.enqueue("engine", 3, "late")
        self.assertIs(signal.getsignal(signal.SIGINT), handler)

    def test_validation_before_enqueue(self):
        with tempfile.TemporaryDirectory() as directory:
            with NominalLogStream().to_file(Path(directory)) as stream:
                with self.assertRaises(ValueError):
                    stream.enqueue("x", 0, "message", {}, args={})
                with self.assertRaises(ValueError):
                    stream.enqueue_batch("x", [1], ["one", "two"])
                with self.assertRaises(ValueError):
                    stream.enqueue_batch("x", [1], ["one"], per_record_args=[])
                self.assertEqual(stream.stats().accepted_records, 0)
                stream.enqueue_from_dict(1, {"x": "one", "y": "two"})
                self.assertEqual(stream.flush().backed_up_records, 2)

    def test_batch_crosses_native_boundary_once(self):
        stream = NominalLogStream()
        stream._impl = Mock()
        timestamps = [1, 2, 3]
        stream.enqueue_batch("x", timestamps, ["a", "b", "c"])
        stream._impl.enqueue_batch.assert_called_once()
        self.assertIs(stream._impl.enqueue_batch.call_args.args[1], timestamps)
        stream._impl.reset_mock()
        stream.enqueue_batch("x", ["1970-01-01T00:00:00.000000001Z"], ["a"])
        stream._impl.enqueue_batch.assert_called_once()
        self.assertEqual(stream._impl.enqueue_batch.call_args.args[1], [1])

    def test_reserved_keys_and_overflow(self):
        with tempfile.TemporaryDirectory() as directory:
            with NominalLogStream().to_file(Path(directory)) as stream:
                for key in ["MESSAGE", "__REALTIME_TIMESTAMP"]:
                    with self.assertRaises(RuntimeError):
                        stream.enqueue("x", 1, "a", args={key: "collision"})
                with self.assertRaises(OverflowError):
                    stream.enqueue("x", 2**63, "a")
                self.assertEqual(stream.stats().accepted_records, 0)

    def test_failed_journal_can_be_rescued(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "not-a-directory"
            bad.write_text("occupied")
            stream = NominalLogStream().to_file(bad).open()
            stream.enqueue("x", 1, "preserve me")
            with self.assertRaises(RuntimeError):
                stream.close()
            self.assertEqual(stream.stats().failed_records, 1)
            rescued = stream.save_failed(Path(directory) / "rescued")
            self.assertEqual(rescued.backed_up_records, 1)
            self.assertEqual(rescued.failed_records, 0)
            stream.close()

    def test_file_only_rejects_fallback_in_either_configuration_order(self):
        with tempfile.TemporaryDirectory() as directory:
            primary = Path(directory) / "primary"
            fallback = Path(directory) / "fallback"
            streams = [
                NominalLogStream().to_file(primary).with_file_fallback(fallback),
                NominalLogStream().with_file_fallback(fallback).to_file(primary),
            ]
            for stream in streams:
                with self.assertRaisesRegex(RuntimeError, "file-only streams"):
                    stream.open()
            self.assertFalse(primary.exists())
            self.assertFalse(fallback.exists())

    def test_background_close_reports_failed_journal_and_allows_rescue(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "occupied"
            bad.write_text("not a directory")
            stream = NominalLogStream().to_file(bad).open()
            stream.enqueue("app", 1, "preserve me")
            self.assertIsNone(stream.close(wait=False))
            with self.assertRaises(RuntimeError):
                stream.close(wait=True)
            self.assertEqual(stream.stats().failed_records, 1)
            stats = stream.save_failed(Path(directory) / "rescued")
            self.assertEqual(stats.backed_up_records, 1)
            self.assertEqual(stats.failed_records, 0)
            stream.close()

    def test_serialized_byte_limit_rejects_atomically_and_splits(self):
        with tempfile.TemporaryDirectory() as directory:
            opts = PyNominalLogStreamOpts(max_request_bytes=512)
            stream = NominalLogStream(opts=opts).to_file(Path(directory)).open()
            with self.assertRaisesRegex(RuntimeError, "max_request_bytes"):
                stream.enqueue_batch("channel", [0, 1], ["small", "🚀" * 128])
            self.assertEqual(stream.stats().accepted_records, 0)
            stream.enqueue_batch("channel", list(range(20)), ["🚀" * 60] * 20)
            stream.close()
            files = list(Path(directory).glob("*.jsonl"))
            self.assertGreater(len(files), 1)
            self.assertEqual(sum(len(p.read_text().splitlines()) for p in files), 20)
            self.assertEqual(stream.stats().backed_up_records, 20)

    def test_bad_duration_is_python_exception(self):
        for value in [float("nan"), float("inf"), -1.0]:
            with self.assertRaises(ValueError):
                PyNominalLogStreamOpts(request_timeout_secs=value)


if __name__ == "__main__":
    unittest.main()
