"""File-only tests; never calls Nominal services."""

import datetime as dt
import json
import signal
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from nominal_streaming import NominalLogStream, PyNominalLogStreamOpts
from nominal_streaming.nominal_log_stream import _timestamp_ns


class LogStreamTests(unittest.TestCase):
    def test_options_support_readable_properties_and_fluent_configuration(self):
        opts = PyNominalLogStreamOpts()
        self.assertEqual(opts.max_request_bytes, 8 * 1024 * 1024)
        self.assertEqual(opts.num_upload_workers, 4)
        self.assertEqual(opts.num_runtime_workers, 2)
        values = {
            "max_request_bytes": 4096,
            "max_batch_bytes": 8192,
            "max_buffered_bytes": 32768,
            "max_points_per_batch": 5,
            "max_request_delay_secs": 0.5,
            "num_upload_workers": 3,
            "num_runtime_workers": 1,
            "base_api_url": "https://example.com/api",
        }
        for name, value in values.items():
            with self.subTest(option=name):
                method = "with_api_base_url" if name == "base_api_url" else "with_" + name
                self.assertIs(getattr(opts, method)(value), opts)
                self.assertEqual(getattr(opts, name), value)
                with self.assertRaises(AttributeError):
                    setattr(opts, name, value)
        self.assertEqual(str(opts), repr(opts))
        self.assertIn("num_upload_workers=3", repr(opts))
        self.assertIn("max_request_bytes=4096", repr(opts))

    def test_options_validate_setters_without_changing_previous_value(self):
        opts = PyNominalLogStreamOpts()
        previous = opts.max_request_delay_secs
        for value in [float("nan"), float("inf"), -1.0]:
            with self.assertRaises(ValueError):
                PyNominalLogStreamOpts(max_request_delay_secs=value)
            with self.assertRaises(ValueError):
                opts.with_max_request_delay_secs(value)
            self.assertEqual(opts.max_request_delay_secs, previous)
        with self.assertRaises(ValueError):
            PyNominalLogStreamOpts(num_runtime_workers=0)
        with self.assertRaises(ValueError):
            opts.with_num_runtime_workers(0)
        self.assertEqual(opts.num_runtime_workers, 2)

    def test_configuration_is_copied_and_frozen_after_open(self):
        with tempfile.TemporaryDirectory() as directory:
            opts = PyNominalLogStreamOpts().with_max_request_bytes(512)
            stream = NominalLogStream().with_options(opts).enable_logging("off")
            opts.with_max_request_bytes(4096)
            with stream.to_file(path=Path(directory)):
                with self.assertRaisesRegex(RuntimeError, "max_request_bytes"):
                    stream.enqueue("app", 0, "x" * 1024)
                with self.assertRaises(RuntimeError):
                    stream.with_options(opts)
                with self.assertRaises(RuntimeError):
                    stream.enable_logging()
                stream.enqueue("app", 1, "ready")
            self.assertEqual(stream.stats().backed_up_records, 1)

    def test_log_factory_and_keyword_methods_use_configured_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            stream = NominalLogStream.create(
                "test-token", "https://example.com/api", max_request_bytes=512, max_points_per_batch=1
            ).to_file(path=Path(directory))
            with stream:
                stream.enqueue(channel_name="app", timestamp=1, value="started", tags={"service": "api"})
                stream.enqueue(channel_name="app", timestamp=2, value="ready")
                with self.assertRaisesRegex(RuntimeError, "max_request_bytes"):
                    stream.enqueue(channel_name="app", timestamp=3, value="x" * 1024)
            self.assertEqual(stream.stats().backed_up_records, 2)
            self.assertEqual(len(list(Path(directory).glob("*.jsonl"))), 2)

    def test_exact_timestamps(self):
        self.assertEqual(_timestamp_ns("2026-09-14T00:00:00.123456789Z") % 1_000_000_000, 123456789)
        self.assertEqual(_timestamp_ns("1970-01-01T01:00:00.000000001+01:00"), 1)
        self.assertEqual(_timestamp_ns(dt.datetime(1969, 12, 31, 23, 59, 59, 999999, dt.timezone.utc)), -1000)
        for invalid in ["2026-09-14", "2026-09-14T00:00:00.1234567890Z", dt.datetime(2026, 1, 1), 1.5, True]:
            with self.assertRaises((TypeError, ValueError)):
                _timestamp_ns(invalid)

    def test_file_writes_and_close(self):
        handler = signal.getsignal(signal.SIGINT)
        with tempfile.TemporaryDirectory() as directory:
            stream = NominalLogStream().to_file(Path(directory)).open()
            stream.enqueue("engine", 1, "start", tags={"shared": "override"})
            stream.enqueue("engine", 2, "stop", args={"shared": "yes", "other": "value"})
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
                self.assertEqual(stream.stats().accepted_records, 0)
                stream.enqueue_from_dict(1, {"x": "one", "y": "two"})
                self.assertEqual(stream.flush().backed_up_records, 2)

    def test_reserved_keys_and_overflow(self):
        with tempfile.TemporaryDirectory() as directory:
            with NominalLogStream().to_file(Path(directory)) as stream:
                for key in ["MESSAGE", "__REALTIME_TIMESTAMP"]:
                    with self.assertRaises(RuntimeError):
                        stream.enqueue("x", 1, "a", args={key: "collision"})
                with self.assertRaises(OverflowError):
                    stream.enqueue("x", 2**63, "a")
                self.assertEqual(stream.stats().accepted_records, 0)

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

    def test_failed_close_retains_records_for_rescue(self):
        for background in (False, True):
            with self.subTest(background=background), tempfile.TemporaryDirectory() as directory:
                bad = Path(directory) / "occupied"
                bad.write_text("not a directory")
                stream = NominalLogStream().to_file(bad).open()
                stream.enqueue("app", 1, "preserve me")
                if background:
                    self.assertIsNone(stream.close(wait=False))
                with self.assertRaises(RuntimeError):
                    stream.close(wait=True)
                self.assertEqual(stream.stats().failed_records, 1)
                stats = stream.save_failed(Path(directory) / "rescued")
                self.assertEqual(stats.backed_up_records, 1)
                self.assertEqual(stats.failed_records, 0)
                stream.close()


if __name__ == "__main__":
    unittest.main()
