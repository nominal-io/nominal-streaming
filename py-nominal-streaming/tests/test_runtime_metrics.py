"""Runtime metrics tests; run after maturin develop with fastavro installed."""

import pathlib
import tempfile
import time
import unittest

from fastavro import reader
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts
from nominal_streaming._nominal_streaming import PyNominalDatasetStream


class RuntimeMetricsTests(unittest.TestCase):
    def test_options_default_and_fluent_setter(self):
        opts = PyNominalStreamOpts()
        self.assertFalse(opts.track_metrics)
        self.assertIs(opts.with_track_metrics(True), opts)
        self.assertTrue(opts.track_metrics)
        self.assertIn("track_metrics=true", repr(opts))
        self.assertFalse(opts.with_track_metrics(False).track_metrics)

    def test_dictionary_metrics_are_opt_in_and_drain_on_close(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "data.avro"
                # Future timestamps also exercise signed staleness arithmetic.
                timestamp = time.time_ns() + 60_000_000_000
                opts = PyNominalStreamOpts(
                    track_metrics=enabled,
                    max_points_per_batch=1,
                    max_buffered_requests=1,
                    num_upload_workers=1,
                    num_runtime_workers=1,
                )
                stream = PyNominalDatasetStream(opts).to_file(path)
                stream.open()
                before = time.time_ns()
                stream.enqueue_from_dict(timestamp, {"temperature": 42.0, "status": "ok"}, {"source": "test"})
                after = time.time_ns()
                stream.close()
                with path.open("rb") as file:
                    records = {record["channel"]: record for record in reader(file)}
                expected = {"temperature", "status"}
                if enabled:
                    expected |= {"enque_dict_start_staleness", "enque_dict_end_staleness"}
                self.assertEqual(set(records), expected)
                self.assertEqual(records["temperature"]["values"], [42.0])
                self.assertEqual(records["status"]["values"], ["ok"])
                if enabled:
                    for name in ("enque_dict_start_staleness", "enque_dict_end_staleness"):
                        metric = records[name]
                        self.assertEqual(metric["timestamps"], [timestamp])
                        self.assertEqual(metric["tags"], {})
                        self.assertGreaterEqual(metric["values"][0], (before - timestamp) / 1e9)
                        self.assertLessEqual(metric["values"][0], (after - timestamp) / 1e9)
                    self.assertLessEqual(
                        records["enque_dict_start_staleness"]["values"][0],
                        records["enque_dict_end_staleness"]["values"][0],
                    )

    def test_factory_forwards_metric_option(self):
        stream = NominalDatasetStream.create("unused", "https://example.invalid", track_metrics=True)
        self.assertTrue(stream._opts.track_metrics)


if __name__ == "__main__":
    unittest.main()
