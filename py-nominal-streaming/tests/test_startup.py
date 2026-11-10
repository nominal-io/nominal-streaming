"""Stream startup failure tests; run after maturin develop."""

import pathlib
import tempfile
import unittest

from nominal_streaming import PyNominalStreamOpts
from nominal_streaming._nominal_streaming import PyNominalDatasetStream


class StartupTests(unittest.TestCase):
    def test_open_without_a_target_reports_the_configuration_error(self):
        stream = PyNominalDatasetStream(None)
        with self.assertRaisesRegex(RuntimeError, r"call with_core_consumer\(\.\.\.\) or to_file\(\.\.\.\)"):
            stream.open()

    def test_open_reports_the_unusable_file_path(self):
        with tempfile.NamedTemporaryFile() as parent:
            path = pathlib.Path(parent.name) / "data.avro"
            stream = PyNominalDatasetStream(None).to_file(path)
            with self.assertRaisesRegex(RuntimeError, "data.avro"):
                stream.open()

    def test_open_reports_invalid_stream_options(self):
        cases = {
            "max_points_per_record": PyNominalStreamOpts(max_points_per_batch=0),
            "request_dispatcher_tasks": PyNominalStreamOpts(num_upload_workers=0),
            "runtime workers": PyNominalStreamOpts(num_upload_workers=4, num_runtime_workers=2),
            "greater than zero": PyNominalStreamOpts(num_upload_workers=0, num_runtime_workers=0),
        }
        for message, opts in cases.items():
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "data.avro"
                stream = PyNominalDatasetStream(opts).to_file(path)
                with self.assertRaisesRegex(RuntimeError, message):
                    stream.open()
                self.assertFalse(path.exists())

    def test_open_reports_an_invalid_base_api_url(self):
        opts = PyNominalStreamOpts(base_api_url="not a url")
        stream = PyNominalDatasetStream(opts).with_core_consumer(
            "ri.catalog.main.dataset.00000000-0000-0000-0000-000000000000", "token"
        )
        with self.assertRaisesRegex(RuntimeError, "not a url"):
            stream.open()

    def test_failed_open_can_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "data.avro"
            for opts in (PyNominalStreamOpts(num_upload_workers=4, num_runtime_workers=2), None):
                stream = PyNominalDatasetStream(opts)
                for _ in range(2):
                    with self.assertRaises(RuntimeError):
                        stream.open()
                stream.to_file(path).with_options(PyNominalStreamOpts()).open()
                stream.enqueue("channel", 1, 1.0)
                stream.close()
                self.assertGreater(path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
