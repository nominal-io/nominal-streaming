"""Startup failures preserve prior fallback data and remain actionable on retry."""

import pathlib
import tempfile
import unittest

from fastavro import reader
from nominal_streaming import NominalDatasetStream


class StartupTests(unittest.TestCase):
    def test_existing_fallback_is_preserved_and_error_can_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            fallback = pathlib.Path(directory) / "prior.avro"
            fallback.write_bytes(b"previous run")
            stream = (
                NominalDatasetStream("test")
                .with_core_consumer("ri.catalog.main.dataset.test")
                .with_file_fallback(fallback, overwrite=False)
            )
            for _ in range(2):
                with self.assertRaisesRegex(RuntimeError, "open fallback") as caught:
                    stream.open()
                self.assertIn(str(fallback), str(caught.exception))
                self.assertEqual(fallback.read_bytes(), b"previous run")

    def test_file_output_and_fallback_are_rejected_before_opening_files(self):
        from nominal_streaming._nominal_streaming import PyNominalDatasetStream

        with tempfile.TemporaryDirectory() as directory:
            primary = pathlib.Path(directory) / "primary.avro"
            fallback = pathlib.Path(directory) / "fallback.avro"
            primary.write_bytes(b"previous run")
            for with_core in (False, True):
                stream = PyNominalDatasetStream(None).to_file(primary).with_file_fallback(fallback)
                if with_core:
                    stream.with_core_consumer("ri.catalog.main.dataset.test", "test")
                for _ in range(2):
                    with self.assertRaisesRegex(RuntimeError, "file output and file fallback"):
                        stream.open()
                    self.assertEqual(primary.read_bytes(), b"previous run")
                    self.assertFalse(fallback.exists())

    def test_failed_validation_does_not_mark_native_stream_open(self):
        from nominal_streaming._nominal_streaming import PyNominalDatasetStream

        stream = PyNominalDatasetStream(None)
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "no streaming target"):
                stream.open()

    def test_primary_refuses_overwrite_without_changing_existing_data(self):
        with tempfile.TemporaryDirectory() as directory:
            primary = pathlib.Path(directory) / "prior.avro"
            primary.write_bytes(b"previous run")
            stream = NominalDatasetStream().to_file(primary, overwrite=False)
            with self.assertRaisesRegex(RuntimeError, "open"):
                stream.open()
            self.assertEqual(primary.read_bytes(), b"previous run")
            primary.unlink()
            with stream:
                stream.enqueue("value", 123, 4.5)
            with primary.open("rb") as file:
                self.assertEqual(list(reader(file))[0]["values"], [4.5])

    def test_each_destination_defaults_to_overwrite_and_accepts_new_protected_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "points.avro"
            for fallback in (False, True):
                for overwrite in (None, True, False):
                    with self.subTest(fallback=fallback, overwrite=overwrite):
                        if path.exists():
                            path.unlink()
                        if overwrite is not False:
                            path.write_bytes(b"previous run")
                        kwargs = {} if overwrite is None else {"overwrite": overwrite}
                        stream = NominalDatasetStream("test")
                        if fallback:
                            stream.with_core_consumer("ri.catalog.main.dataset.test")
                            stream.with_file_fallback(path, **kwargs)
                        else:
                            stream.to_file(path, **kwargs)
                        with stream:
                            pass
                        self.assertNotEqual(path.read_bytes(), b"previous run")

    def test_concurrent_native_opens_share_one_stream(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier

        from nominal_streaming._nominal_streaming import PyNominalDatasetStream

        with tempfile.TemporaryDirectory() as directory:
            primary = pathlib.Path(directory) / "primary.avro"
            stream = PyNominalDatasetStream(None).to_file(primary, overwrite=False)
            barrier = Barrier(4)

            def open_stream(_):
                barrier.wait(timeout=10)
                stream.open()

            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(open_stream, range(4)))
            stream.close()
