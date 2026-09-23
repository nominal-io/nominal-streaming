"""Graceful-close errors remain observable without leaking runtime workers."""

import subprocess
import sys
import textwrap
import unittest

from nominal_streaming import NominalDatasetStream


class CloseTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform != "win32", "requires POSIX file size limits")
    def test_native_close_error_is_sticky_and_context_chains_body(self):
        script = textwrap.dedent("""
            import pathlib
            import resource
            import signal
            import tempfile
            import sys
            from nominal_streaming import NominalDatasetStream
            from nominal_streaming._nominal_streaming import PyNominalDatasetStream

            factory = PyNominalDatasetStream if sys.argv[1] == "native" else NominalDatasetStream
            with tempfile.TemporaryDirectory() as directory:
                stream = factory(None).to_file(pathlib.Path(directory) / "out.avro")
                stream.open()
                # Isolate a real write failure to this process, including its native workers.
                signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
                resource.setrlimit(resource.RLIMIT_FSIZE, (0, resource.RLIM_INFINITY))
                stream.enqueue("point", 0, 1.0)
                body_error = ValueError("body failed")
                try:
                    stream.__exit__(ValueError, body_error, None)
                except RuntimeError as error:
                    assert error.__cause__ is body_error
                    message = str(error)
                    assert "file" in message.lower(), message
                else:
                    raise AssertionError("close hid delivery failure")
                try:
                    stream.close()
                except RuntimeError as error:
                    assert str(error) == message
                else:
                    raise AssertionError("repeated close hid delivery failure")
        """)
        for api in ("native", "wrapper"):
            with self.subTest(api=api):
                result = subprocess.run(
                    [sys.executable, "-c", script, api], capture_output=True, text=True, timeout=15, check=False
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_context_explicitly_chains_body_error_when_close_fails(self):
        class FailedClose:
            def close(self):
                raise RuntimeError("delivery failed")

        stream = NominalDatasetStream()
        stream._impl = FailedClose()
        body_error = ValueError("body failed")
        with self.assertRaises(RuntimeError) as raised:
            stream.__exit__(ValueError, body_error, None)
        self.assertIs(raised.exception.__cause__, body_error)
