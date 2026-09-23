"""Transport configuration parity; all tests run without external requests."""

import unittest
from unittest.mock import patch

from nominal_streaming import NominalDatasetStream, NominalLogStream, PyNominalLogStreamOpts, PyNominalStreamOpts


class TransportOptionsTests(unittest.TestCase):
    def test_defaults_and_factory_forwarding(self):
        defaults = dict(
            max_retries=5,
            retry_backoff_slot_secs=0.25,
            connect_timeout_secs=5.0,
            read_timeout_secs=15.0,
            write_timeout_secs=15.0,
            delivery_timeout_secs=60.0,
        )
        for opts_type in (PyNominalStreamOpts, PyNominalLogStreamOpts):
            opts = opts_type()
            for key, value in defaults.items():
                with self.subTest(options=opts_type.__name__, key=key):
                    self.assertEqual(getattr(opts, key), value)
        values = dict(
            max_retries=0,
            retry_backoff_slot_secs=0.01,
            connect_timeout_secs=1.0,
            read_timeout_secs=2.0,
            write_timeout_secs=3.0,
            delivery_timeout_secs=4.0,
        )
        for stream_type in (NominalDatasetStream, NominalLogStream):
            with patch.object(stream_type, "__init__", return_value=None) as init:
                stream_type.create("unused", "http://localhost", **values)
            auth_header, opts = init.call_args.args
            self.assertEqual(auth_header, "unused")
            self.assertEqual(opts.base_api_url, "http://localhost")
            for key, value in values.items():
                with self.subTest(stream=stream_type.__name__, key=key):
                    self.assertEqual(getattr(opts, key), value)

    def test_invalid_durations_raise_value_error(self):
        for opts_type in (PyNominalStreamOpts, PyNominalLogStreamOpts):
            for key in (
                "retry_backoff_slot_secs",
                "connect_timeout_secs",
                "read_timeout_secs",
                "write_timeout_secs",
                "delivery_timeout_secs",
            ):
                for value in (0, -1, float("nan"), float("inf"), 1e30, 1e-12):
                    with self.subTest(options=opts_type.__name__, key=key, value=value):
                        with self.assertRaisesRegex(ValueError, key):
                            opts_type(**{key: value})
            with self.subTest(options=opts_type.__name__), self.assertRaises(ValueError):
                opts_type(max_retries=32)


if __name__ == "__main__":
    unittest.main()
