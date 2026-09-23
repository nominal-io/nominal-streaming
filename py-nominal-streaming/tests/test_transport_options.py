"""Transport configuration parity; all tests run without external requests."""

import unittest

from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts


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
        opts = PyNominalStreamOpts()
        for key, value in defaults.items():
            self.assertEqual(getattr(opts, key), value)
        values = dict(
            max_retries=0,
            retry_backoff_slot_secs=0.01,
            connect_timeout_secs=1.0,
            read_timeout_secs=2.0,
            write_timeout_secs=3.0,
            delivery_timeout_secs=4.0,
        )
        stream = NominalDatasetStream.create("unused", "http://localhost", **values)
        for key, value in values.items():
            self.assertEqual(getattr(stream._opts, key), value)

    def test_invalid_durations_raise_value_error(self):
        for key in (
            "retry_backoff_slot_secs",
            "connect_timeout_secs",
            "read_timeout_secs",
            "write_timeout_secs",
            "delivery_timeout_secs",
        ):
            for value in (0, -1, float("nan"), float("inf")):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    PyNominalStreamOpts(**{key: value})
        with self.assertRaises(ValueError):
            PyNominalStreamOpts(max_retries=32)


if __name__ == "__main__":
    unittest.main()
