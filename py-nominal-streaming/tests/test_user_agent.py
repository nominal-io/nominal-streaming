"""User agent option tests; run after maturin develop."""

import unittest

from nominal_streaming import PyNominalLogStreamOpts, PyNominalStreamOpts

OPTS_TYPES = (PyNominalStreamOpts, PyNominalLogStreamOpts)


class UserAgentTests(unittest.TestCase):
    def test_options_default_and_fluent_setter(self):
        for opts_type in OPTS_TYPES:
            with self.subTest(opts_type=opts_type.__name__):
                opts = opts_type()
                self.assertIsNone(opts.user_agent)
                self.assertIn("user_agent=None", repr(opts))
                self.assertIs(opts.with_user_agent("nominal-python", "1.2.0"), opts)
                self.assertEqual(opts.user_agent, "nominal-python/1.2.0")
                self.assertIn("nominal-python/1.2.0", repr(opts))

    def test_invalid_name_or_version_raises(self):
        for opts_type in OPTS_TYPES:
            for name, version in (
                ("nominal_python", "1.2.0"),
                ("123", "1.2.0"),
                ("nominal-python", "v1.2.0"),
                ("nominal-python", "1.2.0-beta.1"),
            ):
                with self.subTest(opts_type=opts_type.__name__, name=name, version=version):
                    opts = opts_type()
                    with self.assertRaises(ValueError):
                        opts.with_user_agent(name, version)
                    self.assertIsNone(opts.user_agent)


if __name__ == "__main__":
    unittest.main()
