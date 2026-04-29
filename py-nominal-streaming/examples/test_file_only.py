"""Stream points to a local avro file without configuring a Nominal API consumer.

Run: `uv run python py-nominal-streaming/examples/test_file_only.py`

No Nominal credentials are required: the stream is configured with `to_file(...)`
only, so no `auth_header` or `dataset_rid` is provided.
"""

import logging
import pathlib
import tempfile
import time

from nominal_streaming import NominalDatasetStream

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    with tempfile.TemporaryDirectory() as tmp:
        out_path = pathlib.Path(tmp) / "out.avro"
        stream = NominalDatasetStream().enable_logging("info").to_file(out_path)

        now_ns = int(time.time() * 1e9)
        with stream:
            for i in range(1000):
                stream.enqueue("file_only_channel", now_ns + i * 1_000_000, float(i))

        logger.info("Wrote %d bytes to %s", out_path.stat().st_size, out_path)


if __name__ == "__main__":
    main()
