"""Smoke test for struct and array series enqueues against a live dataset.

Run: `uv run python py-nominal-streaming/examples/test_struct_array.py`
Requires: NominalClient profile "staging" with a valid token.
"""

import logging
import time

from nominal.core import NominalClient
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = NominalClient.from_profile("staging")
    dataset = client.create_dataset("py-nominal-streaming struct+array smoke")

    auth_header = client._clients.auth_header.split()[-1]
    base_api_url = client._clients.authentication._uri

    stream = (
        NominalDatasetStream(
            auth_header=auth_header,
            opts=PyNominalStreamOpts(base_api_url=base_api_url),
        )
        .enable_logging("info")
        .with_core_consumer(dataset.rid)
    )
    logger.info("Streaming to dataset %s", dataset.nominal_url)

    now_ns = int(time.time() * 1e9)
    with stream:
        # struct channel
        for i in range(100):
            stream.enqueue_struct(
                "struct_ch",
                now_ns + i * 1_000_000,
                {"i": i, "label": f"row-{i}", "nested": {"ok": True}},
            )
        # float array channel
        for i in range(100):
            stream.enqueue_float_array(
                "float_array_ch",
                now_ns + i * 1_000_000,
                [float(i), float(i) + 0.5, float(i) + 1.0],
            )
        # string array channel
        for i in range(100):
            stream.enqueue_string_array(
                "string_array_ch",
                now_ns + i * 1_000_000,
                [f"a{i}", f"b{i}"],
            )

    logger.info("Done; dataset %s", dataset.nominal_url)
    dataset.archive()


if __name__ == "__main__":
    main()
