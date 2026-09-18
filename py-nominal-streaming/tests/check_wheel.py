"""Check an installed wheel before installing NumPy: python -I check_wheel.py python311."""

import argparse
import importlib.util
import sys
import tempfile
from pathlib import Path

import nominal_streaming
from nominal_streaming import NominalDatasetStream
from nominal_streaming._nominal_streaming import _BUFFER_FAST_PATH


def main() -> None:
    """Verify the installed variant and list enqueue without the optional dependency."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("feature", choices=("python310", "python311"))
    args = parser.parse_args()
    assert Path(nominal_streaming.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    assert importlib.util.find_spec("numpy") is None, "Run in a clean environment without NumPy"
    assert _BUFFER_FAST_PATH is (args.feature == "python311"), "Wrong wheel capability"
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / "lists.avro"
        with NominalDatasetStream().to_file(output) as stream:
            stream.enqueue_batch("lists", [1, 2], [1.0, 2.0])
        assert output.stat().st_size > 0, "No output written"


if __name__ == "__main__":
    main()
