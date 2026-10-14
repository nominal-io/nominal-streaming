"""Check an installed wheel before installing NumPy: python -I check_wheel.py."""

import importlib.util
import sys
import tempfile
from pathlib import Path

import nominal_streaming
from nominal_streaming import NominalDatasetStream


def main() -> None:
    """Verify the installed wheel and list enqueue without the optional dependency."""
    assert Path(nominal_streaming.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    assert importlib.util.find_spec("numpy") is None, "Run in a clean environment without NumPy"
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / "lists.avro"
        with NominalDatasetStream().to_file(output) as stream:
            stream.enqueue_batch("lists", [1, 2], [1.0, 2.0])
        assert output.stat().st_size > 0, "No output written"


if __name__ == "__main__":
    main()
