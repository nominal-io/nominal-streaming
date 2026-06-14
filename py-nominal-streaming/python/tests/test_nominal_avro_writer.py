"""Tests for NominalAvroWriter (including rotation via max_points_per_file)."""

from __future__ import annotations

import pathlib

import fastavro
import polars as pl
from nominal_streaming import (
    NominalAvroWriter,
    NominalAvroWriterOpts,
)


def _read_records(path: pathlib.Path) -> list[dict]:
    with open(path, "rb") as f:
        return list(fastavro.reader(f))


def test_single_file_roundtrip_all_value_types(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "roundtrip.avro"
    with NominalAvroWriter(path) as w:
        w.write("dbl", 1_000_000_000, 1.5)
        w.write("int", 2_000_000_000, 42)
        w.write("str", 3_000_000_000, "hello")
        w.write_struct("st", 4_000_000_000, {"a": 1})
        w.write_float_array("fa", 5_000_000_000, [1.0, 2.0])
        w.write_string_array("sa", 6_000_000_000, ["x", "y"])

    records = _read_records(path)
    channels = {r["channel"] for r in records}
    assert channels == {"dbl", "int", "str", "st", "fa", "sa"}
    for r in records:
        assert len(r["timestamps"]) == 1


def test_rotator_rotates_at_max(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "roll.avro"
    opts = NominalAvroWriterOpts(max_points_per_file=100)
    with NominalAvroWriter(path, opts) as w:
        for i in range(250):
            w.write("x", (i + 1) * 1_000_000_000, float(i))

    paths = w.close()
    assert len(paths) == 3
    assert paths[0].name == "roll_000.avro"
    assert paths[1].name == "roll_001.avro"
    assert paths[2].name == "roll_002.avro"

    counts = [sum(len(r["timestamps"]) for r in _read_records(p)) for p in paths]
    assert counts == [100, 100, 50]


def test_rotator_write_batch_straddles_boundary(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "straddle.avro"
    opts = NominalAvroWriterOpts(max_points_per_file=100)
    with NominalAvroWriter(path, opts) as w:
        # Pre-fill the first file to 80 points.
        for i in range(80):
            w.write("x", (i + 1) * 1_000_000_000, float(i))
        assert w.points_accepted() == 80
        assert len(w.finalized_paths()) == 0

        # Now write a batch of 50 — should split 20 (fill first) + 30 (new file).
        batch_ts = [i * 1_000_000_000 for i in range(80, 130)]
        batch_v = [float(i) for i in range(80, 130)]
        w.write_batch("x", batch_ts, batch_v)

        assert len(w.finalized_paths()) == 1
        # Total across all files: 80 (first file, finalized) + 20 (fills first to 100) + 30 (second file) = 130
        assert w.points_accepted() == 130

    paths = w.close()
    assert len(paths) == 2
    counts = [sum(len(r["timestamps"]) for r in _read_records(p)) for p in paths]
    assert counts == [100, 30]


def test_write_dataframe_dispatches_per_dtype(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "df.avro"
    df = pl.DataFrame(
        {
            "ts": [1_000_000_000, 2_000_000_000, 3_000_000_000],
            "speed": [1.0, 2.0, 3.0],
            "label": ["a", "b", "c"],
            "arr": [[1.0, 2.0], None, [3.0]],
            "meta": [{"k": 1}, None, {"k": 3}],
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    records = _read_records(path)
    by_channel = {}
    for r in records:
        by_channel.setdefault(r["channel"], []).extend(zip(r["timestamps"], r["values"]))

    assert len(by_channel["speed"]) == 3
    assert len(by_channel["label"]) == 3
    assert len(by_channel["arr"]) == 2
    assert len(by_channel["meta"]) == 2


def test_written_files_observability(tmp_path: pathlib.Path) -> None:
    # NominalAvroWriter (no rotation): current path is visible via .path() while open.
    single = tmp_path / "single.avro"
    w1 = NominalAvroWriter(single)
    w1.write("x", 1, 1.0)
    assert pathlib.Path(w1.path()) == single
    closed_paths = w1.close()
    assert closed_paths == [single]

    # Rotating writer: mid-rotation, should see both finalized and current via written_files().
    base = tmp_path / "rot.avro"
    opts = NominalAvroWriterOpts(max_points_per_file=100)
    w2 = NominalAvroWriter(base, opts)
    for i in range(150):
        w2.write("x", (i + 1) * 1_000_000_000, float(i))
    files_before_close = list(w2.written_files())
    assert len(files_before_close) == 2
    assert files_before_close[0].name == "rot_000.avro"
    assert files_before_close[1].name == "rot_001.avro"

    w2.close()
    files_after_close = list(w2.written_files())
    assert files_after_close == files_before_close


def test_context_manager_finalizes(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "ctx.avro"
    with NominalAvroWriter(path) as w:
        w.write("x", 1, 1.0)

    # After __exit__, the file is finalized and data is on disk.
    assert path.exists()

    records = _read_records(path)
    assert len(records) == 1


def test_rotator_context_manager(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "ctxr.avro"
    opts = NominalAvroWriterOpts(max_points_per_file=10)
    with NominalAvroWriter(path, opts) as w:
        for i in range(25):
            w.write("x", (i + 1) * 1_000_000_000, float(i))

    paths = w.close()
    assert len(paths) == 3
    counts = [sum(len(r["timestamps"]) for r in _read_records(p)) for p in paths]
    assert counts == [10, 10, 5]


def test_flush_and_sync_are_callable_best_effort(tmp_path: pathlib.Path) -> None:
    """flush() and sync() are best-effort after the NominalDatasetStream
    delegation — they don't force a drain. Test only confirms that both
    calls return without error and the file contains the written data
    after close().
    """
    path = tmp_path / "fs.avro"
    with NominalAvroWriter(path) as w:
        w.write("ch", 1_000_000_000, 1.0)
        w.write("ch", 2_000_000_000, 2.0)
        w.flush()  # no-op aside from error check
        w.sync()  # fsync whatever has dispatched
        w.write("ch", 3_000_000_000, 3.0)

    # The file exists and all 3 points are recoverable after close.
    records = _read_records(path)
    total = sum(len(r["timestamps"]) for r in records)
    assert total == 3


def _read_points_by_channel(path: pathlib.Path) -> dict[str, list[tuple[int, object]]]:
    """Helper: flatten an avro file to {channel: [(ts, value), ...]} preserving order."""
    by_channel: dict[str, list[tuple[int, object]]] = {}
    for r in _read_records(path):
        by_channel.setdefault(r["channel"], []).extend(zip(r["timestamps"], r["values"]))
    return by_channel


def test_float_null_skipped_nan_preserved_inf_preserved(tmp_path: pathlib.Path) -> None:
    """Float64: polars null → row skipped; NaN and ±Inf → preserved as IEEE-754."""
    import math

    path = tmp_path / "float_edges.avro"
    df = pl.DataFrame(
        {
            "ts": [1, 2, 3, 4, 5],  # ns since epoch
            "x": [1.0, float("nan"), None, float("inf"), float("-inf")],
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    pts = _read_points_by_channel(path)["x"]
    # Null at ts=3 → skipped. Everything else → emitted with its value.
    tss = [ts for ts, _ in pts]
    assert tss == [1, 2, 4, 5], f"expected nulls skipped, got timestamps {tss}"

    vals = [v for _, v in pts]
    assert vals[0] == 1.0
    assert math.isnan(vals[1]), f"NaN should round-trip, got {vals[1]!r}"
    assert vals[2] == float("inf"), f"+Inf should round-trip, got {vals[2]!r}"
    assert vals[3] == float("-inf"), f"-Inf should round-trip, got {vals[3]!r}"


def test_int_null_skipped(tmp_path: pathlib.Path) -> None:
    """Int64: polars null → row skipped (no sentinel zero)."""
    path = tmp_path / "int_nulls.avro"
    df = pl.DataFrame(
        {
            "ts": [1, 2, 3, 4],
            "n": pl.Series("n", [10, None, 20, None], dtype=pl.Int64),
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    pts = _read_points_by_channel(path)["n"]
    assert [ts for ts, _ in pts] == [1, 3]
    assert [v for _, v in pts] == [10, 20]


def test_string_null_skipped(tmp_path: pathlib.Path) -> None:
    """String: polars null → row skipped (no sentinel empty-string)."""
    path = tmp_path / "str_nulls.avro"
    df = pl.DataFrame(
        {
            "ts": [1, 2, 3],
            "s": pl.Series("s", ["a", None, "c"], dtype=pl.String),
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    pts = _read_points_by_channel(path)["s"]
    assert [ts for ts, _ in pts] == [1, 3]
    assert [v for _, v in pts] == ["a", "c"]


def test_float_all_nulls_column_emits_nothing(tmp_path: pathlib.Path) -> None:
    """Float64 column that is entirely null → no records emitted for that channel."""
    path = tmp_path / "all_null.avro"
    df = pl.DataFrame(
        {
            "ts": [1, 2, 3],
            "x": pl.Series("x", [None, None, None], dtype=pl.Float64),
            "y": pl.Series("y", [1.0, 2.0, 3.0], dtype=pl.Float64),
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    by_channel = _read_points_by_channel(path)
    assert "x" not in by_channel, "all-null column should emit no records"
    assert len(by_channel["y"]) == 3


def test_struct_field_non_finite_is_json_null(tmp_path: pathlib.Path) -> None:
    """Struct fields with NaN/Inf emit JSON `null` (preserves structure); they
    are NOT silently dropped from the resulting JSON object.
    """
    import json

    path = tmp_path / "struct_nonfinite.avro"
    schema = pl.Struct([pl.Field("a", pl.Int64), pl.Field("b", pl.Float64)])
    df = pl.DataFrame(
        {
            "ts": [1, 2, 3],
            "s": pl.Series(
                "s",
                [
                    {"a": 1, "b": 1.5},
                    {"a": 2, "b": float("nan")},
                    {"a": 3, "b": float("inf")},
                ],
                dtype=schema,
            ),
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    pts = _read_points_by_channel(path)["s"]
    assert len(pts) == 3
    parsed = [json.loads(v["json"]) for _, v in pts]
    assert parsed[0] == {"a": 1, "b": 1.5}
    # The "b" key is preserved even when the value is non-finite — it becomes null.
    assert parsed[1] == {"a": 2, "b": None}
    assert parsed[2] == {"a": 3, "b": None}


def test_struct_null_row_skipped(tmp_path: pathlib.Path) -> None:
    """Struct: polars-null row → no record emitted at that timestamp."""
    import json

    path = tmp_path / "struct_null_row.avro"
    schema = pl.Struct([pl.Field("k", pl.Int64)])
    df = pl.DataFrame(
        {
            "ts": [1, 2, 3],
            "s": pl.Series("s", [{"k": 1}, None, {"k": 3}], dtype=schema),
        }
    )
    with NominalAvroWriter(path) as w:
        w.write_dataframe(df, "ts")

    pts = _read_points_by_channel(path)["s"]
    assert [ts for ts, _ in pts] == [1, 3]
    assert [json.loads(v["json"]) for _, v in pts] == [{"k": 1}, {"k": 3}]


def test_timestamp_null_is_hard_error(tmp_path: pathlib.Path) -> None:
    """A null in the timestamp column must raise — no silent handling."""
    import pytest

    path = tmp_path / "ts_null.avro"
    df = pl.DataFrame(
        {
            "ts": pl.Series("ts", [1, None, 3], dtype=pl.Int64),
            "x": [1.0, 2.0, 3.0],
        }
    )
    with NominalAvroWriter(path) as w, pytest.raises(ValueError, match="null in timestamp"):
        w.write_dataframe(df, "ts")
