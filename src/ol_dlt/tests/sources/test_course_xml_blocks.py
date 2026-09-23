"""Tests for the course XML block source."""

import json
from pathlib import Path
from typing import Any

import fsspec
import pytest

from ol_dlt import config
from ol_dlt.sources import course_xml_blocks

_BLOCK: dict[str, Any] = {
    "course_id": "course-v1:MITx+1.00x+1T2026",
    "source_system": "mitxonline",
    "block_id": "0ca66ecd",
    "block_type": "chapter",
    "block_display_name": "Module Conclusion",
    "xml_attributes": {"display_name": "Module Conclusion", "start": "2025-09-02"},
    "xml_path": "course/chapter/0ca66ecd.xml",
    "raw_xml": '<chapter display_name="Module Conclusion"/>',
    "retrieved_at": "2026-08-08T13:23:00.727067+00:00",
    "edx_video_id": None,
    "duration": None,
    "max_attempts": None,
    "weight": 1.0,
    "markdown": None,
}


def test_row_keeps_xml_attributes_as_one_json_column() -> None:
    row = course_xml_blocks._row(json.dumps(_BLOCK))  # noqa: SLF001

    assert json.loads(row["xml_attributes"] or "") == _BLOCK["xml_attributes"]
    assert row["weight"] == "1.0"
    assert row["duration"] is None
    assert list(row) == list(course_xml_blocks.FIELDS)


def test_row_missing_a_field_fails() -> None:
    incomplete = {key: value for key, value in _BLOCK.items() if key != "raw_xml"}
    with pytest.raises(KeyError, match="raw_xml"):
        course_xml_blocks._row(json.dumps(incomplete))  # noqa: SLF001


def test_openedx_lists_each_deployment_prefix() -> None:
    """A glob rooted at the bucket would make fsspec walk the whole landing zone."""
    prefixes = [glob.split("/", 1)[0] for glob in course_xml_blocks.OPENEDX.file_globs]
    assert prefixes == ["mitx", "mitxonline", "xpro"]


def _write_version(root: Path, relative: str, blocks: list[dict[str, Any]]) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(block) + "\n" for block in blocks))


@pytest.mark.integration
def test_openedx_loads_every_deployment_into_one_table(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "landing"
    for deployment in ("mitx", "mitxonline", "xpro"):
        _write_version(
            landing,
            f"{deployment}/openedx/processed_data/course_xml_blocks/"
            f"{deployment}/course-v1:X+Y+Z/v1.json",
            [{**_BLOCK, "source_system": deployment}],
        )
    # A file outside every glob, which must not be read.
    _write_version(landing, "posthog/events/v1.json", [_BLOCK])
    monkeypatch.setattr(
        course_xml_blocks.s3fs, "S3FileSystem", lambda: fsspec.filesystem("file")
    )

    pipeline = course_xml_blocks.course_xml_blocks_pipeline_for(
        course_xml_blocks.OPENEDX.raw_table
    )
    source = course_xml_blocks.course_xml_blocks_source(
        raw_table=course_xml_blocks.OPENEDX.raw_table,
        bucket_url=landing.as_uri(),
    )
    info = pipeline.run(source)
    assert not info.has_failed_jobs

    table = pipeline.dataset()[course_xml_blocks.OPENEDX.raw_table].arrow()
    assert sorted(table.column("source_system").to_pylist()) == [
        "mitx",
        "mitxonline",
        "xpro",
    ]
    assert {"_source_file", "_file_modified_at", *config.DLT_LOAD_ID_COLUMN} <= set(
        table.column_names
    )
    assert (
        json.loads(table.column("xml_attributes")[0].as_py())
        == (_BLOCK["xml_attributes"])
    )

    # A second run finds nothing past the cursor and appends nothing.
    pipeline.run(
        course_xml_blocks.course_xml_blocks_source(
            raw_table=course_xml_blocks.OPENEDX.raw_table,
            bucket_url=landing.as_uri(),
        )
    )
    assert pipeline.dataset()[course_xml_blocks.OPENEDX.raw_table].arrow().num_rows == 3
