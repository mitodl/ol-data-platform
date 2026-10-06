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
    assert list(row) == list(course_xml_blocks.XML_BLOCK_FIELDS)


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


_DOCUMENT: dict[str, Any] = {
    "course_id": "course-v1:MITxT+7.05x+2T2026",
    "source_system": "mitxonline",
    "file_path": "static/handout.pdf",
    "file_extension": ".pdf",
    "content_type": "application/pdf",
    "size_bytes": 1024,
    "content": "Handout text",
    "extraction_status": "extracted",
}


def test_content_text_row_allows_a_missing_file_extension() -> None:
    """A failed extraction row is written without file_extension."""
    failed = {key: value for key, value in _DOCUMENT.items() if key != "file_extension"}
    row = course_xml_blocks._row(  # noqa: SLF001
        json.dumps({**failed, "content": None, "extraction_status": "failed"}),
        course_xml_blocks.OPENEDX_DOCUMENT_TEXT,
    )
    assert row["file_extension"] is None
    assert row["size_bytes"] == "1024"
    assert list(row) == list(course_xml_blocks.CONTENT_TEXT_FIELDS)


def test_content_text_row_still_requires_content() -> None:
    incomplete = {key: value for key, value in _DOCUMENT.items() if key != "content"}
    with pytest.raises(KeyError, match="content"):
        course_xml_blocks._row(  # noqa: SLF001
            json.dumps(incomplete), course_xml_blocks.OPENEDX_TRANSCRIPT_TEXT
        )


def test_file_exclusion_row_keeps_the_flag_as_text() -> None:
    """Staging compares excluded to 'true', so the bool must arrive JSON-encoded."""
    row = course_xml_blocks._row(  # noqa: SLF001
        json.dumps(
            {
                "course_id": "course-v1:MITxT+7.05x+2T2026",
                "source_system": "mitxonline",
                "course_xml_version": "ee211fd8",
                "file_path": "static/unused.pdf",
                "excluded": True,
                "exclusion_reason": "unreferenced_static",
            }
        ),
        course_xml_blocks.OPENEDX_FILE_EXCLUSIONS,
    )
    assert row["excluded"] == "true"
    assert row["exclusion_reason"] == "unreferenced_static"
    assert list(row) == list(course_xml_blocks.FILE_EXCLUSION_FIELDS)
    # Files written before the asset stamped its rows have no extracted_at.
    assert row["extracted_at"] is None


def test_every_table_has_its_own_pipeline() -> None:
    """Two tables on one pipeline name would share, and fight over, one cursor."""
    names = [table.pipeline_name for table in course_xml_blocks.TABLES.values()]
    assert len(set(names)) == len(names)
    # Renaming these would re-read the landing zone and append every row again.
    assert course_xml_blocks.OPENEDX.pipeline_name == "course_xml_blocks__openedx"
    assert course_xml_blocks.EDXORG.pipeline_name == "course_xml_blocks__edxorg"
    assert (
        course_xml_blocks.EDXORG_STRUCTURE_BLOCKS.pipeline_name
        == "course_structure_blocks__edxorg"
    )


@pytest.mark.integration
def test_document_text_reads_only_its_own_prefix(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "landing"
    _write_version(
        landing,
        "mitxonline/openedx/processed_data/course_document_text/"
        "mitxonline/course-v1:MITxT+7.05x+2T2026/v1.jsonl",
        [_DOCUMENT],
    )
    _write_version(
        landing,
        "mitxonline/openedx/processed_data/course_transcript_text/"
        "mitxonline/course-v1:MITxT+7.05x+2T2026/v1.jsonl",
        [{**_DOCUMENT, "file_path": "static/subs_abc.srt.sjson"}],
    )
    monkeypatch.setattr(
        course_xml_blocks.s3fs, "S3FileSystem", lambda: fsspec.filesystem("file")
    )
    raw_table = course_xml_blocks.OPENEDX_DOCUMENT_TEXT.raw_table
    pipeline = course_xml_blocks.course_xml_blocks_pipeline_for(raw_table)
    info = pipeline.run(
        course_xml_blocks.course_xml_blocks_source(
            raw_table=raw_table, bucket_url=landing.as_uri()
        )
    )
    assert not info.has_failed_jobs

    table = pipeline.dataset()[raw_table].arrow()
    assert table.column("file_path").to_pylist() == ["static/handout.pdf"]
    assert table.column("size_bytes").to_pylist() == ["1024"]


def test_empty_text_file_marker_names_its_course() -> None:
    marker = course_xml_blocks._empty_file_marker(  # noqa: SLF001
        "s3://bucket/xpro/openedx/processed_data/course_transcript_text/"
        "xpro/course-v1%3AxPRO%2BQCFx1%2BR24/abc.jsonl",
        course_xml_blocks.OPENEDX_TRANSCRIPT_TEXT,
    )
    assert marker["course_id"] == "course-v1:xPRO+QCFx1+R24"
    assert marker["source_system"] == "xpro"
    assert marker["file_path"] is None
    assert list(marker) == list(course_xml_blocks.CONTENT_TEXT_FIELDS)


@pytest.mark.integration
def test_an_empty_text_file_loads_as_a_marker_row(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A course left with no documents must still land a newer file in raw."""
    landing = tmp_path / "landing"
    prefix = (
        "mitxonline/openedx/processed_data/course_document_text/"
        "mitxonline/course-v1:MITxT+7.05x+2T2026/"
    )
    _write_version(landing, prefix + "v1.jsonl", [_DOCUMENT])
    _write_version(landing, prefix + "v2.jsonl", [])
    monkeypatch.setattr(
        course_xml_blocks.s3fs, "S3FileSystem", lambda: fsspec.filesystem("file")
    )
    raw_table = course_xml_blocks.OPENEDX_DOCUMENT_TEXT.raw_table
    pipeline = course_xml_blocks.course_xml_blocks_pipeline_for(raw_table)
    info = pipeline.run(
        course_xml_blocks.course_xml_blocks_source(
            raw_table=raw_table, bucket_url=landing.as_uri()
        )
    )
    assert not info.has_failed_jobs

    rows = pipeline.dataset()[raw_table].arrow().to_pylist()
    by_file = {row["_source_file"].rsplit("/", 1)[-1]: row for row in rows}
    assert by_file["v1.jsonl"]["file_path"] == "static/handout.pdf"
    assert by_file["v2.jsonl"]["file_path"] is None
    assert by_file["v2.jsonl"]["course_id"] == "course-v1:MITxT+7.05x+2T2026"


_STRUCTURE_BLOCK: dict[str, Any] = {
    "block_content_hash": "ef87a5b9",
    "block_details": {
        "category": "chapter",
        "children": ["block-v1:MITx+RES+1T2027+type@sequential+block@a1"],
        "metadata": {"display_name": "Week 1", "start": "2027-03-31T00:00:00Z"},
    },
    "block_due": None,
    "block_id": "block-v1:MITx+RES+1T2027+type@chapter+block@c1",
    "block_index": 2,
    "block_parent": "block-v1:MITx+RES+1T2027+type@course+block@course",
    "block_start": "2027-03-31T00:00:00Z",
    "block_title": "Week 1",
    "block_type": "chapter",
    "course_content_hash": "1d6aff27",
    "course_id": "MITx-RES-1T2027",
    "course_start": "2027-03-31T00:00:00Z",
    "course_title": "Real Estate Development",
    "retrieved_at": "2026-09-30T03:13:40.123456+00:00",
}


def test_structure_block_row_keeps_the_index_an_integer() -> None:
    row = course_xml_blocks._row(  # noqa: SLF001
        json.dumps(_STRUCTURE_BLOCK), course_xml_blocks.EDXORG_STRUCTURE_BLOCKS
    )

    assert row["block_index"] == _STRUCTURE_BLOCK["block_index"]
    assert json.loads(str(row["block_details"])) == _STRUCTURE_BLOCK["block_details"]
    assert list(row) == list(course_xml_blocks.STRUCTURE_BLOCK_FIELDS)


def test_structure_block_row_lands_a_list_course_id_as_its_json_text() -> None:
    """Older files carry the course id as a one-element list.

    Staging strips the brackets and quotes Airbyte landed it with.
    """
    row = course_xml_blocks._row(  # noqa: SLF001
        json.dumps({**_STRUCTURE_BLOCK, "course_id": ["MITx-RES-1T2027"]}),
        course_xml_blocks.EDXORG_STRUCTURE_BLOCKS,
    )

    assert row["course_id"] == '["MITx-RES-1T2027"]'


@pytest.mark.integration
def test_structure_blocks_load_from_their_own_prefix(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "landing"
    processed = "edxorg-raw-data/edxorg/processed_data"
    _write_version(
        landing,
        f"{processed}/course_blocks/MITx-RES-1T2027|prod/v1.json",
        [_STRUCTURE_BLOCK, {**_STRUCTURE_BLOCK, "block_index": 3}],
    )
    # The XML blocks sit beside them and belong to the other edxorg table.
    _write_version(
        landing, f"{processed}/course_xml_blocks/prod/MITx-RES-1T2027/v1.json", [_BLOCK]
    )
    monkeypatch.setattr(
        course_xml_blocks.s3fs, "S3FileSystem", lambda: fsspec.filesystem("file")
    )
    raw_table = course_xml_blocks.EDXORG_STRUCTURE_BLOCKS.raw_table

    pipeline = course_xml_blocks.course_xml_blocks_pipeline_for(raw_table)
    info = pipeline.run(
        course_xml_blocks.course_xml_blocks_source(
            raw_table=raw_table, bucket_url=landing.as_uri()
        )
    )
    assert not info.has_failed_jobs

    table = pipeline.dataset()[raw_table].arrow()
    assert sorted(table.column("block_index").to_pylist()) == [2, 3]
    assert {"_source_file", "_file_modified_at", *config.DLT_LOAD_ID_COLUMN} <= set(
        table.column_names
    )

    # A second run finds nothing past the cursor and appends nothing.
    pipeline.run(
        course_xml_blocks.course_xml_blocks_source(
            raw_table=raw_table, bucket_url=landing.as_uri()
        )
    )
    assert pipeline.dataset()[raw_table].arrow().num_rows == 2
