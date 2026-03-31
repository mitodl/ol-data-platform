"""Integration tests for process_course_xml_blocks against real course archives.

These tests run against actual course archives downloaded from production S3 buckets.
They are skipped automatically in CI where the archive files are not present.

To run locally:
    cd packages/ol-orchestrate-lib
    PYTHONPATH=src uv run pytest tests/lib/test_openedx_integration.py -v -m integration
"""

import io
import json
import tarfile
from pathlib import Path

import pytest
from ol_orchestrate.lib.openedx import (
    CourseStaticAssetsBundle,
    CourseXmlBlock,
    process_course_xml_blocks,
)

DOWNLOADS = Path.home() / "Downloads"

EXCLUDED_BLOCK_TYPES = {"drafts", "assets", "static", "course"}

ARCHIVES = [
    pytest.param(
        DOWNLOADS / "edxorg-MITProfessionalX-6.BDx_SZH-2015_3T.tar.gz",
        "prod",
        "course-v1:MITProfessionalX+6.BDx_SZH+2015_3T",
        id="edxorg-MITProfessionalX-6.BDx_SZH-2015_3T",
    ),
    pytest.param(
        DOWNLOADS / "edxorg-MITx-0.503x-1T2020.tar.gz",
        "prod",
        "course-v1:MITx+0.503x+1T2020",
        id="edxorg-MITx-0.503x-1T2020",
    ),
    pytest.param(
        DOWNLOADS / "edxorg-MITProfessionalX-CSx-2017_T2.tar.gz",
        "prod",
        "course-v1:MITProfessionalX+CSx+2017_T2",
        id="edxorg-MITProfessionalX-CSx-2017_T2",  # pragma: allowlist secret
    ),
    pytest.param(
        DOWNLOADS / "edxorg-VJx-MITFMT03-2T2023.tar.gz",
        "prod",
        None,
        id="edxorg-VJx-MITFMT03-2T2023-stub",
    ),
    pytest.param(
        DOWNLOADS / "mitxonline-ETU-ET.123x-1T2022.tar.gz",
        "mitxonline",
        "course-v1:ETU+ET.123x+1T2022",
        id="mitxonline-ETU-ET.123x-1T2022",
    ),
    pytest.param(
        DOWNLOADS / "mitxonline-MITx-AL.100x-1T2026.tar.gz",
        "mitxonline",
        "course-v1:MITx+AL.100x+1T2026",
        id="mitxonline-MITx-AL.100x-1T2026",
    ),
    pytest.param(
        DOWNLOADS / "mitx-MITx-1801Ar_5-2023_Fall.tar.gz",
        "mitx",
        "course-v1:MITx+1801Ar_5+2023_Fall",
        id="mitx-MITx-1801Ar_5-2023_Fall",
    ),
    pytest.param(
        DOWNLOADS / "mitx-MITx-ES.7013r_8-2026_Spring.tar.gz",
        "mitx",
        "course-v1:MITx+ES.7013r_8+2026_Spring",
        id="mitx-MITx-ES.7013r_8-2026_Spring",
    ),
    pytest.param(
        DOWNLOADS / "xpro-SysEngx3-R26.tar.gz",
        "xpro",
        "course-v1:xPRO+SysEngx3+R26",
        id="xpro-SysEngx3-R26",
    ),
    pytest.param(
        DOWNLOADS / "xpro-MLxTouchEdu1-SPOC_R10.tar.gz",
        "xpro",
        "course-v1:xPRO+MLxTouchEdu1+SPOC_R10",
        id="xpro-MLxTouchEdu1-SPOC_R10",
    ),
]


def archive_exists(archive_path):
    return Path(archive_path).exists()


def skip_if_missing(archive_path):
    return pytest.mark.skipif(
        not archive_exists(archive_path),
        reason=f"Archive not found locally: {archive_path}",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_returns_typed_blocks(
    archive_path,
    source_system,
    expected_course_id,  # noqa: ARG001
):
    """All blocks are CourseXmlBlock instances with required fields populated."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    blocks, static_assets = process_course_xml_blocks(Path(archive_path), source_system)

    for block in blocks:
        assert isinstance(block, CourseXmlBlock)
        assert block.course_id, "course_id must not be empty"
        assert block.source_system == source_system
        assert block.block_id, "block_id must not be empty"
        assert block.block_type, "block_type must not be empty"
        assert block.xml_path, "xml_path must not be empty"
        assert block.retrieved_at, "retrieved_at must not be empty"
        assert block.raw_xml, "raw_xml must not be empty"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_course_id(archive_path, source_system, expected_course_id):
    """Extracted course_id matches the expected value for non-stub archives."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")
    if expected_course_id is None:
        pytest.skip("Stub archive — no course_id expected")

    blocks, _ = process_course_xml_blocks(Path(archive_path), source_system)

    assert len(blocks) > 0, "Expected at least one block"
    assert all(b.course_id == expected_course_id for b in blocks), (
        f"All blocks should have course_id={expected_course_id}"
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_no_excluded_block_types(
    archive_path,
    source_system,
    expected_course_id,  # noqa: ARG001
):
    """No blocks should be from excluded structural directories."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    blocks, _ = process_course_xml_blocks(Path(archive_path), source_system)

    found_excluded = {
        b.block_type for b in blocks if b.block_type in EXCLUDED_BLOCK_TYPES
    }
    assert not found_excluded, (
        f"Found excluded block types: {found_excluded}. "
        "drafts/assets/static/course directories should be filtered."
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_raw_xml_is_valid(archive_path, source_system, expected_course_id):  # noqa: ARG001
    """raw_xml field contains the block's tag name for every block."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    blocks, _ = process_course_xml_blocks(Path(archive_path), source_system)

    for block in blocks:
        assert isinstance(block.raw_xml, str)
        assert len(block.raw_xml) > 0
        assert block.block_type in block.raw_xml, (
            f"raw_xml for block_type='{block.block_type}' "
            f"should contain the block tag. Got: {block.raw_xml[:80]}"
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_model_dump_json_serializable(
    archive_path,
    source_system,
    expected_course_id,  # noqa: ARG001
):
    """All blocks can be serialized to JSON (as required by the jsonlines writer)."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    blocks, _ = process_course_xml_blocks(Path(archive_path), source_system)

    for block in blocks:
        dumped = block.model_dump()
        json.dumps(dumped)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_static_assets_are_bytes(
    archive_path,
    source_system,
    expected_course_id,  # noqa: ARG001
):
    """Static assets bundle contains (str, bytes) file pairs."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    _, bundle = process_course_xml_blocks(Path(archive_path), source_system)

    assert isinstance(bundle, CourseStaticAssetsBundle)
    for relative_path, asset_bytes in bundle.files:
        assert isinstance(relative_path, str)
        assert len(relative_path) > 0
        assert isinstance(asset_bytes, bytes)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_static_assets_rebundleable(
    archive_path,
    source_system,
    expected_course_id,  # noqa: ARG001
):
    """Static assets bundle can be written into a tar.gz."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    _, bundle = process_course_xml_blocks(Path(archive_path), source_system)

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for relative_path, asset_bytes in bundle.files:
            info = tarfile.TarInfo(name=relative_path)
            info.size = len(asset_bytes)
            tar.addfile(info, io.BytesIO(asset_bytes))

    buf.seek(0)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        assert len(tar.getmembers()) == len(bundle.files)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("archive_path", "source_system", "expected_course_id"), ARCHIVES
)
def test_real_archive_static_assets_bundle_version_and_manifest(
    archive_path,
    source_system,
    expected_course_id,  # noqa: ARG001
):
    """Static assets bundle has a valid data_version and consistent manifest."""
    if not archive_exists(archive_path):
        pytest.skip(f"Archive not found: {archive_path}")

    _, bundle = process_course_xml_blocks(Path(archive_path), source_system)

    # data_version is a 64-char lowercase hex SHA-256 digest
    assert isinstance(bundle.data_version, str)
    assert len(bundle.data_version) == 64
    assert all(c in "0123456789abcdef" for c in bundle.data_version)

    # manifest is JSON-serializable and internally consistent
    manifest_json = json.dumps(bundle.manifest)
    assert manifest_json  # not empty
    assert bundle.manifest["data_version"] == bundle.data_version
    assert bundle.manifest["file_count"] == len(bundle.files)
    assert len(bundle.manifest["files"]) == len(bundle.files)

    # each manifest entry has required keys
    for entry in bundle.manifest["files"]:
        assert "path" in entry
        assert "mime_type" in entry
        assert "size_bytes" in entry
        assert isinstance(entry["size_bytes"], int)

    # no files from excluded structural directories are present
    excluded = {"drafts", "assets", "static"}
    for path, _ in bundle.files:
        top_dir = path.split("/")[0]
        assert top_dir not in excluded, (
            f"File from excluded directory '{top_dir}' found in static_assets: {path}"
        )
