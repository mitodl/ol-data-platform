"""Tests for ol_orchestrate.lib.openedx module."""

import json
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from ol_orchestrate.lib.openedx import (
    CourseExportOutcome,
    CourseStaticAssetsBundle,
    CourseXmlBlock,
    classify_course_export_state,
    generate_block_indexes,
    process_course_xml_blocks,
    un_nest_course_structure,
)


@pytest.fixture
def sample_course_archive():
    """Create a sample course XML archive for testing."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "test_course.tar.gz"

    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()

    # Root course.xml — processed by process_course_xml(), skipped here
    (course_root / "course.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<course url_name="2024_Spring" org="TestX" course="TEST101"/>\n'
    )

    # course/{run_tag}.xml — metadata block, skipped by process_course_xml_blocks
    course_dir = course_root / "course"
    course_dir.mkdir()
    (course_dir / "2024_Spring.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<course display_name="Test Course 101" start="2024-01-01T00:00:00Z"/>\n'
    )

    # chapter
    chapter_dir = course_root / "chapter"
    chapter_dir.mkdir()
    (chapter_dir / "chapter1.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<chapter display_name="Chapter 1" url_name="chapter1"/>\n'
    )

    # sequential
    sequential_dir = course_root / "sequential"
    sequential_dir.mkdir()
    (sequential_dir / "seq1.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sequential display_name="Sequence 1" url_name="seq1"/>\n'
    )

    # vertical
    vertical_dir = course_root / "vertical"
    vertical_dir.mkdir()
    (vertical_dir / "vert1.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<vertical display_name="Unit 1" url_name="vert1"/>\n'
    )

    # video
    video_dir = course_root / "video"
    video_dir.mkdir()
    (video_dir / "video1.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<video display_name="Sample Video" url_name="video1"'
        ' edx_video_id="test-video-123">'
        '<video_asset duration="300.5"/></video>\n'
    )

    # problem
    problem_dir = course_root / "problem"
    problem_dir.mkdir()
    (problem_dir / "problem1.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<problem display_name="Sample Problem" url_name="problem1"'
        ' max_attempts="3" weight="10"/>\n'
    )

    # html
    html_dir = course_root / "html"
    html_dir.mkdir()
    (html_dir / "html1.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<html display_name="HTML Component" url_name="html1"/>\n'
    )

    # unknown block type — should still be included (no allowlist)
    unknown_dir = course_root / "lti_v2"
    unknown_dir.mkdir()
    (unknown_dir / "lti_block.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<lti_v2 display_name="Custom LTI" url_name="lti_block"/>\n'
    )

    # static non-XML assets — in the excluded `static/` directory;
    # these should NOT appear in the bundle (excluded by directory filter)
    static_dir = course_root / "static"
    static_dir.mkdir()
    (static_dir / "logo.png").write_text("fake png data")

    # non-XML content files in non-excluded directories — these SHOULD be collected
    subtitles_dir = course_root / "subtitles"
    subtitles_dir.mkdir()
    (subtitles_dir / "subtitle.srt").write_text(
        "1\n00:00:00,000 --> 00:00:05,000\nSample subtitle"
    )

    html_content_dir = course_root / "html"
    (html_content_dir / "content.html").write_text(
        "<html><body>Sample HTML</body></html>"
    )

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    yield archive_path, temp_dir
    temp_dir.cleanup()


def test_process_course_xml_blocks_returns_typed_blocks(sample_course_archive):
    """Test that blocks are returned as CourseXmlBlock instances."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "test")

    assert len(blocks) > 0
    for block in blocks:
        assert isinstance(block, CourseXmlBlock), (
            "Each block should be a CourseXmlBlock"
        )


def test_process_course_xml_blocks_required_fields(sample_course_archive):
    """Test that all required fields are present on every block."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "test")

    for block in blocks:
        assert block.course_id, "Block should have course_id"
        assert block.source_system == "test", "Block should have correct source_system"
        assert block.block_id, "Block should have block_id"
        assert block.block_type, "Block should have block_type"
        assert block.xml_path, "Block should have xml_path"
        assert block.retrieved_at, "Block should have retrieved_at"
        assert block.raw_xml, "Block should have raw_xml for downstream enrichment"


def test_process_course_xml_blocks_raw_xml_present(sample_course_archive):
    """Test that raw_xml captures the full XML string of each block."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "test")

    for block in blocks:
        assert isinstance(block.raw_xml, str), "raw_xml should be a string"
        assert len(block.raw_xml) > 0, "raw_xml should not be empty"
        # raw_xml should contain the block tag name
        assert block.block_type in block.raw_xml, (
            f"raw_xml should contain block type '{block.block_type}'"
        )


def test_process_course_xml_blocks_course_metadata_skipped(sample_course_archive):
    """Test that course/{run_tag}.xml metadata files are not included as blocks.

    These files are already processed by process_course_xml() to extract
    course-level metadata and should not appear in the structural block output.
    """
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "test")

    block_types = {b.block_type for b in blocks}
    assert "course" not in block_types, (
        "course/ metadata files should be skipped "
        "— already handled by process_course_xml()"
    )


def test_process_course_xml_blocks_unknown_types_included(sample_course_archive):
    """Test that unknown block types are included (no allowlist)."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "test")

    block_types = {b.block_type for b in blocks}
    assert "lti_v2" in block_types, (
        "Unknown block types should be included — raw_xml preserves data for enrichment"
    )


def test_process_course_xml_blocks_known_types(sample_course_archive):
    """Test that all known block types are correctly extracted."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "test")

    block_types = {b.block_type for b in blocks}
    for expected in ("chapter", "sequential", "vertical", "video", "problem", "html"):
        assert expected in block_types, f"Should extract '{expected}' blocks"


def test_process_course_xml_blocks_video_metadata(sample_course_archive):
    """Test that video-specific metadata is extracted correctly."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "prod")

    video_blocks = [b for b in blocks if b.block_type == "video"]
    assert len(video_blocks) > 0
    video = video_blocks[0]
    assert video.edx_video_id == "test-video-123"
    assert video.duration == "300.5"


def test_process_course_xml_blocks_problem_metadata(sample_course_archive):
    """Test that problem-specific metadata is extracted correctly."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "prod")

    problem_blocks = [b for b in blocks if b.block_type == "problem"]
    assert len(problem_blocks) > 0
    problem = problem_blocks[0]
    assert problem.max_attempts == "3"
    assert problem.weight == "10"


def test_process_course_xml_blocks_xml_attributes(sample_course_archive):
    """Test that XML attributes are preserved in xml_attributes dict."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "prod")

    for block in blocks:
        assert isinstance(block.xml_attributes, dict)
        if block.block_display_name:
            assert "display_name" in block.xml_attributes


def test_process_course_xml_blocks_static_assets(sample_course_archive):
    """Test that non-XML static assets are returned as a CourseStaticAssetsBundle."""
    archive_path, _ = sample_course_archive
    _, bundle = process_course_xml_blocks(archive_path, "prod")

    assert isinstance(bundle, CourseStaticAssetsBundle)
    assert len(bundle.files) > 0, "Should have static assets"
    for relative_path, asset_bytes in bundle.files:
        assert isinstance(relative_path, str), "Path should be a string"
        assert isinstance(asset_bytes, bytes), "Content should be bytes"
        assert len(asset_bytes) > 0, "Asset should not be empty"

    paths = [p for p, _ in bundle.files]
    assert any("subtitle.srt" in p for p in paths), "Should include SRT file"
    assert any("content.html" in p for p in paths), "Should include HTML file"

    # Files from excluded structural directories must not appear in the bundle
    excluded_dirs = {"drafts", "assets", "static"}
    for path in paths:
        top_dir = path.split("/")[0]
        assert top_dir not in excluded_dirs, (
            f"File from excluded directory '{top_dir}' should not be in bundle: {path}"
        )


def test_process_course_xml_blocks_model_dump_serializable(sample_course_archive):
    """Test that blocks can be serialized to dicts for jsonlines output."""
    archive_path, _ = sample_course_archive
    blocks, _ = process_course_xml_blocks(archive_path, "prod")

    for block in blocks:
        dumped = block.model_dump()
        assert isinstance(dumped, dict)
        # Should be JSON-serializable
        json.dumps(dumped)


def test_process_course_xml_blocks_missing_course_xml():
    """Test that missing course.xml raises appropriate error."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "invalid_course.tar.gz"
    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()
    (course_root / "chapter").mkdir()
    (course_root / "chapter" / "ch1.xml").write_text('<chapter display_name="Ch1"/>')

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    with pytest.raises(ValueError, match=r"course\.xml not found"):
        process_course_xml_blocks(archive_path, "test")

    temp_dir.cleanup()


def test_process_course_xml_blocks_empty_archive():
    """Test that empty archive raises appropriate error."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "empty.tar.gz"
    with tarfile.open(archive_path, "w:gz"):
        pass

    with pytest.raises(ValueError, match="Unable to retrieve the archive root"):
        process_course_xml_blocks(archive_path, "test")

    temp_dir.cleanup()


def test_process_course_xml_blocks_malformed_xml(sample_course_archive):
    """Test that malformed XML files are skipped gracefully."""
    archive_path, temp_dir = sample_course_archive
    course_root = Path(temp_dir.name) / "course_root"
    (course_root / "video" / "malformed.xml").write_text(
        "This is not valid XML <unclosed tag"
    )
    new_archive = Path(temp_dir.name) / "course_with_malformed.tar.gz"
    with tarfile.open(new_archive, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    blocks, _ = process_course_xml_blocks(new_archive, "test")
    assert len(blocks) > 0, "Should still extract valid blocks"


def test_process_course_xml_blocks_source_system_tracking():
    """Test that source_system is correctly set on all blocks."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "test.tar.gz"
    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()
    (course_root / "course.xml").write_text(
        '<course url_name="test" org="TestX" course="TEST"/>'
    )
    (course_root / "chapter").mkdir()
    (course_root / "chapter" / "ch1.xml").write_text('<chapter display_name="Ch1"/>')

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    for source_sys in ["prod", "edge", "mitxonline", "xpro"]:
        blocks, _ = process_course_xml_blocks(archive_path, source_sys)
        for block in blocks:
            assert block.source_system == source_sys

    temp_dir.cleanup()


def test_process_course_xml_blocks_structural_dirs_excluded():
    """Test that archive structural directories are not included as blocks.

    drafts/ - Studio draft workspace, not published content
    assets/ - Static file storage directory, not a block type
    static/ - Static file storage directory, not a block type
    """
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "test.tar.gz"
    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()

    (course_root / "course.xml").write_text(
        '<course url_name="test" org="TestX" course="TEST"/>'
    )

    # Create a real block
    chapter_dir = course_root / "chapter"
    chapter_dir.mkdir()
    (chapter_dir / "ch1.xml").write_text('<chapter display_name="Ch1"/>')

    # Create structural directories with XML files that should be excluded
    for dir_name in ["drafts", "assets", "static"]:
        d = course_root / dir_name
        d.mkdir()
        (d / "something.xml").write_text(
            f'<{dir_name} display_name="Should be excluded"/>'
        )

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    blocks, _ = process_course_xml_blocks(archive_path, "test")
    block_types = {b.block_type for b in blocks}

    assert "drafts" not in block_types, "drafts/ directory should be excluded"
    assert "assets" not in block_types, "assets/ directory should be excluded"
    assert "static" not in block_types, "static/ directory should be excluded"
    assert "chapter" in block_types, "Real block types should still be included"

    temp_dir.cleanup()


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("Succeeded", CourseExportOutcome.SUCCEEDED),
        ("Failed", CourseExportOutcome.FAILED),
        ("Canceled", CourseExportOutcome.FAILED),
        # The regression: Studio uses Retrying for a task it is about to
        # attempt again, and both poll loops used to count it as terminal.
        ("Retrying", CourseExportOutcome.PENDING),
        ("In Progress", CourseExportOutcome.PENDING),
        ("Pending", CourseExportOutcome.PENDING),
        # An undocumented or absent state keeps the caller waiting rather
        # than inventing a verdict; the poll loop's timeout bounds it.
        ("Something New", CourseExportOutcome.PENDING),
        (None, CourseExportOutcome.PENDING),
    ],
)
def test_classify_course_export_state(
    state: str | None, expected: CourseExportOutcome
) -> None:
    """Retrying must not be terminal; unknown states must not be either."""
    assert classify_course_export_state(state) == expected


def test_retrying_then_succeeded_completes_the_export() -> None:
    """A task that retries and then succeeds must finish, not fail.

    This is the production sequence that broke: the first Retrying poll
    marked the course failed, satisfied the loop's completion condition and
    raised "Unable to export the course XML" while Studio was still working.
    Driving the accumulate logic over the sequence shows the loop now runs to
    the Succeeded state.
    """
    succeeded: set[str] = set()
    failed: set[str] = set()
    course_id = "course-v1:MITxT+MITx+0T2026"

    for state in ("In Progress", "Retrying", "Retrying", "Succeeded"):
        outcome = classify_course_export_state(state)
        if outcome is CourseExportOutcome.SUCCEEDED:
            succeeded.add(course_id)
        elif outcome is CourseExportOutcome.FAILED:
            failed.add(course_id)
        # The loop keeps polling while neither set has the course in it.
        if succeeded or failed:
            break

    assert succeeded == {course_id}
    assert failed == set()


def _block(category: str, children: list[str], display_name: str = "Block"):
    return {
        "category": category,
        "children": children,
        "metadata": {"display_name": display_name, "start": "2026-01-01T00:00:00Z"},
    }


def test_generate_block_indexes_walks_depth_first():
    """Blocks are numbered in the order a learner encounters them."""
    structure = {
        "course": _block("course", ["chapter_1", "chapter_2"]),
        "chapter_1": _block("chapter", ["seq_1"]),
        "seq_1": _block("sequential", []),
        "chapter_2": _block("chapter", []),
    }

    indexes = generate_block_indexes(structure, "course")

    assert list(indexes) == ["course", "chapter_1", "seq_1", "chapter_2"]
    assert indexes["course"] == 1
    assert indexes["chapter_2"] == 4


def test_generate_block_indexes_skips_children_absent_from_the_structure():
    """A child named by its parent but missing from the document is skipped.

    Courses that source content from a library do this: the parent lists the
    block, but the block itself is not in the course structure document.
    Indexing it by key raised
    ``KeyError: 'block-v1:...+type@sequential+block@...'`` and failed the
    whole course_structure asset for that course.
    """
    structure = {
        "course": _block("course", ["chapter_1"]),
        "chapter_1": _block("chapter", ["library_block", "seq_1"]),
        "seq_1": _block("sequential", []),
    }

    indexes = generate_block_indexes(structure, "course")

    assert "library_block" not in indexes
    assert list(indexes) == ["course", "chapter_1", "seq_1"]
    # The sequence stays contiguous over the blocks that actually exist.
    assert list(indexes.values()) == [1, 2, 3]


def test_generate_block_indexes_tolerates_a_missing_root():
    """No block with category 'course' leaves root_block as an empty string."""
    assert generate_block_indexes({"chapter_1": _block("chapter", [])}, "") == {}


def test_un_nest_course_structure_survives_a_library_reference():
    """The full un-nest path completes for a course with a library block."""
    structure = {
        "course": _block("course", ["chapter_1"], display_name="A Course"),
        "chapter_1": _block("chapter", ["library_block"]),
    }

    blocks = un_nest_course_structure("course-v1:Org+Num+Run", structure, None)

    assert {block["block_id"] for block in blocks} == {"course", "chapter_1"}
