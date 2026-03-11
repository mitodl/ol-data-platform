"""Tests for ol_orchestrate.lib.openedx module, specifically process_course_xml_blocks function."""

import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from ol_orchestrate.lib.openedx import process_course_xml_blocks


@pytest.fixture
def sample_course_archive():
    """Create a sample course XML archive for testing."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "test_course.tar.gz"

    # Create a temporary directory structure for the course
    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()

    # Create course.xml in root
    course_xml_path = course_root / "course.xml"
    course_xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<course url_name="2024_Spring" org="TestX" course="TEST101"/>
"""
    course_xml_path.write_text(course_xml_content)

    # Create course directory with metadata
    course_dir = course_root / "course"
    course_dir.mkdir()
    metadata_xml = course_dir / "2024_Spring.xml"
    metadata_content = """<?xml version="1.0" encoding="UTF-8"?>
<course display_name="Test Course 101" start="2024-01-01T00:00:00Z">
</course>
"""
    metadata_xml.write_text(metadata_content)

    # Create chapter directory with sample chapter
    chapter_dir = course_root / "chapter"
    chapter_dir.mkdir()
    chapter_xml = chapter_dir / "chapter1.xml"
    chapter_content = """<?xml version="1.0" encoding="UTF-8"?>
<chapter display_name="Chapter 1" url_name="chapter1">
</chapter>
"""
    chapter_xml.write_text(chapter_content)

    # Create sequential directory with sample sequential
    sequential_dir = course_root / "sequential"
    sequential_dir.mkdir()
    sequential_xml = sequential_dir / "seq1.xml"
    sequential_content = """<?xml version="1.0" encoding="UTF-8"?>
<sequential display_name="Sequence 1" url_name="seq1">
</sequential>
"""
    sequential_xml.write_text(sequential_content)

    # Create vertical directory with sample vertical
    vertical_dir = course_root / "vertical"
    vertical_dir.mkdir()
    vertical_xml = vertical_dir / "vert1.xml"
    vertical_content = """<?xml version="1.0" encoding="UTF-8"?>
<vertical display_name="Unit 1" url_name="vert1">
</vertical>
"""
    vertical_xml.write_text(vertical_content)

    # Create video directory with sample video
    video_dir = course_root / "video"
    video_dir.mkdir()
    video_xml = video_dir / "video1.xml"
    video_content = """<?xml version="1.0" encoding="UTF-8"?>
<video display_name="Sample Video" url_name="video1" edx_video_id="test-video-123">
    <video_asset duration="300.5"/>
</video>
"""
    video_xml.write_text(video_content)

    # Create problem directory with sample problem
    problem_dir = course_root / "problem"
    problem_dir.mkdir()
    problem_xml = problem_dir / "problem1.xml"
    problem_content = """<?xml version="1.0" encoding="UTF-8"?>
<problem display_name="Sample Problem" url_name="problem1" max_attempts="3" weight="10">
</problem>
"""
    problem_xml.write_text(problem_content)

    # Create html directory with sample HTML block
    html_dir = course_root / "html"
    html_dir.mkdir()
    html_xml = html_dir / "html1.xml"
    html_content = """<?xml version="1.0" encoding="UTF-8"?>
<html display_name="HTML Component" url_name="html1">
</html>
"""
    html_xml.write_text(html_content)

    # Create static directory with non-XML assets
    static_dir = course_root / "static"
    static_dir.mkdir()
    (static_dir / "subtitle.srt").write_text(
        "1\n00:00:00,000 --> 00:00:05,000\nSample subtitle"
    )
    (static_dir / "content.html").write_text("<html><body>Sample HTML</body></html>")

    # Create tar archive
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    yield archive_path, temp_dir

    # Cleanup
    temp_dir.cleanup()


def test_process_course_xml_blocks_basic(sample_course_archive):
    """Test basic extraction of course XML blocks."""
    archive_path, _ = sample_course_archive
    source_system = "test"

    blocks, static_assets_path = process_course_xml_blocks(archive_path, source_system)

    # Verify blocks were extracted
    assert len(blocks) > 0, "Should extract at least one block"

    # Verify static assets tar was created
    assert static_assets_path.exists(), "Static assets tar should be created"
    assert static_assets_path.suffix == ".gz", "Static assets should be gzipped"

    # Verify block structure
    for block in blocks:
        assert "course_id" in block, "Block should have course_id"
        assert "source_system" in block, "Block should have source_system"
        assert "block_id" in block, "Block should have block_id"
        assert "block_type" in block, "Block should have block_type"
        assert "retrieved_at" in block, "Block should have retrieved_at"
        assert block["source_system"] == source_system, (
            "Block should have correct source_system"
        )


def test_process_course_xml_blocks_block_types(sample_course_archive):
    """Test that various block types are correctly extracted."""
    archive_path, _ = sample_course_archive
    source_system = "prod"

    blocks, _ = process_course_xml_blocks(archive_path, source_system)

    # Extract block types
    block_types = {block["block_type"] for block in blocks}

    # Verify expected block types are present
    expected_types = {
        "chapter",
        "sequential",
        "vertical",
        "video",
        "problem",
        "html",
        "course",
    }
    assert expected_types.issubset(block_types), (
        f"Should extract all expected block types. Found: {block_types}"
    )


def test_process_course_xml_blocks_video_metadata(sample_course_archive):
    """Test that video-specific metadata is extracted correctly."""
    archive_path, _ = sample_course_archive
    source_system = "prod"

    blocks, _ = process_course_xml_blocks(archive_path, source_system)

    # Find video blocks
    video_blocks = [b for b in blocks if b["block_type"] == "video"]
    assert len(video_blocks) > 0, "Should have at least one video block"

    # Verify video-specific fields
    video_block = video_blocks[0]
    assert "edx_video_id" in video_block, "Video block should have edx_video_id"
    assert video_block["edx_video_id"] == "test-video-123", (
        "Should extract correct video ID"
    )
    assert "duration" in video_block, "Video block should have duration"
    assert video_block["duration"] == "300.5", "Should extract correct duration"


def test_process_course_xml_blocks_problem_metadata(sample_course_archive):
    """Test that problem-specific metadata is extracted correctly."""
    archive_path, _ = sample_course_archive
    source_system = "prod"

    blocks, _ = process_course_xml_blocks(archive_path, source_system)

    # Find problem blocks
    problem_blocks = [b for b in blocks if b["block_type"] == "problem"]
    assert len(problem_blocks) > 0, "Should have at least one problem block"

    # Verify problem-specific fields
    problem_block = problem_blocks[0]
    assert "max_attempts" in problem_block, "Problem block should have max_attempts"
    assert problem_block["max_attempts"] == "3", "Should extract correct max_attempts"
    assert "weight" in problem_block, "Problem block should have weight"
    assert problem_block["weight"] == "10", "Should extract correct weight"


def test_process_course_xml_blocks_xml_attributes(sample_course_archive):
    """Test that XML attributes are preserved correctly."""
    archive_path, _ = sample_course_archive
    source_system = "prod"

    blocks, _ = process_course_xml_blocks(archive_path, source_system)

    # Check that blocks have xml_attributes
    for block in blocks:
        assert "xml_attributes" in block, "Block should have xml_attributes"
        assert isinstance(block["xml_attributes"], dict), (
            "xml_attributes should be a dict"
        )

        # Blocks with display_name should have it in attributes
        if block.get("block_display_name"):
            assert "display_name" in block["xml_attributes"], (
                "display_name should be in xml_attributes"
            )


def test_process_course_xml_blocks_static_assets(sample_course_archive):
    """Test that non-XML static assets are collected correctly."""
    archive_path, _ = sample_course_archive
    source_system = "prod"

    blocks, static_assets_path = process_course_xml_blocks(archive_path, source_system)

    # Verify static assets tar was created and is valid
    assert static_assets_path.exists(), "Static assets tar should exist"

    # Open and check contents
    with tarfile.open(static_assets_path, "r:gz") as tar:
        members = tar.getnames()
        # Should contain non-XML files from static directory
        assert any("subtitle.srt" in m for m in members), "Should contain SRT file"
        assert any("content.html" in m for m in members), "Should contain HTML file"


def test_process_course_xml_blocks_missing_course_xml():
    """Test that missing course.xml raises appropriate error."""
    # Create an archive without course.xml
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "invalid_course.tar.gz"

    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()

    # Create a chapter without course.xml
    chapter_dir = course_root / "chapter"
    chapter_dir.mkdir()
    (chapter_dir / "chapter1.xml").write_text('<chapter display_name="Chapter 1"/>')

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    # Should raise ValueError
    with pytest.raises(ValueError, match="course.xml not found"):
        process_course_xml_blocks(archive_path, "test")

    temp_dir.cleanup()


def test_process_course_xml_blocks_empty_archive():
    """Test that empty archive raises appropriate error."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "empty_course.tar.gz"

    # Create empty tar
    with tarfile.open(archive_path, "w:gz") as tar:
        pass

    # Should raise ValueError about archive root
    with pytest.raises(ValueError, match="Unable to retrieve the archive root"):
        process_course_xml_blocks(archive_path, "test")

    temp_dir.cleanup()


def test_process_course_xml_blocks_malformed_xml(sample_course_archive):
    """Test that malformed XML files are skipped gracefully."""
    archive_path, temp_dir = sample_course_archive

    # Add a malformed XML file to the archive
    course_root = Path(temp_dir.name) / "course_root"
    video_dir = course_root / "video"
    malformed_xml = video_dir / "malformed.xml"
    malformed_xml.write_text("This is not valid XML <unclosed tag")

    # Recreate archive with malformed file
    new_archive = Path(temp_dir.name) / "course_with_malformed.tar.gz"
    with tarfile.open(new_archive, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    # Should complete without raising, skipping malformed file
    blocks, static_assets_path = process_course_xml_blocks(new_archive, "test")

    # Should still extract valid blocks
    assert len(blocks) > 0, "Should extract valid blocks despite malformed XML"


def test_process_course_xml_blocks_block_filtering():
    """Test that only known block types are processed (allowlist approach)."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "test_filtering.tar.gz"

    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()

    # Create course.xml
    (course_root / "course.xml").write_text(
        '<course url_name="test" org="TestX" course="TEST"/>'
    )

    # Create course metadata
    course_dir = course_root / "course"
    course_dir.mkdir()
    (course_dir / "test.xml").write_text('<course display_name="Test"/>')

    # Create a valid block type (chapter)
    chapter_dir = course_root / "chapter"
    chapter_dir.mkdir()
    (chapter_dir / "chapter1.xml").write_text('<chapter display_name="Chapter 1"/>')

    # Create an unknown block type (should be skipped)
    unknown_dir = course_root / "unknown_type"
    unknown_dir.mkdir()
    (unknown_dir / "unknown1.xml").write_text('<unknown display_name="Unknown"/>')

    # Create assets directory (should be skipped)
    assets_dir = course_root / "assets"
    assets_dir.mkdir()
    (assets_dir / "asset1.xml").write_text('<asset name="Asset 1"/>')

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    blocks, _ = process_course_xml_blocks(archive_path, "test")

    # Should only contain chapter and course blocks, not unknown or assets
    block_types = {block["block_type"] for block in blocks}
    assert "chapter" in block_types, "Should include chapter blocks"
    assert "course" in block_types, "Should include course blocks"
    assert "unknown_type" not in block_types, "Should not include unknown block types"
    assert "assets" not in block_types, "Should not include assets directory"

    temp_dir.cleanup()


def test_process_course_xml_blocks_source_system_tracking():
    """Test that source_system is correctly included in all blocks."""
    temp_dir = TemporaryDirectory()
    archive_path = Path(temp_dir.name) / "test_source.tar.gz"

    course_root = Path(temp_dir.name) / "course_root"
    course_root.mkdir()

    # Create minimal valid course
    (course_root / "course.xml").write_text(
        '<course url_name="test" org="TestX" course="TEST"/>'
    )
    course_dir = course_root / "course"
    course_dir.mkdir()
    (course_dir / "test.xml").write_text('<course display_name="Test"/>')

    chapter_dir = course_root / "chapter"
    chapter_dir.mkdir()
    (chapter_dir / "ch1.xml").write_text('<chapter display_name="Chapter 1"/>')

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(course_root, arcname="course_root")

    # Test with different source systems
    for source_sys in ["prod", "edge", "mitxonline", "xpro"]:
        blocks, _ = process_course_xml_blocks(archive_path, source_sys)

        # All blocks should have the correct source_system
        for block in blocks:
            assert block["source_system"] == source_sys, (
                f"Block should have source_system={source_sys}"
            )

    temp_dir.cleanup()
