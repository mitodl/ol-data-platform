"""Tests for the port of MIT Learn's ContentFile exclusion rules.

The cases follow mit-learn's tests for excluded_olx_paths: each is a rule Learn
applies, run over a small OLX export packed the way Studio packs one.
"""

import io
import json
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest
from openedx.assets.content_file_exclusions import build_file_rows, unpack_olx_tree

COURSE_FILES = {
    "course.xml": '<course url_name="2T2026" org="MITx" course="7.05x"/>',
    "course/2T2026.xml": (
        '<course display_name="Biochem">'
        '<chapter url_name="week1"/><chapter url_name="answers"/>'
        "</course>"
    ),
    "chapter/week1.xml": (
        '<chapter display_name="Week 1"><sequential url_name="seq1"/></chapter>'
    ),
    "chapter/answers.xml": (
        '<chapter display_name="Answers" visible_to_staff_only="true">'
        '<sequential url_name="key"/></chapter>'
    ),
    "sequential/seq1.xml": (
        '<sequential><vertical url_name="unit1"/>'
        '<vertical url_name="shared"/></sequential>'
    ),
    # Also reachable from the visible sequential, so it stays.
    "sequential/key.xml": (
        '<sequential><vertical url_name="key_unit"/>'
        '<vertical url_name="shared"/></sequential>'
    ),
    "vertical/unit1.xml": (
        '<vertical><html url_name="intro"/>'
        '<video url_name="lecture" youtube="1.00:abc123"/></vertical>'
    ),
    "vertical/key_unit.xml": (
        '<vertical><html url_name="solutions"/>'
        '<video url_name="hidden_lecture">'
        '<transcript language="en" src="shared-en.srt"/></video>'
        "</vertical>"
    ),
    "vertical/shared.xml": "<vertical/>",
    "html/intro.xml": '<html filename="intro" display_name="Intro"/>',
    "html/intro.html": '<p>See <a href="/static/handout.pdf">the handout</a>.</p>',
    "html/solutions.xml": '<html filename="solutions_body"/>',
    "html/solutions_body.html": '<a href="/static/answer_key.pdf">key</a>',
    "video/lecture.xml": (
        '<video youtube="1.00:abc123">'
        '<transcript language="en" src="shared-en.srt"/></video>'
    ),
    "video/hidden_lecture.xml": "<video/>",
    "policies/assets.json": json.dumps({"unused.pdf": {}, "handout.pdf": {}}),
    "info/updates.items.json": json.dumps(
        [
            {"content": '<a href="/static/announced.pdf">x</a>', "status": "visible"},
            {"content": '<a href="/static/withdrawn.pdf">x</a>', "status": "deleted"},
        ]
    ),
    "static/handout.pdf": "%PDF",
    "static/answer_key.pdf": "%PDF",
    "static/unused.pdf": "%PDF",
    "static/announced.pdf": "%PDF",
    "static/withdrawn.pdf": "%PDF",
    "static/shared-en.srt": "1\n00:00 --> 00:01\nHi",
    "static/subs_abc123.srt.sjson": "{}",
    "static/subs_gone999.srt.sjson": "{}",
    # Named only as a longer file's suffix, so not referenced.
    "static/out.pdf": "%PDF",
}


def pack(files: dict[str, str], tmp_path: Path, root: str = "course") -> Path:
    archive = tmp_path / "course.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, text in files.items():
            data = text.encode()
            info = tarfile.TarInfo(f"{root}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return archive


@pytest.fixture
def rows(tmp_path: Path) -> dict[str, dict[str, Any]]:
    tree = tmp_path / "tree"
    tree.mkdir()
    olx_root = unpack_olx_tree(pack(COURSE_FILES, tmp_path), tree)
    return {
        row["file_path"]: row
        for row in build_file_rows(
            olx_root,
            course_id="course-v1:MITx+7.05x+2T2026",
            source_system="mitxonline",
        )
    }


def reasons(rows: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {
        path: row["exclusion_reason"] for path, row in rows.items() if row["excluded"]
    }


def test_every_file_gets_a_row(rows):
    assert set(rows) == set(COURSE_FILES)
    assert {row["course_id"] for row in rows.values()} == {
        "course-v1:MITx+7.05x+2T2026"
    }


def test_exclusions_match_learns_rules(rows):
    assert reasons(rows) == {
        # The staff-only chapter's own subtree, html body included.
        "chapter/answers.xml": "staff_only",
        "sequential/key.xml": "staff_only",
        "vertical/key_unit.xml": "staff_only",
        "html/solutions.xml": "staff_only",
        "html/solutions_body.html": "staff_only",
        "video/hidden_lecture.xml": "staff_only",
        "policies/assets.json": "non_content",
        "info/updates.items.json": "non_content",
        # Referenced only from staff-only text, which does not count.
        "static/answer_key.pdf": "unreferenced_static",
        "static/unused.pdf": "unreferenced_static",
        # Only a deleted announcement links it.
        "static/withdrawn.pdf": "unreferenced_static",
        "static/subs_gone999.srt.sjson": "unreferenced_static",
        "static/out.pdf": "unreferenced_static",
    }


def test_kept_files_include_what_visible_content_uses(rows):
    for path in (
        # Hangs under the staff-only sequential too, but a learner reaches it.
        "vertical/shared.xml",
        "static/handout.pdf",
        "static/announced.pdf",
        # A hidden video's transcript that the visible copy also uses.
        "static/shared-en.srt",
        # Legacy transcript linked by its video id.
        "static/subs_abc123.srt.sjson",
        "course.xml",
    ):
        assert not rows[path]["excluded"], path
        assert rows[path]["exclusion_reason"] is None


def test_static_files_are_unpacked_empty(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    olx_root = unpack_olx_tree(pack(COURSE_FILES, tmp_path, root="export"), tree)
    assert olx_root.name == "export"
    assert (olx_root / "static/handout.pdf").stat().st_size == 0
    assert (olx_root / "html/intro.html").read_text() == COURSE_FILES["html/intro.html"]


def test_an_archive_without_course_xml_excludes_only_staff_only(tmp_path):
    files = {"static/unused.pdf": "%PDF", "policies/assets.json": "{}"}
    tree = tmp_path / "tree"
    tree.mkdir()
    olx_root = unpack_olx_tree(pack(files, tmp_path), tree)
    assert not any(
        row["excluded"]
        for row in build_file_rows(olx_root, course_id="c", source_system="xpro")
    )


def test_malformed_block_xml_fails_rather_than_guessing(tmp_path):
    files = {**COURSE_FILES, "chapter/week1.xml": "<chapter"}
    tree = tmp_path / "tree"
    tree.mkdir()
    olx_root = unpack_olx_tree(pack(files, tmp_path), tree)
    with pytest.raises(ET.ParseError):
        build_file_rows(olx_root, course_id="c", source_system="xpro")
