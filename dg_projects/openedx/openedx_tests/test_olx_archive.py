"""Learn's own tests for the OlxArchive the exclusion asset ports.

Copied from mit-learn learning_resources/etl/olx_test.py at 5da30ef30
(2026-11-09, mit-learn#4264; unchanged through 3e0ad0e67), less the tests of
what the port leaves out (video metadata, and loading units into Learn's
database), so a change to Learn's rules shows up as a failing case here. The
differences: ETLSource names are spelled out, no test needs the database, units
are built for a run id rather than a run and carry url_name rather than a jump
URL, and excluded is a dict of path to rule rather than a set.
"""

# Learn's line length and datetime import, so the cases diff against Learn's.
# ruff: noqa: E501, ICN001

import datetime
import json
import pathlib

import pytest
from defusedxml import ElementTree
from openedx.assets.content_file_exclusions import (
    OlxArchive,
    _is_context_sequential,
    get_edx_module_id,
)

# Learn's tests build a run; the units take its run id
RUN_ID = "course-v1:MITx+1+run"


def _write_olx(root, rel, text):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _write_tree(root, files):
    """Write each file of a {relative path: text} dict under root"""
    for rel, text in files.items():
        _write_olx(root, rel, text)


def _excluded_keys(archive, run_id):
    """
    Learn's OlxArchive.excluded_keys, which the port leaves out: keys of
    excluded files, minus any key an ingested path shares
    """
    return {
        archive.key(path, run_id)
        for path in archive.text_files
        if path in archive.excluded
    } - {archive.key(path, run_id) for path in archive.paths}


def _ingested(olx, etl_source=None, **kwargs):
    """Return the archive's ingestable paths, relative to its root and sorted"""
    return sorted(
        str(path.relative_to(olx))
        for path in OlxArchive(olx, etl_source, **kwargs).paths
    )


def _units(olx, etl_source="mitxonline"):
    """Return the units of the course at olx, built for RUN_ID"""
    return OlxArchive(olx, etl_source).units(RUN_ID)


def _write_html(root, name, body):
    """Write an html block: html/<name>.xml pointing at html/<name>.html"""
    _write_olx(root, f"html/{name}.xml", f'<html filename="{name}"/>')
    _write_olx(root, f"html/{name}.html", body)


def test_olx_archive_skips_staff_only_subtrees(tmp_path):
    """Files under visible_to_staff_only subtrees (and their transcripts) are skipped"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        olx,
        "course/run.xml",
        '<course><chapter url_name="ch_ok"/><chapter url_name="ch_staff"/></course>',
    )
    _write_olx(
        olx, "chapter/ch_ok.xml", '<chapter><sequential url_name="seq_ok"/></chapter>'
    )
    _write_olx(
        olx,
        "sequential/seq_ok.xml",
        '<sequential><vertical url_name="v_ok"/><vertical url_name="v_staff"/></sequential>',
    )
    _write_olx(
        olx,
        "vertical/v_ok.xml",
        '<vertical><html url_name="h_ok"/><video url_name="vid_ok"/></vertical>',
    )
    _write_html(olx, "h_ok", "<p>visible</p>")
    _write_olx(
        olx,
        "video/vid_ok.xml",
        '<video url_name="vid_ok"><transcript language="en" src="visible.srt"/></video>',
    )
    _write_olx(
        olx,
        "vertical/v_staff.xml",
        '<vertical visible_to_staff_only="true"><problem url_name="p_staff"/></vertical>',
    )
    _write_olx(olx, "problem/p_staff.xml", "<problem/>")
    _write_olx(
        olx,
        "chapter/ch_staff.xml",
        '<chapter visible_to_staff_only="true"><sequential url_name="seq_staff"/></chapter>',
    )
    _write_olx(
        olx,
        "sequential/seq_staff.xml",
        '<sequential><vertical url_name="v_hidden"/></sequential>',
    )
    _write_olx(
        olx,
        "vertical/v_hidden.xml",
        '<vertical><html url_name="h_hidden"/><video url_name="vid_hidden"/></vertical>',
    )
    _write_olx(olx, "html/h_hidden.xml", '<html filename="h_hidden_file"/>')
    _write_olx(olx, "html/h_hidden_file.html", "<p>hidden</p>")
    _write_olx(
        olx,
        "video/vid_hidden.xml",
        '<video url_name="vid_hidden"><transcript language="en" src="hidden.srt"/></video>',
    )
    _write_olx(olx, "static/hidden.srt", "1\n00:00:00,000 --> 00:00:01,000\nhidden\n")
    _write_olx(olx, "static/visible.srt", "1\n00:00:00,000 --> 00:00:01,000\nvisible\n")
    _write_olx(olx, "tabs/syllabus.html", "<p>tab</p>")

    assert _ingested(olx) == [
        "chapter/ch_ok.xml",
        "course.xml",
        "course/run.xml",
        "html/h_ok.html",
        "html/h_ok.xml",
        "sequential/seq_ok.xml",
        "static/visible.srt",
        "tabs/syllabus.html",
        "vertical/v_ok.xml",
        "video/vid_ok.xml",
    ]


def test_olx_archive_keeps_transcripts_a_visible_video_shares(tmp_path):
    """A hidden copy of a video must not take the live copy's transcripts with it"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        olx,
        "course/run.xml",
        '<course><vertical url_name="v_ok"/>'
        '<vertical url_name="v_staff" visible_to_staff_only="true"/></course>',
    )
    _write_olx(
        olx, "vertical/v_ok.xml", '<vertical><video url_name="vid_ok"/></vertical>'
    )
    _write_olx(
        olx, "vertical/v_staff.xml", '<vertical><video url_name="vid_old"/></vertical>'
    )
    shared = '<transcript language="en" src="shared.srt"/>'
    _write_olx(olx, "video/vid_ok.xml", f'<video url_name="vid_ok">{shared}</video>')
    _write_olx(
        olx,
        "video/vid_old.xml",
        f'<video url_name="vid_old">{shared}'
        '<transcript language="es" src="hidden_only.srt"/></video>',
    )
    _write_olx(olx, "static/shared.srt", "1\n00:00:00,000 --> 00:00:01,000\nshared\n")
    _write_olx(olx, "static/hidden_only.srt", "1\n00:00:00,000 --> 00:00:01,000\nold\n")

    excluded = OlxArchive(olx).excluded
    assert olx / "static/shared.srt" not in excluded
    assert olx / "static/hidden_only.srt" in excluded
    assert olx / "video/vid_old.xml" in excluded


def test_olx_archive_keeps_a_block_a_visible_parent_also_holds(tmp_path):
    """A block under two parents is hidden only when every parent hides it"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        olx,
        "course/run.xml",
        '<course><vertical url_name="v_ok"/>'
        '<vertical url_name="v_staff" visible_to_staff_only="true"/></course>',
    )
    shared = '<video url_name="vid_shared"/>'
    _write_olx(olx, "vertical/v_ok.xml", f"<vertical>{shared}</vertical>")
    _write_olx(
        olx,
        "vertical/v_staff.xml",
        f'<vertical>{shared}<video url_name="vid_hidden"/></vertical>',
    )
    _write_olx(
        olx,
        "video/vid_shared.xml",
        '<video url_name="vid_shared"><transcript language="en" src="shared.srt"/></video>',
    )
    _write_olx(
        olx,
        "video/vid_hidden.xml",
        '<video url_name="vid_hidden"><transcript language="en" src="hidden.srt"/></video>',
    )
    _write_olx(olx, "static/shared.srt", "1\n00:00:00,000 --> 00:00:01,000\nshared\n")
    _write_olx(olx, "static/hidden.srt", "1\n00:00:00,000 --> 00:00:01,000\nhidden\n")

    excluded = OlxArchive(olx).excluded
    assert olx / "video/vid_shared.xml" not in excluded
    assert olx / "static/shared.srt" not in excluded
    assert olx / "video/vid_hidden.xml" in excluded
    assert olx / "static/hidden.srt" in excluded


def test_olx_archive_skips_staff_only_inline_structure(tmp_path):
    """Structure held inline in course.xml (no pointer files) is still filtered"""
    olx = tmp_path / "course"
    _write_olx(
        olx,
        "course.xml",
        '<course org="MITx" course="1">'
        '<chapter display_name="ok"><vertical><html url_name="h_ok"/></vertical></chapter>'
        '<chapter visible_to_staff_only="true"><vertical>'
        '<html url_name="h_staff"/><video url_name="vid_staff"/>'
        "</vertical></chapter>"
        "</course>",
    )
    _write_html(olx, "h_ok", "<p>visible</p>")
    _write_html(olx, "h_staff", "<p>hidden</p>")
    _write_olx(
        olx,
        "video/vid_staff.xml",
        '<video url_name="vid_staff"><transcript language="en" src="hidden.srt"/></video>',
    )
    _write_olx(olx, "static/hidden.srt", "1\n00:00:00,000 --> 00:00:01,000\nhidden\n")

    assert _ingested(olx) == ["course.xml", "html/h_ok.html", "html/h_ok.xml"]


@pytest.mark.parametrize("bad_file", ["course.xml", "chapter/a.xml"])
def test_olx_archive_malformed_block_fails_closed(tmp_path, bad_file):
    """A malformed block raises on first use, not when the archive is built"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run"/>')
    _write_olx(olx, "course/run.xml", '<course><chapter url_name="a"/></course>')
    _write_olx(olx, "chapter/a.xml", '<chapter visible_to_staff_only="true"/>')
    _write_olx(olx, bad_file, "<broken")
    archive = OlxArchive(olx)
    with pytest.raises(ElementTree.ParseError):
        archive.paths  # noqa: B018


def test_olx_archive_without_course_xml_is_not_olx(tmp_path):
    """Non-OLX trees (canvas, tutor problem sets) exclude nothing"""
    olx = tmp_path / "course"
    _write_olx(olx, "chapter/a.xml", '<chapter visible_to_staff_only="true"/>')
    _write_olx(olx, "web_resources/b.html", "<p>b</p>")
    archive = OlxArchive(olx)
    assert archive.is_olx is False
    assert not archive.excluded
    assert archive.release_dates == {}
    assert _ingested(olx) == ["chapter/a.xml", "web_resources/b.html"]


def test_olx_archive_root_may_be_a_file(tmp_path):
    """A path that is not even a directory has nothing to ingest"""
    not_a_dir = tmp_path / "empty.pdf"
    not_a_dir.write_bytes(b"")
    archive = OlxArchive(not_a_dir)
    assert archive.is_olx is False
    assert archive.paths == []


def test_olx_archive_paths_honour_valid_file_types(tmp_path):
    """The caller's suffix list decides what is ingestable"""
    olx = tmp_path / "course"
    _write_olx(olx, "problems/set1.py", "print(1)")
    _write_olx(olx, "problems/set1.html", "<p>1</p>")
    assert _ingested(olx) == ["problems/set1.html"]
    assert _ingested(olx, valid_file_types=[".py"]) == ["problems/set1.py"]


# relative to the test run, so "future" never goes stale; tests compare order
FUTURE_YEAR = datetime.datetime.now(tz=datetime.UTC).year + 10
START_FUTURE = datetime.datetime(FUTURE_YEAR, 1, 1, tzinfo=datetime.UTC)
START_LATER = datetime.datetime(FUTURE_YEAR + 1, 1, 1, tzinfo=datetime.UTC)
START_PAST = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)


def _start(date):
    """Return an OLX start attribute for date"""
    return f'start="{date.isoformat()}"'


def _assert_release_dates(olx, expected, etl_source=None):
    """release_dates gives each listed path its expected date (None: absent)"""
    dates = OlxArchive(olx, etl_source).release_dates
    assert {rel: dates.get(olx / rel) for rel in expected} == expected


def _dated_olx(tmp_path):
    """
    Build an OLX course with chapters ch_dated (START_FUTURE), ch_undated and
    ch_both_parent (undated unless a test dates it)
    """
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        olx,
        "course/run.xml",
        "<course>"
        f'<chapter url_name="ch_dated" {_start(START_FUTURE)}/>'
        '<chapter url_name="ch_undated"/>'
        '<chapter url_name="ch_both_parent"/>'
        "</course>",
    )
    return olx


def test_release_dates_inherits_latest_ancestor(tmp_path):
    """A block's release is the max start on its path from the course down"""
    olx = _dated_olx(tmp_path)
    # earlier than the chapter's own start, so the chapter's wins
    earlier = START_FUTURE.replace(year=FUTURE_YEAR - 1, month=6)
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        "<chapter>"
        f'<vertical url_name="v_dated_child" {_start(earlier)}/>'
        f'<sequential url_name="seq_further_dated" {_start(START_LATER)}/>'
        "</chapter>",
    )
    _write_olx(
        olx,
        "vertical/v_dated_child.xml",
        '<vertical><html url_name="h_in_vertical"/></vertical>',
    )
    _write_html(olx, "h_in_vertical", "<p>vertical</p>")
    _write_olx(
        olx,
        "sequential/seq_further_dated.xml",
        '<sequential><html url_name="h_in_sequential"/></sequential>',
    )
    _write_html(olx, "h_in_sequential", "<p>sequential</p>")

    _assert_release_dates(
        olx,
        {
            "vertical/v_dated_child.xml": START_FUTURE,
            "html/h_in_vertical.xml": START_FUTURE,
            "html/h_in_vertical.html": START_FUTURE,
            "sequential/seq_further_dated.xml": START_LATER,
            "html/h_in_sequential.xml": START_LATER,
            "html/h_in_sequential.html": START_LATER,
        },
    )


def test_release_dates_undated_blocks_absent(tmp_path):
    """A block with only undated paths is absent; the root <course>'s start is ignored"""
    olx = _dated_olx(tmp_path)
    # a start on the course.xml pointer is ignored too
    _write_olx(
        olx,
        "course.xml",
        f'<course url_name="run" org="MITx" course="1" {_start(START_FUTURE)}/>',
    )
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        '<chapter><html url_name="h_under_undated"/></chapter>',
    )
    _write_html(olx, "h_under_undated", "<p>undated</p>")

    assert OlxArchive(olx).release_dates == {}


def test_release_dates_dotted_name_is_a_different_block(tmp_path):
    """An undated `notes` block does not claim, and undate, `notes.extra`'s files"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx, "chapter/ch_undated.xml", '<chapter><html url_name="notes"/></chapter>'
    )
    _write_olx(
        olx, "chapter/ch_dated.xml", '<chapter><html url_name="notes.extra"/></chapter>'
    )
    _write_html(olx, "notes", "<p>undated</p>")
    _write_html(olx, "notes.extra", "<p>dated</p>")

    _assert_release_dates(
        olx,
        {
            "html/notes.xml": None,
            "html/notes.html": None,
            "html/notes.extra.xml": START_FUTURE,
            "html/notes.extra.html": START_FUTURE,
        },
    )


def test_release_dates_two_parents_takes_earliest(tmp_path):
    """An undated parent leaves a block undated; two dated parents give the earliest"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        "<chapter>"
        '<vertical url_name="v_mixed_parents"/>'
        '<vertical url_name="v_two_dated"/>'
        "</chapter>",
    )
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        '<chapter><vertical url_name="v_mixed_parents"/></chapter>',
    )
    _write_olx(
        olx,
        "chapter/ch_both_parent.xml",
        f"<chapter {_start(START_FUTURE.replace(year=FUTURE_YEAR + 2))}>"
        '<vertical url_name="v_two_dated"/>'
        "</chapter>",
    )
    _write_olx(olx, "vertical/v_mixed_parents.xml", "<vertical/>")
    _write_olx(olx, "vertical/v_two_dated.xml", "<vertical/>")

    _assert_release_dates(
        olx,
        {
            "vertical/v_mixed_parents.xml": None,
            "vertical/v_two_dated.xml": START_FUTURE,
        },
    )


def test_release_dates_staff_only_path_does_not_vote(tmp_path):
    """A staff-only path to a block neither excludes it nor makes it undated"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        '<chapter><vertical url_name="v_staff_shared"/></chapter>',
    )
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        '<chapter visible_to_staff_only="true">'
        '<vertical url_name="v_staff_shared"/></chapter>',
    )
    _write_olx(
        olx,
        "vertical/v_staff_shared.xml",
        '<vertical><html url_name="h_staff_shared"/></vertical>',
    )
    _write_html(olx, "h_staff_shared", "<p>shared</p>")

    _assert_release_dates(
        olx,
        {
            "vertical/v_staff_shared.xml": START_FUTURE,
            "html/h_staff_shared.xml": START_FUTURE,
            "html/h_staff_shared.html": START_FUTURE,
        },
    )
    assert olx / "vertical/v_staff_shared.xml" not in OlxArchive(olx).excluded


def test_release_dates_self_paced_returns_empty(tmp_path):
    """A self-paced run never gets release dates, however its blocks are dated"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        '<chapter><html url_name="h_self_paced"/></chapter>',
    )
    _write_html(olx, "h_self_paced", "<p>content</p>")
    _write_olx(
        olx,
        "policies/run/policy.json",
        json.dumps({"course/run": {"self_paced": True}}),
    )

    assert OlxArchive(olx).release_dates == {}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2030-01-01T00:00:00Z", datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)),
        ("2030-01-01T00:00:00", datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)),
        (
            "2030-01-01T00:00:00-05:00",
            datetime.datetime(2030, 1, 1, 5, tzinfo=datetime.UTC),
        ),
    ],
)
def test_release_dates_parse_starts_as_utc(tmp_path, raw, expected):
    """OLX start attributes are parsed into UTC, converting any offset"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        f'<chapter start="{raw}"><html url_name="h_raw"/></chapter>',
    )
    _write_html(olx, "h_raw", "<p>content</p>")

    _assert_release_dates(olx, {"html/h_raw.xml": expected})


# the second parses, then overflows converting to UTC
@pytest.mark.parametrize("start", ["next tuesday", "0001-01-01T00:00:00+14:00"])
def test_release_dates_malformed_start_ignored(tmp_path, caplog, start):
    """A chapter with an unparsable start is treated as undated, with a warning"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        f'<chapter start="{start}"><html url_name="h_malformed"/></chapter>',
    )
    _write_html(olx, "h_malformed", "<p>content</p>")

    _assert_release_dates(
        olx, {"html/h_malformed.xml": None, "html/h_malformed.html": None}
    )
    assert any(
        record.levelname == "WARNING"
        and record.message == f"Ignoring unparsable OLX start attribute '{start}'"
        for record in caplog.records
    )


def test_release_dates_assets(tmp_path):
    """
    An asset takes the earliest date of the blocks naming it (by filename or legacy
    transcript id), or none if an undated block names it; a <transcript src> file
    takes its video's date
    """
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        '<chapter><html url_name="html_future"/><video url_name="video_future"/>'
        "</chapter>",
    )
    _write_html(
        olx, "html_future", '<p><a href="a.pdf">a</a> <a href="b.pdf">b</a></p>'
    )
    _write_olx(
        olx,
        "video/video_future.xml",
        '<video url_name="video_future">'
        '<transcript language="en" src="transcript_future.srt"/></video>',
    )
    _write_olx(
        olx, "static/transcript_future.srt", "1\n00:00:00,000 --> 00:00:01,000\nhi\n"
    )

    _write_olx(
        olx,
        "chapter/ch_both_parent.xml",
        f"<chapter {_start(START_LATER)}>"
        '<html url_name="html_later"/>'
        '<video url_name="video_later"/>'
        '<video url_name="video_later_shared"/>'
        "</chapter>",
    )
    _write_html(olx, "html_later", '<p><a href="a.pdf">a</a></p>')
    _write_olx(
        olx, "video/video_later.xml", '<video url_name="video_later" sub="ABC123"/>'
    )
    _write_olx(
        olx,
        "video/video_later_shared.xml",
        '<video url_name="video_later_shared" sub="XYZ789"/>',
    )

    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        "<chapter>"
        '<html url_name="html_undated"/>'
        '<video url_name="video_undated_shared"/>'
        "</chapter>",
    )
    _write_html(olx, "html_undated", '<p><a href="b.pdf">b</a></p>')
    _write_olx(
        olx,
        "video/video_undated_shared.xml",
        '<video url_name="video_undated_shared" sub="XYZ789"/>',
    )

    _write_olx(olx, "static/a.pdf", "pdf-a")
    _write_olx(olx, "static/b.pdf", "pdf-b")
    _write_olx(olx, "static/orphan.pdf", "pdf-orphan")
    _write_olx(olx, "static/subs_ABC123.srt.sjson", "{}")
    _write_olx(olx, "static/subs_XYZ789.srt.sjson", "{}")

    _assert_release_dates(
        olx,
        {
            "static/a.pdf": START_FUTURE,
            "static/b.pdf": None,
            "static/transcript_future.srt": START_FUTURE,
            "static/subs_ABC123.srt.sjson": START_LATER,
            "static/subs_XYZ789.srt.sjson": None,
            "static/orphan.pdf": None,
        },
    )


@pytest.mark.parametrize("release", [None, START_FUTURE, START_LATER])
@pytest.mark.parametrize("link", ["none", "staff_only", "visible"])
def test_release_dates_through_static_pages(tmp_path, release, link):
    """Only reachable pages propagate dates, including chains and cycles."""
    olx = _dated_olx(tmp_path)
    reachable = link == "visible"
    chapter = "ch_undated" if release is None else "ch_both_parent"
    start = "" if release is None else _start(release)
    staff = 'visible_to_staff_only="true"' if link == "staff_only" else ""
    _write_olx(
        olx,
        f"chapter/{chapter}.xml",
        f'<chapter {start} {staff}><html url_name="references"/></chapter>',
    )
    _write_html(
        olx,
        "references",
        "" if link == "none" else '<a href="/static/resources.html">resources</a>',
    )
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        f'<chapter {_start(START_LATER)}><html url_name="direct"/></chapter>',
    )
    _write_html(olx, "direct", '<a href="/static/handout.pdf">handout</a>')
    _write_olx(olx, "static/resources.html", '<a href="second.html">next</a>')
    _write_olx(
        olx,
        "static/second.html",
        '<a href="resources.html">back</a><a href="handout.pdf">handout</a>',
    )
    _write_olx(olx, "static/handout.pdf", "pdf")

    expected = release if reachable else START_LATER
    _assert_release_dates(
        olx,
        {
            "static/handout.pdf": expected,
            "static/resources.html": release if reachable else None,
            "static/second.html": release if reachable else None,
        },
    )


@pytest.mark.parametrize(
    ("linker", "release"),
    [("undated", None), ("earlier", START_FUTURE), ("static_page", None)],
)
def test_release_dates_longer_name_does_not_date_asset(tmp_path, linker, release):
    """A link to notes.pdf.txt, however reached, leaves notes.pdf its own date"""
    olx = _dated_olx(tmp_path)
    link = '<a href="/static/notes.pdf.txt?v=1">notes</a>'
    start = _start(release) if release else ""
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        f'<chapter {start}><html url_name="linker"/></chapter>',
    )
    if linker == "static_page":
        _write_html(olx, "linker", '<a href="/static/resources.html">more</a>')
        _write_olx(olx, "static/resources.html", link)
    else:
        _write_html(olx, "linker", link)
    _write_olx(
        olx,
        "chapter/ch_dated.xml",
        f'<chapter {_start(START_LATER)}><html url_name="dated"/></chapter>',
    )
    _write_html(olx, "dated", "<p>Read notes.pdf.</p>")
    _write_olx(olx, "static/notes.pdf", "pdf")
    _write_olx(olx, "static/notes.pdf.txt", "txt")

    _assert_release_dates(
        olx, {"static/notes.pdf": START_LATER, "static/notes.pdf.txt": release}
    )


def test_release_dates_assets_separator_is_part_of_the_name(tmp_path):
    """An undated link to lecture1.pdf does not release lecture_1.pdf"""
    olx = _dated_olx(tmp_path)
    _write_olx(olx, "chapter/ch_dated.xml", '<chapter><html url_name="h_d"/></chapter>')
    _write_html(olx, "h_d", '<a href="/static/lecture_1.pdf">d</a>')
    _write_olx(
        olx, "chapter/ch_undated.xml", '<chapter><html url_name="h_u"/></chapter>'
    )
    _write_html(olx, "h_u", '<a href="/static/lecture1.pdf">u</a>')
    _write_olx(olx, "static/lecture1.pdf", "released")
    _write_olx(olx, "static/lecture_1.pdf", "not yet")

    _assert_release_dates(
        olx, {"static/lecture_1.pdf": START_FUTURE, "static/lecture1.pdf": None}
    )


def test_release_dates_assets_dated_inline_block_in_undated_parent(tmp_path):
    """A dated inline block dates what it names; its undated parent's text doesn't"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        "<chapter>"
        f'<video url_name="vid_inline" sub="ABC123" {_start(START_FUTURE)}>'
        '<transcript language="en" src="inline.srt"/></video>'
        f'<html url_name="html_inline" {_start(START_FUTURE)}>'
        '<p><a href="/static/inline.pdf">i</a></p></html>'
        '<html url_name="html_undated"/>'
        "</chapter>",
    )
    _write_html(olx, "html_undated", '<p><a href="/static/shared.pdf">s</a></p>')
    _write_olx(olx, "static/inline.srt", "1\n00:00:00,000 --> 00:00:01,000\nhi\n")
    _write_olx(olx, "static/subs_ABC123.srt.sjson", "{}")
    _write_olx(olx, "static/inline.pdf", "pdf")
    _write_olx(olx, "static/shared.pdf", "pdf")

    _assert_release_dates(
        olx,
        {
            "static/inline.srt": START_FUTURE,
            "static/subs_ABC123.srt.sjson": START_FUTURE,
            "static/inline.pdf": START_FUTURE,
            "static/shared.pdf": None,
        },
    )


def test_release_dates_assets_unnamed_inline_block_stays_its_parents(tmp_path):
    """An unnamed dated inline block's links count as its undated parent's"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_undated.xml",
        f"<chapter><html {_start(START_FUTURE)}>"
        '<p><a href="/static/notes.pdf">notes</a></p></html></chapter>',
    )
    _write_olx(olx, "static/notes.pdf", "pdf")

    assert "static/notes.pdf" in _ingested(olx)
    _assert_release_dates(olx, {"static/notes.pdf": None})


@pytest.mark.parametrize(
    ("undated_twin", "expected"), [(False, START_FUTURE), (True, None)]
)
def test_release_dates_assets_unparsable_xml_dates_legacy_transcript(
    tmp_path, undated_twin, expected
):
    """
    A malformed XML file elsewhere makes every legacy transcript count as
    referenced, but only confirmed ids date one: a dated video's transcript
    keeps its date unless an undated video also declares it
    """
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx, "chapter/ch_dated.xml", '<chapter><video url_name="vid_dated"/></chapter>'
    )
    _write_olx(olx, "video/vid_dated.xml", '<video url_name="vid_dated" sub="ABC123"/>')
    if undated_twin:
        _write_olx(
            olx,
            "chapter/ch_undated.xml",
            '<chapter><video url_name="vid_open"/></chapter>',
        )
        _write_olx(
            olx, "video/vid_open.xml", '<video url_name="vid_open" sub="ABC123"/>'
        )
    _write_olx(olx, "tabs/broken.xml", "<tab")
    _write_olx(olx, "static/subs_ABC123.srt.sjson", "{}")

    assert "static/subs_ABC123.srt.sjson" in _ingested(olx)
    _assert_release_dates(olx, {"static/subs_ABC123.srt.sjson": expected})


def _tab_policy(olx, tabs):
    """Write the run's policy.json with the given tab list"""
    _write_olx(
        olx, "policies/run/policy.json", json.dumps({"course/run": {"tabs": tabs}})
    )


def _static_tab(slug, **flags):
    return {"type": "static_tab", "name": slug, "url_slug": slug, **flags}


@pytest.mark.parametrize(
    ("page", "tabs", "etl_source", "dated"),
    [
        # learner-visible non-block pages keep the asset undated
        ("info/handouts.html", None, None, False),
        ("tabs/notes.html", [_static_tab("notes")], None, False),
        ("tabs/notes.html", [_static_tab("notes", is_hidden=True)], None, True),
        ("tabs/notes.html", [_static_tab("notes", course_staff_only=True)], None, True),
        ("tabs/notes.html", [], None, True),
        # only Open Learning Library shows the about page
        ("about/overview.html", None, "oll", False),
        ("about/overview.html", None, "mitxonline", True),
    ],
)
def test_release_dates_assets_hidden_page_does_not_release(
    tmp_path, page, tabs, etl_source, dated
):
    """A page learners cannot reach does not undate an asset a dated block owns"""
    olx = _dated_olx(tmp_path)
    _write_olx(olx, "chapter/ch_dated.xml", '<chapter><html url_name="h_d"/></chapter>')
    _write_html(olx, "h_d", '<p><a href="/static/notes.pdf">n</a></p>')
    _write_olx(olx, page, '<p><a href="/static/notes.pdf">n</a></p>')
    if tabs is not None:
        _tab_policy(olx, tabs)
    _write_olx(olx, "static/notes.pdf", "pdf")

    dates = OlxArchive(olx, etl_source).release_dates
    assert (dates.get(olx / "static/notes.pdf") == START_FUTURE) is dated


def test_release_dates_assets_reconcile_owned_dates(tmp_path):
    """A transcript an undated file links is undated, else takes the earliest date"""
    olx = _dated_olx(tmp_path)
    _write_olx(
        olx,
        "chapter/ch_both_parent.xml",
        f"<chapter {_start(START_LATER)}>"
        '<video url_name="vid_handout"/><video url_name="vid_early"/></chapter>',
    )
    for name in ("handout", "early"):
        _write_olx(
            olx,
            f"video/vid_{name}.xml",
            f'<video url_name="vid_{name}">'
            f'<transcript language="en" src="{name}_t.srt"/></video>',
        )
        _write_olx(olx, f"static/{name}_t.srt", "1\n00:00:00,000 --> 00:00:01,000\n")
    _write_olx(olx, "info/handouts.html", '<a href="handout_t.srt">t</a>')
    _write_olx(
        olx, "chapter/ch_dated.xml", '<chapter><html url_name="h_early"/></chapter>'
    )
    _write_html(olx, "h_early", '<a href="early_t.srt">t</a>')

    _assert_release_dates(
        olx,
        {
            "static/handout_t.srt": None,
            "static/early_t.srt": START_FUTURE,
            "video/vid_early.xml": START_LATER,
        },
    )


def _reference_olx(tmp_path, html="<p>no links</p>", **static_files):
    """Minimal OLX course with one visible html block and the given static files"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(olx, "course/run.xml", '<course><chapter url_name="ch"/></course>')
    _write_olx(olx, "chapter/ch.xml", '<chapter><vertical url_name="v"/></chapter>')
    _write_olx(olx, "vertical/v.xml", '<vertical><html url_name="h"/></vertical>')
    _write_html(olx, "h", html)
    for name, text in static_files.items():
        _write_olx(olx, f"static/{name}", text)
    return olx


def test_olx_archive_keeps_no_parsed_trees_or_text(tmp_path):
    """Ingestion holds the archive through every extraction, so it keeps only paths"""
    olx = _reference_olx(
        tmp_path,
        html='<a href="/static/notes.pdf">course body</a>',
        **{"notes.pdf": "x", "late.pdf": "x"},
    )
    # a dated inline block, so release dating holds inline elements too
    _write_olx(
        olx,
        "vertical/v.xml",
        f'<vertical><html url_name="h"/><html url_name="inl" {_start(START_FUTURE)}>'
        '<a href="/static/late.pdf">inline body</a></html></vertical>',
    )
    archive = OlxArchive(olx)
    assert archive.paths
    assert archive.release_dates == {olx / "static" / "late.pdf": START_FUTURE}

    cached = repr(vars(archive))
    assert "<Element" not in cached
    assert "course body" not in cached
    assert "inline body" not in cached


@pytest.mark.parametrize(
    ("reference", "kept"),
    [
        ('<a href="/static/notes.pdf">notes</a>', True),
        # courses also link assets by their asset key rather than /static/
        ('<a href="/asset-v1:MITx+1+run+type@asset+block/notes.pdf">n</a>', True),
        # percent-encoded and entity-escaped spellings are still references
        ('<a href="/static/notes%2Epdf">notes</a>', True),
        ('<a href="/static/notes.pdf?raw=1&amp;v=2">notes</a>', True),
        ("<p>nothing here</p>", False),
    ],
)
def test_olx_archive_static_reference_spellings(tmp_path, reference, kept):
    """A static file survives only if a block spells its name (hq#13350)"""
    olx = _reference_olx(tmp_path, reference, **{"notes.pdf": "pdf bytes"})
    assert ("static/notes.pdf" in _ingested(olx)) is kept


def test_olx_archive_cdata_reference_with_entity(tmp_path):
    """
    HTML held in an XML file's CDATA keeps its entities as written, so its
    reference matches like one in an .html file
    """
    olx = _reference_olx(tmp_path, **{"notes&guide.pdf": "pdf bytes"})
    _write_olx(
        olx,
        "html/h.xml",
        '<html><![CDATA[<a href="/static/notes&amp;guide.pdf">n</a>]]></html>',
    )
    assert "static/notes&guide.pdf" in _ingested(olx)


@pytest.mark.parametrize(
    "reference",
    [
        '<a href="/static/Course_Syllabus_V2.pdf">s</a>',
        '<a href="/static/Course Syllabus V2.pdf">s</a>',
        '<a href="/static/Course%20Syllabus%20V2.pdf">s</a>',
        # one name can mix the two spellings (15.671.1x spells its Turkish
        # transcripts "17 Principles_Turkish.srt")
        '<a href="/static/Course Syllabus_V2.pdf">s</a>',
        # an unlinked mention is a weak reference, but keeping the file costs
        # less than dropping something a learner can see
        "<p>The syllabus is Course Syllabus V2.pdf</p>",
    ],
)
def test_olx_archive_normalizes_spaces_and_underscores(tmp_path, reference):
    """A file stored with spaces matches a reference however it spells them"""
    olx = _reference_olx(tmp_path, reference, **{"Course Syllabus V2.pdf": "pdf"})
    assert "static/Course Syllabus V2.pdf" in _ingested(olx)


@pytest.mark.parametrize(
    ("reference", "kept"),
    [
        # a name that only appears inside a longer one is not referenced
        ('<a href="/static/final_exam.srt">exam</a>', False),
        # a space is where a name can start, so a reference written with one is
        # kept as a reference to the shorter name too
        ('<a href="/static/final exam.srt">exam</a>', True),
        # but anything a name can start after still counts
        ('<a href="/static/exam.srt">exam</a>', True),
        ("<p>the transcript is exam.srt</p>", True),
        ('<a href="/asset-v1:MITx+1+run+type@asset+block/exam.srt">e</a>', True),
    ],
)
def test_olx_archive_partial_names_are_not_references(tmp_path, reference, kept):
    """A link to final_exam.srt is not a link to exam.srt"""
    olx = _reference_olx(
        tmp_path, reference, **{"final_exam.srt": "1", "exam.srt": "2"}
    )
    assert ("static/exam.srt" in _ingested(olx)) is kept


@pytest.mark.parametrize(
    ("reference", "name", "kept"),
    [
        # a longer name is a different file
        ('<a href="/static/notes.pdf.txt">n</a>', "notes.pdf", False),
        # _ alone is no break at the end either, as with final_exam.srt
        ('<a href="/static/notes.pdf_old">n</a>', "notes.pdf", False),
        ('<a href="/static/notes.pdf?v=1#p2">n</a>', "notes.pdf", True),
        # a sentence's final period does not lengthen the name
        ("<p>Read notes.pdf.</p>", "notes.pdf", True),
        # a period inside a name still matches before a separator
        ('<a href="/static/Lec.%201.pdf">l</a>', "Lec. 1.pdf", True),
    ],
)
def test_olx_archive_names_end_at_a_break(tmp_path, reference, name, kept):
    """A reference names a file only if the name ends where the reference does"""
    olx = _reference_olx(tmp_path, reference, **{name: "1", f"{name}.txt": "2"})
    assert (f"static/{name}" in _ingested(olx)) is kept


@pytest.mark.parametrize(
    ("link", "kept", "dropped"),
    [
        ("lecture1.pdf", "lecture1.pdf", "lecture_1.pdf"),
        ("lecture_1.pdf", "lecture_1.pdf", "lecture1.pdf"),
    ],
)
def test_olx_archive_separator_is_part_of_the_name(tmp_path, link, kept, dropped):
    """lecture1.pdf and lecture_1.pdf are different files; a link keeps only its own"""
    olx = _reference_olx(
        tmp_path,
        f'<a href="/static/{link}">l</a>',
        **{"lecture1.pdf": "one", "lecture_1.pdf": "other"},
    )
    paths = _ingested(olx)
    assert f"static/{kept}" in paths
    assert f"static/{dropped}" not in paths


def test_olx_archive_name_at_start_of_source(tmp_path):
    """A name at the very start of a source matches, even one starting with _"""
    olx = _reference_olx(tmp_path, "_notes.pdf", **{"_notes.pdf": "pdf"})
    assert "static/_notes.pdf" in _ingested(olx)


def test_olx_archive_keeps_reuploaded_asset_keys(tmp_path):
    """Assets re-uploaded under a flattened asset key still match (15.671.1x)"""
    name = "asset-v1_MITx+15.671.1x+3T2020+type@asset+block@09_Conversation.pdf"
    olx = _reference_olx(tmp_path, **{name: "pdf"})
    _write_olx(
        olx,
        "html/h.html",
        '<a href="/asset-v1:MITxT+15.671.1x+2T2023+type@asset+block@'
        'asset-v1_MITx_15.671.1x_3T2020_type_asset_block_09_Conversation.pdf">c</a>',
    )
    assert f"static/{name}" in _ingested(olx)


def test_olx_archive_partial_name_does_not_unhide_staff_transcript(tmp_path):
    """A hidden video's transcript stays hidden when only its name's tail matches"""
    olx = _reference_olx(
        tmp_path,
        '<a href="/static/final_exam.srt">exam</a>',
        **{"exam.srt": "hidden", "final_exam.srt": "shown"},
    )
    _write_olx(
        olx,
        "chapter/ch.xml",
        '<chapter><vertical url_name="v"/>'
        '<vertical url_name="v_staff" visible_to_staff_only="true"/></chapter>',
    )
    _write_olx(
        olx, "vertical/v_staff.xml", '<vertical><video url_name="vid"/></vertical>'
    )
    _write_olx(
        olx,
        "video/vid.xml",
        '<video><transcript language="en" src="exam.srt"/></video>',
    )
    assert "static/exam.srt" not in _ingested(olx)


def test_olx_archive_ignores_asset_manifests(tmp_path):
    """Manifests list every asset, so they cannot count as references"""
    olx = _reference_olx(tmp_path, **{"stale.pdf": "stale"})
    _write_olx(olx, "policies/assets.json", '{"stale.pdf": {"filename": "stale.pdf"}}')
    _write_olx(olx, "assets/assets.xml", "<assets><asset>stale.pdf</asset></assets>")
    paths = _ingested(olx)
    assert "static/stale.pdf" not in paths
    assert "policies/assets.json" not in paths
    assert "assets/assets.xml" not in paths


def test_olx_archive_ignores_deleted_announcements(tmp_path):
    """Deleted announcements are archived by edX but no longer part of the course"""
    olx = _reference_olx(tmp_path, **{"old.pdf": "old", "new.pdf": "new"})
    _write_olx(
        olx,
        "info/updates.items.json",
        '[{"id": 1, "status": "deleted", "content": "see /static/old.pdf"},'
        ' {"id": 2, "status": "visible", "content": "see /static/new.pdf"}]',
    )
    paths = _ingested(olx)
    assert "static/old.pdf" not in paths
    assert "static/new.pdf" in paths
    # the file itself is a decade of announcements, not current course content
    assert "info/updates.items.json" not in paths


def test_olx_archive_skips_legacy_announcements(tmp_path):
    """updates.html is not ingested, but files its announcements link still are"""
    olx = _reference_olx(tmp_path, **{"notes.pdf": "notes"})
    _write_olx(
        olx,
        "info/updates.html",
        '<ol><li><h2>May 1</h2><a href="/static/notes.pdf">notes</a></li></ol>',
    )
    _write_olx(olx, "info/handouts.html", "<p>handouts</p>")
    paths = _ingested(olx)
    assert "info/updates.html" not in paths
    assert "static/notes.pdf" in paths
    # the Course Handouts box on the course home page
    assert "info/handouts.html" in paths


def test_olx_archive_drops_assets_only_staff_blocks_mention(tmp_path):
    """An answer key a hidden block links is unreferenced, not referenced"""
    olx = _reference_olx(tmp_path, **{"answers.pdf": "answers"})
    _write_olx(
        olx,
        "chapter/ch.xml",
        '<chapter><vertical url_name="v"/>'
        '<vertical url_name="v_staff" visible_to_staff_only="true"/></chapter>',
    )
    _write_olx(
        olx, "vertical/v_staff.xml", '<vertical><html url_name="h_staff"/></vertical>'
    )
    _write_html(olx, "h_staff", '<a href="/static/answers.pdf">key</a>')
    assert "static/answers.pdf" not in _ingested(olx)


def test_olx_archive_drafts_do_not_keep_assets(tmp_path):
    """Drafts are not ingested, so their links do not keep an asset alive"""
    olx = _reference_olx(tmp_path, **{"draft_only.pdf": "pdf"})
    _write_olx(olx, "drafts/html/d.html", '<a href="/static/draft_only.pdf">d</a>')
    assert "static/draft_only.pdf" not in _ingested(olx)


@pytest.mark.parametrize(
    "video_xml",
    [
        # every spelling of the attribute is valid XML and must be honoured
        '<video url_name="vid" sub="AbC123"/>',
        "<video url_name='vid' sub='AbC123'/>",
        '<video url_name="vid" sub = "AbC123"/>',
        '<video url_name="vid" youtube="1.00:AbC123"/>',
        '<video url_name="vid" youtube="0.75:Fast,1.00:AbC123,1.50:Slow"/>',
        '<video url_name="vid" youtube_id_1_0="AbC123"/>',
    ],
)
def test_olx_archive_keeps_legacy_transcripts(tmp_path, video_xml):
    """subs_<id>.srt.sjson is named for the video id, never for its own filename"""
    olx = _reference_olx(
        tmp_path,
        **{"subs_AbC123.srt.sjson": "{}", "es_subs_AbC123.srt.sjson": "{}"},
    )
    _write_olx(olx, "vertical/v.xml", '<vertical><video url_name="vid"/></vertical>')
    _write_olx(olx, "video/vid.xml", video_xml)
    paths = _ingested(olx)
    assert "static/subs_AbC123.srt.sjson" in paths
    assert "static/es_subs_AbC123.srt.sjson" in paths


def test_olx_archive_drops_orphaned_legacy_transcripts(tmp_path):
    """A legacy transcript whose video is gone belongs to an earlier offering"""
    olx = _reference_olx(
        tmp_path, **{"subs_Current.srt.sjson": "{}", "subs_Removed.srt.sjson": "{}"}
    )
    _write_olx(olx, "vertical/v.xml", '<vertical><video url_name="vid"/></vertical>')
    _write_olx(olx, "video/vid.xml", '<video url_name="vid" sub="Current"/>')
    paths = _ingested(olx)
    assert "static/subs_Current.srt.sjson" in paths
    assert "static/subs_Removed.srt.sjson" not in paths


def test_olx_archive_keeps_inline_video_transcripts(tmp_path):
    """A <video> inline in a parent declares its transcripts just as a file does"""
    olx = _reference_olx(tmp_path, **{"subs_Inline.srt.sjson": "{}"})
    _write_olx(
        olx,
        "vertical/v.xml",
        '<vertical><video url_name="vid" sub="Inline"/></vertical>',
    )
    assert "static/subs_Inline.srt.sjson" in _ingested(olx)


# non-UTF-8 bytes are malformed too, though the text is read leniently, and so is
# an entity declaration, which defusedxml refuses
@pytest.mark.parametrize(
    "broken",
    [b"<tab", b"<tab>\xff</tab>", b'<!DOCTYPE tab [<!ENTITY e "x">]><tab>&e;</tab>'],
)
def test_olx_archive_malformed_xml_keeps_legacy_transcripts(tmp_path, broken):
    """Unknown video ids must not be read as "no video uses this transcript\""""
    olx = _reference_olx(tmp_path, **{"subs_AbC123.srt.sjson": "{}"})
    (olx / "tabs").mkdir()
    (olx / "tabs/broken.xml").write_bytes(broken)
    assert "static/subs_AbC123.srt.sjson" in _ingested(olx)


@pytest.mark.parametrize(
    ("tab", "kept"),
    [
        (_static_tab("resources", course_staff_only=False), True),
        # Studio exports every tab page it stores, but the LMS 404s a slug the
        # tab list does not name
        (_static_tab("other", course_staff_only=False), False),
        # these still render by URL, but no navigation leads a learner there
        (_static_tab("resources", course_staff_only=True), False),
        (_static_tab("resources", is_hidden=True), False),
    ],
)
# edX also reads tab pages from a folder named for the run
@pytest.mark.parametrize("tab_dir", ["tabs", "tabs/run"])
def test_olx_archive_skips_tabs_learners_cannot_reach(tmp_path, tab, kept, tab_dir):
    """Only the tab pages the course's navigation leads to are ingested"""
    olx = _reference_olx(tmp_path)
    _write_olx(olx, f"{tab_dir}/resources.html", "<p>resources</p>")
    _tab_policy(olx, [{"type": "courseware", "course_staff_only": False}, tab])
    assert (f"{tab_dir}/resources.html" in _ingested(olx)) is kept


@pytest.mark.parametrize(
    "policy",
    [
        None,
        "{broken",
        json.dumps({"course/run": {}}),
        json.dumps({"course/run": {"tabs": None}}),
    ],
)
def test_olx_archive_keeps_tabs_without_a_tab_list(tmp_path, policy):
    """A tab list that cannot be read is no reason to drop a tab page"""
    olx = _reference_olx(tmp_path)
    _write_olx(olx, "tabs/resources.html", "<p>resources</p>")
    if policy is not None:
        _write_olx(olx, "policies/run/policy.json", policy)
    assert "tabs/resources.html" in _ingested(olx)


def test_olx_archive_unlisted_tabs_do_not_keep_assets(tmp_path):
    """A page learners cannot reach does not make the files it links reachable"""
    olx = _reference_olx(tmp_path, **{"old.pdf": "old", "current.pdf": "current"})
    _write_olx(olx, "tabs/old.html", '<a href="/static/old.pdf">old</a>')
    _write_olx(olx, "tabs/current.html", '<a href="/static/current.pdf">now</a>')
    _tab_policy(olx, [_static_tab("current")])
    paths = _ingested(olx)
    assert "static/old.pdf" not in paths
    assert "static/current.pdf" in paths


def test_olx_archive_skips_policy_files_but_keeps_what_they_name(tmp_path):
    """Course settings are not content, but the textbooks they list are"""
    olx = _reference_olx(tmp_path, **{"book.pdf": "book"})
    textbook = {"tab_title": "Book", "chapters": [{"url": "/static/book.pdf"}]}
    _write_olx(
        olx,
        "policies/run/policy.json",
        json.dumps({"course/run": {"pdf_textbooks": [textbook]}}),
    )
    _write_olx(olx, "policies/run/grading_policy.json", '{"GRADER": []}')
    paths = _ingested(olx)
    assert "static/book.pdf" in paths
    assert [path for path in paths if path.startswith("policies/")] == []


@pytest.mark.parametrize(
    ("etl_source", "kept"),
    [
        # Open Learning Library is the only platform that shows the about page
        ("oll", ["overview.html", "short_description.html"]),
        ("mitxonline", []),
        (None, []),
    ],
)
# edX also reads about files from a folder named for the run
@pytest.mark.parametrize("about_dir", ["about", "about/run"])
def test_olx_archive_skips_about_files_learners_cannot_see(
    tmp_path, etl_source, kept, about_dir
):
    """About files are ingested only where the about page is shown, and only its prose"""
    olx = _reference_olx(tmp_path)
    for name in ("overview", "short_description", "effort", "prerequisites"):
        _write_olx(olx, f"{about_dir}/{name}.html", name)
    paths = _ingested(olx, etl_source)
    assert [path for path in paths if path.startswith("about/")] == [
        f"{about_dir}/{name}" for name in kept
    ]


def test_olx_archive_hidden_about_page_does_not_keep_assets(tmp_path):
    """A file only a hidden about page links is not reachable"""
    olx = _reference_olx(tmp_path, **{"intro.pdf": "pdf"})
    _write_olx(olx, "about/overview.html", '<a href="/static/intro.pdf">intro</a>')
    assert "static/intro.pdf" not in _ingested(olx, "mitxonline")
    assert "static/intro.pdf" in _ingested(olx, "oll")


def _write_unit_olx(olx):
    """Write an OLX export with two chapters of sequentials and course-wide pages"""
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": (
            '<course><chapter url_name="ch1"/><chapter url_name="ch_staff"/>'
            '<chapter url_name="ch_toc"/><chapter url_name="ch2"/></course>'
        ),
        "chapter/ch1.xml": (
            '<chapter><sequential url_name="seq1"/>'
            '<sequential url_name="seq_toc"/></chapter>'
        ),
        "sequential/seq1.xml": (
            '<sequential display_name="Lecture 1"><vertical url_name="v1"/>'
            '<vertical url_name="v_staff"/></sequential>'
        ),
        "vertical/v1.xml": (
            '<vertical><html url_name="h1"/><video url_name="vid1"/>'
            '<problem url_name="p_shared"/></vertical>'
        ),
        # the body has its own filename, and links a static file
        "html/h1.xml": '<html filename="h1_body"/>',
        "html/h1_body.html": '<a href="/static/notes_1.pdf">notes</a>',
        "video/vid1.xml": (
            '<video url_name="vid1" youtube="1.00:ABC123">'
            '<transcript language="en" src="vid1.srt"/></video>'
        ),
        "problem/p_shared.xml": "<problem/>",
        "vertical/v_staff.xml": (
            '<vertical visible_to_staff_only="true"><html url_name="h_staff"/></vertical>'
        ),
        "html/h_staff.xml": '<html filename="h_staff"/>',
        "sequential/seq_toc.xml": (
            '<sequential hide_from_toc="true"><vertical url_name="v_toc"/></sequential>'
        ),
        "vertical/v_toc.xml": '<vertical><html url_name="h_toc"/></vertical>',
        "html/h_toc.xml": '<html filename="h_toc"/>',
        "chapter/ch_staff.xml": (
            '<chapter visible_to_staff_only="true">'
            '<sequential url_name="seq_staff"/></chapter>'
        ),
        "sequential/seq_staff.xml": "<sequential/>",
        # hidden from the outline, so its sequential is too
        "chapter/ch_toc.xml": (
            '<chapter hide_from_toc="true">'
            '<sequential url_name="seq_in_toc_chapter"/></chapter>'
        ),
        "sequential/seq_in_toc_chapter.xml": (
            '<sequential display_name="Hidden Lecture">'
            '<vertical url_name="v_toc_chapter"/></sequential>'
        ),
        "vertical/v_toc_chapter.xml": '<vertical><html url_name="h_toc"/></vertical>',
        # the second sequential holds its children inline, under a library block,
        # and shares a problem with the first
        "chapter/ch2.xml": (
            '<chapter><sequential url_name="seq2" display_name="Problem Set 1">'
            '<vertical><library_content url_name="lib">'
            '<problem url_name="p_lib"/></library_content>'
            '<problem url_name="p_shared"/></vertical></sequential></chapter>'
        ),
        "library_content/lib.xml": (
            '<library_content><problem url_name="p_lib"/></library_content>'
        ),
        "problem/p_lib.xml": '<problem><a href="/static/notes_1.pdf"/></problem>',
        "static/notes_1.pdf": "pdf",
        "static/vid1.srt": "1\n00:00:00,000 --> 00:00:01,000\nhi\n",
        "static/subs_ABC123.srt.sjson": "{}",
        "static/unlinked.pdf": "pdf",
        "tabs/syllabus.html": "<p>syllabus</p>",
        "info/handouts.html": "<p>handouts</p>",
        # excluded, so not course context
        "info/updates.html": "<p>old announcement</p>",
        "about/overview.html": "<p>about</p>",
    }
    for rel, text in files.items():
        _write_olx(olx, rel, text)


def test_olx_archive_units(tmp_path):
    """Each visible sequential is a unit in tree order, with its files and links"""
    olx = tmp_path / "course"
    _write_unit_olx(olx)

    def key(rel):
        return get_edx_module_id(f"{olx}/{rel}", RUN_ID)

    units = _units(olx)

    assert [(unit["kind"], unit["title"]) for unit in units] == [
        ("unit", "Lecture 1"),
        ("unit", "Problem Set 1"),
        ("context", "Course context"),
    ]
    lecture, problem_set, context = units
    assert lecture["external_id"] == key("sequential/seq1.xml")
    # Learn's url is a jump URL ending in this url_name; dbt builds it
    assert lecture["url_name"] == "seq1"
    # in lesson order: a linked static file follows the first block linking it
    assert list(lecture["content_files"].items()) == [
        (key("html/h1.xml"), "core"),
        (key("html/h1_body.html"), "core"),
        (key("static/notes_1.pdf"), "supplementary"),
        (key("video/vid1.xml"), "core"),
        (key("static/vid1.srt"), "core"),
        (key("static/subs_ABC123.srt.sjson"), "core"),
        (key("problem/p_shared.xml"), "assessment"),
    ]
    assert list(problem_set["content_files"].items()) == [
        (key("library_content/lib.xml"), "core"),
        (key("problem/p_lib.xml"), "assessment"),
        (key("static/notes_1.pdf"), "supplementary"),
        (key("problem/p_shared.xml"), "assessment"),
    ]
    assert context["external_id"] == "context"
    # updates.html is excluded, as are about pages outside OLL
    assert set(context["content_files"]) == {
        key("tabs/syllabus.html"),
        key("info/handouts.html"),
    }


def test_olx_archive_units_oll_about_pages(tmp_path):
    """OLL shows its about pages, so they are course context"""
    olx = tmp_path / "course"
    _write_unit_olx(olx)

    def key(rel):
        return get_edx_module_id(f"{olx}/{rel}", RUN_ID)

    context = _units(olx, "oll")[-1]

    assert context["external_id"] == "context"
    assert set(context["content_files"]) == {
        key("tabs/syllabus.html"),
        key("info/handouts.html"),
        key("about/overview.html"),
    }


def test_olx_archive_units_missing_transcript(tmp_path):
    """A transcript the export doesn't have is left out, and the units still build"""
    olx = tmp_path / "course"
    _write_unit_olx(olx)
    _write_olx(
        olx,
        "video/vid1.xml",
        '<video url_name="vid1" youtube="1.00:ABC123">'
        '<transcript language="en" src="vid1.srt"/>'
        '<transcript language="es" src="missing.srt"/></video>',
    )

    lecture = _units(olx)[0]

    assert (
        get_edx_module_id(f"{olx}/static/vid1.srt", RUN_ID) in lecture["content_files"]
    )
    assert not any("missing.srt" in key for key in lecture["content_files"])


def test_olx_archive_units_url_name_with_glob_characters(tmp_path):
    """A url_name is a name, not a pattern: [ still finds the block's own file"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        olx,
        "course/run.xml",
        '<course><chapter><sequential url_name="seq1"><vertical>'
        '<problem url_name="p[1]"/><problem url_name="p1"/>'
        "</vertical></sequential></chapter></course>",
    )
    _write_olx(olx, "problem/p[1].xml", "<problem/>")
    _write_olx(olx, "problem/p1.xml", "<problem/>")

    unit = _units(olx)[0]

    assert list(unit["content_files"]) == [
        get_edx_module_id(f"{olx}/problem/p[1].xml", RUN_ID),
        get_edx_module_id(f"{olx}/problem/p1.xml", RUN_ID),
    ]


def test_olx_archive_units_not_olx(tmp_path):
    """A directory with no course.xml has no units"""
    _write_olx(tmp_path, "files/notes.pdf", "pdf")
    assert _units(tmp_path) == []


def test_olx_archive_units_intro_and_conclusion_go_to_context(tmp_path):
    """A chapter's only sequential goes to context when titled as an intro or wrap-up"""
    olx = tmp_path / "course"
    chapters = {
        # the only sequential, titled as an introduction: context
        "ch_intro": [("seq_welcome", "Welcome to the Course")],
        # an introduction beside other sequentials is a step in the course
        "ch_week1": [("seq_intro", "Introduction to Week 1"), ("seq_l1", "Lecture 1")],
        # the only sequential, but not titled as an intro or conclusion
        "ch_exam": [("seq_exam", "Final Exam")],
        "ch_end": [("seq_wrap", "Course Wrap-up")],
        # a module overview is context wherever it sits, and doesn't leave the
        # lecture beside it counted as the chapter's only sequential
        "ch_module": [
            ("seq_goals", "Module  Overview and Learning Goals"),
            ("seq_summary", "Summary of Results"),
        ],
    }
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        olx,
        "course/run.xml",
        "<course>"
        + "".join(f'<chapter url_name="{name}"/>' for name in chapters)
        + "</course>",
    )
    for chapter, sequentials in chapters.items():
        _write_olx(
            olx,
            f"chapter/{chapter}.xml",
            "<chapter>"
            + "".join(f'<sequential url_name="{seq}"/>' for seq, _ in sequentials)
            + "</chapter>",
        )
        for seq, title in sequentials:
            _write_olx(
                olx,
                f"sequential/{seq}.xml",
                f'<sequential display_name="{title}">'
                f'<vertical><problem url_name="p_{seq}"/></vertical></sequential>',
            )
            _write_olx(olx, f"problem/p_{seq}.xml", "<problem/>")

    def key(rel):
        return get_edx_module_id(f"{olx}/{rel}", RUN_ID)

    units = _units(olx)

    assert [unit["title"] for unit in units] == [
        "Introduction to Week 1",
        "Lecture 1",
        "Final Exam",
        "Summary of Results",
        "Course context",
    ]
    assert units[-1]["content_files"] == {
        key("problem/p_seq_welcome.xml"): "assessment",
        key("problem/p_seq_wrap.xml"): "assessment",
        key("problem/p_seq_goals.xml"): "assessment",
    }


@pytest.mark.parametrize(
    ("title", "is_context"),
    [
        ("Welcome", True),
        ("Welcome to Just Money", True),
        ("Welcome to 14.444x - Energy Economics and Policy", True),
        ("Welcome & Logistics", True),
        ("Welcome & Introduction", True),
        ("Course Introduction", True),
        ("Introduction", True),
        ("Introduction to the Course", True),
        ("Course Overview", True),
        ("Module Summary", True),
        ("Conclusion", True),
        ("Course Wrap-up", True),
        ("Wrapping Up", True),
        ("Course  Overview:", True),
        ("Introduction to Probability", False),
        ("Summary Statistics", False),
        ("Summary of Results", False),
        ("Lecture 1 - Introduction and Course Overview", False),
        ("Session 1: Introduction", False),
        ("Chapter overview", False),
        ("Nonintrusive Methods", False),
        ("Wrapper Classes", False),
    ],
)
def test_is_context_sequential_title(title, is_context):
    """Only titles about the course itself make a lone sequential context"""
    assert _is_context_sequential({"title": title}, chapter_size=1) is is_context
    # a chapter of several sequentials keeps each of them as a step
    assert _is_context_sequential({"title": title}, chapter_size=2) is False


def test_olx_archive_units_inline_lessons(tmp_path):
    """A file holding a component inline is a lesson; a pure outline file is not"""
    olx = tmp_path / "course"
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": '<course><chapter url_name="ch1"/><chapter url_name="ch2"/></course>',
        "chapter/ch1.xml": (
            '<chapter><sequential url_name="seq1"/><sequential url_name="seq2"/>'
            "</chapter>"
        ),
        # the only lesson is an html body stored inline in the vertical
        "sequential/seq1.xml": '<sequential><vertical url_name="v_inline"/></sequential>',
        "vertical/v_inline.xml": (
            '<vertical><html display_name="Notes"><p>inline body</p></html></vertical>'
        ),
        # an inline problem, beside a pure outline vertical
        "sequential/seq2.xml": (
            '<sequential><vertical url_name="v_problem"/>'
            '<vertical url_name="v_outline"/></sequential>'
        ),
        "vertical/v_problem.xml": "<vertical><problem><p>2+2?</p></problem></vertical>",
        "vertical/v_outline.xml": '<vertical><html url_name="h1"/></vertical>',
        "html/h1.xml": '<html filename="h1"/>',
        # a sequential stored inline in its chapter, holding an inline video
        "chapter/ch2.xml": (
            '<chapter><sequential url_name="seq3"><vertical>'
            '<video youtube="1.00:XYZ789"/></vertical></sequential>'
            '<sequential url_name="seq4"/></chapter>'
        ),
        "static/subs_XYZ789.srt.sjson": "{}",
        # an unnamed inline html block whose body is in html/body.html
        "sequential/seq4.xml": '<sequential><vertical url_name="v_body"/></sequential>',
        "vertical/v_body.xml": '<vertical><html filename="body"/></vertical>',
        "html/body.html": '<a href="/static/body.pdf">reading</a>',
        "static/body.pdf": "pdf",
    }
    for rel, text in files.items():
        _write_olx(olx, rel, text)

    def key(rel):
        return get_edx_module_id(f"{olx}/{rel}", RUN_ID)

    first, second, third, fourth, _context = _units(olx)

    assert first["content_files"] == {key("vertical/v_inline.xml"): "core"}
    assert list(second["content_files"].items()) == [
        (key("vertical/v_problem.xml"), "assessment"),
        (key("html/h1.xml"), "core"),
    ]
    assert list(third["content_files"].items()) == [
        (key("chapter/ch2.xml"), "core"),
        (key("static/subs_XYZ789.srt.sjson"), "core"),
    ]
    # the body is the lesson, not the vertical holding its pointer
    assert list(fourth["content_files"].items()) == [
        (key("html/body.html"), "core"),
        (key("static/body.pdf"), "supplementary"),
    ]


def test_olx_archive_units_ignores_staff_only_references(tmp_path):
    """A link only staff-only content makes does not put an asset in the unit"""
    olx = tmp_path / "course"
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": '<course><chapter url_name="ch"/></course>',
        "chapter/ch.xml": (
            '<chapter><sequential url_name="seq_a"/><sequential url_name="seq_b"/>'
            "</chapter>"
        ),
        # seq_a links shared.pdf only from a staff-only inline vertical, but
        # links inline.pdf from a visible inline html block
        "sequential/seq_a.xml": (
            '<sequential><vertical url_name="v_a"/>'
            '<vertical visible_to_staff_only="true">'
            '<html><a href="/static/shared.pdf">answers</a></html></vertical>'
            "</sequential>"
        ),
        "vertical/v_a.xml": (
            '<vertical><html><a href="/static/inline.pdf">notes</a></html>'
            '<problem visible_to_staff_only="true">'
            '<a href="/static/shared.pdf"/></problem></vertical>'
        ),
        # seq_b is where learners see shared.pdf
        "sequential/seq_b.xml": '<sequential><vertical url_name="v_b"/></sequential>',
        "vertical/v_b.xml": '<vertical><html url_name="h_b"/></vertical>',
        "html/h_b.xml": '<html filename="h_b"/>',
        "html/h_b.html": '<a href="/static/shared.pdf">handout</a>',
        "static/shared.pdf": "pdf",
        "static/inline.pdf": "pdf",
    }
    for rel, text in files.items():
        _write_olx(olx, rel, text)

    def key(rel):
        return get_edx_module_id(f"{olx}/{rel}", RUN_ID)

    seq_a, seq_b, _context = _units(olx)

    assert list(seq_a["content_files"]) == [
        key("vertical/v_a.xml"),
        key("static/inline.pdf"),
    ]
    assert key("static/shared.pdf") in seq_b["content_files"]


def test_olx_archive_units_context_static_files(tmp_path):
    """The context unit gets the static files that only course-wide pages link"""
    olx = tmp_path / "course"
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": '<course><chapter url_name="ch"/></course>',
        "chapter/ch.xml": '<chapter><sequential url_name="seq"/></chapter>',
        "sequential/seq.xml": '<sequential><vertical url_name="v"/></sequential>',
        "vertical/v.xml": '<vertical><html url_name="h"/></vertical>',
        "html/h.xml": "<html><p>lesson</p></html>",
        "info/handouts.html": '<a href="/static/syllabus.pdf">Syllabus</a>',
        "info/updates.items.json": json.dumps(
            [
                {"content": '<a href="/static/live.pdf">x</a>', "status": "visible"},
                {"content": '<a href="/static/deleted.pdf">x</a>', "status": "deleted"},
            ]
        ),
        # only the "shown" tab is in the navigation
        "policies/run/policy.json": json.dumps(
            {"course/run": {"tabs": [{"type": "static_tab", "url_slug": "shown"}]}}
        ),
        "tabs/shown.html": '<a href="/static/tab_notes.pdf">notes</a>',
        "tabs/hidden.html": '<a href="/static/hidden_tab.pdf">notes</a>',
        # mitxonline doesn't show the about page
        "about/overview.html": '<a href="/static/about.pdf">about</a>',
    }
    for name in (
        "syllabus",
        "live",
        "deleted",
        "tab_notes",
        "hidden_tab",
        "about",
    ):
        files[f"static/{name}.pdf"] = "pdf"
    for rel, text in files.items():
        _write_olx(olx, rel, text)

    def key(rel):
        return get_edx_module_id(f"{olx}/{rel}", RUN_ID)

    unit, context = _units(olx)

    linked = {
        k
        for k, relation in context["content_files"].items()
        if relation == "supplementary"
    }
    assert linked == {
        key("static/syllabus.pdf"),
        key("static/live.pdf"),
        key("static/tab_notes.pdf"),
    }
    assert key("info/handouts.html") in context["content_files"]
    assert not any(k.startswith("asset") for k in unit["content_files"])


def test_olx_archive_units_missing_course_file(tmp_path):
    """An export whose course.xml points to a missing file is not read as empty"""
    _write_olx(tmp_path, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    with pytest.raises(ValueError, match=r"course/run\.xml is missing"):
        _units(tmp_path)


@pytest.mark.parametrize(
    ("chapter", "missing", "message"),
    [
        ("ch", "chapter/ch.xml", r"chapter/ch\.xml is missing"),
        ("ch", "sequential/seq.xml", r"sequential/seq\.xml is missing"),
        # a colon-named block's message names the file edX would have written
        ("ch:one", "chapter/ch/one.xml", r"chapter/ch/one\.xml is missing"),
    ],
)
def test_olx_archive_units_missing_outline_file(tmp_path, chapter, missing, message):
    """A chapter or sequential pointing to a missing file is not read as empty"""
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": f'<course><chapter url_name="{chapter}"/></course>',
        f"chapter/{chapter.replace(':', '/')}.xml": (
            '<chapter><sequential url_name="seq"/></chapter>'
        ),
        "sequential/seq.xml": "<sequential><vertical><html><p>hi</p></html></vertical></sequential>",
    }
    del files[missing]
    _write_tree(tmp_path, files)
    with pytest.raises(ValueError, match=message):
        _units(tmp_path)


def test_olx_archive_units_missing_staff_only_file(tmp_path):
    """A missing file is no error for a block learners can't see, or an inline one"""
    _write_olx(tmp_path, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        tmp_path,
        "course/run.xml",
        '<course><chapter url_name="ch"/>'
        '<chapter url_name="gone" visible_to_staff_only="true"/></course>',
    )
    _write_olx(
        tmp_path,
        "chapter/ch.xml",
        '<chapter><sequential url_name="gone" visible_to_staff_only="true"/>'
        # inline, with no file of its own to miss
        '<sequential url_name="inline" display_name="Inline"><vertical>'
        "<html><p>hi</p></html></vertical></sequential></chapter>",
    )
    assert [unit["title"] for unit in _units(tmp_path)] == [
        "Inline",
        "Course context",
    ]


def test_olx_archive_units_missing_hidden_file(tmp_path):
    """A missing file is no error for a block hidden from the table of contents"""
    _write_olx(tmp_path, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(
        tmp_path,
        "course/run.xml",
        '<course><chapter url_name="ch"/>'
        '<chapter url_name="gone" hide_from_toc="true"/></course>',
    )
    _write_olx(
        tmp_path,
        "chapter/ch.xml",
        '<chapter><sequential url_name="gone" hide_from_toc="true"/>'
        '<sequential url_name="inline" display_name="Inline"><vertical>'
        "<html><p>hi</p></html></vertical></sequential></chapter>",
    )
    assert [unit["title"] for unit in _units(tmp_path)] == [
        "Inline",
        "Course context",
    ]


def test_olx_archive_units_inline_course(tmp_path):
    """A course.xml that holds the course inline is read without a course file"""
    _write_olx(
        tmp_path,
        "course.xml",
        '<course url_name="run" org="MITx" course="1">'
        '<chapter><sequential url_name="seq" display_name="Lecture 1">'
        "<vertical><html><p>hi</p></html></vertical></sequential></chapter></course>",
    )
    units = _units(tmp_path)
    assert [unit["title"] for unit in units] == ["Lecture 1", "Course context"]
    assert list(units[0]["content_files"]) == [
        get_edx_module_id(f"{tmp_path}/course.xml", RUN_ID)
    ]


def test_olx_archive_units_list_static_once(monkeypatch, tmp_path):
    """static/ is listed once per course, not once per sequential"""
    olx = tmp_path / "course"
    pointers = "".join(f'<sequential url_name="s{i}"/>' for i in range(3))
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(olx, "course/run.xml", '<course><chapter url_name="ch"/></course>')
    _write_olx(olx, "chapter/ch.xml", f"<chapter>{pointers}</chapter>")
    for i in range(3):
        _write_olx(
            olx,
            f"sequential/s{i}.xml",
            f'<sequential display_name="Lesson {i}"><html url_name="h{i}"/>'
            "</sequential>",
        )
        _write_olx(olx, f"html/h{i}.xml", f'<html><a href="/static/n{i}.pdf"/></html>')
        _write_olx(olx, f"static/n{i}.pdf", "pdf")
    # mocker.spy in Learn's; pytest-mock is not a dependency here
    glob_patterns = []
    glob = pathlib.Path.glob

    def spy(self, pattern, *args, **kwargs):
        glob_patterns.append(pattern)
        return glob(self, pattern, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "glob", spy)

    units = _units(olx)

    assert [list(unit["content_files"]) for unit in units[:-1]] == [
        [
            get_edx_module_id(str(olx / f"html/h{i}.xml"), RUN_ID),
            get_edx_module_id(str(olx / f"static/n{i}.pdf"), RUN_ID),
        ]
        for i in range(3)
    ]
    static_listings = [pattern for pattern in glob_patterns if pattern == "static/**/*"]
    assert len(static_listings) == 1


def _inline_markup_olx(tmp_path):
    """Write a sequential with an inline html and problem that hold markup"""
    olx = tmp_path / "course"
    _write_olx(olx, "course.xml", '<course url_name="run" org="MITx" course="1"/>')
    _write_olx(olx, "course/run.xml", '<course><chapter url_name="ch"/></course>')
    _write_olx(olx, "chapter/ch.xml", '<chapter><sequential url_name="seq"/></chapter>')
    _write_olx(
        olx, "sequential/seq.xml", '<sequential><vertical url_name="v"/></sequential>'
    )
    _write_olx(
        olx,
        "vertical/v.xml",
        "<vertical>"
        '<html><p>Read <a href="/static/notes.pdf">the <b>notes</b></a></p>'
        '<img src="/static/figure.png"/></html>'
        "<problem><choiceresponse><checkboxgroup>"
        '<choice correct="true">yes</choice><choice>no</choice>'
        '</checkboxgroup></choiceresponse><solution><a href="/static/key.pdf"/>'
        "</solution></problem>"
        "</vertical>",
    )
    for name in ("notes.pdf", "figure.png", "key.pdf"):
        _write_olx(olx, f"static/{name}", "file")
    return olx


def test_olx_archive_units_read_component_markup(tmp_path):
    """Links anywhere in a component's markup reach its unit, in component order"""
    olx = _inline_markup_olx(tmp_path)

    unit, _context = _units(olx)

    def key(rel):
        return get_edx_module_id(str(olx / rel), RUN_ID)

    assert unit["content_files"] == {
        key("vertical/v.xml"): "core",
        key("static/figure.png"): "supplementary",
        key("static/notes.pdf"): "supplementary",
        key("static/key.pdf"): "supplementary",
    }


def test_olx_archive_sequential_blocks_skip_markup(tmp_path):
    """A component's markup elements are not walked as blocks of their own"""
    olx = _inline_markup_olx(tmp_path)
    archive = OlxArchive(olx)
    pointer = ElementTree.fromstring('<sequential url_name="seq"/>')

    sequential = ElementTree.parse(olx / "sequential/seq.xml").getroot()

    blocks = archive._sequential_blocks(pointer, sequential, olx / "chapter/ch.xml")

    # the sequential, the vertical, the html and the problem
    assert len(blocks) == 4


def _colon_olx(olx):
    """
    Write a course whose blocks have url_names with a colon, stored as edX
    writes them: a:b in <type>/a/b.xml
    """
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": '<course><chapter url_name="ch:one"/></course>',
        "chapter/ch/one.xml": '<chapter><sequential url_name="s:one"/></chapter>',
        "sequential/s/one.xml": (
            f"<sequential {_start(START_FUTURE)}>"
            '<vertical url_name="v:1"/><vertical url_name="v:staff"/></sequential>'
        ),
        # an inline html block reads its body from its own folder too
        "vertical/v/1.xml": (
            '<vertical><html url_name="h:ok"/>'
            '<html url_name="h:in" filename="body"/>'
            '<video url_name="vid:1"/></vertical>'
        ),
        "html/h/ok.xml": '<html filename="other"/>',
        "html/h/other.html": "<p>ok</p>",
        "html/h/body.html": "<p>inline body</p>",
        "video/vid/1.xml": (
            '<video url_name="vid:1" display_name="V">'
            '<transcript language="en" src="v1.srt"/></video>'
        ),
        "static/v1.srt": "1\n00:00:00,000 --> 00:00:01,000\nhi\n",
        "vertical/v/staff.xml": (
            '<vertical visible_to_staff_only="true"><problem url_name="p:staff"/>'
            '<html url_name="h:staff"/></vertical>'
        ),
        "problem/p/staff.xml": "<problem/>",
        "html/h/staff.xml": '<html filename="../private"/>',
        "html/private.html": "<p>staff only</p>",
    }
    _write_tree(olx, files)


def test_olx_archive_colon_url_names(tmp_path):
    """
    A url_name with a colon is read from where edX exports it, so its block's
    units, staff-only status and release date apply, and its files are keyed
    by the url_name (Learn's case less video_metadata, which the port leaves out)
    """
    olx = tmp_path / "course"
    _colon_olx(olx)
    archive = OlxArchive(olx, "mitxonline")

    assert _ingested(olx) == [
        "chapter/ch/one.xml",
        "course.xml",
        "course/run.xml",
        "html/h/body.html",
        "html/h/ok.xml",
        "html/h/other.html",
        "sequential/s/one.xml",
        "static/v1.srt",
        "vertical/v/1.xml",
        "video/vid/1.xml",
    ]
    _assert_release_dates(
        olx,
        {
            "html/h/ok.xml": START_FUTURE,
            "html/h/body.html": START_FUTURE,
            "video/vid/1.xml": START_FUTURE,
        },
    )
    block = "block-v1:MITx+1+run+type@"
    unit, context = archive.units(RUN_ID)
    assert unit["external_id"] == f"{block}sequential+block@s:one"
    assert unit["url_name"] == "s:one"
    # the inline body is the lesson, not the vertical holding it
    assert list(unit["content_files"]) == [
        f"{block}html+block@h:ok",
        f"{block}html+block@h:other",
        f"{block}html+block@h:body",
        f"{block}video+block@vid:1",
        "asset-v1:MITx+1+run+type@asset+block@v1.srt",
    ]
    assert context["content_files"] == {}
    assert _excluded_keys(archive, RUN_ID) == {
        f"{block}vertical+block@v:staff",
        f"{block}problem+block@p:staff",
        f"{block}html+block@h:staff",
        f"{block}html+block@private",
    }


@pytest.mark.parametrize(
    ("rel", "is_olx", "expected"),
    [
        ("sequential/801x/a.xml", True, "type@sequential+block@801x:a"),
        ("problem/a/b/c d.xml", True, "type@problem+block@a:b:c_d"),
        ("problem/p.xml", True, "type@problem+block@p"),
        # folders that hold no blocks keep their old keys
        ("static/images/x.png", True, "type@images+block@x"),
        ("tabs/t/page.html", True, "type@t+block@page"),
        # so does any file of a tree that is not OLX, like Canvas
        ("sequential/801x/a.xml", False, "type@801x+block@a"),
    ],
)
def test_olx_archive_key(tmp_path, rel, is_olx, expected):
    """An OLX tree keys a nested block file by its url_name; other trees don't"""
    if is_olx:
        _write_olx(tmp_path, "course.xml", "<course/>")
    assert OlxArchive(tmp_path).key(tmp_path / rel, "course-v1:test_run") == (
        f"block-v1:test_run+{expected}"
    )


def test_olx_archive_odd_url_name_parts(tmp_path):
    """
    url_names with an empty, . or .. part stay in the tree, keep staff-only
    exclusion, and keep their declared key
    """
    olx = tmp_path / "course"
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": '<course><chapter url_name="ch"/></course>',
        "chapter/ch.xml": (
            '<chapter><vertical url_name=":v" visible_to_staff_only="true"/>'
            '<html url_name="h::x"/><problem url_name="q"/>'
            '<problem url_name="q:" visible_to_staff_only="true"/>'
            '<html url_name="a:.." visible_to_staff_only="true"/>'
            '<html url_name="a:..:b"/></chapter>'
        ),
        # a:.. is the file ..., a:..:b resolves to html/b.xml
        "html/a/...xml": "<html><p>staff only</p></html>",
        "html/a/../b.xml": "<html><p>b</p></html>",
        # q: is stored at problem/q/.xml; it mustn't take q's file
        "problem/q.xml": "<problem/>",
        "problem/q/.xml": "<problem/>",
        "vertical/v.xml": '<vertical><problem url_name="p"/></vertical>',
        "problem/p.xml": "<problem/>",
        "html/h/x.xml": "<html><p>hi</p></html>",
    }
    _write_tree(olx, files)
    run_id = "course-v1:test_run"
    archive = OlxArchive(olx)

    assert _ingested(olx) == [
        "chapter/ch.xml",
        "course.xml",
        "course/run.xml",
        "html/b.xml",
        "html/h/x.xml",
        "problem/q.xml",
    ]
    assert archive.key(olx / "html/h/x.xml", run_id) == (
        "block-v1:test_run+type@html+block@h::x"
    )
    assert archive.key(olx / "problem/q.xml", run_id) == (
        "block-v1:test_run+type@problem+block@q"
    )
    assert archive.key(olx / "html/b.xml", run_id) == (
        "block-v1:test_run+type@html+block@a:..:b"
    )
    # Learn's case also expects html+block@a:.. here. Under Python 3.14, which
    # this code location runs, pathlib gives "...xml" no suffix (3.12, Learn's,
    # reads it as .xml), so the file is not a text file and has no key to
    # exclude. Production exports had no such file name on 2026-11-10.
    assert _excluded_keys(archive, run_id) == {
        "block-v1:test_run+type@vertical+block@:v",
        "block-v1:test_run+type@problem+block@p",
    }


def test_olx_archive_html_filename_ending_in_html(tmp_path):
    """
    An html filename that already ends in .html names <filename>.html, and
    only if that is missing the file as written, as in edX
    """
    olx = tmp_path / "course"
    files = {
        "course.xml": '<course url_name="run" org="MITx" course="1"/>',
        "course/run.xml": (
            '<course><chapter url_name="ch" visible_to_staff_only="true"/></course>'
        ),
        "chapter/ch.xml": (
            '<chapter><html url_name="c:only" filename="private.html"/>'
            '<html url_name="c:two" filename="both.html"/></chapter>'
        ),
        "html/c/private.html": "<p>staff only</p>",
        # both.html.html is the body; both.html is another file
        "html/c/both.html.html": "<p>staff only</p>",
        "html/c/both.html": "<p>not this block's</p>",
    }
    _write_tree(olx, files)
    assert _ingested(olx) == ["course.xml", "course/run.xml", "html/c/both.html"]
