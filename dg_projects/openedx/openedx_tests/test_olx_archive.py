"""Learn's own tests for the OlxArchive the exclusion asset ports.

Copied from mit-learn learning_resources/etl/olx_test.py at c9f269c86
(2026-10-22), less the tests of what the port leaves out (units, keys, video
metadata), so a change to Learn's rules shows up as a failing case here. The
differences: ETLSource names are spelled out, no test needs the database, and
excluded is a dict of path to rule rather than a set.
"""

# Learn's line length and datetime import, so the cases diff against Learn's.
# ruff: noqa: E501, ICN001

import datetime
import json

import pytest
from defusedxml import ElementTree
from openedx.assets.content_file_exclusions import OlxArchive


def _write_olx(root, rel, text):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _ingested(olx, etl_source=None, **kwargs):
    """Return the archive's ingestable paths, relative to its root and sorted"""
    return sorted(
        str(path.relative_to(olx))
        for path in OlxArchive(olx, etl_source, **kwargs).paths
    )


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


# non-UTF-8 bytes are malformed too, though the text is read leniently
@pytest.mark.parametrize("broken", [b"<tab", b"<tab>\xff</tab>"])
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
