"""Tests for the Canvas course content source."""

import io
import json
import runpy
import time
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any

import pypdf
import pytest
import requests

from ol_dlt import config
from ol_dlt.extraction import FAILURES_BEFORE_PROBE, PROBE_DOCUMENT, collect
from ol_dlt.sources import canvas_content
from ol_dlt.sources.canvas_content import cartridge

_BUCKET = "landing-zone-test"
_NOW = datetime(2026, 6, 1, tzinfo=UTC)
_CC = 'xmlns="http://canvas.instructure.com/xsd/cccv1p0"'
_IMS = 'xmlns="http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1"'


def _manifest(resources: list[str]) -> str:
    body = "".join(resources)
    return f"<manifest {_IMS}><resources>{body}</resources></manifest>"


def _webcontent(identifier: str, *files: str) -> str:
    hrefs = "".join(f'<file href="{file}"/>' for file in files)
    return f'<resource identifier="{identifier}" type="webcontent">{hrefs}</resource>'


def _page(identifier: str, href: str, *, intendeduse: str = "") -> str:
    use = f' intendeduse="{intendeduse}"' if intendeduse else ""
    return (
        f'<resource identifier="{identifier}" type="webcontent" href="{href}"{use}>'
        f'<file href="{href}"/></resource>'
    )


def _html(title: str, state: str = "active", body: str = "") -> str:
    return (
        f'<html><head><title>{title}</title><meta name="workflow_state" '
        f'content="{state}"/></head><body>{body}</body></html>'
    )


def _archive(
    members: dict[str, str], *, settings: str = "", code: str = "6.100"
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            cartridge.SETTINGS,
            f"<course {_CC}><title>Intro</title><course_code>{code}</course_code>"
            f"<start_at>2026-02-01T05:00:00</start_at>{settings}</course>",
        )
        archive.writestr(
            cartridge.CONTEXT,
            f"<context_info {_CC}><course_id>42</course_id>"
            "<canvas_domain>canvas.mit.edu</canvas_domain></context_info>",
        )
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def _open(body: bytes) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(body))


def _published(
    members: dict[str, str],
    config_by_key: dict[str, Any] | None = None,
    *,
    settings: str = "",
) -> dict[str, dict[str, Any]]:
    archive = _open(_archive(members, settings=settings))
    return cartridge.published_items(archive, config_by_key or {}, _NOW)


def test_a_file_only_in_the_manifest_is_published() -> None:
    members = {
        cartridge.MANIFEST: _manifest([_webcontent("a", "web_resources/notes.pdf")]),
        "web_resources/notes.pdf": "x",
    }
    assert _published(members) == {
        "web_resources/notes.pdf": {
            "path": "web_resources/notes.pdf",
            "title": "notes.pdf",
        }
    }


def test_a_file_in_a_hidden_folder_is_not_published() -> None:
    members = {
        cartridge.MANIFEST: _manifest(
            [_webcontent("a", "web_resources/secret/deep/notes.pdf")]
        ),
        cartridge.FILES_META: (
            f'<fileMeta {_CC}><folders><folder path="secret"><hidden>true</hidden>'
            "</folder></folders></fileMeta>"
        ),
    }
    assert _published(members) == {}


def test_tutor_files_are_not_content_files() -> None:
    members = {
        cartridge.MANIFEST: _manifest(
            [_webcontent("a", f"{cartridge.TUTOR_FOLDER}set1/problem.pdf")]
        ),
    }
    assert _published(members) == {}


def test_files_meta_hides_a_file_and_titles_a_visible_one() -> None:
    members = {
        cartridge.MANIFEST: _manifest(
            [
                _webcontent("hidden", "web_resources/hidden.pdf"),
                _webcontent("shown", "web_resources/shown.pdf"),
            ]
        ),
        cartridge.FILES_META: (
            f'<fileMeta {_CC}><files><file identifier="hidden"><hidden>true</hidden>'
            '</file><file identifier="shown"><display_name>Shown</display_name>'
            "</file></files></fileMeta>"
        ),
    }
    published = _published(members)
    assert list(published) == ["web_resources/shown.pdf"]
    assert published["web_resources/shown.pdf"]["title"] == "Shown"


def test_a_hidden_files_tab_keeps_only_module_items() -> None:
    tabs = json.dumps([{"id": cartridge.FILES_TAB_ID, "hidden": True}])
    members = {
        cartridge.MANIFEST: (
            f"<manifest {_IMS}><organizations><item>"
            '<item identifierref="b"/></item></organizations><resources>'
            f"{_webcontent('a', 'web_resources/loose.pdf')}"
            f"{_webcontent('b', 'web_resources/in-module.pdf')}"
            "</resources></manifest>"
        ),
        cartridge.MODULE_META: (
            f"<modules {_CC}><module><title>Week 1</title><items><item>"
            "<workflow_state>active</workflow_state><title>Reading</title>"
            "<identifierref>b</identifierref><content_type>Attachment</content_type>"
            "</item></items></module></modules>"
        ),
    }
    published = _published(
        members, settings=f"<tab_configuration>{tabs}</tab_configuration>"
    )
    assert list(published) == ["web_resources/in-module.pdf"]
    assert published["web_resources/in-module.pdf"]["title"] == "Reading"


def test_a_page_link_publishes_a_hidden_file_but_not_a_locked_one() -> None:
    body = (
        '<a href="$IMS-CC-FILEBASE$/linked%20file.pdf?wrap=1">one</a>'
        '<a href="$IMS-CC-FILEBASE$/locked.pdf">two</a>'
    )
    members = {
        cartridge.MANIFEST: _manifest([_page("p", "wiki_content/week.html")]),
        "wiki_content/week.html": _html("Week", body=body),
    }
    config_by_key = {
        "/linked file.pdf": {"hidden": True},
        "/locked.pdf": {"locked": True},
    }
    published = _published(members, config_by_key)
    assert sorted(published) == [
        "web_resources/linked file.pdf",
        "wiki_content/week.html",
    ]
    assert published["web_resources/linked file.pdf"]["title"] == ""


def test_an_unpublished_page_is_not_published() -> None:
    members = {
        cartridge.MANIFEST: _manifest([_page("p", "wiki_content/draft.html")]),
        "wiki_content/draft.html": _html("Draft", state="unpublished"),
    }
    assert _published(members) == {}


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ("", ["course_settings/syllabus.html"]),
        (
            "<public_syllabus>false</public_syllabus>"
            "<public_syllabus_to_auth>false</public_syllabus_to_auth>",
            [],
        ),
    ],
)
def test_the_syllabus_follows_the_course_settings(
    settings: str, expected: list[str]
) -> None:
    href = "course_settings/syllabus.html"
    members = {
        cartridge.MANIFEST: _manifest([_page("s", href, intendeduse="syllabus")]),
        href: _html("Syllabus", state="unpublished"),
    }
    assert list(_published(members, settings=settings)) == expected


@pytest.mark.parametrize(
    ("xml", "published"),
    [
        ("<workflow_state>published</workflow_state>", True),
        ("<workflow_state>unpublished</workflow_state>", False),
        (
            "<workflow_state>published</workflow_state>"
            "<only_visible_to_overrides>true</only_visible_to_overrides>",
            False,
        ),
        (
            "<workflow_state>published</workflow_state>"
            "<lock_at>2026-01-01T00:00:00</lock_at>",
            False,
        ),
    ],
)
def test_an_assignment_follows_its_settings(xml: str, *, published: bool) -> None:
    members = {
        cartridge.MANIFEST: _manifest(
            [
                '<resource identifier="a" type="associatedcontent" '
                'href="g1/assignment.html"><file href="g1/assignment.html"/>'
                '<file href="g1/assignment_settings.xml"/></resource>'
            ]
        ),
        "g1/assignment.html": "<html></html>",
        "g1/assignment_settings.xml": (
            f"<assignment {_CC}><title>PS1</title>{xml}</assignment>"
        ),
    }
    result = _published(members)
    assert bool(result) is published
    if published:
        assert result["g1/assignment.html"]["title"] == "PS1"


@pytest.mark.parametrize(
    ("lock_at", "unlock_at", "locked"),
    [
        (None, None, False),
        ("nil", "nil", False),
        ("2026-05-01T00:00:00", None, True),
        ("2026-07-01T00:00:00", None, False),
        (None, "2026-07-01T00:00:00", True),
        (None, "2026-05-01T00:00:00", False),
        ("not a date", None, False),
        # Read as US/Eastern, so 01:00 is still four hours off at 01:00 UTC.
        (None, "2026-06-01T01:00:00", True),
    ],
)
def test_date_locks(
    lock_at: str | None, unlock_at: str | None, *, locked: bool
) -> None:
    assert cartridge.is_date_locked(lock_at, unlock_at, _NOW) is locked


def test_url_config_keys_files_by_path_and_items_by_name() -> None:
    config_by_key = cartridge.url_config(
        {
            "course_files": [
                {"file_path": "course files/a+b/notes.pdf", "url": "https://f"}
            ],
            "assignments": [{"name": "PS1", "html_url": "https://a"}],
            "pages": [{"title": "Week", "html_url": "https://p"}],
        }
    )
    assert config_by_key["/a b/notes.pdf"]["url"] == "https://f"
    assert config_by_key["PS1"]["url"] == "https://a"
    assert config_by_key["Week"]["url"] == "https://p"


def test_the_checksum_ignores_export_noise_and_follows_the_published_set() -> None:
    def checksum(
        manifest_note: str,
        published: dict[str, dict[str, Any]],
        fields: list[str | None] | None = None,
    ) -> str:
        members = {
            cartridge.MANIFEST: _manifest([]) + manifest_note,
            "web_resources/a.pdf": "x",
        }
        return cartridge.course_checksum(
            _open(_archive(members)), published, {}, fields or []
        )

    item = {"web_resources/a.pdf": {"title": "A"}}
    assert checksum("", item) == checksum("<!-- exported later -->", item)
    assert checksum("", item) != checksum("", {})
    assert checksum("", item) != checksum("", item, ["A new course code"])


def test_the_checksum_covers_a_published_member_under_the_settings_folder() -> None:
    syllabus = "course_settings/syllabus.html"

    def checksum(body: str, published: dict[str, dict[str, Any]]) -> str:
        archive = _open(_archive({syllabus: body}))
        return cartridge.course_checksum(archive, published, {}, [])

    item = {syllabus: {"title": "Syllabus"}}
    assert checksum("<p>Old</p>", item) != checksum("<p>New</p>", item)
    # Not published, so it is noise like the rest of the folder.
    assert checksum("<p>Old</p>", {}) == checksum("<p>New</p>", {})


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (
            "web_resources/Lecture Notes/L 1.pdf",
            "block-v1:1-A+canvas+type@Lecture_Notes+block@L_1",
        ),
        ("g1/assignment.html", "block-v1:1-A+canvas+type@g1+block@assignment"),
        ("/assessment.xml", "block-v1:1-A+canvas+type@+block@assessment"),
        ("web_resources/static/a.pdf", "asset-v1:1-A+canvas+type@asset+block@a.pdf"),
    ],
)
def test_file_key(path: str, expected: str) -> None:
    assert canvas_content.file_key(path, "1-A+canvas") == expected


def test_derived_title() -> None:
    assert canvas_content.derived_title("g1/assignment_settings.xml") == (
        "Assignment Settings"
    )
    assert canvas_content.derived_title("a/lecture-1_notes.pdf") == "Lecture 1 Notes"


def test_documents_takes_published_text_files_and_groups_them_by_key() -> None:
    names = [
        "web_resources/a.pdf",
        "web_resources/a.docx",
        "web_resources/movie.mp4",
        "web_resources/not-published.pdf",
        "web_resources/draft_notes/b.pdf",
    ]
    published: dict[str, dict[str, Any]] = {
        name: {} for name in names if name != names[3]
    }
    archive = _open(_archive(dict.fromkeys(names, "x")))
    members = canvas_content.documents(archive, published, "1-A+canvas")
    assert [
        [info.filename for info in candidates] for candidates in members.values()
    ] == [["web_resources/a.docx", "web_resources/a.pdf"]]


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("1\n00:00:01,000 --> 00:00:02,000\nHello\n\n2\n", "Hello "),
    ],
)
def test_text_from_srt(content: str, expected: str) -> None:
    assert canvas_content.text_from_srt(content) == expected


def test_text_from_sjson() -> None:
    assert canvas_content.text_from_sjson('{"text": ["a", "b"]}') == "a b"
    assert canvas_content.text_from_sjson("not json") is None
    assert canvas_content.text_from_sjson('["a list"]') is None


def _pdf() -> bytes:
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=72, height=72)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def test_pdf_is_valid() -> None:
    assert canvas_content.pdf_is_valid(_pdf())
    assert not canvas_content.pdf_is_valid(b"not a pdf")


class FakeS3:
    """The slice of ``s3fs.S3FileSystem`` the source calls, over a dict of keys."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.modified: dict[str, datetime] = {}

    def find(self, path: str, *, detail: bool = True) -> dict[str, dict[str, Any]]:  # noqa: ARG002
        prefix = path.removeprefix(f"{_BUCKET}/")
        return {
            f"{_BUCKET}/{key}": {
                "LastModified": self.modified.get(key, _NOW),
                "size": len(body),
            }
            for key, body in self.objects.items()
            if key.startswith(prefix)
        }

    def cat_file(self, path: str) -> bytes:
        key = path.removeprefix(f"{_BUCKET}/")
        if key not in self.objects:
            raise FileNotFoundError(path)
        return self.objects[key]

    def open(self, path: str, **_kwargs: Any) -> io.BytesIO:
        return io.BytesIO(self.cat_file(path))


class FakeTika:
    """Tika that fails every document, or every one but the outage probe."""

    def __init__(self, *, fail: bool = False, probe_ok: bool = False) -> None:
        self.fail = fail
        self.probe_ok = probe_ok
        self.calls: list[str | None] = []

    def extract_text(
        self, body: bytes, timeout: float = 0, content_type: str | None = None
    ) -> str | None:
        _ = timeout
        self.calls.append(content_type)
        if self.fail and not (self.probe_ok and body == PROBE_DOCUMENT):
            raise requests.ConnectionError
        return f"  text of {len(body)} bytes \n"


def _export(course_id: str, version: str, members: dict[str, str]) -> dict[str, bytes]:
    root = f"{canvas_content.PREFIX}/{course_id}/{version}"
    return {
        f"{root}.imscc": _archive(members),
        f"{root}.metadata.json": json.dumps(
            {
                "course_files": [
                    {
                        "file_path": "course files/notes.txt",
                        "url": "https://canvas.mit.edu/courses/42/files/1/",
                    }
                ]
            }
        ).encode(),
    }


_MEMBERS = {
    cartridge.MANIFEST: _manifest(
        [_webcontent("a", "web_resources/notes.txt"), _page("p", "wiki_content/w.html")]
    ),
    "web_resources/notes.txt": "Notes",
    "wiki_content/w.html": _html("Week 1"),
}


@pytest.fixture
def bucket(monkeypatch: pytest.MonkeyPatch) -> FakeS3:
    fake = FakeS3(_export("42", "v1", _MEMBERS))
    monkeypatch.setattr(canvas_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    return fake


@pytest.fixture
def fake_tika(monkeypatch: pytest.MonkeyPatch) -> FakeTika:
    fake = FakeTika()
    monkeypatch.setattr(canvas_content.tika, "client_for_profile", lambda: fake)
    return fake


def _load(**kwargs: Any) -> list[dict[str, Any]]:
    """Run one load and return every row the table now holds."""
    pipeline = config.pipeline_for("canvas", pipeline_name="canvas_content")
    info = pipeline.run(
        config.with_nullable_load_id(
            canvas_content.canvas_content_source(bucket=_BUCKET, **kwargs)
        )
    )
    assert not info.has_failed_jobs
    return pipeline.dataset()[canvas_content.RAW_TABLE].arrow().to_pylist()


@pytest.mark.usefixtures("test_profile", "bucket")
def test_load_writes_the_course_and_its_files(fake_tika: FakeTika) -> None:
    rows = _load()
    course = next(row for row in rows if row["content_kind"] == "course")
    assert course["run_readable_id"] == "42-6.100+canvas"
    assert course["course_title"] == "Intro"
    assert course["course_url"] == "https://canvas.mit.edu/courses/42/"
    assert course["course_start_at"] == "2026-02-01T05:00:00"

    files = {r["source_path"]: r for r in rows if r["content_kind"] == "file"}
    notes = files["web_resources/notes.txt"]
    assert (
        notes["file_key"] == "block-v1:42-6.100+canvas+type@web_resources+block@notes"
    )
    assert notes["title"] == notes["content_title"] == "notes.txt"
    assert notes["url"] == "https://canvas.mit.edu/courses/42/files/1/"
    # Stripped, as Learn strips what Tika returns.
    assert notes["content"] == "text of 5 bytes"
    assert notes["extraction_status"] == "extracted"
    assert files["wiki_content/w.html"]["title"] == "Week 1"
    assert set(fake_tika.calls) == {"text/plain", "text/html"}


@pytest.mark.usefixtures("test_profile", "bucket")
def test_a_course_that_did_not_change_is_not_read_again(fake_tika: FakeTika) -> None:
    first = len(_load())
    calls = len(fake_tika.calls)
    assert len(_load()) == first
    assert len(fake_tika.calls) == calls


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_the_newest_export_is_the_course(bucket: FakeS3) -> None:
    _load()
    members = {**_MEMBERS, "web_resources/notes.txt": "Changed notes"}
    bucket.objects.update(_export("42", "v2", members))
    bucket.modified[f"{canvas_content.PREFIX}/42/v2.imscc"] = _NOW + timedelta(hours=6)
    rows = _load()
    newest = max(row["course_retrieved_at"] for row in rows)
    latest = [row for row in rows if row["course_retrieved_at"] == newest]
    assert {row["archive_key"] for row in latest} == {
        f"{canvas_content.PREFIX}/42/v2.imscc"
    }


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_a_new_course_code_alone_writes_the_new_run(bucket: FakeS3) -> None:
    """The settings are left out of the digest's bytes, as Canvas rewrites them."""
    _load()
    root = f"{canvas_content.PREFIX}/42/v2"
    bucket.objects[f"{root}.imscc"] = _archive(_MEMBERS, code="6.100L")
    bucket.objects[f"{root}.metadata.json"] = bucket.objects[
        f"{canvas_content.PREFIX}/42/v1.metadata.json"
    ]
    bucket.modified[f"{root}.imscc"] = _NOW + timedelta(hours=6)
    rows = _load()
    newest = max(row["course_retrieved_at"] for row in rows)
    assert {
        row["run_readable_id"] for row in rows if row["course_retrieved_at"] == newest
    } == {"42-6.100L+canvas"}


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_a_broken_export_does_not_stop_the_courses_after_it(bucket: FakeS3) -> None:
    bucket.objects.update(_export("43", "v1", _MEMBERS))
    bucket.objects.update(_export("44", "v1", _MEMBERS))
    bucket.objects[f"{canvas_content.PREFIX}/43/v1.imscc"] = b"not a zip"
    rows = _load()
    assert {row["course_id"] for row in rows} == {"42", "44"}


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_no_export_that_parses_fails_the_load(bucket: FakeS3) -> None:
    bucket.objects[f"{canvas_content.PREFIX}/42/v1.imscc"] = b"not a zip"
    with pytest.raises(Exception, match="could be parsed"):
        _load()


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_a_course_gone_from_the_landing_zone_is_marked(bucket: FakeS3) -> None:
    other = _export("7", "v1", _MEMBERS)
    bucket.objects.update(other)
    _load()
    for key in other:
        del bucket.objects[key]
    rows = _load()
    marker = [row for row in rows if row["content_kind"] == "unpublished"]
    assert [row["course_id"] for row in marker] == ["7"]


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_an_empty_landing_zone_fails_the_load(bucket: FakeS3) -> None:
    _load()
    bucket.objects.clear()
    with pytest.raises(Exception, match="No Canvas exports"):
        _load()


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_an_export_without_its_metadata_waits_for_the_next_load(bucket: FakeS3) -> None:
    del bucket.objects[f"{canvas_content.PREFIX}/42/v1.metadata.json"]
    pipeline = config.pipeline_for("canvas", pipeline_name="canvas_content")
    info = pipeline.run(
        config.with_nullable_load_id(
            canvas_content.canvas_content_source(bucket=_BUCKET)
        )
    )
    assert not info.has_failed_jobs
    assert not info.load_packages or not any(
        job.job_file_info.table_name == canvas_content.RAW_TABLE
        for package in info.load_packages
        for job in package.jobs["completed_jobs"]
    )


@pytest.mark.usefixtures("test_profile", "bucket")
def test_a_file_tika_fails_on_is_recorded_and_read_again_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeTika(fail=True, probe_ok=True)
    monkeypatch.setattr(canvas_content.tika, "client_for_profile", lambda: fake)
    rows = _load()
    statuses = {r["extraction_status"] for r in rows if r["content_kind"] == "file"}
    assert statuses == {"failed"}
    # Another read waits WAIT_BETWEEN_READS, so the second load reads nothing.
    calls = len(fake.calls)
    _load()
    assert len(fake.calls) == calls


@pytest.mark.usefixtures("test_profile")
def test_a_tika_outage_fails_the_load(
    bucket: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeTika(fail=True)
    monkeypatch.setattr(canvas_content.tika, "client_for_profile", lambda: fake)
    assert FAILURES_BEFORE_PROBE > len(bucket.objects)
    with pytest.raises(Exception, match="Tika failed"):
        _load()


def test_a_run_of_failures_is_probed_before_the_rest_of_the_course() -> None:
    names = [f"web_resources/{number}.txt" for number in range(40)]
    archive = _open(_archive(dict.fromkeys(names, "x")))
    published: dict[str, dict[str, Any]] = {name: {} for name in names}

    class SlowsAfterTheProbePoint(FakeTika):
        """Hold the worker back, so it cannot finish before the load gives up."""

        def extract_text(
            self, body: bytes, timeout: float = 0, content_type: str | None = None
        ) -> str | None:
            if len(self.calls) >= FAILURES_BEFORE_PROBE:
                time.sleep(0.2)
            return super().extract_text(body, timeout, content_type)

    fake = SlowsAfterTheProbePoint(fail=True)
    pool = canvas_content.ThreadPoolExecutor(max_workers=1)
    with pytest.raises(RuntimeError, match="Tika failed"):
        canvas_content.read_course(
            pool=pool,
            client=fake,
            archive=archive,
            course_id="42",
            archive_key="k",
            published=published,
            config_by_key={},
            version="v",
        )
    pool.shutdown(wait=False, cancel_futures=True)
    assert len(fake.calls) < len(names)


def _read(
    members: dict[str, str], tika_client: FakeTika, course_id: str = "42"
) -> list[dict[str, Any]]:
    """Read a course with every member published, and return its file rows."""
    archive = _open(_archive(members))
    pool = canvas_content.ThreadPoolExecutor(max_workers=1)
    rows, _bytes, _retry = canvas_content.read_course(
        pool=pool,
        client=tika_client,
        archive=archive,
        course_id=course_id,
        archive_key="k",
        published={name: {} for name in members},
        config_by_key={},
        version="v",
    )
    pool.shutdown(wait=False, cancel_futures=True)
    return [row for row in rows if row["content_kind"] == "file"]


@pytest.mark.parametrize(
    ("members", "kept"),
    [
        # The PDF sorts last and cannot be read, so the docx is the one with text.
        ({"web_resources/a.docx": "x", "web_resources/a.pdf": "bad"}, ".docx"),
        ({"web_resources/a.docx": "x", "web_resources/a.txt": "y"}, ".txt"),
    ],
)
def test_the_last_file_with_text_is_the_one_a_key_keeps(
    members: dict[str, str], kept: str
) -> None:
    (row,) = _read(members, FakeTika())
    assert row["file_extension"] == kept
    assert row["extraction_status"] == "extracted"


def test_a_key_with_no_readable_file_is_failed() -> None:
    (row,) = _read(
        {"web_resources/a.pdf": "bad", "web_resources/a.txt": ""}, FakeTika()
    )
    assert row["file_extension"] == ".pdf"
    assert row["extraction_status"] == "failed"


def test_files_that_never_reach_tika_are_no_reason_to_probe_it() -> None:
    members = {f"web_resources/{number}.pdf": "bad" for number in range(10)}
    fake = FakeTika(fail=True)
    rows = _read(members, fake)
    assert fake.calls == []
    assert {row["extraction_status"] for row in rows} == {"failed"}


def test_an_unreadable_file_does_not_hide_a_tika_outage() -> None:
    """A course of failures with one bad PDF among them still looks like one."""
    failed = (None, "failed", False)
    half = [failed] * (FAILURES_BEFORE_PROBE // 2)
    results = [*half, (None, "unreadable", False), *half]
    with pytest.raises(RuntimeError, match="Tika failed"):
        collect(FakeTika(fail=True), "42", results)


@pytest.mark.parametrize("error", [RuntimeError, NotImplementedError])
def test_a_member_zipfile_refuses_is_failed(
    monkeypatch: pytest.MonkeyPatch, error: type[Exception]
) -> None:
    def refuse(*_args: Any) -> bytes:
        msg = "File is encrypted, password required for extraction"
        raise error(msg)

    monkeypatch.setattr(zipfile.ZipFile, "read", refuse, raising=True)
    archive = zipfile.ZipFile(io.BytesIO(_zip_of({"web_resources/a.txt": "x"})))
    extraction = canvas_content._read_member(  # noqa: SLF001
        FakeTika(), archive, archive.getinfo("web_resources/a.txt"), {}
    )
    assert extraction == (None, "unreadable", False)


def _zip_of(members: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


@pytest.mark.usefixtures("test_profile", "fake_tika")
def test_a_runtime_error_parsing_a_course_skips_only_that_course(
    bucket: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    bucket.objects.update(_export("43", "v1", _MEMBERS))
    real = cartridge.parse_settings
    calls: list[int] = []

    def fail_once(archive: zipfile.ZipFile) -> dict[str, Any]:
        calls.append(1)
        if len(calls) == 1:
            msg = "File is encrypted, password required for extraction"
            raise RuntimeError(msg)
        return real(archive)

    monkeypatch.setattr(cartridge, "parse_settings", fail_once)
    rows = _load()
    assert {row["course_id"] for row in rows} == {"43"}


@pytest.mark.usefixtures("test_profile", "bucket")
def test_a_tika_client_that_cannot_be_built_fails_the_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_token() -> FakeTika:
        msg = "Vault refused the token"
        raise KeyError(msg)

    monkeypatch.setattr(canvas_content.tika, "client_for_profile", no_token)
    with pytest.raises(Exception, match="Vault refused the token"):
        _load()


def test_python_dash_m_runs_one_load(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(
        canvas_content.canvas_content_pipeline,
        "run",
        lambda source: calls.append(source),
    )
    runpy.run_module("ol_dlt.sources.canvas_content", run_name="__main__")
    assert len(calls) == 1


def test_an_oversized_file_is_failed_without_being_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(canvas_content, "MAX_FILE_BYTES", 3)
    archive = _open(_archive({"web_resources/big.txt": "too big"}))
    tika_client = FakeTika()
    md5s: dict[str, str] = {}
    extraction = canvas_content._read_member(  # noqa: SLF001
        tika_client,
        archive,
        archive.getinfo("web_resources/big.txt"),
        md5s,
    )
    assert extraction == (None, "unreadable", False)
    assert md5s == {}
    assert tika_client.calls == []


def test_an_unreadable_pdf_is_failed_without_going_to_tika() -> None:
    tika_client = FakeTika()
    for content in ("not a pdf", ""):
        archive = _open(_archive({"web_resources/bad.pdf": content}))
        extraction = canvas_content._read_member(  # noqa: SLF001
            tika_client,
            archive,
            archive.getinfo("web_resources/bad.pdf"),
            {},
        )
        assert extraction == (None, "unreadable", False)
    assert tika_client.calls == []
