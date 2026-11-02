"""Tests for the Canvas course content source."""

import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any

import pypdf
import pytest
import requests

from ol_dlt import config
from ol_dlt.extraction import FAILURES_BEFORE_PROBE, PROBE_DOCUMENT
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
    def checksum(manifest_note: str, published: dict[str, dict[str, Any]]) -> str:
        members = {
            cartridge.MANIFEST: _manifest([]) + manifest_note,
            "web_resources/a.pdf": "x",
        }
        return cartridge.course_checksum(_open(_archive(members)), published, {})

    item = {"web_resources/a.pdf": {"title": "A"}}
    assert checksum("", item) == checksum("<!-- exported later -->", item)
    assert checksum("", item) != checksum("", {})


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


def test_documents_takes_published_text_files_and_one_per_key() -> None:
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
    assert {info.filename for info in members.values()} == {"web_resources/a.pdf"}


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


def test_an_oversized_file_is_failed_without_being_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(canvas_content, "MAX_FILE_BYTES", 3)
    archive = _open(_archive({"web_resources/big.txt": "too big"}))
    tika_client = FakeTika()
    extraction, md5 = canvas_content._read_member(  # noqa: SLF001
        tika_client,
        archive,
        archive.getinfo("web_resources/big.txt"),
    )
    assert extraction == (None, "failed", False)
    assert md5 is None
    assert tika_client.calls == []


def test_an_unreadable_pdf_is_failed_without_going_to_tika() -> None:
    archive = _open(_archive({"web_resources/bad.pdf": "not a pdf"}))
    tika_client = FakeTika()
    extraction, _md5 = canvas_content._read_member(  # noqa: SLF001
        tika_client,
        archive,
        archive.getinfo("web_resources/bad.pdf"),
    )
    assert extraction == (None, "failed", False)
    assert tika_client.calls == []
