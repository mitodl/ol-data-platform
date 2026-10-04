"""Tests for the OCW course content source."""

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import requests
from dlt.pipeline.exceptions import PipelineStepFailed

from ol_dlt import config
from ol_dlt.sources import ocw_content

_BUCKET = "ocw-test"


class FakeS3:
    """The slice of ``s3fs.S3FileSystem`` the source calls, over a dict of keys."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def ls(self, path: str, *, detail: bool = False) -> list[str]:  # noqa: ARG002
        prefix = path.removeprefix(f"{_BUCKET}/")
        slugs = {key.removeprefix(prefix).split("/")[0] for key in self.objects}
        return [f"{_BUCKET}/{prefix}{slug}" for slug in sorted(slugs)]

    def find(self, path: str, *, detail: bool = True) -> dict[str, dict[str, Any]]:  # noqa: ARG002
        prefix = path.removeprefix(f"{_BUCKET}/")
        return {
            f"{_BUCKET}/{key}": {
                "ETag": f'"{hashlib.md5(body).hexdigest()}"',  # noqa: S324
                "size": len(body),
            }
            for key, body in self.objects.items()
            if key.startswith(prefix)
        }

    def cat_file(self, path: str) -> bytes:
        return self.objects[path.removeprefix(f"{_BUCKET}/")]


class FakeTika:
    """Tika that fails every document, or every one but the outage probe."""

    def __init__(self, *, fail: bool = False, probe_ok: bool = False) -> None:
        self.fail = fail
        self.probe_ok = probe_ok
        self.calls = 0

    def extract_text(self, body: bytes) -> str | None:
        self.calls += 1
        if self.fail and not (self.probe_ok and body == ocw_content.PROBE_DOCUMENT):
            raise requests.ConnectionError
        return f"text of {body.decode()}"


def _course(slug: str, *, files: int = 1) -> dict[str, bytes]:
    root = f"courses/{slug}"
    objects = {
        f"{root}/data.json": json.dumps({"site_uid": f"uid-{slug}"}).encode(),
        f"{root}/index.html": b"<html>theme</html>",
        f"{root}/pages/syllabus/data.json": json.dumps(
            {"title": "Syllabus", "content": "<p>Read.</p>"}
        ).encode(),
        f"{root}/pages/syllabus/index.html": b"<html>syllabus</html>",
    }
    for number in range(files):
        objects[f"{root}/resources/notes-{number}/data.json"] = json.dumps(
            {
                "title": f"Notes {number}",
                "resourcetype": "Document",
                "file": f"/{root}/abc_notes{number}.pdf",
                "file_type": "application/pdf",
            }
        ).encode()
        objects[f"{root}/abc_notes{number}.pdf"] = f"{slug} notes {number}".encode()
    return objects


@pytest.fixture
def bucket(monkeypatch: pytest.MonkeyPatch) -> FakeS3:
    fake = FakeS3({**_course("a-course"), **_course("b-course")})
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    return fake


@pytest.fixture
def fake_tika(monkeypatch: pytest.MonkeyPatch) -> FakeTika:
    fake = FakeTika()
    monkeypatch.setattr(ocw_content.tika, "client_for_profile", lambda: fake)
    return fake


def _load(**kwargs: Any) -> list[dict[str, Any]]:
    """Run one load and return every row the table now holds."""
    pipeline = config.pipeline_for("ocw", pipeline_name="ocw_content")
    info = pipeline.run(
        config.with_nullable_load_id(
            ocw_content.ocw_content_source(bucket=_BUCKET, **kwargs)
        )
    )
    assert not info.has_failed_jobs
    return pipeline.dataset()[ocw_content.RAW_TABLE].arrow().to_pylist()


@pytest.mark.parametrize(
    ("resource", "expected"),
    [
        (
            {"resourcetype": "Document", "file": "/courses/a/x_notes.pdf"},
            "courses/a/x_notes.pdf",
        ),
        (
            {"resourcetype": "Document", "file": "https://cdn/courses/a/x%20y.PDF"},
            "courses/a/x y.PDF",
        ),
        ({"resourcetype": "Image", "file": "/courses/a/x.jpg"}, None),
        ({"resourcetype": "Document", "file": None}, None),
        ({"resourcetype": "Document", "file": "https://elsewhere/x.pdf"}, None),
        (
            {
                "resourcetype": "Other",
                "title": "3play caption file",
                "file": "/courses/a/x.srt",
            },
            None,
        ),
        # A video's text is its transcript, never its own file.
        (
            {
                "resourcetype": "Video",
                "file": "/courses/a/x.pdf",
                "video_files": {"video_transcript_file": "/courses/a/t.pdf"},
            },
            "courses/a/t.pdf",
        ),
        ({"resourcetype": "Video", "file": None, "video_files": {}}, None),
        (
            {"resource_type": "Video", "transcript_file": "/courses/a/t.pdf"},
            "courses/a/t.pdf",
        ),
        ({"file": "/courses/a/legacy.txt"}, "courses/a/legacy.txt"),
        # Learn takes what lies between the first two "courses".
        (
            {"resourcetype": "Document", "file": "/courses/a/courses/x.pdf"},
            "courses/a/",
        ),
    ],
)
def test_text_file_key_follows_learn(
    resource: dict[str, Any], expected: str | None
) -> None:
    assert ocw_content.text_file_key(resource) == expected


def test_course_version_ignores_site_files(bucket: FakeS3) -> None:
    before = ocw_content.course_version(
        "a-course", ocw_content.list_course(bucket, _BUCKET, "a-course")
    )
    bucket.objects["courses/a-course/index.html"] = b"<html>new theme</html>"
    bucket.objects["courses/a-course/pages/syllabus/index.html"] = b"<html>new</html>"
    bucket.objects["courses/a-course/content_map.json"] = b"{}"
    after = ocw_content.course_version(
        "a-course", ocw_content.list_course(bucket, _BUCKET, "a-course")
    )
    assert after == before

    bucket.objects["courses/a-course/abc_notes0.pdf"] = b"revised notes"
    assert (
        ocw_content.course_version(
            "a-course", ocw_content.list_course(bucket, _BUCKET, "a-course")
        )
        != before
    )


@pytest.mark.integration
def test_loads_each_course_once_until_it_changes(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    rows = _load()

    assert len(rows) == 6  # noqa: PLR2004
    by_key = {row["s3_key"]: row for row in rows}
    resource = by_key["courses/a-course/resources/notes-0/data.json"]
    assert resource["content_kind"] == "resource"
    assert resource["file_key"] == "courses/a-course/abc_notes0.pdf"
    assert resource["content"] == "text of a-course notes 0"
    assert resource["extraction_status"] == "extracted"
    assert json.loads(resource["data_json"])["title"] == "Notes 0"
    page = by_key["courses/a-course/pages/syllabus/data.json"]
    assert (page["content_kind"], page["extraction_status"]) == ("page", None)
    assert by_key["courses/a-course/data.json"]["content_kind"] == "course"

    assert len(_load()) == 6  # noqa: PLR2004
    assert fake_tika.calls == 2  # noqa: PLR2004

    bucket.objects["courses/b-course/pages/syllabus/data.json"] = b'{"title": "New"}'
    rows = _load()
    assert len(rows) == 9  # noqa: PLR2004
    assert len({row["course_version"] for row in rows}) == 3  # noqa: PLR2004
    # One stamp per read of a course, so staging can pick its newest set.
    reads = {(row["course_slug"], row["course_retrieved_at"]) for row in rows}
    assert sorted(slug for slug, _stamp in reads) == [
        "a-course",
        "b-course",
        "b-course",
    ]


@pytest.mark.integration
def test_course_removed_from_the_bucket_loads_an_unpublished_row(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    _load()
    for key in [key for key in bucket.objects if key.startswith("courses/b-course/")]:
        del bucket.objects[key]

    rows = _load()

    newest = max(row["course_retrieved_at"] for row in rows)
    assert [
        (row["course_slug"], row["content_kind"])
        for row in rows
        if row["course_retrieved_at"] == newest
    ] == [("b-course", "unpublished")]
    assert len(_load()) == len(rows)


@pytest.mark.integration
def test_refuses_to_unpublish_most_of_the_catalog(
    test_profile: Path,
    bucket: FakeS3,
    fake_tika: FakeTika,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ocw_content, "MIN_COURSES_FOR_UNPUBLISH_GUARD", 2)
    rows = _load()
    for key in [key for key in bucket.objects if key.startswith("courses/b-course/")]:
        del bucket.objects[key]

    with pytest.raises(PipelineStepFailed, match="Refusing to unpublish"):
        _load()

    pipeline = config.pipeline_for("ocw", pipeline_name="ocw_content")
    assert pipeline.dataset()[ocw_content.RAW_TABLE].arrow().num_rows == len(rows)


@pytest.mark.integration
def test_budget_ends_a_load_and_the_next_resumes_after_it(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    assert {row["course_slug"] for row in _load(budget_bytes=1)} == {"a-course"}
    assert {row["course_slug"] for row in _load(budget_bytes=1)} == {
        "a-course",
        "b-course",
    }
    # The sweep reached the end, so the next one starts over and finds nothing.
    assert len(_load(budget_bytes=1)) == 6  # noqa: PLR2004
    # A course before the last cursor is still seen once it changes.
    bucket.objects["courses/a-course/pages/syllabus/data.json"] = b'{"title": "New"}'
    assert len(_load(budget_bytes=1)) == 9  # noqa: PLR2004


@pytest.mark.integration
def test_load_starting_mid_sweep_wraps_round_to_the_courses_before_it(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    _load(budget_bytes=1)
    bucket.objects["courses/a-course/pages/syllabus/data.json"] = b'{"title": "New"}'

    rows = _load()

    assert len(rows) == 9  # noqa: PLR2004


@pytest.mark.integration
def test_prefix_left_without_data_json_supersedes_its_last_set(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    _load()
    for key in [key for key in bucket.objects if key.endswith("data.json")]:
        if key.startswith("courses/b-course/"):
            del bucket.objects[key]

    rows = _load()

    newest = max(
        row["course_retrieved_at"] for row in rows if row["course_slug"] == "b-course"
    )
    assert [
        row["content_kind"]
        for row in rows
        if row["course_slug"] == "b-course" and row["course_retrieved_at"] == newest
    ] == ["unpublished"]
    assert len(_load()) == len(rows)


@pytest.mark.integration
def test_named_courses_are_read_again_unchanged(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    _load()

    rows = _load(courses=["b-course", "not-a-course"])

    assert len(rows) == 9  # noqa: PLR2004
    assert fake_tika.calls == 3  # noqa: PLR2004


@pytest.mark.integration
def test_one_failed_file_is_recorded_and_the_course_still_loads(
    test_profile: Path, bucket: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ocw_content.tika, "client_for_profile", lambda: FakeTika(fail=True)
    )
    del bucket.objects["courses/b-course/abc_notes0.pdf"]

    statuses = {
        row["course_slug"]: row["extraction_status"]
        for row in _load()
        if row["file_key"]
    }

    assert statuses == {"a-course": "failed", "b-course": "missing"}


@pytest.mark.integration
def test_course_tika_cannot_read_is_recorded_when_tika_is_up(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Otherwise every load would fail at this course and none after it load."""
    fake = FakeS3(
        {
            **_course("a-course", files=ocw_content.MIN_FILES_FOR_OUTAGE),
            **_course("b-course"),
        }
    )
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    tika = FakeTika(fail=True, probe_ok=True)
    monkeypatch.setattr(ocw_content.tika, "client_for_profile", lambda: tika)

    rows = _load()

    assert {row["course_slug"] for row in rows} == {"a-course", "b-course"}
    assert {row["extraction_status"] for row in rows if row["file_key"]} == {"failed"}
    # Read again on a later day, not again in the same run.
    calls = tika.calls
    assert len(_load()) == len(rows)
    assert tika.calls == calls


def test_failing_course_is_read_again_on_a_bounded_number_of_days() -> None:
    state = ocw_content.record_read(None, "v1", "2026-10-01", retry=True)
    assert not ocw_content.needs_read(state, "v1", "2026-10-01")
    assert ocw_content.needs_read(state, "v1", "2026-10-02")
    assert ocw_content.needs_read(state, "v2", "2026-10-01")

    for day in ("2026-10-02", "2026-10-03"):
        state = ocw_content.record_read(state, "v1", day, retry=True)
    assert state == "v1|3|2026-10-03"
    assert not ocw_content.needs_read(state, "v1", "2026-10-04")

    assert ocw_content.record_read(state, "v1", "2026-10-04", retry=False) == "v1"
    assert not ocw_content.needs_read("v1", "v1", "2026-10-05")
    # A new version starts its attempts over.
    assert ocw_content.record_read(state, "v2", "2026-10-04", retry=True) == (
        "v2|1|2026-10-04"
    )


def test_file_over_the_size_limit_is_failed_without_being_read(
    bucket: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ocw_content, "MAX_FILE_BYTES", 3)
    tika = FakeTika()

    result = ocw_content._extract_text(  # noqa: SLF001
        bucket, tika, _BUCKET, "courses/a-course/abc_notes0.pdf", 4
    )

    assert result == (None, "failed", False)
    assert tika.calls == 0


@pytest.mark.integration
def test_course_with_no_text_at_all_fails_the_load(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeS3(_course("a-course", files=ocw_content.MIN_FILES_FOR_OUTAGE))
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    monkeypatch.setattr(
        ocw_content.tika, "client_for_profile", lambda: FakeTika(fail=True)
    )

    with pytest.raises(PipelineStepFailed, match="Tika outage"):
        _load()
