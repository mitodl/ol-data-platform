"""Tests for the OCW course content source."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
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
        # Keys still listed that can no longer be read.
        self.vanished: set[str] = set()

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

    def info(self, path: str) -> dict[str, Any]:
        key = path.removeprefix(f"{_BUCKET}/")
        if key in self.objects:
            return {**self.find(path)[path], "type": "file"}
        if any(other.startswith(key) for other in self.objects):
            return {"type": "directory", "size": 0}
        raise FileNotFoundError(path)

    def cat_file(self, path: str) -> bytes:
        key = path.removeprefix(f"{_BUCKET}/")
        if key in self.vanished:
            raise FileNotFoundError(path)
        return self.objects[key]


class FakeTika:
    """Tika that fails every document, or every one but the outage probe."""

    def __init__(self, *, fail: bool = False, probe_ok: bool = False) -> None:
        self.fail = fail
        self.probe_ok = probe_ok
        self.calls = 0

    def extract_text(self, body: bytes, timeout: float = 0) -> str | None:  # noqa: ARG002
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


def _at(day: int, hour: int = 4, minute: int = 35) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC)


def test_known_course_seen_empty_is_unpublished_on_a_later_day() -> None:
    """A publish in progress can leave a prefix without data.json for a moment."""
    seen = ocw_content.settle_empty_course("v1", "v0", _at(1, 23, 59))
    assert seen == (False, "!empty|2026-10-01T23:59:00+00:00")
    # A run that crosses midnight has not waited a day.
    assert ocw_content.settle_empty_course(seen[1], "v0", _at(2, 0, 1)) == seen
    assert ocw_content.settle_empty_course(seen[1], "v0", _at(3)) == (True, "v0")
    # Settled, and a prefix that never held a course loads nothing.
    assert ocw_content.settle_empty_course("v0", "v0", _at(4)) == (False, "v0")
    assert ocw_content.settle_empty_course(None, "v0", _at(1)) == (False, "v0")
    # The course coming back is a new version, so it is read.
    assert ocw_content.needs_read(seen[1], "v2", _at(1, 23, 59))
    assert ocw_content.record_read(seen[1], "v2", _at(2), retry=True) == (
        "v2|1|2026-10-02T04:35:00+00:00"
    )


@pytest.mark.integration
def test_prefix_left_without_data_json_keeps_its_set_until_seen_empty_again(
    test_profile: Path,
    bucket: FakeS3,
    fake_tika: FakeTika,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _load()
    for key in [key for key in bucket.objects if key.endswith("data.json")]:
        if key.startswith("courses/b-course/"):
            del bucket.objects[key]

    assert len(_load()) == len(rows)

    class Tomorrow(ocw_content.datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return super().now(tz) + timedelta(days=1)

    monkeypatch.setattr(ocw_content, "datetime", Tomorrow)
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
def test_file_under_another_course_is_read(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    bucket.objects["courses/a-course/resources/shared/data.json"] = json.dumps(
        {
            "title": "Shared notes",
            "resourcetype": "Document",
            "file": "/courses/b-course/abc_notes0.pdf",
        }
    ).encode()

    by_key = {row["s3_key"]: row for row in _load()}

    shared = by_key["courses/a-course/resources/shared/data.json"]
    assert shared["extraction_status"] == "extracted"
    assert shared["content"] == "text of b-course notes 0"


@pytest.mark.integration
def test_file_value_naming_only_a_prefix_is_missing(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    """Otherwise reading the prefix as a file would fail every load here."""
    bucket.objects["courses/a-course/resources/odd/data.json"] = json.dumps(
        {
            "title": "Odd",
            "resourcetype": "Document",
            "file": "/courses/a-course/courses.pdf",
        }
    ).encode()

    by_key = {row["s3_key"]: row for row in _load()}

    odd = by_key["courses/a-course/resources/odd/data.json"]
    assert (odd["file_key"], odd["extraction_status"]) == (
        "courses/a-course/",
        "missing",
    )


@pytest.mark.integration
def test_small_course_with_every_file_failed_fails_the_load_when_tika_is_down(
    test_profile: Path, bucket: FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ocw_content.tika, "client_for_profile", lambda: FakeTika(fail=True)
    )

    with pytest.raises(PipelineStepFailed, match="Tika outage"):
        _load()


@pytest.mark.integration
def test_run_of_failures_is_probed_before_the_rest_of_the_course_is_tried(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = ocw_content.FAILURES_BEFORE_PROBE * 20
    fake = FakeS3(_course("a-course", files=files))
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    tika = FakeTika(fail=True)
    monkeypatch.setattr(ocw_content.tika, "client_for_profile", lambda: tika)
    monkeypatch.setattr(ocw_content, "WORKERS", 1)

    with pytest.raises(PipelineStepFailed, match="Tika outage"):
        _load()

    assert tika.calls < files


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
        ocw_content.tika,
        "client_for_profile",
        lambda: FakeTika(fail=True, probe_ok=True),
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
            **_course("a-course", files=ocw_content.FAILURES_BEFORE_PROBE),
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
    state = ocw_content.record_read(None, "v1", _at(1, 23, 59), retry=True)
    assert not ocw_content.needs_read(state, "v1", _at(1, 23, 59))
    # Not again in a run that crosses midnight.
    assert not ocw_content.needs_read(state, "v1", _at(2, 0, 1))
    assert ocw_content.needs_read(state, "v1", _at(2, 23, 59))
    assert ocw_content.needs_read(state, "v2", _at(1, 23, 59))

    for day in (3, 4):
        state = ocw_content.record_read(state, "v1", _at(day), retry=True)
    assert state == "v1|3|2026-10-04T04:35:00+00:00"
    assert not ocw_content.needs_read(state, "v1", _at(5))

    assert ocw_content.record_read(state, "v1", _at(5), retry=False) == "v1"
    assert not ocw_content.needs_read("v1", "v1", _at(6))
    # A new version starts its attempts over.
    assert ocw_content.record_read(state, "v2", _at(5), retry=True) == (
        "v2|1|2026-10-05T04:35:00+00:00"
    )


@pytest.mark.integration
def test_course_is_read_again_when_a_file_it_reads_from_another_course_changes(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    key = "courses/a-course/resources/shared/data.json"
    bucket.objects[key] = json.dumps(
        {
            "title": "Shared notes",
            "resourcetype": "Document",
            "file": "/courses/c-other/abc_shared.pdf",
        }
    ).encode()
    bucket.objects["courses/c-other/abc_shared.pdf"] = b"shared notes"
    first = _load()
    assert len(_load()) == len(first)

    # c-other has no data.json, so only a-course can notice this.
    bucket.objects["courses/c-other/abc_shared.pdf"] = b"revised shared notes"
    rows = _load()

    texts = {row["content"] for row in rows if row["s3_key"] == key}
    assert texts == {"text of shared notes", "text of revised shared notes"}
    assert len(_load()) == len(rows)


@pytest.mark.integration
def test_refuses_to_unpublish_many_courses_left_without_data_json(
    test_profile: Path,
    bucket: FakeS3,
    fake_tika: FakeTika,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ocw_content, "MIN_COURSES_FOR_UNPUBLISH_GUARD", 2)
    _load()
    for key in [key for key in bucket.objects if key.endswith("data.json")]:
        del bucket.objects[key]
    rows = _load()

    class Tomorrow(ocw_content.datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return super().now(tz) + timedelta(days=1)

    monkeypatch.setattr(ocw_content, "datetime", Tomorrow)
    with pytest.raises(PipelineStepFailed, match="Refusing to unpublish"):
        _load()

    pipeline = config.pipeline_for("ocw", pipeline_name="ocw_content")
    assert pipeline.dataset()[ocw_content.RAW_TABLE].arrow().num_rows == len(rows)


@pytest.mark.integration
def test_file_gone_between_listing_and_read_is_missing_and_read_again_later(
    test_profile: Path,
    bucket: FakeS3,
    fake_tika: FakeTika,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bucket.vanished.add("courses/a-course/abc_notes0.pdf")

    rows = _load()

    statuses = {
        row["course_slug"]: row["extraction_status"] for row in rows if row["file_key"]
    }
    assert statuses == {"a-course": "missing", "b-course": "extracted"}
    assert len(_load()) == len(rows)

    class Tomorrow(ocw_content.datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return super().now(tz) + timedelta(days=1)

    bucket.vanished.clear()
    monkeypatch.setattr(ocw_content, "datetime", Tomorrow)
    again = _load()
    notes = [
        row["extraction_status"]
        for row in again
        if row["s3_key"] == "courses/a-course/resources/notes-0/data.json"
    ]
    assert sorted(notes) == ["extracted", "missing"]


@pytest.mark.integration
def test_data_json_gone_between_listing_and_read_skips_the_course_for_now(
    test_profile: Path, bucket: FakeS3, fake_tika: FakeTika
) -> None:
    """A course caught mid-publish must not fail the load for every course."""
    bucket.vanished.add("courses/a-course/pages/syllabus/data.json")

    assert {row["course_slug"] for row in _load()} == {"b-course"}

    bucket.vanished.clear()
    assert {row["course_slug"] for row in _load()} == {"a-course", "b-course"}


def test_unpublish_limit_counts_across_loads() -> None:
    """A sweep is split across loads, so a count per load would never trip."""
    state: dict[str, Any] = {}
    for _load_number in range(3):
        ocw_content.count_unpublished(state, _at(1), 1, 30)
    with pytest.raises(RuntimeError, match="Refusing to unpublish"):
        ocw_content.count_unpublished(state, _at(1, 6), 1, 30)
    assert state["unpublish_window"]["count"] == 3  # noqa: PLR2004
    # A later day starts the count again.
    ocw_content.count_unpublished(state, _at(2, 6), 1, 30)
    assert state["unpublish_window"]["count"] == 1


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
@pytest.mark.parametrize("files", [1, ocw_content.FAILURES_BEFORE_PROBE])
def test_rejected_tika_token_fails_the_load_whatever_the_course_size(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch, files: int
) -> None:
    """A bad token must not record every course as read with failed files."""

    class Unauthorized:
        def extract_text(self, _body: bytes, timeout: float = 0) -> str | None:  # noqa: ARG002
            response = requests.Response()
            response.status_code = 401
            raise requests.HTTPError(response=response)

    fake = FakeS3(_course("a-course", files=files))
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    monkeypatch.setattr(ocw_content.tika, "client_for_profile", Unauthorized)

    with pytest.raises(PipelineStepFailed, match="rejected the access token"):
        _load()


@pytest.mark.integration
def test_course_tika_refuses_outright_is_probed_before_it_is_recorded(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every file refused with a 4xx still asks whether Tika itself is down."""

    class Refuses:
        def extract_text(self, _body: bytes, timeout: float = 0) -> str | None:  # noqa: ARG002
            response = requests.Response()
            response.status_code = 422
            raise requests.HTTPError(response=response)

    fake = FakeS3(_course("a-course", files=ocw_content.FAILURES_BEFORE_PROBE))
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    monkeypatch.setattr(ocw_content.tika, "client_for_profile", Refuses)

    with pytest.raises(PipelineStepFailed, match="Tika outage"):
        _load()


@pytest.mark.integration
def test_course_with_no_text_at_all_fails_the_load(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeS3(_course("a-course", files=ocw_content.FAILURES_BEFORE_PROBE))
    monkeypatch.setattr(ocw_content.s3fs, "S3FileSystem", lambda **_kwargs: fake)
    monkeypatch.setattr(
        ocw_content.tika, "client_for_profile", lambda: FakeTika(fail=True)
    )

    with pytest.raises(PipelineStepFailed, match="Tika outage"):
        _load()
