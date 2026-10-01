"""Unit + materialization tests for the MIT edX programs and MITx catalog source."""

import json
from pathlib import Path
from typing import Any

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from ol_dlt import config, vault
from ol_dlt.sources import mit_edx_programs
from tests.conftest import FakeResponse

_PROGRAMS_URL = "https://edx/programs"
_COURSES_URL = "https://edx/courses"

RUN = {
    "key": "course-v1:MITx+6.002x+1T2026",
    "start": "2026-01-10T00:00:00Z",
    "end": "2026-05-01T00:00:00Z",
    "status": "published",
    "is_enrollable": True,
    "pacing_type": "instructor_paced",
    "seats": [{"type": "verified", "price": "99.00", "currency": "USD"}],
}


def program(**overrides: Any) -> dict[str, Any]:
    """Build a trimmed discovery API program, field names as the API returns them."""
    return {
        "uuid": "mit-1",
        "title": "Circuits and Electronics",
        "subtitle": "Learn the fundamentals.",
        "type": "XSeries",
        "status": "active",
        "authoring_organizations": [{"key": "MITx"}, {"key": "MITx_PRO"}],
        "data_modified_timestamp": "2026-01-01T00:00:00Z",
        "marketing_url": "https://www.edx.org/xseries/mitx-circuits",
        "banner_image": {
            "medium": {"url": "https://cdn.example.com/banner.medium.png"}
        },
        "level_type_override": "Intermediate",
        "courses": [
            {
                "key": "MITx+6.002.1x",
                "title": "Circuits 1",
                "short_description": "Part 1",
                "course_type": "verified-audit",
                "excluded_from_search": False,
                "course_runs": [RUN],
            }
        ],
        **overrides,
    }


def course_run(**overrides: Any) -> dict[str, Any]:
    """Build a trimmed catalog course run."""
    return {
        "key": "course-v1:MITx+6.002x+1T2026",
        "title": "Circuits",
        "short_description": "short",
        "full_description": "full",
        "marketing_url": "https://www.edx.org/course/circuits",
        "level_type": "Intermediate",
        "content_language": "en-us",
        "start": "2026-01-10T00:00:00Z",
        "end": "2026-05-01T00:00:00Z",
        "enrollment_start": None,
        "enrollment_end": None,
        "announcement": None,
        "pacing_type": "instructor_paced",
        "type": "verified",
        "availability": "Upcoming",
        "status": "published",
        "is_enrollable": True,
        "image": {"src": "https://cdn.example.com/run.png", "description": None},
        "seats": [{"type": "verified", "price": "99.00"}],
        "staff": [{"given_name": "Ada", "family_name": "Lovelace"}],
        "weeks_to_complete": 12,
        "min_effort": 5,
        "max_effort": 8,
        "estimated_hours": 0,
        "modified": "2026-01-01T00:00:00Z",
        **overrides,
    }


def course(**overrides: Any) -> dict[str, Any]:
    """Build a trimmed catalog course."""
    return {
        "key": "MITx+6.002x",
        "title": "Circuits",
        "owners": [{"key": "MITx"}],
        "short_description": "short",
        "full_description": "full",
        "level_type": "Intermediate",
        "marketing_url": "https://www.edx.org/course/circuits",
        "image": {"src": "https://cdn.example.com/course.png", "description": None},
        "course_type": "verified-audit",
        "subjects": [{"name": "Engineering", "slug": "engineering"}],
        "prerequisites": [],
        "prerequisites_raw": "",
        "modified": "2026-01-01T00:00:00Z",
        "course_runs": [course_run()],
        **overrides,
    }


_PROGRAMS: dict[str, Any] = {
    "results": [
        program(),
        program(uuid="other-1", authoring_organizations=[{"key": "HarvardX"}]),
        program(uuid="mit-mm", type="MicroMasters"),
    ],
    "next": None,
}
_COURSES: dict[str, Any] = {"results": [course()], "next": None}


def test_is_mit_program_filters() -> None:
    assert mit_edx_programs._is_mit_program(_PROGRAMS["results"][0]) is True
    assert mit_edx_programs._is_mit_program(_PROGRAMS["results"][1]) is False
    assert mit_edx_programs._is_mit_program(_PROGRAMS["results"][2]) is False


def test_program_record_keeps_learn_fields() -> None:
    """The fields MIT Learn builds program records from survive flattening."""
    record = mit_edx_programs.program_record(
        program(), retrieved_at="2026-09-19T00:00:00+00:00"
    )
    assert record["authoring_organizations"] == "MITx, MITx_PRO"
    assert record["marketing_url"] == "https://www.edx.org/xseries/mitx-circuits"
    assert record["banner_image_url"] == "https://cdn.example.com/banner.medium.png"
    assert record["level_type_override"] == "Intermediate"
    assert record["retrieved_at"] == "2026-09-19T00:00:00+00:00"


def test_program_record_tolerates_missing_banner_and_level() -> None:
    """A program without a banner image or level override yields nulls."""
    record = mit_edx_programs.program_record(
        program(banner_image=None, level_type_override=None), retrieved_at="t"
    )
    assert record["banner_image_url"] is None
    assert record["level_type_override"] is None


def test_program_course_records_keep_runs_as_json() -> None:
    """Each course keeps its search exclusion and its runs, whole, as JSON."""
    (record,) = mit_edx_programs.program_course_records(program(), retrieved_at="t")
    assert record["program_uuid"] == "mit-1"
    assert record["course_key"] == "MITx+6.002.1x"
    assert record["course_position"] == 1
    assert record["excluded_from_search"] is False
    assert json.loads(record["course_runs"]) == [RUN]
    assert record["retrieved_at"] == "t"


def test_mitx_course_record_serializes_nested_fields_as_compact_json() -> None:
    """Nested fields are JSON strings shaped as the Airbyte tables held them."""
    record = mit_edx_programs.mitx_course_record(course(), retrieved_at="t")
    assert record["owner"] == "MITx"
    assert record["image"] == (
        '{"url":"https://cdn.example.com/course.png","description":null}'
    )
    assert record["subjects"] == '[{"name":"Engineering"}]'
    # stg__edxorg__api__course compares subjects against the literal '[]'.
    assert (
        mit_edx_programs.mitx_course_record(
            course(subjects=[], image=None), retrieved_at="t"
        )["subjects"]
        == "[]"
    )
    assert record["prerequisites"] == "[]"


def test_mitx_course_run_records() -> None:
    (record,) = mit_edx_programs.mitx_course_run_records(course(), retrieved_at="t")
    assert record["course_key"] == "MITx+6.002x"
    assert record["run_key"] == "course-v1:MITx+6.002x+1T2026"
    assert record["languages"] == "en-us"
    assert record["enrollment_type"] == "verified"
    assert record["staff"] == '[{"first_name":"Ada","last_name":"Lovelace"}]'
    assert record["seats"] == '[{"type":"verified","price":"99.00"}]'


@pytest.mark.parametrize(
    ("columns", "record"),
    [
        (
            mit_edx_programs._PROGRAM_COLUMNS,
            mit_edx_programs.program_record(program(), retrieved_at="t"),
        ),
        (
            mit_edx_programs._PROGRAM_COURSE_COLUMNS,
            mit_edx_programs.program_course_records(program(), retrieved_at="t")[0],
        ),
        (
            mit_edx_programs._MITX_COURSE_COLUMNS,
            mit_edx_programs.mitx_course_record(course(), retrieved_at="t"),
        ),
        (
            mit_edx_programs._MITX_COURSE_RUN_COLUMNS,
            mit_edx_programs.mitx_course_run_records(course(), retrieved_at="t")[0],
        ),
    ],
)
def test_every_emitted_field_has_a_declared_type(
    columns: dict[str, Any], record: dict[str, Any]
) -> None:
    """An undeclared field would get dlt's inferred type, e.g. a timestamp."""
    assert set(columns) == set(record)


def test_missing_credentials_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "EDX_API_CLIENT_ID",
        "EDX_API_CLIENT_SECRET",
        "EDX_API_ACCESS_TOKEN_URL",
        "EDX_PROGRAMS_API_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    source = mit_edx_programs.mit_edx_programs_source()
    # dlt wraps the resource generator's ValueError in ResourceExtractionError.
    with pytest.raises(ResourceExtractionError, match="Missing required credentials"):
        list(source.resources["raw__edxorg__discovery__api__programs"])


def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDX_API_CLIENT_ID", "cid")
    monkeypatch.setenv("EDX_API_CLIENT_SECRET", "secret")  # noqa: S105
    monkeypatch.setenv("EDX_API_ACCESS_TOKEN_URL", "https://edx/token")
    monkeypatch.setenv("EDX_PROGRAMS_API_URL", _PROGRAMS_URL)
    monkeypatch.setenv("EDX_MITX_COURSES_API_URL", _COURSES_URL)


def _mock_http(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Serve programs and courses by URL; return the list of fetched URLs."""
    fetched: list[str] = []
    pages = {_PROGRAMS_URL: _PROGRAMS, _COURSES_URL: _COURSES}

    def _get(url: str, **_kwargs: Any) -> FakeResponse:
        fetched.append(url)
        return FakeResponse(json_data=pages[url])

    monkeypatch.setattr(
        mit_edx_programs.requests,
        "post",
        lambda *_a, **_k: FakeResponse(json_data={"access_token": "tok"}),
    )
    monkeypatch.setattr(mit_edx_programs.requests, "get", _get)
    return fetched


def test_only_mit_programs_yielded(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    _mock_http(monkeypatch)
    source = mit_edx_programs.mit_edx_programs_source()
    records = list(source.resources["raw__edxorg__discovery__api__programs"])
    assert [r["uuid"] for r in records] == ["mit-1"]


def test_flattened_tables_keep_every_program(monkeypatch: pytest.MonkeyPatch) -> None:
    """The flattened program tables are unfiltered: MicroMasters and non-MIT too."""
    _set_creds(monkeypatch)
    _mock_http(monkeypatch)
    source = mit_edx_programs.mit_edx_programs_source()
    programs = list(source.resources["raw__edxorg__discovery__api__program"])
    assert [r["uuid"] for r in programs] == ["mit-1", "other-1", "mit-mm"]
    assert len({r["retrieved_at"] for r in programs}) == 1


def test_each_endpoint_is_read_once_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transformers share one extraction per endpoint and one retrieved_at."""
    _set_creds(monkeypatch)
    fetched = _mock_http(monkeypatch)
    source = mit_edx_programs.mit_edx_programs_source()
    rows = list(source)
    assert sorted(fetched) == [_COURSES_URL, _PROGRAMS_URL]
    program_times = {
        r["retrieved_at"]
        for r in rows
        if "program_uuid" in r or "banner_image_url" in r
    }
    assert len(program_times) == 1


def test_selected_resources_are_the_tables() -> None:
    source = mit_edx_programs.build_source()
    assert sorted(source.selected_resources) == [
        "raw__edxorg__discovery__api__mitx_course",
        "raw__edxorg__discovery__api__mitx_course_run",
        "raw__edxorg__discovery__api__program",
        "raw__edxorg__discovery__api__program_course",
        "raw__edxorg__discovery__api__programs",
    ]


def test_declared_column_types_survive_the_load_id_hint() -> None:
    """with_nullable_load_id must not replace a resource's own column hints."""
    source = mit_edx_programs.build_source()
    columns = source.resources["raw__edxorg__discovery__api__mitx_course_run"].columns
    assert columns["estimated_hours"]["data_type"] == "double"
    assert columns["_dlt_load_id"]["nullable"] is True


@pytest.mark.parametrize("profile", ["qa", "production"])
def test_deployed_profiles_read_the_oauth_client_from_vault(
    monkeypatch: pytest.MonkeyPatch, profile: str
) -> None:
    """The deployment has no EDX_API_* env, so Vault is the only credential source."""
    for var in (
        "EDX_API_CLIENT_ID",
        "EDX_API_CLIENT_SECRET",
        "EDX_API_ACCESS_TOKEN_URL",
        "EDX_PROGRAMS_API_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DLT_PROFILE", profile)
    reads: list[tuple[str, str]] = []

    def _read_kv_secret(mount: str, path: str) -> dict[str, str]:
        reads.append((mount, path))
        return {
            "id": "vault-cid",
            "secret": "vault-secret",  # pragma: allowlist secret
            "token_url": "https://api.edx.org/oauth2/v1/access_token",
            "url": "https://api.edx.org",
        }

    monkeypatch.setattr(vault, "read_kv_secret", _read_kv_secret)
    posted: dict[str, Any] = {}
    fetched: list[str] = []

    def _post(url: str, **kwargs: Any) -> FakeResponse:
        posted.update(url=url, data=kwargs["data"])
        return FakeResponse(json_data={"access_token": "tok"})

    def _get(url: str, **_kwargs: Any) -> FakeResponse:
        fetched.append(url)
        return FakeResponse(json_data=_PROGRAMS)

    monkeypatch.setattr(mit_edx_programs.requests, "post", _post)
    monkeypatch.setattr(mit_edx_programs.requests, "get", _get)

    source = mit_edx_programs.mit_edx_programs_source()
    records = list(source.resources["raw__edxorg__discovery__api__programs"])

    assert [r["uuid"] for r in records] == ["mit-1"]
    assert reads == [("secret-data", "pipelines/edx/edxorg/edx-oauth-client")]
    assert posted["url"] == "https://api.edx.org/oauth2/v1/access_token"
    assert posted["data"]["client_id"] == "vault-cid"
    assert fetched == [mit_edx_programs.EDX_PROGRAMS_API_URL]


@pytest.mark.integration
def test_mit_edx_programs_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_creds(monkeypatch)
    _mock_http(monkeypatch)
    pipeline = config.pipeline_for("mit_edx_programs")
    info = pipeline.run(mit_edx_programs.build_source())
    assert not info.has_failed_jobs

    dataset = pipeline.dataset()
    assert dataset["raw__edxorg__discovery__api__programs"].arrow().num_rows == 1
    assert dataset["raw__edxorg__discovery__api__program"].arrow().num_rows == 3
    assert dataset["raw__edxorg__discovery__api__program_course"].arrow().num_rows == 3
    assert dataset["raw__edxorg__discovery__api__mitx_course"].arrow().num_rows == 1
    runs = dataset["raw__edxorg__discovery__api__mitx_course_run"].arrow()
    assert runs.num_rows == 1
    # An int from the API still lands in the double column.
    assert str(runs.schema.field("estimated_hours").type) == "double"
    # ISO dates stay strings: staging and the programs delivery parse them.
    for column in ("retrieved_at", "start_on", "modified"):
        assert str(runs.schema.field(column).type) == "string", column
    programs = dataset["raw__edxorg__discovery__api__program"].arrow()
    assert str(programs.schema.field("retrieved_at").type) == "string"
    # A column null in every row of the load is still created.
    assert "announcement" in runs.column_names
    # Nested values stay JSON strings rather than becoming child tables.
    assert str(runs.schema.field("staff").type) == "string"
    assert not [name for name in pipeline.default_schema.tables if "__staff" in name]
