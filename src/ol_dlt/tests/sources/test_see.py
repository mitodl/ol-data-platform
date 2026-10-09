"""Unit + materialization tests for the Sloan Executive Education source."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from ol_dlt import config, oauth, vault
from ol_dlt.sources import see
from tests.conftest import FakeResponse

COURSES = [
    {
        "Course_Id": "a0R3l00000AbCdE",
        "Title": "Leading Change",
        "URL": "https://exec.mit.edu/leading-change",
        "Topics": "Leadership: Change Management",
        "SourceLastModifiedDate": "2026-09-11T17:05:14.000Z",
    },
]
OFFERINGS = [
    {
        "CO_Title": "Leading Change (June 2026)",
        "Course_Id": "a0R3l00000AbCdE",
        "Delivery": "Online",
        "Format": "Asynchronous (On-Demand)",
        "Tuition_Cost(non-USD)": None,
    },
]
_RESPONSES = {
    f"{see.SEE_API_URL}courses": COURSES,
    f"{see.SEE_API_URL}course-offerings": OFFERINGS,
}


def _fake_post(_url: str, **_kwargs: Any) -> FakeResponse:
    return FakeResponse(json_data={"access_token": "tok"})


def _fake_get(url: str, **_kwargs: Any) -> FakeResponse:
    return FakeResponse(json_data=_RESPONSES[url])


@pytest.fixture
def fake_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEE_API_CLIENT_ID", "cid")
    monkeypatch.setenv("SEE_API_CLIENT_SECRET", "secret")  # noqa: S105
    monkeypatch.setenv("SEE_API_ACCESS_TOKEN_URL", "https://see/token")
    monkeypatch.setattr(see.requests, "post", _fake_post)
    monkeypatch.setattr(see.requests, "get", _fake_get)


@pytest.mark.usefixtures("fake_api")
def test_records_are_loaded_whole() -> None:
    """The offering's Format is what the delivery extract dropped."""
    source = see.see_source()
    offerings = list(source.resources["raw__see__api__course_offerings"])
    assert offerings[0]["Format"] == "Asynchronous (On-Demand)"
    assert "retrieved_at" in offerings[0]
    assert offerings[0]["api_position"] == 0
    courses = list(source.resources["raw__see__api__courses"])
    assert [c["Course_Id"] for c in courses] == ["a0R3l00000AbCdE"]


@pytest.mark.parametrize("profile", ["qa", "production"])
def test_deployed_profiles_read_the_oauth_client_from_vault(
    monkeypatch: pytest.MonkeyPatch, profile: str
) -> None:
    for var in (
        "SEE_API_CLIENT_ID",
        "SEE_API_CLIENT_SECRET",
        "SEE_API_ACCESS_TOKEN_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DLT_PROFILE", profile)
    reads: list[tuple[str, str]] = []

    def _read_kv_secret(mount: str, path: str) -> dict[str, str]:
        reads.append((mount, path))
        return {
            "id": "vault-cid",
            "secret": "vault-secret",  # pragma: allowlist secret
            "token_url": "https://see/oauth/token",
            "url": "https://see",
        }

    posted: dict[str, Any] = {}

    def _post(url: str, **kwargs: Any) -> FakeResponse:
        posted.update(url=url, data=kwargs["data"])
        return FakeResponse(json_data={"access_token": "tok"})

    monkeypatch.setattr(vault, "read_kv_secret", _read_kv_secret)
    monkeypatch.setattr(see.requests, "post", _post)
    monkeypatch.setattr(see.requests, "get", _fake_get)

    list(see.see_source().resources["raw__see__api__courses"])

    assert reads == [(oauth.KV_MOUNT, see.SEE_OAUTH_VAULT_PATH)]
    assert posted["url"] == "https://see/oauth/token"
    assert posted["data"]["client_id"] == "vault-cid"


@pytest.mark.integration
@pytest.mark.usefixtures("fake_api")
def test_see_materialization(test_profile: Path) -> None:
    pipeline = config.pipeline_for("see")
    info = pipeline.run(see.build_source())
    assert not info.has_failed_jobs

    offerings = pipeline.dataset()["raw__see__api__course_offerings"].arrow()
    assert offerings.num_rows == 1
    assert {"co_title", "course_id", "delivery", "format"} <= set(
        offerings.column_names
    )
    # Currency and Faculty_Name are absent or null in the fixture; staging reads them.
    assert set(see.COURSE_OFFERING_COLUMNS) <= set(offerings.column_names)
    courses = pipeline.dataset()["raw__see__api__courses"].arrow()
    assert set(see.COURSE_COLUMNS) <= set(courses.column_names)
    assert pa.types.is_string(courses.schema.field("retrieved_at").type)
    assert pa.types.is_string(courses.schema.field("source_last_modified_date").type)
    assert pa.types.is_int64(courses.schema.field("api_position").type)


@pytest.mark.integration
@pytest.mark.usefixtures("fake_api")
def test_empty_response_does_not_truncate_table(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = config.pipeline_for("see")
    pipeline.run(see.build_source())

    monkeypatch.setattr(
        see.requests, "get", lambda *_a, **_k: FakeResponse(json_data=[])
    )
    with pytest.raises(PipelineStepFailed, match="refusing to replace"):
        pipeline.run(see.build_source())
    assert pipeline.dataset()["raw__see__api__courses"].arrow().num_rows == 1
