"""Unit + materialization tests for the MIT edX programs source."""

from pathlib import Path
from typing import Any

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from ol_dlt import config
from ol_dlt.sources import mit_edx_programs
from tests.conftest import FakeResponse

_PROGRAMS: dict[str, Any] = {
    "results": [
        {
            "uuid": "mit-1",
            "status": "active",
            "type": "professional certificate",
            "authoring_organizations": [{"key": "MITx"}],
        },
        {
            "uuid": "other-1",
            "status": "active",
            "type": "professional certificate",
            "authoring_organizations": [{"key": "HarvardX"}],
        },
        {
            "uuid": "mit-mm",
            "status": "active",
            "type": "MicroMasters",
            "authoring_organizations": [{"key": "MITx"}],
        },
    ],
    "next": None,
}


def test_is_mit_program_filters() -> None:
    assert mit_edx_programs._is_mit_program(_PROGRAMS["results"][0]) is True
    assert mit_edx_programs._is_mit_program(_PROGRAMS["results"][1]) is False
    assert mit_edx_programs._is_mit_program(_PROGRAMS["results"][2]) is False


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
    monkeypatch.setenv("EDX_PROGRAMS_API_URL", "https://edx/programs")


def _mock_http(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        mit_edx_programs.requests,
        "post",
        lambda *_a, **_k: FakeResponse(json_data={"access_token": "tok"}),
    )
    monkeypatch.setattr(
        mit_edx_programs.requests,
        "get",
        lambda *_a, **_k: FakeResponse(json_data=_PROGRAMS),
    )


def test_only_mit_programs_yielded(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    _mock_http(monkeypatch)
    source = mit_edx_programs.mit_edx_programs_source()
    records = list(source.resources["raw__edxorg__discovery__api__programs"])
    assert [r["uuid"] for r in records] == ["mit-1"]


@pytest.mark.integration
def test_mit_edx_programs_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_creds(monkeypatch)
    _mock_http(monkeypatch)
    pipeline = config.pipeline_for("mit_edx_programs")
    info = pipeline.run(mit_edx_programs.mit_edx_programs_source())
    assert not info.has_failed_jobs

    table = pipeline.dataset()["raw__edxorg__discovery__api__programs"].arrow()
    assert table.num_rows == 1
    assert "uuid" in table.column_names
