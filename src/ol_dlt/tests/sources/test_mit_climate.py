"""Unit + materialization tests for the MIT Climate source."""

from pathlib import Path

import pytest

from ol_dlt import config
from ol_dlt.sources import mit_climate
from tests.conftest import FakeResponse

_EXPLAINERS = [{"uuid": "a", "title": "Explainer A"}]
_ASK = [{"uuid": "b", "title": "Ask B"}]


def _fake_get(url: str, **_kwargs: object) -> FakeResponse:
    payload = _EXPLAINERS if "explainers" in url else _ASK
    return FakeResponse(json_data=payload)


def test_mit_climate_tags_feed_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mit_climate.requests, "get", _fake_get)
    source = mit_climate.mit_climate_source()
    records = list(source.resources["raw__mit_climate__api__articles"])
    by_uuid = {r["uuid"]: r for r in records}
    assert by_uuid["a"]["feed_type"] == "explainer"
    assert by_uuid["b"]["feed_type"] == "ask_mit_climate"


@pytest.mark.integration
def test_mit_climate_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mit_climate.requests, "get", _fake_get)
    pipeline = config.pipeline_for("mit_climate")
    info = pipeline.run(mit_climate.mit_climate_source())
    assert not info.has_failed_jobs

    table = pipeline.dataset()["raw__mit_climate__api__articles"].arrow()
    assert table.num_rows == 2  # noqa: PLR2004
    assert "feed_type" in table.column_names
