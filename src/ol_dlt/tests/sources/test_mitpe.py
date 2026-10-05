"""Unit + materialization tests for the MIT Professional Education source."""

from pathlib import Path

import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from ol_dlt import config
from ol_dlt.sources import mitpe
from tests.conftest import FakeResponse

_PAGES: dict[int, list[dict[str, str]]] = {
    0: [
        {"title": "Course 1", "url": "https://pe.mit.edu/1"},
        {"title": "Course 2", "url": "https://pe.mit.edu/2"},
    ],
    1: [],
}
_NEWS = [
    {
        "id": "6eda0dd3-b3be-4e47-9f22-784bbaf9ae3d",
        "title": "The ripple effect of learning at MIT",
        "date": "2026-06-23",
        "author": "MIT Professional Education",
        "summary": "Summary",
        "image": "/sites/default/files/news.png",
        "url": "/news/articles/ripple-effect-learning-mit",
    }
]
_EVENTS = [
    {
        "id": "4d608f71-d97e-4dc8-b212-e09922d2e663",
        "title": "Open House: Crisis Management &amp; Business Resiliency",
        "start_date": "2026-09-24",
        "end_date": "2026-09-24",
        "time_range": "12:30PM EDT",
        "summary": "Summary",
        "image": "",
        "url": "/events/open-house",
    }
]


def _fake_get(url: str, *, params: dict[str, int], **_kwargs: object) -> FakeResponse:
    if url.endswith("/feeds/news/"):
        return FakeResponse(json_data=_NEWS if params["page"] == 0 else [])
    if url.endswith("/feeds/events/"):
        return FakeResponse(json_data=_EVENTS if params["page"] == 0 else [])
    return FakeResponse(json_data=_PAGES[params["page"]])


def test_mitpe_paginates_until_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mitpe.requests, "get", _fake_get)
    source = mitpe.mitpe_source()
    records = list(source.resources["raw__mitpe__api__courses"])
    assert [r["title"] for r in records] == ["Course 1", "Course 2"]


@pytest.mark.integration
def test_mitpe_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mitpe.requests, "get", _fake_get)
    pipeline = config.pipeline_for("mitpe")
    info = pipeline.run(mitpe.mitpe_source())
    assert not info.has_failed_jobs

    table = pipeline.dataset()["raw__mitpe__api__courses"].arrow()
    assert table.num_rows == 2  # noqa: PLR2004
    assert {"title", "url"} <= set(table.column_names)

    news = pipeline.dataset()["raw__mitpe__api__news"].arrow()
    assert news.column("date").to_pylist() == ["2026-06-23"]
    events = pipeline.dataset()["raw__mitpe__api__events"].arrow()
    assert events.column("time_range").to_pylist() == ["12:30PM EDT"]
    assert events.schema.field("start_date").type == "string"


def test_feed_rows_carry_retrieved_at(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mitpe.requests, "get", _fake_get)
    source = mitpe.mitpe_source()
    (event,) = list(source.resources["raw__mitpe__api__events"])
    assert event["id"] == _EVENTS[0]["id"]
    assert event["retrieved_at"]


@pytest.mark.integration
def test_transient_empty_page_does_not_truncate_table(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that ends early on a transient empty page must not shrink the table.

    write_disposition="replace" means a truncated fetch WOULD normally
    overwrite the table with a short result. config.guard_against_replace_truncation
    refuses to commit a fetch whose row count dropped sharply from the last
    successful load, so the run fails loudly instead, and the table -- since
    the pipeline fails during extract, before normalize/load ever runs --
    keeps its previous, correct row count.
    """
    monkeypatch.setattr(mitpe.requests, "get", _fake_get)
    pipeline = config.pipeline_for("mitpe")
    pipeline.run(mitpe.mitpe_source())
    assert pipeline.dataset()["raw__mitpe__api__courses"].arrow().num_rows == 2  # noqa: PLR2004

    monkeypatch.setattr(
        mitpe.requests,
        "get",
        lambda *_a, **_k: FakeResponse(json_data=[]),
    )
    with pytest.raises(PipelineStepFailed, match="refusing to replace"):
        pipeline.run(mitpe.mitpe_source())
    assert pipeline.dataset()["raw__mitpe__api__courses"].arrow().num_rows == 2  # noqa: PLR2004
