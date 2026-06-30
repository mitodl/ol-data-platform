"""Unit + materialization tests for the OLL source."""

from pathlib import Path

import pytest

from ol_dlt import config
from ol_dlt.sources import oll
from tests.conftest import FakeResponse

_CSV = (
    "readable_id,title,url\n"
    "course-v1:OLL+ABC,Intro to ABC,https://example.com/abc\n"
    "course-v1:OLL+XYZ,Advanced XYZ,https://example.com/xyz\n"
)


def test_oll_parses_csv_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        oll.requests,
        "get",
        lambda *_a, **_k: FakeResponse(content=_CSV.encode("utf-8")),
    )
    source = oll.oll_source()
    records = list(source.resources["raw__oll__google_sheets__courses"])
    assert [r["readable_id"] for r in records] == [
        "course-v1:OLL+ABC",
        "course-v1:OLL+XYZ",
    ]
    assert records[0]["title"] == "Intro to ABC"


@pytest.mark.integration
def test_oll_materialization(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        oll.requests,
        "get",
        lambda *_a, **_k: FakeResponse(content=_CSV.encode("utf-8")),
    )
    pipeline = config.pipeline_for("oll")
    info = pipeline.run(oll.oll_source())
    assert not info.has_failed_jobs

    table = pipeline.dataset()["raw__oll__google_sheets__courses"].arrow()
    assert table.num_rows == 2  # noqa: PLR2004
    assert {"readable_id", "title", "url"} <= set(table.column_names)
