"""Unit + materialization tests for the OLL source."""

from pathlib import Path

import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

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


@pytest.mark.integration
def test_truncated_csv_fetch_does_not_truncate_table(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A truncated/empty CSV fetch must not replace a good table with an empty one.

    write_disposition="replace" means a truncated fetch would normally
    overwrite the table with a short result.
    config.guard_against_replace_truncation refuses to commit a fetch whose
    row count dropped sharply from the last successful load, so the run fails
    loudly instead, and the table keeps its previous, correct row count.
    """
    monkeypatch.setattr(
        oll.requests,
        "get",
        lambda *_a, **_k: FakeResponse(content=_CSV.encode("utf-8")),
    )
    pipeline = config.pipeline_for("oll")
    pipeline.run(oll.oll_source())
    assert pipeline.dataset()["raw__oll__google_sheets__courses"].arrow().num_rows == 2  # noqa: PLR2004

    monkeypatch.setattr(
        oll.requests,
        "get",
        lambda *_a, **_k: FakeResponse(content=b"readable_id,title,url\n"),
    )
    with pytest.raises(PipelineStepFailed, match="refusing to replace"):
        pipeline.run(oll.oll_source())
    assert pipeline.dataset()["raw__oll__google_sheets__courses"].arrow().num_rows == 2  # noqa: PLR2004


@pytest.mark.integration
def test_source_instance_reusable_across_runs(
    test_profile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single source instance must re-extract on every run.

    The Dagster wrapper builds one source via build_source() at import time and
    reuses it across materializations (see defs/ingestion/assets.py). dlt builds
    resources from generator *functions*, so each pipeline.run() re-invokes them
    — guard against a regression where the instance is consumed after one run and
    later runs silently produce no rows.
    """
    monkeypatch.setattr(
        oll.requests,
        "get",
        lambda *_a, **_k: FakeResponse(content=_CSV.encode("utf-8")),
    )
    source = oll.build_source()  # one instance, reused below
    pipeline = config.pipeline_for("oll")

    for _ in range(2):
        pipeline.run(source)
        counts = pipeline.last_trace.last_normalize_info.row_counts
        assert counts.get("raw__oll__google_sheets__courses") == 2  # noqa: PLR2004
