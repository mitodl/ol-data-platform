"""The Google Ads asset's load order: other tables, backfill batches, lookback."""

import logging
from types import SimpleNamespace
from typing import Any

import pytest
from dagster import MaterializeResult
from data_loading.defs.ingestion import assets
from ol_dlt.sources import google_ads


class _FakeDlt:
    def __init__(self, pipeline: Any, windows: list[int]) -> None:
        self.pipeline = pipeline
        self.windows = windows
        self.loads: list[str] = []

    def run(self, *, dlt_source: Any, **_: Any) -> Any:
        tables = set(dlt_source.selected_resources)
        backfill = (
            dlt_source.resources[
                "raw__google_ads__api__keyword_daily"
            ].write_disposition
            == "append"
        )
        fetched = (self.windows.pop(0) if self.windows else 0) if backfill else 0
        self.loads.append("backfill" if backfill else ",".join(sorted(tables)))
        self.pipeline.last_trace = SimpleNamespace(
            last_normalize_info=SimpleNamespace(
                row_counts={google_ads.FETCH_LOG_TABLE: fetched}
            )
        )
        return iter(
            MaterializeResult(asset_key=["ol_warehouse_raw_data", table])
            for table in tables
        )


def _run(monkeypatch: pytest.MonkeyPatch, windows: list[int]) -> list[str]:
    pipeline = SimpleNamespace(last_trace=None)
    monkeypatch.setattr(google_ads, "google_ads_pipeline", pipeline)
    dlt = _FakeDlt(pipeline, windows)
    context = SimpleNamespace(
        is_subset=False,
        selected_asset_keys=set(),
        log=logging.getLogger("test_google_ads_assets"),
    )
    list(assets.google_ads_assets.op.compute_fn.decorated_fn(context, dlt))
    return dlt.loads


def test_other_tables_then_backfill_then_lookback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loads = _run(monkeypatch, [150, 150, 0])

    assert loads[0] == ",".join(sorted(assets._GOOGLE_ADS_OTHER_TABLES))
    assert loads[1:4] == ["backfill"] * 3
    assert loads[4] == ",".join(sorted(assets._GOOGLE_ADS_DAILY_TABLES))


def test_an_undrained_backfill_skips_the_lookback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(assets, "_MAX_BATCHES_PER_RUN", 2)

    loads = _run(monkeypatch, [150, 150, 150])

    assert loads[1:] == ["backfill", "backfill"]
