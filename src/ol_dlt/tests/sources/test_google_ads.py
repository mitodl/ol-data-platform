"""Unit + materialization tests for the Google Ads source."""

import importlib
import re
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dlt
import pytest
from google.ads.googleads.client import _DEFAULT_VERSION

from ol_dlt import config
from ol_dlt.sources import google_ads

GoogleAdsRow = importlib.import_module(
    f"google.ads.googleads.{_DEFAULT_VERSION}.services.types.google_ads_service"
).GoogleAdsRow

MANAGER_ID = "1000000000"
COURSE_ID = "2000000000"
TODAY = date(2026, 11, 9)


def _account(customer_id: str, *, manager: bool) -> Any:  # noqa: ANN401
    row = GoogleAdsRow()
    row.customer_client.id = int(customer_id)
    row.customer_client.manager = manager
    row.customer_client.status = "ENABLED"
    return row


def _keyword_row(day: str, clicks: int) -> Any:  # noqa: ANN401
    row = GoogleAdsRow()
    row.segments.date = day
    row.segments.ad_network_type = "SEARCH"
    row.campaign.id = 11
    row.campaign.name = "Course - Search - USA - Broad"
    row.ad_group.id = 22
    row.ad_group_criterion.criterion_id = 33
    row.ad_group_criterion.keyword.text = "systems thinking"
    row.ad_group_criterion.keyword.match_type = "BROAD"
    row.metrics.impressions = 40
    row.metrics.clicks = clicks
    return row


def _change_event(index: int, at: str) -> Any:  # noqa: ANN401
    row = GoogleAdsRow()
    row.change_event.resource_name = f"customers/{COURSE_ID}/changeEvents/{index}"
    row.change_event.change_date_time = at
    return row


def _match(pattern: str, text: str) -> re.Match[str]:
    match = re.search(pattern, text)
    assert match, f"{pattern!r} not in {text!r}"
    return match


class FakeClient:
    login_customer_id = MANAGER_ID

    def __init__(
        self,
        *,
        client_accounts: tuple[str, ...] = (COURSE_ID,),
        change_event_times: tuple[str, ...] = (),
    ) -> None:
        self.queries: list[tuple[str, str]] = []
        self.client_accounts = client_accounts
        self.change_event_times = change_event_times
        # Stamped on keyword rows, so a test can tell which run wrote a row.
        self.clicks = 0

    def get_service(self, _name: str) -> "FakeClient":
        return self

    def search_stream(self, *, customer_id: str, query: str) -> list[Any]:
        self.queries.append((customer_id, query))
        resource = _match(r" FROM (\w+)", query).group(1)
        if resource == "customer_client":
            rows = [_account(MANAGER_ID, manager=True)] + [
                _account(c, manager=False) for c in self.client_accounts
            ]
        elif resource == "keyword_view" and "metrics.impressions" in query:
            first, last = _match(r"BETWEEN '([\d-]+)' AND '([\d-]+)'", query).groups()
            rows = [_keyword_row(first, self.clicks), _keyword_row(last, self.clicks)]
        elif resource == "change_event":
            start, end = _match(r">= '([^']+)' AND .* < '([^']+)'", query).groups()
            limit = int(_match(r"LIMIT (\d+)", query).group(1))
            rows = [
                _change_event(i, at)
                for i, at in enumerate(self.change_event_times)
                if start <= at < end
            ][:limit]
        else:
            rows = []
        return [SimpleNamespace(results=rows)]


def _keyword_windows(client: FakeClient) -> list[tuple[str, str]]:
    return [
        (match.group(1), match.group(2))
        for match in (
            _match(r"BETWEEN '([\d-]+)' AND '([\d-]+)'", q)
            for _, q in client.queries
            if "FROM keyword_view" in q and "metrics.impressions > 0" in q
        )
    ]


def test_daily_reports_skip_managers_and_unset_share_is_null() -> None:
    client = FakeClient()
    source = google_ads.google_ads_source(
        client_factory=lambda: client, start_date="2026-10-01", today=TODAY
    )
    rows = list(source.resources["raw__google_ads__api__keyword_daily"])

    assert {c for c, q in client.queries if "FROM keyword_view" in q} == {COURSE_ID}
    assert _keyword_windows(client) == [
        ("2026-10-01", "2026-10-31"),
        ("2026-11-01", "2026-11-08"),
    ]
    assert rows[0]["customer_id"] == COURSE_ID
    assert rows[0]["ad_group_criterion__keyword__match_type"] == "BROAD"
    assert rows[0]["metrics__clicks"] == 0
    assert rows[0]["metrics__search_impression_share"] is None


def test_full_change_event_window_is_split_not_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(google_ads, "CHANGE_EVENT_LIMIT", 3)
    # Five events on one day: a whole-day query returns a full page of 3.
    times = tuple(f"2026-11-08 {hour:02d}:00:00" for hour in (1, 5, 9, 13, 20))
    client = FakeClient(change_event_times=times)
    source = google_ads.google_ads_source(client_factory=lambda: client, today=TODAY)

    rows = list(source.resources["raw__google_ads__api__change_events"])

    assert sorted(r["change_event__change_date_time"] for r in rows) == list(times)


def test_no_client_accounts_fails_the_run() -> None:
    client = FakeClient(client_accounts=())
    source = google_ads.google_ads_source(client_factory=lambda: client, today=TODAY)

    with pytest.raises(Exception, match="No enabled client accounts"):
        list(source.resources["raw__google_ads__api__keyword_daily"])


@pytest.mark.integration
def test_one_source_object_advances_each_run(
    sqlite_iceberg_lake: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = {"today": TODAY}

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> "FakeDatetime":  # noqa: ANN401
            day = clock["today"]
            return cls(day.year, day.month, day.day, 10, tzinfo=tz)

    monkeypatch.setattr(google_ads, "datetime", FakeDatetime)
    client = FakeClient()
    # Iceberg, as in production: plain parquet appends instead of merging.
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_test",
        destination=dlt.destinations.filesystem(
            bucket_url=sqlite_iceberg_lake.as_uri()
        ),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    source = google_ads.google_ads_source(
        client_factory=lambda: client, start_date="2026-06-01", lookback_days=7
    ).with_resources("raw__google_ads__api__keyword_daily")
    source.resources["raw__google_ads__api__keyword_daily"].apply_hints(
        table_format="iceberg"
    )
    source = config.with_nullable_load_id(source)

    def run(today: date) -> None:
        clock["today"] = today
        client.queries.clear()
        client.clicks += 1
        assert not pipeline.run(source).has_failed_jobs

    run(TODAY)
    run(date(2026, 11, 12))
    assert _keyword_windows(client) == [("2026-11-01", "2026-11-11")]
    run(date(2026, 11, 12))
    assert _keyword_windows(client) == [("2026-11-04", "2026-11-11")]

    table = pipeline.dataset()["raw__google_ads__api__keyword_daily"].arrow()
    dates = table.column("segments__date").to_pylist()
    clicks = dict(zip(dates, table.column("metrics__clicks").to_pylist(), strict=True))
    # Runs 2 and 3 both re-pull 11-11: one row, holding run 3's value.
    assert dates.count("2026-11-11") == 1
    assert clicks["2026-11-11"] == 3  # noqa: PLR2004
    assert len(dates) == len(set(dates))
