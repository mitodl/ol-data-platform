"""Unit + materialization tests for the Google Ads source."""

import importlib
import re
from datetime import date, datetime, timedelta
from pathlib import Path
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
        # Dates the next keyword fetch leaves out, as if Google retracted them.
        self.dropped_dates: set[str] = set()
        # A keyword window starting on this date fails, as a killed pod would.
        self.fail_window: str | None = None

    def get_service(self, _name: str) -> "FakeClient":
        return self

    def search(self, *, customer_id: str, query: str) -> list[Any]:
        self.queries.append((customer_id, query))
        resource = _match(r" FROM (\w+)", query).group(1)
        if resource == "customer_client":
            rows = [_account(MANAGER_ID, manager=True)] + [
                _account(c, manager=False) for c in self.client_accounts
            ]
        elif resource == "keyword_view" and "metrics.impressions" in query:
            first, last = _match(r"BETWEEN '([\d-]+)' AND '([\d-]+)'", query).groups()
            if first == self.fail_window:
                msg = f"window {first} failed"
                raise RuntimeError(msg)
            rows = [
                _keyword_row(day, self.clicks)
                for day in (first, last)
                if day not in self.dropped_dates
            ]
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
        return rows


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
    rows = [
        r
        for r in source.resources["raw__google_ads__api__keyword_daily"]
        if isinstance(r, dict)
    ]

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


def test_full_one_second_window_fails_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(google_ads, "CHANGE_EVENT_LIMIT", 3)
    client = FakeClient(change_event_times=("2026-11-08 05:00:00",) * 4)
    source = google_ads.google_ads_source(client_factory=lambda: client, today=TODAY)

    with pytest.raises(Exception, match="can't return them all"):
        list(source.resources["raw__google_ads__api__change_events"])


def test_report_window_stays_inside_the_retention_limit() -> None:
    assert google_ads.report_window(None, "2021-01-01", 90, TODAY) == (
        date(2023, 10, 10),
        date(2026, 11, 8),
    )


def test_no_client_accounts_fails_the_run() -> None:
    client = FakeClient(client_accounts=())
    source = google_ads.google_ads_source(client_factory=lambda: client, today=TODAY)

    with pytest.raises(Exception, match="No enabled client accounts"):
        list(source.resources["raw__google_ads__api__keyword_daily"])


@pytest.mark.integration
def test_one_source_object_advances_each_run(
    sqlite_iceberg_lake: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock: dict[str, Any] = {"today": TODAY, "minute": 0}

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> "FakeDatetime":  # noqa: ANN401
            day = clock["today"]
            return cls(day.year, day.month, day.day, 10, clock["minute"], tzinfo=tz)

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
        clock["minute"] += 1
        client.queries.clear()
        client.clicks += 1
        assert not pipeline.run(source).has_failed_jobs

    run(TODAY)
    run(date(2026, 11, 12))
    assert _keyword_windows(client) == [("2026-11-01", "2026-11-11")]
    run(date(2026, 11, 12))
    assert _keyword_windows(client) == [("2026-11-04", "2026-11-11")]
    client.dropped_dates = {"2026-11-11"}
    run(date(2026, 11, 12))

    dataset = pipeline.dataset()
    table = dataset["raw__google_ads__api__keyword_daily"].arrow().to_pylist()
    dates = [r["segments__date"] for r in table]
    assert len(dates) == len(set(dates))
    by_date = {r["segments__date"]: r for r in table}
    # Runs 3 and 4 both re-pull 11-04: one row, holding run 4's value.
    assert by_date["2026-11-04"]["metrics__clicks"] == 4  # noqa: PLR2004

    fetches = dataset["raw__google_ads__api__fetch_log"].arrow().to_pylist()
    # One per window: run 1's 06-01 to 11-08 is six 31-day windows.
    assert len(fetches) == 9  # noqa: PLR2004

    def is_current(row: dict[str, Any]) -> bool:
        covering = [
            f["retrieved_at"]
            for f in fetches
            if f["window_start"] <= row["segments__date"] <= f["window_end"]
        ]
        return row["retrieved_at"] == max(covering)

    # Run 4 re-read 11-11 and Google no longer returned it, so staging drops it.
    assert is_current(by_date["2026-11-04"])
    assert not is_current(by_date["2026-11-11"])


@pytest.mark.integration
def test_backfill_loads_in_batches_and_resumes_after_a_failure(
    sqlite_iceberg_lake: Path, tmp_path: Path
) -> None:
    client = FakeClient()
    destination = dlt.destinations.filesystem(bucket_url=sqlite_iceberg_lake.as_uri())
    pipelines_dir = str(tmp_path / "pipelines")
    keyword_table = "raw__google_ads__api__keyword_daily"

    def run(backfill_windows: int | None) -> Any:  # noqa: ANN401
        # Dagster drops the local pipeline before each load, so cursors come
        # back from the destination.
        pipeline = dlt.pipeline(
            pipeline_name="google_ads_backfill_test",
            destination=destination,
            dataset_name="raw",
            pipelines_dir=pipelines_dir,
        )
        pipeline.drop()
        pipeline = dlt.pipeline(
            pipeline_name="google_ads_backfill_test",
            destination=destination,
            dataset_name="raw",
            pipelines_dir=pipelines_dir,
        )
        source = google_ads.google_ads_source(
            client_factory=lambda: client,
            start_date="2026-06-01",
            today=TODAY,
            backfill_windows=backfill_windows,
        ).with_resources(keyword_table)
        source.resources[keyword_table].apply_hints(table_format="iceberg")
        client.queries.clear()
        pipeline.run(config.with_nullable_load_id(source))
        return pipeline

    # 06-01 to 11-08 is six windows: two batches of 2 windows each.
    pipeline = run(2)
    assert _keyword_windows(client) == [
        ("2026-06-01", "2026-07-01"),
        ("2026-07-02", "2026-08-01"),
    ]
    pipeline = run(2)
    assert _keyword_windows(client)[0] == ("2026-08-02", "2026-09-01")
    tables = pipeline.default_schema.tables
    assert tables[keyword_table]["write_disposition"] == "append"

    # A failed batch saves no cursor, so the next one re-reads its windows.
    client.fail_window = "2026-11-03"
    with pytest.raises(Exception, match="window 2026-11-03 failed"):
        run(2)
    client.fail_window = None
    pipeline = run(2)
    assert _keyword_windows(client) == [
        ("2026-10-03", "2026-11-02"),
        ("2026-11-03", "2026-11-08"),
    ]

    pipeline = run(2)
    assert _keyword_windows(client) == []

    # Once current, a normal run merges its lookback over the appended rows.
    pipeline = run(None)
    rows = pipeline.dataset()[keyword_table].arrow().to_pylist()
    dates = [r["segments__date"] for r in rows]
    assert len(dates) == len(set(dates))
    assert {"2026-06-01", "2026-11-08"} <= set(dates)


def test_snapshots_append_and_daily_reports_merge() -> None:
    source = google_ads.google_ads_source(client_factory=FakeClient, today=TODAY)

    for name in google_ads._SNAPSHOTS:  # noqa: SLF001
        assert source.resources[name].write_disposition == "append"
    assert source.resources[
        "raw__google_ads__api__keyword_daily"
    ].write_disposition == ("merge")


@pytest.mark.integration
def test_a_snapshot_is_taken_once_per_day(
    sqlite_iceberg_lake: Path, tmp_path: Path
) -> None:
    client = FakeClient()
    destination = dlt.destinations.filesystem(bucket_url=sqlite_iceberg_lake.as_uri())
    table = "raw__google_ads__api__campaign_snapshots"

    def run(today: date) -> list[str]:
        # Dropped first, as Dagster does, so the state comes from the destination.
        dlt.pipeline(
            pipeline_name="google_ads_snapshot_test",
            destination=destination,
            dataset_name="raw",
            pipelines_dir=str(tmp_path / "pipelines"),
        ).drop()
        pipeline = dlt.pipeline(
            pipeline_name="google_ads_snapshot_test",
            destination=destination,
            dataset_name="raw",
            pipelines_dir=str(tmp_path / "pipelines"),
        )
        source = google_ads.google_ads_source(
            client_factory=lambda: client, today=today
        ).with_resources(table)
        source.resources[table].apply_hints(table_format="iceberg")
        client.queries.clear()
        pipeline.run(config.with_nullable_load_id(source))
        return [q for _, q in client.queries if "FROM campaign " in q]

    assert len(run(TODAY)) == 1
    assert run(TODAY) == []
    assert len(run(TODAY + timedelta(days=1))) == 1
