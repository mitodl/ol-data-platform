"""Google Ads ingestion via dlt.

Loads the Search campaign data the Google Ads budget optimizer (mitodl/ad_opt_v2)
reads, for every client account under the MIT Open Learning manager account. The
GAQL follows ad_opt_v2's queries (``utils/gaql_queries.py``,
``scripts/pull_change_events.py``) with ids added and its campaign-name filters
left to staging.

Data flow, per client account:
    customer_client     -> raw__google_ads__api__customer_clients    (replace)
    keyword_view        -> raw__google_ads__api__keyword_daily       (merge)
    keyword_view        -> raw__google_ads__api__keyword_conversions_daily (merge)
    campaign            -> raw__google_ads__api__campaign_daily      (merge)
    change_event        -> raw__google_ads__api__change_events       (merge)
    campaign            -> raw__google_ads__api__campaign_snapshots  (merge)
    ad_group_criterion  -> raw__google_ads__api__keyword_snapshots   (merge)
    campaign_criterion  -> raw__google_ads__api__campaign_criterion_snapshots
    campaign_shared_set -> raw__google_ads__api__campaign_shared_set_snapshots
    shared_criterion    -> raw__google_ads__api__shared_criterion_snapshots
    (every report and snapshot fetch) -> raw__google_ads__api__fetch_log (merge)

A merge never deletes, so a conversion Google retracts, or an entity removed
between two snapshots on one day, keeps its old row. The fetch log records each
account window a report or snapshot table re-read, with that fetch's
``retrieved_at``. Staging keeps a row only if its ``retrieved_at`` is the latest
fetch covering its date (or snapshot date); this holds for empty fetches too.

Credentials: the qa and production profiles read Vault
``secret-data/pipelines/google_ads``, which ol-infrastructure's dagster stack
populates in production only (the credential can change bids on the live
accounts, so it stays out of QA) with three keys copied from the Concourse
secret ``secret-concourse/main/google_ads_optimization``:

    google_ads_json         service-account key JSON
    google_developer_token  Google Ads API developer token
    google_manager_account  manager account id, sent as login_customer_id

Any other profile reads a ``google-ads.yaml`` from the path in
GOOGLE_ADS_CONFIG_PATH, the same file ad_opt_v2 uses.

Run standalone:
    DLT_PROFILE=ci GOOGLE_ADS_CONFIG_PATH=~/google-ads.yaml \
        python -m ol_dlt.sources.google_ads
"""

import functools
import json
import logging
import tempfile
from collections.abc import Callable, Generator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dlt
import proto
from dateutil.relativedelta import relativedelta
from google.ads.googleads.client import GoogleAdsClient

from ol_dlt import config, vault

logger = logging.getLogger(__name__)

GOOGLE_ADS_VAULT_MOUNT = "secret-data"
GOOGLE_ADS_VAULT_PATH = "pipelines/google_ads"

DEFAULT_START_DATE = "2021-01-01"
# Customers come from a HubSpot offline import days to weeks after the click,
# and Google restates past days' conversions, so each run re-pulls this window.
DEFAULT_LOOKBACK_DAYS = 90
REPORT_CHUNK_DAYS = 31
# Google rejects segments.date older than 37 months, since 2026-06-01:
# https://ads-developers.googleblog.com/2026/05/new-data-retention-policy-for-google.html
REPORT_RETENTION = relativedelta(months=37)

# The API keeps 30 days of change events and caps a query at 10,000 rows.
CHANGE_EVENT_RETENTION_DAYS = 29
CHANGE_EVENT_LIMIT = 10_000
CHANGE_EVENT_MIN_WINDOW = timedelta(seconds=1)

FETCH_LOG_TABLE = "raw__google_ads__api__fetch_log"
FETCH_LOG_KEY = (
    "customer_id",
    "table_name",
    "window_start",
    "window_end",
    "retrieved_at",
)
FETCH_LOG_COLUMNS: dict[str, dict[str, Any]] = {
    **{name: {"data_type": "text", "nullable": False} for name in FETCH_LOG_KEY},
    # The same type dlt infers for retrieved_at on the data tables, so they join.
    "retrieved_at": {"data_type": "timestamp", "nullable": False},
    **config.DLT_LOAD_ID_COLUMN,
}

CUSTOMER_CLIENT_FIELDS = (
    "customer_client.id",
    "customer_client.descriptive_name",
    "customer_client.manager",
    "customer_client.status",
    "customer_client.level",
    "customer_client.currency_code",
    "customer_client.time_zone",
)

_CAMPAIGN_FIELDS = (
    "campaign.id",
    "campaign.name",
    "campaign.status",
    "campaign.advertising_channel_type",
)
_KEYWORD_FIELDS = (
    "ad_group.id",
    "ad_group.name",
    "ad_group_criterion.criterion_id",
    "ad_group_criterion.keyword.text",
    "ad_group_criterion.keyword.match_type",
)

KEYWORD_DAILY_FIELDS = (
    "segments.date",
    "segments.ad_network_type",
    "customer.currency_code",
    *_CAMPAIGN_FIELDS,
    *_KEYWORD_FIELDS,
    "ad_group_criterion.position_estimates.first_page_cpc_micros",
    "metrics.impressions",
    "metrics.clicks",
    "metrics.cost_micros",
    "metrics.search_impression_share",
)
KEYWORD_CONVERSION_FIELDS = (
    "segments.date",
    "segments.ad_network_type",
    "segments.conversion_action",
    "segments.conversion_action_name",
    *_CAMPAIGN_FIELDS,
    *_KEYWORD_FIELDS,
    "metrics.conversions",
    "metrics.all_conversions",
)
CAMPAIGN_DAILY_FIELDS = (
    "segments.date",
    *_CAMPAIGN_FIELDS,
    "campaign_budget.amount_micros",
    "metrics.impressions",
    "metrics.clicks",
    "metrics.cost_micros",
    "metrics.search_impression_share",
    "metrics.search_budget_lost_impression_share",
    "metrics.search_rank_lost_impression_share",
)
# user_email is left out: it identifies a person and the optimizer doesn't use it.
CHANGE_EVENT_FIELDS = (
    "change_event.resource_name",
    "change_event.change_date_time",
    "change_event.change_resource_type",
    "change_event.change_resource_name",
    "change_event.resource_change_operation",
    "change_event.changed_fields",
    "change_event.client_type",
    "change_event.campaign",
    "change_event.ad_group",
    "change_event.old_resource",
    "change_event.new_resource",
)
CHANGE_EVENT_RESOURCE_TYPES = (
    "CAMPAIGN",
    "CAMPAIGN_BUDGET",
    "AD_GROUP",
    "AD_GROUP_CRITERION",
    "CAMPAIGN_CRITERION",
)

_SNAPSHOTS: dict[str, tuple[str, tuple[str, ...], str]] = {
    "raw__google_ads__api__campaign_snapshots": (
        "campaign",
        (
            "campaign.resource_name",
            *_CAMPAIGN_FIELDS,
            "campaign.start_date_time",
            "campaign.end_date_time",
            "campaign.bidding_strategy_type",
            "campaign_budget.resource_name",
            "campaign_budget.amount_micros",
            "campaign_budget.reference_count",
        ),
        "campaign.status != 'REMOVED'",
    ),
    "raw__google_ads__api__keyword_snapshots": (
        "ad_group_criterion",
        (
            "ad_group_criterion.resource_name",
            "campaign.id",
            "ad_group.status",
            *_KEYWORD_FIELDS,
            "ad_group_criterion.status",
            "ad_group_criterion.negative",
        ),
        "ad_group_criterion.type = 'KEYWORD' "
        "AND ad_group_criterion.status != 'REMOVED' "
        "AND ad_group.status != 'REMOVED' AND campaign.status != 'REMOVED'",
    ),
    "raw__google_ads__api__campaign_criterion_snapshots": (
        "campaign_criterion",
        (
            "campaign_criterion.resource_name",
            "campaign.id",
            "campaign_criterion.type",
            "campaign_criterion.negative",
            "campaign_criterion.keyword.text",
            "campaign_criterion.keyword.match_type",
            "campaign_criterion.location.geo_target_constant",
        ),
        "campaign_criterion.type IN ('KEYWORD', 'LOCATION') "
        "AND campaign_criterion.status != 'REMOVED' AND campaign.status != 'REMOVED'",
    ),
    "raw__google_ads__api__campaign_shared_set_snapshots": (
        "campaign_shared_set",
        (
            "campaign_shared_set.resource_name",
            "campaign.id",
            "shared_set.id",
            "shared_set.name",
            "shared_set.type",
            "campaign_shared_set.status",
        ),
        "campaign_shared_set.status = 'ENABLED' AND campaign.status != 'REMOVED'",
    ),
    "raw__google_ads__api__shared_criterion_snapshots": (
        "shared_criterion",
        (
            "shared_criterion.resource_name",
            "shared_set.id",
            "shared_criterion.type",
            "shared_criterion.keyword.text",
            "shared_criterion.keyword.match_type",
        ),
        "shared_set.status = 'ENABLED' AND shared_criterion.type = 'KEYWORD'",
    ),
}


def column_name(field: str) -> str:
    """Map a GAQL field path to its raw column name."""
    return field.replace(".", "__")


def _attr(obj: Any, name: str) -> Any:  # noqa: ANN401
    # proto-plus renames fields that clash with Python names (type -> type_).
    try:
        return getattr(obj, name)
    except AttributeError:
        return getattr(obj, f"{name}_")


def _plain(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, proto.Enum):
        return value.name
    if isinstance(value, proto.Message):
        return type(value).to_json(
            value,
            use_integers_for_enums=False,
            preserving_proto_field_name=True,
            indent=None,
            # Otherwise every unset field prints as its default, which reads as a
            # change when staging compares old_resource with new_resource.
            always_print_fields_with_no_presence=False,
        )
    if hasattr(value, "paths"):
        return ",".join(value.paths)
    return value


def flatten_row(row: Any, fields: tuple[str, ...]) -> dict[str, Any]:  # noqa: ANN401
    """Return one flat dict of ``fields`` read from a ``GoogleAdsRow``."""
    record = {}
    for field in fields:
        *path, leaf = field.split(".")
        parent = row
        for part in path:
            parent = _attr(parent, part)
        value = _attr(parent, leaf)
        # Google leaves impression share unset when a row isn't eligible; proto
        # would read that as 0.0, a real 0% share.
        if isinstance(value, float) and leaf not in parent:
            value = None
        record[column_name(field)] = _plain(value)
    return record


def build_query(
    resource: str, fields: tuple[str, ...], where: str | None = None
) -> str:
    # GAQL from module constants and computed dates, never from user input.
    query = f"SELECT {', '.join(fields)} FROM {resource}"  # noqa: S608
    return f"{query} WHERE {where}" if where else query


def date_chunks(start: date, end: date, days: int) -> Iterator[tuple[date, date]]:
    """Yield inclusive ``(first, last)`` windows covering ``start`` to ``end``."""
    first = start
    while first <= end:
        last = min(first + timedelta(days=days - 1), end)
        yield first, last
        first = last + timedelta(days=1)


def report_window(
    last_loaded: str | None, start_date: str, lookback_days: int, today: date
) -> tuple[date, date] | None:
    """Return the dates to pull for one account, or None when it is current."""
    end = today - timedelta(days=1)
    # One day inside the retention limit, so a run late in the day stays inside it.
    oldest = today - REPORT_RETENTION + timedelta(days=1)
    first = max(date.fromisoformat(start_date), oldest)
    if last_loaded:
        first = max(
            first, date.fromisoformat(last_loaded) - timedelta(days=lookback_days)
        )
    return (first, end) if first <= end else None


def manager_id(value: object) -> str:
    """Return a customer id without the hyphens the Ads UI shows."""
    return str(value).replace("-", "")


def load_client() -> GoogleAdsClient:
    """Build the Google Ads client for the active profile."""
    if config.active_profile() not in config.DEPLOYED_PROFILES:
        path = config.require_secrets(
            GOOGLE_ADS_CONFIG_PATH=config.resolve_secret(None, "GOOGLE_ADS_CONFIG_PATH")
        )["GOOGLE_ADS_CONFIG_PATH"]
        return GoogleAdsClient.load_from_storage(path)
    secret = vault.read_kv_secret(GOOGLE_ADS_VAULT_MOUNT, GOOGLE_ADS_VAULT_PATH)
    json_key = secret["google_ads_json"]
    # The client only takes a service-account key as a file path, and reads it
    # while it loads.
    with tempfile.NamedTemporaryFile("w", suffix=".json") as key_file:
        key_file.write(json_key if isinstance(json_key, str) else json.dumps(json_key))
        key_file.flush()
        return GoogleAdsClient.load_from_dict(
            {
                "developer_token": secret["google_developer_token"],
                "login_customer_id": manager_id(secret["google_manager_account"]),
                "json_key_file_path": key_file.name,
                "use_proto_plus": True,
            }
        )


@dlt.source(name="google_ads_ingest")
def google_ads_source(
    client_factory: Callable[[], GoogleAdsClient] = load_client,
    start_date: str = DEFAULT_START_DATE,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    today: date | None = None,
) -> Generator[Any]:
    """Load Google Ads Search data for every client account under the manager.

    Args:
        client_factory: Returns a ``GoogleAdsClient`` whose ``login_customer_id``
            is the manager account.
        start_date: First date the daily reports load on a new account.
        lookback_days: Days the daily reports re-pull before the last loaded date.
        today: Overrides the current date (tests).
    """
    table_format = config.active_table_format()

    # Dagster builds this source once, when the code location loads, and runs it
    # every day after. Anything that changes per run is read in the resource
    # bodies, which run on each extraction, never here.
    def _run_date() -> date:
        return today or datetime.now(tz=UTC).date()

    def _now() -> str:
        return datetime.now(tz=UTC).isoformat()

    _client = functools.cache(client_factory)

    @functools.cache
    def _service() -> Any:  # noqa: ANN401
        return _client().get_service("GoogleAdsService")

    def _search(
        customer_id: str, query: str, fields: tuple[str, ...]
    ) -> Iterator[dict[str, Any]]:
        # Not search_stream: the OpenTelemetry gRPC instrumentation on Dagster run
        # workers turns a streaming response into a generator, which breaks the
        # google-ads client's own interceptors. The pager fetches pages lazily.
        for row in _service().search(customer_id=customer_id, query=query):
            yield flatten_row(row, fields)

    # Every table resource is a transformer of this one, and dlt runs a shared
    # parent once per extraction, so a run reads the account list once and every
    # table sees the same accounts.
    @dlt.resource(name="google_ads_accounts", selected=False)
    def accounts() -> Iterator[dict[str, Any]]:
        login_id = manager_id(_client().login_customer_id)
        # No level filter: client accounts under a sub-manager sit below level 1.
        rows = list(
            _search(
                login_id,
                build_query("customer_client", CUSTOMER_CLIENT_FIELDS),
                CUSTOMER_CLIENT_FIELDS,
            )
        )
        client_ids = [
            str(a["customer_client__id"])
            for a in rows
            if not a["customer_client__manager"]
            and a["customer_client__status"] == "ENABLED"
        ]
        logger.info(
            "Found %d enabled client accounts under manager %s",
            len(client_ids),
            login_id,
        )
        # Without this a lost grant loads nothing and still materializes, so the
        # change-event freshness check would pass while history ages out.
        if not client_ids:
            msg = f"No enabled client accounts under manager {login_id}"
            raise ValueError(msg)
        yield {"accounts": rows, "client_ids": client_ids}

    def _table(name: str, primary_key: tuple[str, ...]) -> dict[str, Any]:
        return {
            "name": name,
            "write_disposition": "merge",
            "primary_key": list(primary_key),
            "table_format": table_format,
            "schema_contract": config.JSON_API_SCHEMA_CONTRACT,
        }

    def _fetch_log(
        table: str, customer_id: str, first: str, last: str, retrieved_at: str
    ) -> Any:  # noqa: ANN401
        return dlt.mark.with_hints(
            {
                "customer_id": customer_id,
                "table_name": table,
                "window_start": first,
                "window_end": last,
                "retrieved_at": retrieved_at,
            },
            dlt.mark.make_hints(
                table_name=FETCH_LOG_TABLE,
                # Its own key: a variant otherwise inherits the parent's, which
                # these rows don't have.
                write_disposition="merge",
                primary_key=list(FETCH_LOG_KEY),
                columns=FETCH_LOG_COLUMNS,
                table_format=table_format,
                schema_contract=config.JSON_API_SCHEMA_CONTRACT,
            ),
            create_table_variant=True,
        )

    @dlt.transformer(
        data_from=accounts,
        name="raw__google_ads__api__customer_clients",
        primary_key="customer_client__id",
        write_disposition="replace",
        table_format=table_format,
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def customer_clients(listing: dict[str, Any]) -> Iterator[dict[str, Any]]:
        retrieved_at = _now()
        for account in listing["accounts"]:
            yield {**account, "retrieved_at": retrieved_at}

    def _daily_report(
        table: str,
        client_ids: list[str],
        resource: str,
        fields: tuple[str, ...],
        where: str,
    ) -> Iterator[Any]:
        last_loaded: dict[str, str] = dlt.current.resource_state().setdefault(
            "last_loaded_date", {}
        )
        run_date, retrieved_at = _run_date(), _now()
        for customer_id in client_ids:
            window = report_window(
                last_loaded.get(customer_id), start_date, lookback_days, run_date
            )
            if window is None:
                continue
            for first, last in date_chunks(*window, REPORT_CHUNK_DAYS):
                query = build_query(
                    resource,
                    fields,
                    f"segments.date BETWEEN '{first}' AND '{last}' AND {where}",
                )
                for record in _search(customer_id, query, fields):
                    yield {
                        "customer_id": customer_id,
                        **record,
                        "retrieved_at": retrieved_at,
                    }
            yield _fetch_log(
                table,
                customer_id,
                window[0].isoformat(),
                window[1].isoformat(),
                retrieved_at,
            )
            last_loaded[customer_id] = window[1].isoformat()

    keyword_key = (
        "customer_id",
        "segments__date",
        "segments__ad_network_type",
        "ad_group__id",
        "ad_group_criterion__criterion_id",
    )

    @dlt.transformer(
        data_from=accounts, **_table("raw__google_ads__api__keyword_daily", keyword_key)
    )
    def keyword_daily(listing: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from _daily_report(
            "raw__google_ads__api__keyword_daily",
            listing["client_ids"],
            "keyword_view",
            KEYWORD_DAILY_FIELDS,
            "metrics.impressions > 0",
        )

    @dlt.transformer(
        data_from=accounts,
        **_table(
            "raw__google_ads__api__keyword_conversions_daily",
            (*keyword_key, "segments__conversion_action"),
        ),
    )
    def keyword_conversions_daily(listing: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from _daily_report(
            "raw__google_ads__api__keyword_conversions_daily",
            listing["client_ids"],
            "keyword_view",
            KEYWORD_CONVERSION_FIELDS,
            "metrics.all_conversions > 0",
        )

    @dlt.transformer(
        data_from=accounts,
        **_table(
            "raw__google_ads__api__campaign_daily",
            ("customer_id", "segments__date", "campaign__id"),
        ),
    )
    def campaign_daily(listing: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from _daily_report(
            "raw__google_ads__api__campaign_daily",
            listing["client_ids"],
            "campaign",
            CAMPAIGN_DAILY_FIELDS,
            "metrics.impressions > 0",
        )

    types = ", ".join(f"'{t}'" for t in CHANGE_EVENT_RESOURCE_TYPES)

    def _change_events_between(
        customer_id: str, first: datetime, last: datetime
    ) -> Iterator[dict[str, Any]]:
        query = build_query(
            "change_event",
            CHANGE_EVENT_FIELDS,
            f"change_event.change_date_time >= '{first:%Y-%m-%d %H:%M:%S}' "
            f"AND change_event.change_date_time < '{last:%Y-%m-%d %H:%M:%S}' "
            f"AND change_event.change_resource_type IN ({types}) "
            f"ORDER BY change_event.change_date_time "
            f"LIMIT {CHANGE_EVENT_LIMIT}",
        )
        rows = list(_search(customer_id, query, CHANGE_EVENT_FIELDS))
        if len(rows) < CHANGE_EVENT_LIMIT:
            yield from rows
            return
        # A full page may be truncated, so halve the window. A full one-second
        # window can't be split further; fail loudly rather than load a page that
        # may be missing events, which the freshness check would not notice.
        if last - first <= CHANGE_EVENT_MIN_WINDOW:
            msg = (
                f"Account {customer_id} has {CHANGE_EVENT_LIMIT} or more change "
                f"events from {first} to {last}; the API can't return them all"
            )
            raise RuntimeError(msg)
        # Whole seconds: the query compares change_date_time at second precision.
        middle = first + timedelta(seconds=(last - first).total_seconds() // 2)
        yield from _change_events_between(customer_id, first, middle)
        yield from _change_events_between(customer_id, middle, last)

    @dlt.transformer(
        data_from=accounts,
        **_table(
            "raw__google_ads__api__change_events",
            ("customer_id", "change_event__resource_name"),
        ),
    )
    def change_events(listing: dict[str, Any]) -> Iterator[dict[str, Any]]:
        run_date, retrieved_at = _run_date(), _now()
        oldest = run_date - timedelta(days=CHANGE_EVENT_RETENTION_DAYS)
        for customer_id in listing["client_ids"]:
            # Re-pulling all 29 days is cheap and the merge key makes it
            # idempotent.
            for day, _ in date_chunks(oldest, run_date, 1):
                first = datetime.combine(day, datetime.min.time())
                for record in _change_events_between(
                    customer_id, first, first + timedelta(days=1)
                ):
                    yield {
                        "customer_id": customer_id,
                        **record,
                        "retrieved_at": retrieved_at,
                    }

    def _snapshot(name: str) -> Any:  # noqa: ANN401
        resource, fields, where = _SNAPSHOTS[name]
        key_column = column_name(fields[0])

        @dlt.transformer(
            data_from=accounts,
            **_table(name, ("customer_id", "snapshot_date", key_column)),
        )
        def snapshot(listing: dict[str, Any]) -> Iterator[dict[str, Any]]:
            query = build_query(resource, fields, where)
            snapshot_date, retrieved_at = _run_date().isoformat(), _now()
            for customer_id in listing["client_ids"]:
                for record in _search(customer_id, query, fields):
                    yield {
                        "customer_id": customer_id,
                        "snapshot_date": snapshot_date,
                        **record,
                        "retrieved_at": retrieved_at,
                    }
                yield _fetch_log(
                    name, customer_id, snapshot_date, snapshot_date, retrieved_at
                )

        return snapshot

    yield accounts
    yield customer_clients
    yield keyword_daily
    yield keyword_conversions_daily
    yield campaign_daily
    yield change_events
    for name in _SNAPSHOTS:
        yield _snapshot(name)


google_ads_pipeline = config.pipeline_for("google_ads")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source with nullable ``_dlt_load_id`` columns."""
    return config.with_nullable_load_id(google_ads_source())
