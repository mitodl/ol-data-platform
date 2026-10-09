"""Pure helpers shared by the StarRocks dbt asset and the MV-refresh asset.

These live here rather than next to their callers so they can be unit-tested:
importing `lakehouse.assets.lakehouse.dbt_starrocks` evaluates a `@dbt_assets`
decorator at module scope, which raises DagsterDbtManifestNotFoundError unless a
parsed dbt manifest is already on disk. Nothing in this module imports dagster
or dbt.
"""

import logging
import re
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from jinja2 import Environment

# Retries are of the whole `dbt build`, since dbt-starrocks has no adapter-level
# retry of its own. Two failure modes need covering, and the slower one sets the
# schedule: a rolling restart of the 3-replica FE StatefulSet, measured at ~3
# minutes end to end on 2026-07-22 (first pod killed 20:24:37, cluster whole
# again 20:27:33). 4 attempts at a 30s doubling base spend 30 + 60 + 120 = 210s
# asleep, plus however far each failed build got before dying -- comfortably
# past that. The previous 3 attempts at a 1s base gave up after 3s of sleep, so
# all three landed inside the same rollout and the build failed outright.
# Vault credential propagation, the other (original) failure mode, resolves well
# inside 30s; it just waits a little longer to notice now.
MAX_BUILD_ATTEMPTS = 4
RETRY_BASE_DELAY = 30

# Error signatures worth another attempt, in two families.
#
# 1044/1045/2003/2006/2013 -- MySQL wire-protocol codes, the same set
# StarRocksResource._RETRIABLE_ERRORS retries. (It used to omit 2003
# CR_CONN_HOST_ERROR on the theory that the resource never sees a failed
# connect; it does -- every attempt in _run() opens its own connection, and an
# FE rolling restart is exactly how that fails.)
# 1044/1045 are the Vault case: a just-created dynamic user may not be visible
# yet on the FE node dbt happened to connect to.
#
# "forward failed" / "SocketTimeoutException" -- FE-side Java exceptions, not
# wire-protocol codes. dbt connects to the round-robin fe-service, so most
# statements land on a follower FE, which must forward every DDL to the leader
# over Thrift. When an FE rollout takes the leader (or the follower) down
# mid-statement, StarRocks reports "forward failed: unknown result" or
# "java.net.SocketTimeoutException: Connect timed out" wrapped in a *generic*
# 1064, so the numeric alternation cannot catch them.
#
# dbt-starrocks connects via mysql-connector-python, whose Error.__str__ formats
# as "<errno> (<sqlstate>): <msg>", and dbt-core passes str(e) through to the
# node's logged error message unmodified -- so these do reach us, but as plain
# text inside a multi-line message rather than a structured field. Hence the
# word boundaries on the numeric codes, so an unrelated number (a row count, a
# line number, part of a timestamp) can't trip a retry. The three text signatures
# need no such guard; none of them appears in a successful build's output.
#
# "base-table dropped" -- also wrapped in a generic 1064.  The b2b_analytics MVs
# read the `dimensional` tables through an Iceberg external catalog, and the
# Trino project rebuilds those (materialized='table') on every run.  For a window
# afterwards StarRocks refuses to analyze a CREATE against one, reporting the
# base table as dropped, and the condition then clears on its own.
#
# Measured on the 2026-09-03 17:31 UTC run: the first four models dbt started
# concurrently all failed on dim_organization in 0.61-0.70s, models 5-8 built OK
# in that same invocation seconds later, and a re-run 60s afterwards built all
# eight.  A second attempt is all this needs.
#
# A base table that is genuinely gone rather than momentarily stale costs the
# full 210s of backoff before failing the same way.  That is the trade the
# signatures above already accept, and cheaper than a red asset that only ever
# needed a second attempt.
RETRIABLE_ERROR_PATTERN = re.compile(
    r"\b(1044|1045|2003|2006|2013)\b|forward failed|SocketTimeoutException"
    r"|base-table dropped"
)

# dbt_project.yml tags the StarRocks-targeted models with this; it is also what
# starrocks_dbt_assets selects on.
STARROCKS_TAG = "starrocks"


def looks_retriable(exc: Exception) -> bool:
    """Whether a failed `dbt build` is worth another attempt."""
    return bool(RETRIABLE_ERROR_PATTERN.search(str(exc)))


def retry_delay(attempt: int) -> int:
    """Seconds to wait before `attempt`, indexed the same way the build loop
    counts: attempt 0 is the initial build and never waits, attempt 1 is the
    first retry.

    Attempt 0 is spelled out rather than left to `2 ** -1` -- that returns
    15.0, which is both a float (breaking the annotation) and a nonsensical
    "wait half the base delay before doing anything".
    """
    if attempt < 1:
        return 0
    return RETRY_BASE_DELAY * (2 ** (attempt - 1))


def materialized_view_relations(manifest: Mapping[str, Any]) -> list[str]:
    """Schema-qualified names of every StarRocks materialized view dbt builds.

    Derived from the parsed manifest rather than hand-listed, so adding or
    renaming a model in models/b2b_analytics/ needs no corresponding Python
    edit. The schema comes from the manifest too, which is the point: it is
    whatever generate_schema_name resolved to, so REFRESH targets exactly the
    relation dbt created instead of re-deriving it and drifting.
    """
    relations = sorted(
        f"{node['schema']}.{node['alias']}"
        for node in _materialized_view_nodes(manifest)
    )
    if not relations:
        # Not defensive: an empty list would make the refresh asset a silent
        # no-op that still reports success, leaving every MV stale with nothing
        # in the logs to say so.
        msg = (
            "No materialized_view models tagged "
            f"'{STARROCKS_TAG}' found in the dbt manifest"
        )
        raise ValueError(msg)
    return relations


# Two ways a REFRESH fails because the Trino project rebuilt an Iceberg base
# table under it. Every `dimensional` table the MVs read is materialized='table',
# so each Trino build replaces it with a new Iceberg table, and the automation
# sensor runs that build several times a day on no fixed schedule.
#
# "does not exist when collecting snapshot infos" -- the REFRESH landed inside
# the swap itself. On 2026-09-25 run 1889c9b0 rebuilt bridge_organization_courserun
# from 06:04:10 to 06:04:33 UTC and the nightly began refreshing
# mv_b2b_learner_enrollment at 06:04:22.
#
# "was recreated but its table type is not supported for automatic meta
# repair" -- the first REFRESH of an MV after the swap. StarRocks marks the MV
# inactive and asks for a manual refresh, which is the next REFRESH. The
# 09-25 Dagster run retries showed it: each retry got past the MV that failed
# the attempt before and failed on the next MV in the list.
#
# The recreate error was observed clearing on the next REFRESH. The mid-swap
# error wasn't retried on 09-25 (the retries never got back to that MV), so
# that it clears once the new table exists is inferred. The delay covers a
# swap still in progress (the 09-25 CREATE TABLE ran 22s).
MV_REFRESH_RETRIABLE_PATTERN = re.compile(
    r"does not exist when collecting snapshot infos"
    r"|was recreated but its table type is not supported for automatic meta repair"
)
MAX_MV_REFRESH_ATTEMPTS = 3
MV_REFRESH_RETRY_DELAY_SECONDS = 30


class MaterializedViewRefreshError(Exception):
    """One or more MVs still failed to refresh after their retries."""

    def __init__(self, failures: Mapping[str, Exception]) -> None:
        self.failures = dict(failures)
        detail = "\n".join(f"{name}: {exc}" for name, exc in self.failures.items())
        super().__init__(
            f"{len(self.failures)} materialized view(s) failed to refresh "
            f"or in the step after it:\n{detail}"
        )


def refresh_materialized_views(
    relations: Iterable[str],
    execute: Callable[[str], None],
    *,
    log: logging.Logger,
    sleep: Callable[[float], None] = time.sleep,
    after_refresh: Callable[[str], None] | None = None,
) -> None:
    """Refresh every MV in *relations*, retrying base-table-rebuild failures.

    One MV failing doesn't stop the rest. Before this, the first failure ended
    the asset and every MV after it in the list kept the previous day's data. A
    failure outside `MV_REFRESH_RETRIABLE_PATTERN` is not retried, but it is
    still reported with the others at the end.

    :param relations: Schema-qualified MV names, as `materialized_view_relations`
        returns them.
    :param execute: Runs one SQL statement. `StarRocksResource.execute` in the
        asset.
    :param log: Where progress goes. `context.log` in the asset.
    :param sleep: Injected so tests don't wait out the retry delay.
    :param after_refresh: Called with a relation straight after its REFRESH
        succeeds, and never for one that failed. If it raises, that relation is
        reported as failed, and its REFRESH is not retried.
    :raises MaterializedViewRefreshError: if any MV still failed, or its
        `after_refresh` did.
    """
    failures: dict[str, Exception] = {}
    for relation in relations:
        log.info("Refreshing %s", relation)
        for attempt in range(1, MAX_MV_REFRESH_ATTEMPTS + 1):
            try:
                execute(f"REFRESH MATERIALIZED VIEW {relation} WITH SYNC MODE")
            except Exception as exc:
                if (
                    attempt == MAX_MV_REFRESH_ATTEMPTS
                    or not MV_REFRESH_RETRIABLE_PATTERN.search(str(exc))
                ):
                    log.exception("Refresh of %s failed", relation)
                    failures[relation] = exc
                    break
                log.warning(
                    "Refresh of %s failed on a rebuilt base table (attempt %d/%d), "
                    "retrying in %ds: %s",
                    relation,
                    attempt,
                    MAX_MV_REFRESH_ATTEMPTS,
                    MV_REFRESH_RETRY_DELAY_SECONDS,
                    exc,
                )
                sleep(MV_REFRESH_RETRY_DELAY_SECONDS)
            else:
                log.info("Refreshed %s", relation)
                if after_refresh:
                    # Outside the retried block: the REFRESH succeeded, so an
                    # error here must not run it again, whatever its text.
                    try:
                        after_refresh(relation)
                    except Exception as exc:
                        log.exception("Follow-up to refreshing %s failed", relation)
                        failures[relation] = exc
                break
    if failures:
        raise MaterializedViewRefreshError(failures)


def _materialized_view_nodes(
    manifest: Mapping[str, Any],
) -> Iterator[Mapping[str, Any]]:
    return (
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
        and node["config"]["materialized"] == "materialized_view"
        and STARROCKS_TAG in node["tags"]
    )


def documented_columns(manifest: Mapping[str, Any]) -> dict[str, set[str]]:
    """Map each StarRocks MV to the columns its schema YAML documents.

    Read from the manifest's `columns` rather than by parsing the model's
    SELECT, because the YAML is the contract ol-analytics-api is written
    against: a column nobody documented is a column no consumer projects.

    This is the model's FULL output schema, which is what lets
    `drifted_relations` compare by equality. `ol-dbt validate`'s yaml_sql_sync
    check errors in both directions -- a documented column with no matching
    SQL alias, and a SQL column the YAML omits (ol-data-platform#2555) -- so a
    model whose YAML and SELECT disagree cannot merge. If that check is ever
    relaxed back to a warning, or starts skipping these models (it skips any
    model whose SELECT * sqlglot cannot expand; none do today), this stops
    being a full schema and the equality check has to weaken with it.

    A model with no documented columns at all is omitted -- there is nothing
    to check it against.
    """
    return {
        f"{node['schema']}.{node['alias']}": {
            name.lower() for name in node.get("columns", {})
        }
        for node in _materialized_view_nodes(manifest)
        if node.get("columns")
    }


def live_column_query(relations: Mapping[str, Any]) -> tuple[str, tuple[str, ...]]:
    """Build a parameterized information_schema query for *relations*' schemas.

    Filtering by schema rather than by name keeps the statement short and the
    parameter list to one entry per schema (in practice: one). Rows for tables
    dbt doesn't own come back too and are dropped by the relation lookup in
    `drifted_relations`.
    """
    schemas = sorted({relation.split(".", 1)[0] for relation in relations})
    placeholders = ", ".join(["%s"] * len(schemas))
    # S608: the only thing interpolated is a run of `%s` placeholders -- the
    # schema names themselves are bound by the driver, never formatted in.
    query = (
        "select table_schema, table_name, column_name "  # noqa: S608
        "from information_schema.columns "
        f"where table_schema in ({placeholders})"
    )
    return query, tuple(schemas)


def live_columns(rows: list[Mapping[str, Any]]) -> dict[str, set[str]]:
    """Fold `live_column_query` rows into {relation: {column, ...}}.

    Keys are the lowercase labels the query selects; values are lowercased to
    match `documented_columns`, since a column name is case-insensitive over
    the MySQL wire protocol but the two sources spell it independently.
    """
    columns: dict[str, set[str]] = {}
    for row in rows:
        relation = f"{row['table_schema']}.{row['table_name']}"
        columns.setdefault(relation, set()).add(row["column_name"].lower())
    return columns


def drifted_relations(
    documented: Mapping[str, set[str]], live: Mapping[str, set[str]]
) -> list[str]:
    """MVs whose columns in StarRocks no longer match what dbt says they are.

    These need `dbt build --full-refresh` to catch up: dbt-core only replaces
    an existing materialized view under that flag, and dbt-starrocks'
    `get_materialized_view_configuration_changes` is an empty macro, so an
    edited SELECT is otherwise a silent no-op (a plain build logs "no
    configuration changes were identified" and the MV keeps its old query).

    Set equality, so an added, renamed, or removed column all count. That
    rests on `documented_columns` being the model's full output schema, which
    ol-dbt validate now enforces -- read the caveat there before weakening it.

    A relation missing from *live* does not exist yet -- this build creates it
    with the current SELECT, so there is nothing to rebuild.
    """
    return sorted(
        relation
        for relation, columns in documented.items()
        if relation in live and columns != live[relation]
    )


class MissingBaseTablesError(Exception):
    """A full refresh was called for while a base table could not be read."""

    def __init__(self, failures: Mapping[str, Exception]) -> None:
        self.failures = dict(failures)
        detail = "\n".join(f"{name}: {exc}" for name, exc in self.failures.items())
        super().__init__(
            f"{len(self.failures)} base table(s) of the StarRocks materialized "
            "views could not be read, so the views were not rebuilt with "
            f"--full-refresh and still hold their previous definition:\n{detail}"
        )


def _render_source_part(
    source: Mapping[str, Any], part: str, env: Mapping[str, str]
) -> str:
    """Render a source's database or schema against *env*.

    The manifest's rendered `database` and `schema` can't be used. One image
    serves every environment, so its manifest is parsed at build time with a
    placeholder DBT_DATA_LAKE_ENV (see the Dockerfile), and the build itself
    re-parses with the run-time value. Rendering the YAML's own template with
    the run-time environment names the catalog the build will read.
    """
    template = source.get(f"unrendered_{part}")
    if not template:
        return source[part]

    def env_var(name: str, default: str | None = None) -> str:
        return env[name] if default is None else env.get(name, default)

    # S701: the output is an identifier inside a SQL statement, not HTML.
    return Environment().from_string(template).render(env_var=env_var)  # noqa: S701


def base_table_relations(
    manifest: Mapping[str, Any], env: Mapping[str, str]
) -> list[str]:
    """List the source tables the StarRocks MVs select from.

    Only a view's own `source()` calls are followed, including the ones its
    macros make. Every view reads its sources directly today.

    :param manifest: The parsed StarRocks dbt manifest.
    :param env: The process environment the build will run with, which is
        where `DBT_DATA_LAKE_ENV` picks the lake.
    :returns: Catalog-qualified names, quoted as dbt-starrocks quotes them.
    """
    sources = [
        manifest["sources"][unique_id]
        for node in _materialized_view_nodes(manifest)
        for unique_id in node["depends_on"]["nodes"]
        if unique_id.startswith("source.")
    ]
    return sorted(
        {
            ".".join(
                f"`{part}`"
                for part in (
                    _render_source_part(source, "database", env),
                    _render_source_part(source, "schema", env),
                    source["identifier"],
                )
            )
            for source in sources
        }
    )


def unreadable_base_tables(
    relations: Iterable[str], fetch: Callable[[str], Any], *, log: logging.Logger
) -> dict[str, Exception]:
    """Probe each of *relations* and return the ones StarRocks can't resolve.

    Run this before a `dbt build --full-refresh`. That build drops every
    selected MV and recreates it, and a CREATE whose base table is missing
    fails after the drop. On 2026-10-09 four views were lost that way: #2881
    pointed them at afact_learner_courserun_progress, which production had
    never built, and ol-analytics-api answered 500 on the endpoints that read
    them until the next successful build.

    `limit 0` reads no data; the statement only has to get through analysis,
    which is where a missing table is reported. It does not check that the
    table has the columns the view selects.

    A probe that fails on one of the build's own retriable signatures (a
    dropped connection, an FE restart, a base table Trino has just rebuilt) is
    not a finding. Those clear on their own and the build retries them.

    :param relations: Catalog-qualified names, as `base_table_relations`
        returns them.
    :param fetch: Runs one SELECT. `StarRocksResource.fetch` in the asset.
    :param log: Where progress goes. `context.log` in the asset.
    :returns: The error for each relation that could not be read.
    """
    failures: dict[str, Exception] = {}
    for relation in relations:
        try:
            fetch(f"select 1 from {relation} limit 0")  # noqa: S608
        except Exception as exc:
            if looks_retriable(exc):
                log.warning(
                    "Probe of %s failed on a retriable error, leaving it to "
                    "the build: %s",
                    relation,
                    exc,
                )
                continue
            log.exception("Base table %s can't be read", relation)
            failures[relation] = exc
    return failures


# Model `meta` key that opts a materialized view into a change log.
CHANGE_TRACKING_META_KEY = "change_tracking"
CHANGE_LOG_SUFFIX = "_changes"

# ASCII unit and record separators. A value is cast to varchar before hashing,
# so a separator that can appear in an email or a title would let two different
# rows hash alike ("a|b", "c" against "a", "b|c"). The record separator stands
# in for null, which concat_ws would otherwise skip, hashing (null, 'x') and
# ('x', null) alike.
_FIELD_SEPARATOR = "char(31)"
_NULL_MARKER = "char(30)"


@dataclass(frozen=True)
class ChangeTrackedView:
    """A materialized view whose rows get a change time after each refresh.

    :param relation: Schema-qualified MV name.
    :param key: Columns that identify one row of the MV, its grain.
    :param identity: Columns copied to the change log so a row that has left
        the MV can still be named to a consumer. The key columns are warehouse
        surrogates the consumer never sees.
    :param tracked: Every other column. A change in any of them, or in an
        identity column, is a change to the row.
    """

    relation: str
    key: tuple[str, ...]
    identity: tuple[str, ...]
    tracked: tuple[str, ...]

    @property
    def change_log(self) -> str:
        return f"{self.relation}{CHANGE_LOG_SUFFIX}"


def change_tracked_views(manifest: Mapping[str, Any]) -> list[ChangeTrackedView]:
    """Return the StarRocks MVs whose schema YAML sets `meta.change_tracking`.

    The hashed columns are the model's documented columns in YAML order, which
    `documented_columns` explains is the full output schema. Adding a column to
    the MV therefore changes every row's hash, and every row is stamped once.
    That is correct: each record gained a field.

    :raises ValueError: if a `key` or `identity` column is not documented on
        the model. The statements would otherwise fail in StarRocks on an
        unknown column with nothing pointing at the YAML.
    """
    views = []
    for node in _materialized_view_nodes(manifest):
        tracking = node["config"]["meta"].get(CHANGE_TRACKING_META_KEY)
        if tracking is None:
            continue
        relation = f"{node['schema']}.{node['alias']}"
        key = tuple(tracking["key"])
        identity = tuple(tracking["identity"])
        columns = list(node["columns"])
        undocumented = sorted(set(key + identity) - set(columns))
        if not key or undocumented:
            msg = (
                f"{relation}: meta.{CHANGE_TRACKING_META_KEY} needs a non-empty "
                f"key of documented columns; not documented: {undocumented}"
            )
            raise ValueError(msg)
        views.append(
            ChangeTrackedView(
                relation=relation,
                key=key,
                identity=identity,
                tracked=tuple(c for c in columns if c not in key + identity),
            )
        )
    return sorted(views, key=lambda view: view.relation)


def _current_rows_sql(view: ChangeTrackedView) -> str:
    """One row per key of the MV, with a hash of everything else.

    Grouped by the key so the change log stays unique on it whatever the MV
    holds. The grain tests on these models are severity: warn, and the stamp
    joins the MV to the log on the key: a key duplicated in the MV would be
    written to the log twice, and each later stamp would multiply it again.

    A duplicated key is hashed as a group, over the sorted hashes of all its
    rows, so a change to any one of them moves the hash. Its identity columns
    all come from the one row with the greatest hash; taking each column's own
    maximum could pair values from different rows.
    """
    hashed = ", ".join(
        f"coalesce(cast(`{column}` as varchar), {_NULL_MARKER})"
        for column in view.identity + view.tracked
    )
    key = ", ".join(f"`{column}`" for column in view.key)
    rows = ", ".join(
        [
            *(f"`{column}`" for column in view.key + view.identity),
            f"md5(concat_ws({_FIELD_SEPARATOR}, {hashed})) as content_hash",
        ]
    )
    select = ", ".join(
        [
            key,
            *(
                f"max_by(`{column}`, content_hash) as `{column}`"
                for column in view.identity
            ),
            "md5(array_join(array_sort(array_agg(content_hash)), ',')) as row_hash",
        ]
    )
    return (
        f"select {select} "  # noqa: S608
        f"from (select {rows} from {view.relation}) r group by {key}"
    )


def seed_change_log_sql(view: ChangeTrackedView) -> str:
    """Create the change log from the MV's current rows, if it doesn't exist.

    Every row is stamped with the creation time, so the first incremental read
    after the log is created returns every record once.
    """
    return (
        f"create table if not exists {view.change_log} as "  # noqa: S608
        f"select m.*, utc_timestamp() as changed_on, false as is_deleted "
        f"from ({_current_rows_sql(view)}) m"
    )


def stamp_change_log_sql(view: ChangeTrackedView) -> str:
    """Rewrite the change log against the MV's current rows.

    A row keeps its `changed_on` unless it is new, its hash differs, or it left
    or re-entered the MV; those take the statement's time. A row that left stays
    in the log with `is_deleted` set and its last identity columns, which is
    the only record that it was ever there.

    INSERT OVERWRITE replaces the table's contents atomically, so a reader
    never sees a half-written log, and the statement can be re-run: a second
    run finds nothing changed and keeps every `changed_on`.

    The target columns are named so that a log created under a different `key`
    or `identity` list fails on an unknown column instead of taking values in
    the wrong positions.

    The join is null-safe (`<=>`) so a null key column matches itself rather
    than being read as one row leaving and another arriving on every run.
    """
    join = " and ".join(f"m.`{column}` <=> c.`{column}`" for column in view.key)
    select = [f"coalesce(m.`{column}`, c.`{column}`)" for column in view.key]
    select += [
        f"if(m.row_hash is null, c.`{column}`, m.`{column}`)"
        for column in view.identity
    ]
    select += [
        "coalesce(m.row_hash, c.row_hash)",
        (
            "case when c.row_hash is null or (m.row_hash is null) != c.is_deleted "
            "or m.row_hash != c.row_hash then utc_timestamp() else c.changed_on end"
        ),
        "m.row_hash is null",
    ]
    columns = ", ".join(
        f"`{column}`"
        for column in (
            *view.key,
            *view.identity,
            "row_hash",
            "changed_on",
            "is_deleted",
        )
    )
    return (
        f"insert overwrite {view.change_log} ({columns}) "  # noqa: S608
        f"select {', '.join(select)} "
        f"from ({_current_rows_sql(view)}) m "
        f"full outer join {view.change_log} c on {join}"
    )


def stamp_change_log(
    view: ChangeTrackedView, execute: Callable[[str], None], *, log: logging.Logger
) -> None:
    """Bring *view*'s change log up to date with the refresh that just ran.

    Call this only straight after the view's REFRESH succeeds, which is what
    `refresh_materialized_views`' `after_refresh` does. `changed_on` is the
    cursor ol-analytics-api compares to a client's `updated_since`, and the
    client passes the `as_of` of its previous read, which is the MV's last
    refresh time. Stamping after the refresh that first exposes a change puts
    `changed_on` at or after that refresh's finish time, and so at or after
    every `as_of` the client could hold from before it saw the change. The
    comparison has to stay inclusive (`>=`) for that to mean the change is
    never skipped. A source timestamp can't give this: the change reaches the
    MV some time after it was made.

    A view that was not refreshed must not be stamped. `dbt build
    --full-refresh` recreates an MV empty, and an MV whose refresh then fails
    stays empty; stamping it would mark every record deleted. The stamp reads
    the MV as it is when the statement runs, so a second run of the build
    overlapping this one can still do that; the next stamp reverses it and
    restamps every row.
    """
    log.info("Stamping %s", view.change_log)
    execute(seed_change_log_sql(view))
    execute(stamp_change_log_sql(view))
