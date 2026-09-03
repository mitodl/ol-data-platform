"""Pure helpers shared by the StarRocks dbt asset and the MV-refresh asset.

These live here rather than next to their callers so they can be unit-tested:
importing `lakehouse.assets.lakehouse.dbt_starrocks` evaluates a `@dbt_assets`
decorator at module scope, which raises DagsterDbtManifestNotFoundError unless a
parsed dbt manifest is already on disk. Nothing in this module imports dagster
or dbt.
"""

import re
from collections.abc import Iterator, Mapping
from typing import Any

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
# line number, part of a timestamp) can't trip a retry. The two text signatures
# need no such guard; neither string appears in a successful build's output.
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
