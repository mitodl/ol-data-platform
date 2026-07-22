"""Pure helpers shared by the StarRocks dbt asset and the MV-refresh asset.

These live here rather than next to their callers so they can be unit-tested:
importing `lakehouse.assets.lakehouse.dbt_starrocks` evaluates a `@dbt_assets`
decorator at module scope, which raises DagsterDbtManifestNotFoundError unless a
parsed dbt manifest is already on disk. Nothing in this module imports dagster
or dbt.
"""

import re
from collections.abc import Mapping
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
# 1044/1045/2003/2006/2013 -- MySQL wire-protocol codes.
# StarRocksResource._RETRIABLE_ERRORS carries the same set minus 2003
# CR_CONN_HOST_ERROR, which only a *fresh* connect to an already-gone FE returns
# and which the resource therefore never sees (it connects once, up front).
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
RETRIABLE_ERROR_PATTERN = re.compile(
    r"\b(1044|1045|2003|2006|2013)\b|forward failed|SocketTimeoutException"
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
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
        and node["config"]["materialized"] == "materialized_view"
        and STARROCKS_TAG in node["tags"]
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
