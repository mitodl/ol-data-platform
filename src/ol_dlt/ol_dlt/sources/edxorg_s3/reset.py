"""Empty an edxorg raw table and rewind its cursor so the loader reads it again.

The edxorg loader appends, and its ``modification_date`` cursor moves past a
file once it is read, so a load that got a file wrong is never repaired by a
later one. Between #2695 and #2725 every TSV read shared DuckDB's default
connection, and starting the next file's read ended the suspended one after
its first 5,000-row batch without an error. Production raw kept those loads
(2026-09-19 18:07 to 2026-09-20 03:37 UTC), so 25 tables hold 5,000 rows of
files that have far more.

This truncates the Iceberg table (one delete commit, table and history kept)
and then resets the table's cursor in its pipeline's destination state. The
next load, run by the ``data_loading`` sensor like any other, finds no cursor
and walks the landing zone from the oldest file.

It does not use dlt's ``refresh="drop_data"``. That picks the tables to
truncate from the newest stored ``edxorg_s3`` schema, which every per-table
pipeline writes to, so it can reset the cursor of a table that schema does not
list and skip the truncate, doubling the table instead of repairing it.

Pause the edxorg ingestion sensor and let in-flight runs for the table finish
first. A load that commits after the reset carries the old cursor with it and
leaves the table empty with nothing to reload it; one that commits between the
dry run's plan and the reset is caught, and nothing is changed. The sensor only
fires on new upstream exports, so launch ``edxorg_s3_ingest_job`` by hand after
unpausing it.

Dry run (default) prints what would change::

    DLT_PROFILE=production python -m ol_dlt.sources.edxorg_s3.reset \
        courseware_studentmodule auth_user

    DLT_PROFILE=production python -m ol_dlt.sources.edxorg_s3.reset \
        courseware_studentmodule auth_user --no-dry-run
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import dlt
from cyclopts import App
from dlt.common.libs.pyiceberg import get_catalog, truncate_iceberg_table
from dlt.extract.state import reset_resource_state

from ol_dlt import config
from ol_dlt.sources.edxorg_s3 import edxorg_s3_pipeline_for

if TYPE_CHECKING:
    from pyiceberg.table import Table as IcebergTable

logger = logging.getLogger(__name__)

_SOURCE_NAME = "edxorg_s3"

app = App(
    name="edxorg-s3-reset",
    help="Empty edxorg raw tables and rewind their cursors for a full reload.",
)


def resource_name_for(table_name: str) -> str:
    """Return the raw table (and dlt resource) name for an edxorg table."""
    return f"raw__edxorg__s3__tables__{table_name}"


def cursor_key_for(table_name: str) -> str:
    """Return the resource-state key that holds ``table_name``'s cursor.

    dlt keys a piped resource's incremental state by "<parent>_<resource>", and
    the parent is ``edxorg_files``, which is registered as "filesystem".
    """
    return f"filesystem_{resource_name_for(table_name)}"


class NothingToResetError(LookupError):
    """The table's pipeline state holds no cursor for it."""


class StateMovedError(RuntimeError):
    """A load committed state between planning a reset and applying it."""


@dataclass(frozen=True)
class ResetPlan:
    """What resetting one table will change, resolved before anything does."""

    pipeline: dlt.Pipeline
    table_name: str
    table: "IcebergTable"
    rows: int
    state_version_hash: str


def _synced(pipeline: dlt.Pipeline) -> dlt.Pipeline:
    """Return ``pipeline`` holding the destination's state and nothing local.

    drop() first because sync_destination keeps local state whose version is
    higher, and a working dir left by an old local run can have one:
    edxorg_s3__student_courseaccessrole's is version 3 with no cursor while
    production's is version 1 with one.
    """
    pipeline = pipeline.drop()
    pipeline.sync_destination()
    return pipeline


def plan_reset(pipeline: dlt.Pipeline, table_name: str) -> ResetPlan:
    """Resolve ``table_name``'s cursor and Iceberg table without changing either.

    :param pipeline: The table's own pipeline (``edxorg_s3_pipeline_for``).
    :param table_name: The edxorg table, e.g. ``courseware_studentmodule``.
    :returns: The plan for :func:`apply_reset`.
    :rtype: ResetPlan
    :raises NothingToResetError: The destination state has no cursor for the
        table: it was never loaded under this name, or it was already reset.
    """
    pipeline = _synced(pipeline)
    resources = (
        pipeline.state.get("sources", {}).get(_SOURCE_NAME, {}).get("resources", {})
    )
    cursor = resources.get(cursor_key_for(table_name))
    if cursor is None:
        msg = (
            f"{table_name}: {pipeline.pipeline_name} has no cursor in its "
            "destination state, so it was never loaded or is already reset."
        )
        raise NothingToResetError(msg)

    resource_name = resource_name_for(table_name)
    table = get_catalog().load_table((pipeline.dataset_name, resource_name))
    snapshot = table.current_snapshot()
    rows = int(snapshot.summary["total-records"]) if snapshot else 0
    logger.info(
        "%s: %s rows in %s.%s, cursor %s",
        table_name,
        rows,
        pipeline.dataset_name,
        resource_name,
        _describe(cursor),
    )
    return ResetPlan(
        pipeline=pipeline,
        table_name=table_name,
        table=table,
        rows=rows,
        state_version_hash=pipeline.state["_version_hash"],
    )


def apply_reset(plan: ResetPlan) -> None:
    """Truncate the planned table and delete its cursor.

    :param plan: From :func:`plan_reset`.
    :raises StateMovedError: A load committed state since the plan was made, so
        the table may hold rows the plan did not see. Nothing is changed.
    """
    pipeline = _synced(plan.pipeline)
    if pipeline.state["_version_hash"] != plan.state_version_hash:
        msg = (
            f"{plan.table_name}: {pipeline.pipeline_name} committed a load after "
            "the reset was planned. Pause the ingestion sensor, let the run "
            "finish, and run this again."
        )
        raise StateMovedError(msg)

    # Truncate first. If the state commit below then fails, the table is empty
    # with its old cursor, which is visible and fixed by running this again. The
    # other order could let a load append the backlog on top of the old rows.
    truncate_iceberg_table(plan.table)

    with pipeline.managed_state(extract_state=True) as state:
        reset_resource_state(
            cursor_key_for(plan.table_name), state["sources"][_SOURCE_NAME]
        )
    pipeline.normalize()
    pipeline.load()
    logger.info("%s: truncated and cursor reset.", plan.table_name)


def _describe(cursor: dict[str, Any]) -> str:
    incremental = cursor.get("incremental", {}).get("modification_date", {})
    return str(incremental.get("last_value", cursor))


@app.default
def run(*table_names: str, dry_run: bool = True) -> None:
    """Reset each named edxorg table for a full reload.

    Args:
        table_names: edxorg tables, e.g. ``courseware_studentmodule``.
        dry_run: Report what would change without changing it.
    """
    # force: dlt has already configured the root logger by now.
    logging.basicConfig(level=logging.INFO, force=True)
    if config.active_table_format() != "iceberg":
        msg = f"DLT_PROFILE={config.active_profile()} does not write Iceberg tables."
        raise ValueError(msg)
    # Every table is resolved before any is changed, so a typo or an
    # already-reset table stops the run with nothing half done.
    plans = [
        plan_reset(edxorg_s3_pipeline_for(table_name), table_name)
        for table_name in table_names
    ]
    if dry_run:
        return
    for plan in plans:
        apply_reset(plan)


if __name__ == "__main__":
    app()
