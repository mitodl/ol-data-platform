"""Which raw tables the load-side history guard checks, and how it reads dbt's answer.

A load that leaves a raw table short makes its raw history snapshot record a
delete for every missing key, and the history table cannot be rebuilt. The
snapshot-side guard (``raw_history_delete_guard``) fails such a snapshot run.
This is the load-side half: a ``history_load_not_short`` check on each raw table
a live, ``absence: deleted`` snapshot reads, and the snapshot's automation
condition waits for that check to pass against the latest load
(``raw_load_check_passed``).

The counting is the dbt macro ``raw_history_load_guard``, so it reads the same
relations on the same engine as the snapshot, and trips on the same rule and
vars as the snapshot-side guard.

Kept apart from ``assets.history_load_guard`` so it can be tested without a
parsed manifest.
"""

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from dagster import AssetCheckKey, AssetKey

# The dbt source name of the raw tables, and so the first part of their keys.
RAW_DATA_SOURCE = "ol_warehouse_raw_data"
CHECK_NAME = "history_load_not_short"
# Each table's line from the raw_history_load_guard macro starts with this.
LOG_PREFIX = "raw_history_load_guard "


@dataclass(frozen=True)
class GuardedTable:
    """A raw table whose load the guard checks, and the snapshot it protects."""

    snapshot: str
    raw_table: str
    unique_key: tuple[str, ...]

    @property
    def raw_key(self) -> AssetKey:
        """The raw table's asset key, as the dbt source and loaders name it."""
        return AssetKey([RAW_DATA_SOURCE, self.raw_table])

    def macro_arg(self) -> dict[str, Any]:
        """Return this table's entry in the macro's ``tables`` argument."""
        return {
            "snapshot": self.snapshot,
            "raw_table": self.raw_table,
            "unique_key": list(self.unique_key),
        }


def is_raw_history_snapshot(dbt_resource_props: Mapping[str, Any]) -> bool:
    """Whether a dbt node is one of the generated raw history snapshots.

    :param dbt_resource_props: the manifest node.
    :returns: True for a snapshot carrying ``raw_history`` meta.
    :rtype: bool
    """
    return (
        dbt_resource_props["resource_type"] == "snapshot"
        and "raw_history" in dbt_resource_props["config"]["meta"]
    )


def is_guarded_snapshot(dbt_resource_props: Mapping[str, Any]) -> bool:
    """Whether a dbt node is a raw history snapshot that records deletes.

    An ``absence: aged_out`` snapshot leaves a missing record as it was, so a
    short load writes nothing there that a later load cannot correct.

    :param dbt_resource_props: the manifest node.
    :returns: True for a generated raw history snapshot with ``absence: deleted``.
    :rtype: bool
    """
    return (
        is_raw_history_snapshot(dbt_resource_props)
        and dbt_resource_props["config"]["meta"]["raw_history"]["absence"] == "deleted"
    )


def snapshot_raw_table(dbt_resource_props: Mapping[str, Any]) -> str:
    """Return the raw table a raw history snapshot keeps the history of.

    The snapshot also reads its keyset table where it has one, so that source
    is left out.

    :param dbt_resource_props: the manifest node of a raw history snapshot.
    :returns: the raw table's name in the ``ol_warehouse_raw_data`` source.
    :rtype: str
    :raises ValueError: when the snapshot reads other than one raw table.
    """
    keyset = dbt_resource_props["config"]["meta"]["raw_history"].get("keyset_table")
    raw = []
    for dep in dbt_resource_props["depends_on"]["nodes"]:
        # source.<project>.<source name>.<table>
        kind, _, source, table = dep.split(".")
        if kind == "source" and source == RAW_DATA_SOURCE and table != keyset:
            raw.append(table)
    if len(raw) != 1:
        msg = (
            f"{dbt_resource_props['name']} reads {raw or 'no'} raw tables; "
            "expected exactly one"
        )
        raise ValueError(msg)
    return raw[0]


def load_check_key(dbt_resource_props: Mapping[str, Any]) -> AssetCheckKey:
    """Return the guard check a raw history snapshot waits on.

    The key exists only where lakehouse loads the raw table (Airbyte). The
    snapshot's gate passes for a key with no check behind it.

    :param dbt_resource_props: the manifest node of a raw history snapshot.
    :returns: the ``history_load_not_short`` check key on its raw table.
    :rtype: AssetCheckKey
    """
    raw_key = AssetKey([RAW_DATA_SOURCE, snapshot_raw_table(dbt_resource_props)])
    return AssetCheckKey(raw_key, CHECK_NAME)


def guarded_tables(manifest: Mapping[str, Any]) -> list[GuardedTable]:
    """Every raw table a live, delete-recording raw history snapshot reads.

    Only enabled snapshots are under ``nodes``; a unit not rolled out
    (``history.live: false``) generates disabled ones, so it gets no check.

    :param manifest: the parsed dbt manifest.
    :returns: one entry per guarded snapshot, sorted by snapshot name.
    :rtype: list[GuardedTable]
    """
    tables = []
    for node in manifest["nodes"].values():
        if not is_guarded_snapshot(node):
            continue
        unique_key = node["config"]["unique_key"]
        tables.append(
            GuardedTable(
                snapshot=node["name"],
                raw_table=snapshot_raw_table(node),
                unique_key=(
                    (unique_key,) if isinstance(unique_key, str) else tuple(unique_key)
                ),
            )
        )
    return sorted(tables, key=lambda table: table.snapshot)


@dataclass(frozen=True)
class LoadCount:
    """One table's line from the macro."""

    snapshot: str
    raw_table: str
    loaded_keys: int
    snapshot_open_records: int | None
    shortfall: int
    max_fraction: float
    min_count: int
    short: bool

    def describe(self) -> str:
        """Return a one-line account of the comparison, for the check's result."""
        if self.snapshot_open_records is None:
            return (
                f"{self.loaded_keys} keys loaded; {self.snapshot} has not run yet, "
                "so there is nothing to compare with"
            )
        verdict = "short" if self.short else "within the limit"
        return (
            f"{self.loaded_keys} keys loaded against {self.snapshot_open_records} "
            f"open records in {self.snapshot}: {self.shortfall} fewer, {verdict} "
            f"(fails above {self.max_fraction} of them, unless no more than "
            f"{self.min_count} and under half)"
        )


def parse_load_counts(messages: Iterable[str]) -> dict[str, LoadCount]:
    """Read the macro's lines out of dbt's log messages, keyed by raw table.

    :param messages: the ``msg`` of each dbt log event.
    :returns: one count per table the macro reported.
    :rtype: dict[str, LoadCount]
    """
    counts = {}
    for message in messages:
        if message.startswith(LOG_PREFIX):
            count = LoadCount(**json.loads(message.removeprefix(LOG_PREFIX)))
            counts[count.raw_table] = count
    return counts
