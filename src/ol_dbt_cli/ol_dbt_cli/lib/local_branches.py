"""Which ingestion branches a build on the local lake had: RFC 12711 step 4, locally.

A model above staging unions several ingestion units, and a build of one does
not fail when a branch has no rows: it comes out partial. QA answers that with a
declared contract checked against a committed observation
(``lib.qa_contract``). The local lake holds whatever one developer loaded, so
there is nothing to declare and nothing to commit. What a local build can do is
say, for each union it built, which of its units had rows in the raw schema.

A unit is a branch here for the reason it is one in QA: ``(deployment, layer)``
is what gets loaded or left out, by ``ol-dbt fixtures load`` or by a dlt source.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from ol_dbt_cli.lib.dimensional_layering import classify_layer
from ol_dbt_cli.lib.inventory import tables_by_raw_name
from ol_dbt_cli.lib.qa_contract import UNCONTRACTED_LAYERS, upstream_tables
from ol_dbt_cli.lib.raw_fixtures import FIXTURE_STRATEGIES, FIXTURE_STRATEGY

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.manifest import ManifestRegistry

BranchState = Literal["present", "partial", "absent"]
MAX_TABLES_LISTED = 5
BUILT_RESOURCE_TYPES = frozenset({"model", "snapshot"})
"""What a dbt run writes a table for from other tables. A seed's rows come from a file."""


@dataclass(frozen=True)
class Branch:
    """One unit a built model reads, and how much of it the local raw schema holds."""

    unit: str
    strategy: str
    tables: tuple[str, ...]
    """The unit's raw tables in the model's lineage, not every table it declares."""
    absent: tuple[str, ...]
    empty: tuple[str, ...]
    has_fixture: bool

    @property
    def state(self) -> BranchState:
        lacking = len(self.absent) + len(self.empty)
        if lacking == 0:
            return "present"
        return "absent" if lacking == len(self.tables) else "partial"

    @property
    def remedy(self) -> str:
        """What fills the branch, or why nothing will."""
        if self.strategy not in FIXTURE_STRATEGIES:
            return f"strategies.local is {self.strategy}"
        if self.strategy != FIXTURE_STRATEGY:
            ingest = f"run `ol-dbt ingest local --unit {self.unit}` with the app running"
            return f"{ingest}, or `ol-dbt fixtures load`" if self.has_fixture and self.absent else ingest
        if not self.has_fixture:
            return "no fixture committed for this unit"
        if self.absent:
            return "run `ol-dbt fixtures load`, or add the tables to the fixture"
        return "the tables hold no rows"


def union_tables(
    manifest: ManifestRegistry, units: list[Unit], models: Iterable[str]
) -> dict[str, dict[str, list[str]]]:
    """Return, for each union among ``models``, the raw tables it reads by unit.

    :param models: names of the models the build produced
    :returns: model name to unit key to raw tables, for models above staging
        that read more than one unit. A model reading one unit cannot come out
        partial across branches, and with no rows in it the build is empty, not
        partial.
    """
    owners = tables_by_raw_name(units)
    lineage = upstream_tables(manifest, units)
    layers = {node.name: classify_layer(node.original_file_path) for node in manifest.nodes.values() if node.is_model}

    unions: dict[str, dict[str, list[str]]] = {}
    for model in sorted(set(models)):
        by_unit: dict[str, list[str]] = {}
        for table in sorted(lineage.get(model, ())):
            by_unit.setdefault(owners[table], []).append(table)
        if len(by_unit) > 1 and layers[model] not in UNCONTRACTED_LAYERS:
            unions[model] = dict(sorted(by_unit.items()))
    return unions


def local_branches(
    unions: Mapping[str, Mapping[str, list[str]]],
    units: list[Unit],
    rows: Mapping[str, int],
    fixture_units: set[str],
) -> dict[str, list[Branch]]:
    """Return the branches of each union, judged against the raw schema's row counts.

    :param unions: what :func:`union_tables` returned
    :param rows: row count of every table in the local raw schema; a table not
        in it does not exist there
    :param fixture_units: keys of the units with a committed fixture file
    """
    strategies = {unit.key: str(unit.data["strategies"]["local"]) for unit in units}
    return {
        model: [
            Branch(
                unit=unit,
                strategy=strategies[unit],
                tables=tuple(tables),
                absent=tuple(t for t in tables if t not in rows),
                empty=tuple(t for t in tables if rows.get(t) == 0),
                has_fixture=unit in fixture_units,
            )
            for unit, tables in by_unit.items()
        ]
        for model, by_unit in unions.items()
    }


def parents_not_built(manifest: ManifestRegistry, models: Iterable[str], built: set[str]) -> dict[str, int]:
    """Count, for each of ``models``, the tables upstream of it that the run did not build.

    A union reads its parents' tables, not the raw schema. With saved state or a
    selection without ``+``, dbt rebuilds the union alone, on parents built
    from whatever the raw schema held at the time. The row counts then say
    nothing certain about the table just built.

    A snapshot is a parent like any model: `dbt run` builds every model above
    one and leaves the snapshot's own table as it was.

    :param built: names of the models and snapshots the run built
    :returns: model name to the number of its upstream models and snapshots
        outside ``built``, for the models with any
    """
    by_name = {node.name: node for node in manifest.nodes.values() if node.is_model}
    memo: dict[str, set[str]] = {}

    def ancestors(unique_id: str) -> set[str]:
        if unique_id not in memo:
            found: set[str] = set()
            for parent_id in manifest.nodes[unique_id].depends_on:
                parent = manifest.nodes.get(parent_id)
                if parent is not None and parent.resource_type in BUILT_RESOURCE_TYPES:
                    found |= {parent.name} | ancestors(parent_id)
            memo[unique_id] = found
        return memo[unique_id]

    counts = {model: len(ancestors(by_name[model].unique_id) - built) for model in models}
    return {model: count for model, count in counts.items() if count}


def render_local_branches(
    report: Mapping[str, list[Branch]], raw_schema: str, not_rebuilt: Mapping[str, int]
) -> list[str]:
    """Return the lines to print after a local build.

    A union with every branch present is counted, not listed: the report is for
    the ones that came out partial.

    :param not_rebuilt: what :func:`parents_not_built` returned for the unions
    """
    if not report:
        return []
    partial = {model: branches for model, branches in report.items() if any(b.state != "present" for b in branches)}
    lines = [
        f"Ingestion branches in {raw_schema}: {len(report) - len(partial)} of {len(report)} "
        "union model(s) built have rows in every branch."
    ]
    if not_rebuilt:
        names = sorted(not_rebuilt)
        more = len(names) - MAX_TABLES_LISTED
        listed = ", ".join(names[:MAX_TABLES_LISTED]) + (f", and {more} more" if more > 0 else "")
        lines.append(
            f"  {len(names)} of them were built on parents this run did not rebuild, so their rows "
            f"may predate these counts: {listed}. A `+` before the model in --select rebuilds its parents too."
        )
    for model, branches in partial.items():
        held = sum(1 for b in branches if b.state == "present")
        lines.append(f"  {model}: {held} of {len(branches)} branches present")
        for branch in branches:
            if branch.state == "present":
                lines.append(f"    {branch.unit}: present")
                continue
            if branch.state == "absent":
                # Every table it reads is missing or empty: the count says it all.
                lines.append(f"    {branch.unit}: absent ({len(branch.tables)} table(s)); {branch.remedy}")
                continue
            lacking = [
                *(f"{table} (absent)" for table in branch.absent),
                *(f"{table} (no rows)" for table in branch.empty),
            ]
            more = len(lacking) - MAX_TABLES_LISTED
            listed = ", ".join(lacking[:MAX_TABLES_LISTED]) + (f", and {more} more" if more > 0 else "")
            lines.append(
                f"    {branch.unit}: partial ({len(lacking)} of {len(branch.tables)} table(s)); {branch.remedy}"
            )
            lines.append(f"      {listed}")
    return lines
