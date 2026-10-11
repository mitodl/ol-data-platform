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
from ol_dbt_cli.lib.raw_fixtures import FIXTURE_STRATEGY

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.manifest import ManifestRegistry

BranchState = Literal["present", "partial", "absent"]
MAX_TABLES_LISTED = 5


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
        if self.strategy != FIXTURE_STRATEGY:
            return f"strategies.local is {self.strategy}"
        if not self.has_fixture:
            return "no fixture committed for this unit"
        if self.absent:
            return "run `ol-dbt fixtures load`, or add the tables to the fixture"
        return "the tables hold no rows"


def local_branches(
    manifest: ManifestRegistry,
    units: list[Unit],
    models: Iterable[str],
    rows: Mapping[str, int],
    fixture_units: set[str],
) -> dict[str, list[Branch]]:
    """Return the branches of each union among ``models``.

    :param models: names of the models the build produced
    :param rows: row count of every table in the local raw schema; a table not
        in it does not exist there
    :param fixture_units: keys of the units with a committed fixture file
    :returns: model name to its branches, for models above staging that read
        more than one unit. A model reading one unit cannot come out partial
        across branches, and with no rows in it the build is empty, not partial.
    """
    owners = tables_by_raw_name(units)
    strategies = {unit.key: str(unit.data["strategies"]["local"]) for unit in units}
    lineage = upstream_tables(manifest, units)
    layers = {node.name: classify_layer(node.original_file_path) for node in manifest.nodes.values() if node.is_model}

    report: dict[str, list[Branch]] = {}
    for model in sorted(set(models)):
        by_unit: dict[str, list[str]] = {}
        for table in sorted(lineage.get(model, ())):
            by_unit.setdefault(owners[table], []).append(table)
        if len(by_unit) < 2 or layers[model] in UNCONTRACTED_LAYERS:  # noqa: PLR2004
            continue
        report[model] = [
            Branch(
                unit=unit,
                strategy=strategies[unit],
                tables=tuple(tables),
                absent=tuple(t for t in tables if t not in rows),
                empty=tuple(t for t in tables if rows.get(t) == 0),
                has_fixture=unit in fixture_units,
            )
            for unit, tables in sorted(by_unit.items())
        ]
    return report


def raw_tables_read(manifest: ManifestRegistry, units: list[Unit], models: Iterable[str]) -> set[str]:
    """Return every inventory-declared raw table in the lineage of ``models``."""
    lineage = upstream_tables(manifest, units)
    return {table for model in models for table in lineage.get(model, ())}


def render_local_branches(report: Mapping[str, list[Branch]], raw_schema: str) -> list[str]:
    """Return the lines to print after a local build.

    A union with every branch present is counted, not listed: the report is for
    the ones that came out partial.
    """
    if not report:
        return []
    partial = {model: branches for model, branches in report.items() if any(b.state != "present" for b in branches)}
    lines = [
        f"Ingestion branches in {raw_schema}: {len(report) - len(partial)} of {len(report)} "
        "union model(s) built with every branch present."
    ]
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
