"""Generate the dbt raw-source declarations from the ingestion inventory (spec §5, step 7).

The inventory owns two facts the sources YAML repeats: which raw tables dbt
models (``modeled: true``, §1.4) and which loader lands each of them (§1.2).
This brings the YAML in line with both and touches nothing else. Columns and
descriptions stay hand-curated, and a table the inventory does not declare
(retired, or not yet reconciled) is left where it is, because deleting a source
dbt still reads is not something a generator gets to decide.

``loader`` is a property of the source block, not of the table, so a file whose
tables come from more than one loader cannot be described by one block.
``_edxorg_sources.yml`` is the case that exists: Airbyte, dlt and Dagster
tables under a single ``loader: airbyte``. Such a block is split into one block
per loader, all under the same source name, which dbt accepts and which leaves
every ``source('ol_warehouse_raw_data', ...)`` reference unchanged.

Files are edited with ruamel's round-trip mode so comments and quoting survive.
Its line wrapping differs from the ruamel the ``yamlfmt`` pre-commit hook pins,
so a rewritten file is only byte-stable after that hook runs; files with nothing
to change are never rewritten.
"""

from __future__ import annotations

import copy
import io
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

if TYPE_CHECKING:
    from pathlib import Path

    from ol_dbt_cli.lib.inventory import Unit

RAW_SOURCE_NAME = "ol_warehouse_raw_data"


@dataclass(frozen=True)
class LoaderChange:
    raw_table: str
    previous: str | None
    loader: str


@dataclass
class SourcesPlan:
    """What regenerating the sources would change, before anything is written."""

    contents: dict[Path, str] = field(default_factory=dict)
    loader_changes: list[LoaderChange] = field(default_factory=list)
    added: dict[str, Path] = field(default_factory=dict)
    unplaced: list[str] = field(default_factory=list)


def _yaml() -> YAML:
    # The settings the yamlfmt pre-commit hook formats these files with.
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.explicit_start = True
    yaml.width = 80
    yaml.indent(mapping=2, sequence=2, offset=0)
    return yaml


def _raw_blocks(document: Any) -> list[CommentedMap]:
    sources = document.get("sources") if isinstance(document, dict) else None
    return [block for block in sources or [] if block.get("name") == RAW_SOURCE_NAME]


def _split_by_loader(block: CommentedMap, loaders: dict[str, str], changes: list[LoaderChange]) -> list[CommentedMap]:
    """Return the block, or the blocks it becomes, with each carrying one loader.

    A table the inventory does not declare keeps the block's current loader.
    The group holding the block's first table reuses the block itself, so a
    file's leading block stays where it was; the others follow it in order of
    first appearance.
    """
    current = block.get("loader")
    groups: dict[str, list[Any]] = {}
    for table in block.get("tables") or []:
        loader = loaders.get(table["name"], current)
        groups.setdefault(loader, []).append(table)
        if loader != current:
            changes.append(LoaderChange(table["name"], current, loader))

    if not groups or list(groups) == [current]:
        return [block]

    blocks = []
    for index, (loader, tables) in enumerate(groups.items()):
        # Deep copies, because a nested value (`freshness`, `meta`) shared
        # between two blocks is serialized as a YAML anchor and alias.
        target = block if index == 0 else CommentedMap((k, copy.deepcopy(v)) for k, v in block.items() if k != "tables")
        # None is the group of undeclared tables in a block that had no loader;
        # they keep having none rather than gaining a `loader:` null.
        if "loader" in target:
            target["loader"] = loader
        elif loader is not None:
            target.insert(1, "loader", loader)
        target["tables"] = CommentedSeq(tables)
        blocks.append(target)
    return blocks


def plan_sources(units: list[Unit], sources_files: list[Path]) -> SourcesPlan:
    """Work out how each sources file must change to agree with the inventory.

    :param units: The loaded inventory.
    :param sources_files: Every dbt schema file that may declare raw sources.
    :returns: New contents for the files that change, and why each does.
    :rtype: SourcesPlan
    """
    loaders: dict[str, str] = {}
    unit_of: dict[str, str] = {}
    modeled: list[str] = []
    for unit in units:
        for table in unit.tables:
            loaders[table["raw_table"]] = unit.data["loader"]
            unit_of[table["raw_table"]] = unit.key
            if table.get("modeled"):
                modeled.append(table["raw_table"])

    yaml = _yaml()
    plan = SourcesPlan()
    documents: dict[Path, Any] = {}
    changed: set[Path] = set()
    declared: set[str] = set()

    for path in sorted(sources_files):
        document = yaml.load(path.read_text())
        blocks = _raw_blocks(document)
        if not blocks:
            continue
        documents[path] = document
        for block in blocks:
            declared.update(table["name"] for table in block.get("tables") or [])
            before = len(plan.loader_changes)
            split = _split_by_loader(block, loaders, plan.loader_changes)
            if len(plan.loader_changes) == before:
                continue
            changed.add(path)
            sources = document["sources"]
            position = next(i for i, candidate in enumerate(sources) if candidate is block)
            sources[position + 1 : position + 1] = split[1:]

    # A modeled table dbt does not declare yet goes next to a table from the same
    # unit under the same loader. With no such neighbour there is no file that is
    # obviously its home, and guessing one is how a table ends up somewhere
    # nobody looks for it.
    for raw_table in modeled:
        if raw_table in declared:
            continue
        home = next(
            (
                (path, block)
                for path, document in documents.items()
                for block in _raw_blocks(document)
                if block.get("loader") == loaders[raw_table]
                and any(unit_of.get(table["name"]) == unit_of[raw_table] for table in block.get("tables") or [])
            ),
            None,
        )
        if home is None:
            plan.unplaced.append(raw_table)
            continue
        path, block = home
        block["tables"].append(CommentedMap([("name", raw_table), ("description", "")]))
        plan.added[raw_table] = path
        changed.add(path)

    for path in sorted(changed):
        buffer = io.StringIO()
        yaml.dump(documents[path], buffer)
        plan.contents[path] = buffer.getvalue()
    return plan
