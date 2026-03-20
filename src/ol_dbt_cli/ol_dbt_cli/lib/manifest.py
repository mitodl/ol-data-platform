"""dbt manifest.json parser.

Parses ``target/manifest.json`` produced by ``dbt parse`` or ``dbt compile``
to extract accurate model metadata including columns and lineage — without
needing to re-parse Jinja-laden SQL.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ManifestColumn:
    """Column metadata from the dbt manifest."""

    name: str
    description: str = ""
    data_type: str = ""


@dataclass
class ManifestModel:
    """A single dbt node as extracted from manifest.json."""

    unique_id: str
    name: str
    resource_type: str  # "model", "seed", "source", "snapshot", etc.
    original_file_path: str
    schema: str
    database: str
    columns: dict[str, ManifestColumn] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    """unique_ids of parent nodes."""

    @property
    def column_names(self) -> set[str]:
        return set(self.columns)

    @property
    def is_model(self) -> bool:
        return self.resource_type == "model"


@dataclass
class ManifestRegistry:
    """Registry built from a dbt manifest.json."""

    nodes: dict[str, ManifestModel] = field(default_factory=dict)
    """unique_id -> ManifestModel for all nodes (models, seeds, sources…)."""
    by_name: dict[str, ManifestModel] = field(default_factory=dict)
    """model name -> ManifestModel (models only, last-write-wins on collision)."""
    sources: dict[str, ManifestModel] = field(default_factory=dict)
    """source identifier -> ManifestModel for source nodes.
    Key is ``source_name.table_name`` (e.g. ``ol_warehouse.users``)."""
    children: dict[str, list[str]] = field(default_factory=dict)
    """unique_id -> list of child unique_ids (reverse of depends_on)."""

    def get_model(self, name: str) -> ManifestModel | None:
        return self.by_name.get(name)

    def get_node(self, unique_id: str) -> ManifestModel | None:
        return self.nodes.get(unique_id)

    def get_source(self, source_key: str) -> ManifestModel | None:
        """Return the source node for *source_key* (``source_name.table_name``)."""
        return self.sources.get(source_key)

    def get_children(self, unique_id: str) -> list[ManifestModel]:
        """Return all direct children of *unique_id*."""
        child_ids = self.children.get(unique_id, [])
        return [self.nodes[cid] for cid in child_ids if cid in self.nodes]

    def get_all_descendants(self, unique_id: str) -> list[ManifestModel]:
        """BFS walk returning all descendant nodes of *unique_id*."""
        seen: set[str] = set()
        queue: deque[str] = deque([unique_id])
        result: list[ManifestModel] = []
        while queue:
            current = queue.popleft()
            for child in self.get_children(current):
                if child.unique_id not in seen:
                    seen.add(child.unique_id)
                    result.append(child)
                    queue.append(child.unique_id)
        return result

    def column_consumers(self, unique_id: str, column_name: str) -> list[tuple[ManifestModel, str]]:
        """Return ``(child_model, col_name)`` pairs where a child declares *column_name*.

        This is a heuristic: a child "consumes" a column if its own column set
        contains a column of the same name (implying it was selected or aliased
        from the parent).
        """
        matches: list[tuple[ManifestModel, str]] = []
        for child in self.get_all_descendants(unique_id):
            if column_name in child.column_names:
                matches.append((child, column_name))
        return matches


def _parse_node(node_data: dict[str, Any]) -> ManifestModel:
    """Convert a raw manifest node dict into a :class:`ManifestModel`."""
    columns: dict[str, ManifestColumn] = {}
    for col_name, col_data in node_data.get("columns", {}).items():
        columns[col_name.lower()] = ManifestColumn(
            name=col_name.lower(),
            description=col_data.get("description", ""),
            data_type=col_data.get("data_type", ""),
        )

    depends_on = node_data.get("depends_on", {}).get("nodes", [])

    return ManifestModel(
        unique_id=node_data["unique_id"],
        name=node_data.get("name", ""),
        resource_type=node_data.get("resource_type", "model"),
        original_file_path=node_data.get("original_file_path", ""),
        schema=node_data.get("schema", ""),
        database=node_data.get("database", ""),
        columns=columns,
        depends_on=depends_on,
    )


def load_manifest(manifest_path: Path) -> ManifestRegistry:
    """Load and parse a dbt ``manifest.json`` file into a :class:`ManifestRegistry`."""
    raw: dict[str, Any] = json.loads(manifest_path.read_text())

    registry = ManifestRegistry()

    all_nodes: dict[str, Any] = {}
    all_nodes.update(raw.get("nodes", {}))
    all_nodes.update(raw.get("sources", {}))
    all_nodes.update(raw.get("seeds", {}))
    all_nodes.update(raw.get("snapshots", {}))

    for uid, node_data in all_nodes.items():
        model = _parse_node(node_data)
        registry.nodes[uid] = model
        if model.is_model:
            registry.by_name[model.name] = model
        elif model.resource_type == "source":
            # Index by "source_name.table_name" matching the sql_parser placeholder format
            source_name = node_data.get("source_name", "")
            table_name = node_data.get("name", "")
            if source_name and table_name:
                registry.sources[f"{source_name}.{table_name}"] = model

    # Build child map (reverse of depends_on)
    for uid, model in registry.nodes.items():
        for parent_uid in model.depends_on:
            registry.children.setdefault(parent_uid, [])
            if uid not in registry.children[parent_uid]:
                registry.children[parent_uid].append(uid)

    return registry


def find_manifest(dbt_dir: Path) -> Path | None:
    """Find the dbt manifest.json in common locations relative to *dbt_dir*."""
    candidates = [
        dbt_dir / "target" / "manifest.json",
        dbt_dir / "manifest.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None
