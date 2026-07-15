"""YAML schema registry — parses dbt _*.yml schema files under models/."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class YamlColumn:
    """A column definition as declared in a dbt YAML schema file."""

    name: str
    description: str = ""
    tests: list[str] = field(default_factory=list)


@dataclass
class YamlSourceTable:
    """A table entry inside a dbt ``sources:`` block."""

    name: str
    source_name: str
    """The parent source name (e.g. ``ol_warehouse_raw_data``)."""
    description: str = ""
    columns: dict[str, YamlColumn] = field(default_factory=dict)

    @property
    def column_names(self) -> set[str]:
        return set(self.columns)


@dataclass
class YamlSource:
    """A source definition as parsed from a dbt YAML schema file."""

    name: str
    source_file: Path
    tables: dict[str, YamlSourceTable] = field(default_factory=dict)


@dataclass
class YamlModel:
    """A model definition as parsed from a dbt YAML schema file."""

    name: str
    source_file: Path
    description: str = ""
    columns: dict[str, YamlColumn] = field(default_factory=dict)

    @property
    def column_names(self) -> set[str]:
        return set(self.columns)


@dataclass
class YamlRegistry:
    """Registry of all models and sources discovered from dbt YAML schema files."""

    models: dict[str, YamlModel] = field(default_factory=dict)
    # Path -> list of model names declared in that file (for impact by changed file)
    file_to_models: dict[Path, list[str]] = field(default_factory=dict)
    sources: dict[str, YamlSource] = field(default_factory=dict)
    """Source definitions keyed by source name (e.g. ``"ol_warehouse_raw_data"``)."""

    def get_model(self, name: str) -> YamlModel | None:
        return self.models.get(name)

    def models_in_file(self, yaml_path: Path) -> list[YamlModel]:
        names = self.file_to_models.get(yaml_path, [])
        return [self.models[n] for n in names if n in self.models]

    def get_source_table(self, source_name: str, table_name: str) -> YamlSourceTable | None:
        """Return the :class:`YamlSourceTable` for *source_name* / *table_name*, or ``None``."""
        source = self.sources.get(source_name)
        if source is None:
            return None
        return source.tables.get(table_name)

    def get_source_columns(self, source_name: str, table_name: str) -> set[str] | None:
        """Return the column name set for *source_name* / *table_name*, or ``None`` if unknown."""
        table = self.get_source_table(source_name, table_name)
        if table is None or not table.columns:
            return None
        return table.column_names


def _parse_column_tests(col_raw: dict[str, Any]) -> list[str]:
    """Flatten a column's dbt test specs to test name strings.

    Reads both the legacy ``tests:`` key and the dbt>=1.8 ``data_tests:``
    spelling (this project uses ``data_tests:`` almost exclusively). Each entry
    is either a bare string (``unique``) or a ``{name: {...}}`` config dict.
    """
    result: list[str] = []
    for key in ("tests", "data_tests"):
        for entry in col_raw.get(key, []) or []:
            if isinstance(entry, str):
                result.append(entry)
            elif isinstance(entry, dict):
                result.extend(entry.keys())
    return result


def _parse_columns(raw_columns: list[Any]) -> dict[str, YamlColumn]:
    """Parse a raw ``columns:`` list into a column dict.

    Skips any entry that itself has a ``columns:`` key — these are nested model
    definitions accidentally placed inside another model's columns list (a YAML
    authoring error).  Real column entries only carry ``name``, ``description``,
    ``tests``, ``tags``, etc.
    """
    columns: dict[str, YamlColumn] = {}
    for col_raw in raw_columns:
        if not isinstance(col_raw, dict):
            continue
        # A column entry that has its own "columns:" sub-key is actually a nested
        # model definition, not a column.  Skip it to avoid polluting the column list.
        if "columns" in col_raw:
            continue
        raw_name = col_raw.get("name", "")
        if not raw_name:
            continue
        # Normalize case at this single boundary: the manifest and sql_parser both
        # lowercase column names, so a verbatim YAML name like `CEUs` would otherwise
        # read as both a phantom (not in the lowercased SQL set) and undocumented.
        col_name = raw_name.lower()
        columns[col_name] = YamlColumn(
            name=col_name,
            description=col_raw.get("description", ""),
            tests=_parse_column_tests(col_raw),
        )
    return columns


def _parse_yaml_file(path: Path) -> tuple[list[YamlModel], list[YamlSource]]:
    """Parse a single _*.yml file and return ``(models, sources)``."""
    try:
        raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError:
        return [], []

    models: list[YamlModel] = []
    models_raw = raw.get("models", [])
    if isinstance(models_raw, list):
        for model_raw in models_raw:
            if not isinstance(model_raw, dict):
                continue
            name = model_raw.get("name", "")
            if not name:
                continue
            models.append(
                YamlModel(
                    name=name,
                    source_file=path,
                    description=model_raw.get("description", ""),
                    columns=_parse_columns(model_raw.get("columns", [])),
                )
            )
            # Also rescue nested model definitions that were accidentally placed
            # inside this model's columns list (a YAML authoring error).  Any
            # column entry that has its own "columns:" key is actually a model.
            for col_raw in model_raw.get("columns", []):
                if not isinstance(col_raw, dict) or "columns" not in col_raw:
                    continue
                nested_name = col_raw.get("name", "")
                if not nested_name:
                    continue
                models.append(
                    YamlModel(
                        name=nested_name,
                        source_file=path,
                        description=col_raw.get("description", ""),
                        columns=_parse_columns(col_raw.get("columns", [])),
                    )
                )

    sources: list[YamlSource] = []
    sources_raw = raw.get("sources", [])
    if isinstance(sources_raw, list):
        for source_raw in sources_raw:
            if not isinstance(source_raw, dict):
                continue
            source_name = source_raw.get("name", "")
            if not source_name:
                continue
            tables: dict[str, YamlSourceTable] = {}
            for table_raw in source_raw.get("tables", []):
                if not isinstance(table_raw, dict):
                    continue
                table_name = table_raw.get("name", "")
                if not table_name:
                    continue
                tables[table_name] = YamlSourceTable(
                    name=table_name,
                    source_name=source_name,
                    description=table_raw.get("description", ""),
                    columns=_parse_columns(table_raw.get("columns", [])),
                )
            sources.append(YamlSource(name=source_name, source_file=path, tables=tables))

    return models, sources


def build_yaml_registry(models_dir: Path) -> YamlRegistry:
    """Walk *models_dir* recursively and build a registry from every YAML schema file.

    Indexes all ``.yml``/``.yaml`` files (matching how dbt itself discovers
    schema/property files, and how :func:`git_utils.get_changed_yaml_models`
    detects changed ones). Restricting to the ``_*.yml`` convention here would
    silently drop any differently-named schema file: it would be detected as
    changed but never resolve to a model, so ``validate --changed-only`` would
    skip it. Non-schema YAML is harmless — :func:`_parse_yaml_file` only reads
    ``models:``/``sources:`` keys and yields nothing for anything else.
    """
    registry = YamlRegistry()
    yaml_paths = sorted({*models_dir.rglob("*.yml"), *models_dir.rglob("*.yaml")})
    for yaml_path in yaml_paths:
        models, sources = _parse_yaml_file(yaml_path)
        file_names: list[str] = []
        for model in models:
            registry.models[model.name] = model
            file_names.append(model.name)
        if file_names:
            registry.file_to_models[yaml_path] = file_names
        for source in sources:
            registry.sources[source.name] = source
    return registry
