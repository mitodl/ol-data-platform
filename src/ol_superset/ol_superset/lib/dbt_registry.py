"""dbt model registry — parses local dbt YAML schema files for validation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DbtModel:
    """Metadata for a single dbt model as parsed from YAML schema files."""

    name: str
    layer: str  # e.g. "marts", "dimensional", "reporting"
    expected_schema: str  # full production schema e.g. "ol_warehouse_production_mart"
    columns: set[str] = field(default_factory=set)


@dataclass
class DbtRegistry:
    """Registry of all dbt models discovered from YAML schema files."""

    models: dict[str, DbtModel] = field(default_factory=dict)
    # layer name -> dbt schema suffix (e.g. "marts" -> "mart")
    schema_suffixes: dict[str, str] = field(default_factory=dict)
    # full schema name -> layer (reverse of the above, for lookup from dataset)
    schema_to_layer: dict[str, str] = field(default_factory=dict)

    def get_model(self, name: str) -> DbtModel | None:
        return self.models.get(name)

    def expected_schema_for_model(self, name: str) -> str | None:
        model = self.models.get(name)
        return model.expected_schema if model else None


def _parse_schema_mappings(
    dbt_project_yml: Path,
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Read dbt_project.yml and extract layer -> schema_suffix mappings.

    Returns (suffix_map, full_schema_map) where:
      suffix_map: layer -> schema suffix (e.g. {"marts": "mart"})
      full_schema_map: full_schema -> layer
        (e.g. {"ol_warehouse_production_mart": "marts"})
    """
    data: dict[str, Any] = yaml.safe_load(dbt_project_yml.read_text())

    # Read the production base schema from profiles.yml if available, falling
    # back to a well-known default for this project.
    base_schema = _infer_base_schema(dbt_project_yml.parent)

    # Navigate to models.<project_name> config block
    models_config = data.get("models", {})
    project_name = data.get("name", "")
    project_models = models_config.get(project_name, models_config)

    suffix_map: dict[str, str] = {}
    for layer, config in project_models.items():
        if isinstance(config, dict):
            schema_suffix = config.get("+schema")
            if schema_suffix:
                suffix_map[layer] = schema_suffix

    full_schema_map: dict[str, str] = {
        f"{base_schema}_{suffix}": layer for layer, suffix in suffix_map.items()
    }
    return suffix_map, full_schema_map


def _infer_base_schema(dbt_dir: Path) -> str:
    """
    Infer the production base schema from profiles.yml.

    Falls back to the well-known project default if the file cannot be parsed.
    """
    profiles_yml = dbt_dir / "profiles.yml"
    if profiles_yml.exists():
        try:
            profiles: dict[str, Any] = yaml.safe_load(profiles_yml.read_text())
            for _profile_name, profile in profiles.items():
                outputs = profile.get("outputs", {})
                prod = outputs.get("production", {})
                schema = prod.get("schema")
                if schema and "{{" not in schema:
                    return schema
        except Exception:  # noqa: BLE001, S110
            pass
    return "ol_warehouse_production"


def _extract_model_layer(model_path: Path, models_root: Path) -> str:
    """Return the layer name (top-level directory under models/) for a model file."""
    try:
        rel = model_path.relative_to(models_root)
        return rel.parts[0] if rel.parts else "unknown"
    except ValueError:
        return "unknown"


def _parse_models_from_yaml(
    yaml_file: Path,
    models_root: Path,
    suffix_map: dict[str, str],
    base_schema: str,
) -> list[DbtModel]:
    """Parse all models defined in a single dbt YAML schema file."""
    try:
        data: Any = yaml.safe_load(yaml_file.read_text())
    except Exception:  # noqa: BLE001
        return []

    if not isinstance(data, dict):
        return []

    layer = _extract_model_layer(yaml_file, models_root)
    schema_suffix = suffix_map.get(layer, layer)
    expected_schema = f"{base_schema}_{schema_suffix}"

    parsed: list[DbtModel] = []
    for model_def in data.get("models", []):
        if not isinstance(model_def, dict):
            continue
        name = model_def.get("name")
        if not name:
            continue

        columns: set[str] = set()
        for col in model_def.get("columns", []):
            if isinstance(col, dict) and col.get("name"):
                columns.add(col["name"])

        parsed.append(
            DbtModel(
                name=name,
                layer=layer,
                expected_schema=expected_schema,
                columns=columns,
            )
        )

    return parsed


def build_dbt_registry(dbt_dir: Path) -> DbtRegistry:
    """
    Build a DbtRegistry from a dbt project directory.

    Parses all ``_*.yml`` files under ``models/`` and ``seeds/`` to extract
    model names, layers, expected production schemas, and documented columns.

    Args:
        dbt_dir: Path to the dbt project root (contains dbt_project.yml).

    Returns:
        DbtRegistry populated with all discovered models.
    """
    dbt_project_yml = dbt_dir / "dbt_project.yml"
    if not dbt_project_yml.exists():
        msg = f"dbt_project.yml not found in {dbt_dir}"
        raise FileNotFoundError(msg)

    suffix_map, full_schema_map = _parse_schema_mappings(dbt_project_yml)
    base_schema = _infer_base_schema(dbt_dir)

    registry = DbtRegistry(schema_suffixes=suffix_map, schema_to_layer=full_schema_map)

    models_root = dbt_dir / "models"
    for yaml_file in sorted(models_root.rglob("_*.yml")):
        for model in _parse_models_from_yaml(
            yaml_file, models_root, suffix_map, base_schema
        ):
            registry.models[model.name] = model

    # Also scan seeds directory
    seeds_root = dbt_dir / "seeds"
    if seeds_root.exists():
        for yaml_file in sorted(seeds_root.rglob("_*.yml")):
            for model in _parse_models_from_yaml(
                yaml_file, seeds_root, {"seeds": "seeds"}, base_schema
            ):
                registry.models[model.name] = model

    return registry


def extract_sql_table_refs(sql: str) -> set[str]:
    """
    Extract table names referenced in a virtual dataset SQL string.

    Only extracts identifiers that appear in table position (after FROM or JOIN
    keywords), avoiding false positives from column alias references like
    ``alias.column_name``.

    Handles two-part (``schema.table``) and three-part
    (``catalog.schema.table``) references.

    Returns a set of bare table names (last component, lowercased).
    """
    if not sql:
        return set()

    # Only match identifiers that follow FROM or JOIN keywords so we avoid
    # picking up column-alias references like "a.user_id".
    pattern = re.compile(
        r"\b(?:from|join)\s+"
        r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){1,2})"
        r"(?:\s+(?:as\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?",
        re.IGNORECASE,
    )
    tables: set[str] = set()
    for match in pattern.finditer(sql):
        ref = match.group(1)
        # Take the last part as the table name
        table_name = ref.rsplit(".", 1)[-1].lower()
        tables.add(table_name)

    return tables
