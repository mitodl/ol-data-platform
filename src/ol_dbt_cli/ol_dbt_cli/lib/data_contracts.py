"""OpenMetadata data contracts kept as code in the repository's ``contracts/`` directory.

Each file names one OpenMetadata entity and carries the native OpenMetadata
``CreateDataContract`` body for it:

    entity:
      type: table
      dbt_model: dim_user          # or dbt_source: <source>.<table>, or fqn: <OM FQN>
    contract:
      name: dim_user
      schema: [{name: user_pk, dataType: VARCHAR}, ...]
      semantics: [...]
      sla: {...}

The body is the native shape rather than ODCS because OpenMetadata's ODCS
importer (2.0.2) has no field for ``semantics``, and a ``mode=replace`` import
clears them. OpenMetadata references the entity and any owners by id, which
only exist in the live catalog, so ``ol-dbt contracts sync`` resolves them. A
dbt binding resolves through the model's or source's relation in the manifest,
which keeps the file free of environment-specific names.

OpenMetadata never blocks a build: it only records a failed validation. For dbt
bindings the check here closes that gap before anything deploys. A contracted
column the model no longer declares or selects, or declares with a type
OpenMetadata would treat as incompatible, is an ERROR. No OpenMetadata
credentials are needed because the contract is in the repo. An ``fqn`` binding
has no local schema to check against; OpenMetadata validates it after ingestion.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry
    from ol_dbt_cli.lib.sql_parser import ParsedModel

DATA_CONTRACT_CHECK = "data_contract"
DEFAULT_CONTRACTS_DIR = Path("contracts")

_BINDING_KEYS = ("dbt_model", "dbt_source", "fqn")

# OpenMetadata's collection path for each entity type a contract may name. The
# four with schema validation in 2.0.2, plus containers for semantics-only
# contracts on files Dagster produces; extend as contracts need more.
ENTITY_COLLECTIONS = {
    "table": "tables",
    "topic": "topics",
    "apiEndpoint": "apiEndpoints",
    "dashboardDataModel": "dashboard/datamodels",
    "container": "containers",
}

# OpenMetadata's DataContractRepository.areTypesCompatible treats types within
# one family as interchangeable, so a VARCHAR -> STRING change is not a
# violation there and must not be one here either.
_TYPE_FAMILIES: tuple[frozenset[str], ...] = (
    frozenset({"STRING", "VARCHAR", "CHAR", "TEXT", "MEDIUMTEXT", "NTEXT", "CLOB"}),
    frozenset({"INT", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "LONG"}),
    frozenset({"DECIMAL", "NUMERIC", "NUMBER", "DOUBLE", "FLOAT", "MONEY"}),
    frozenset({"BOOLEAN"}),
    frozenset({"DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPZ", "TIME"}),
    frozenset({"BINARY", "VARBINARY", "BLOB", "BYTEA", "BYTES", "LONGBLOB", "MEDIUMBLOB"}),
    frozenset({"ARRAY", "MAP", "STRUCT", "JSON"}),
)

# Warehouse spellings (Trino, DuckDB, StarRocks) that are not already an
# OpenMetadata ColumnDataType name.
_WAREHOUSE_TYPE_ALIASES = {
    "INTEGER": "INT",
    "REAL": "FLOAT",
    "ROW": "STRUCT",
    "LARGEINT": "BIGINT",
    "HUGEINT": "BIGINT",
}

# Entity types whose contract `schema` OpenMetadata 2.0.2 checks for column
# types as well as names. Topics and API endpoints are checked by name only, so
# a retyped field would pass; containers reject a schema outright.
SCHEMA_TYPED_ENTITIES = frozenset({"table", "dashboardDataModel"})

_TYPE_HEAD = re.compile(r"^\s*([A-Za-z]+)")


@dataclass(frozen=True)
class EntityBinding:
    """Which OpenMetadata entity a contract applies to, and how to find it."""

    entity_type: str
    kind: str
    """One of ``dbt_model``, ``dbt_source`` or ``fqn``."""
    target: str


@dataclass(frozen=True)
class ContractColumn:
    name: str
    data_type: str


@dataclass(frozen=True)
class DataContract:
    path: Path
    entity: EntityBinding
    body: dict[str, Any]

    @property
    def columns(self) -> list[ContractColumn]:
        return [ContractColumn(name=c["name"].lower(), data_type=c["dataType"]) for c in self.body.get("schema", [])]


def _parse_binding(path: Path, raw: Any) -> EntityBinding:
    if not isinstance(raw, dict) or "type" not in raw:
        msg = f"{path}: `entity` needs a `type` and one of {', '.join(_BINDING_KEYS)}"
        raise ValueError(msg)
    keys = [k for k in _BINDING_KEYS if k in raw]
    if len(keys) != 1:
        msg = f"{path}: `entity` needs exactly one of {', '.join(_BINDING_KEYS)}, got {keys or 'none'}"
        raise ValueError(msg)
    kind = keys[0]
    if raw["type"] not in ENTITY_COLLECTIONS:
        msg = f"{path}: entity type {raw['type']!r} is not one of {', '.join(ENTITY_COLLECTIONS)}"
        raise ValueError(msg)
    if kind != "fqn" and raw["type"] != "table":
        msg = f"{path}: a {kind} binding is always a table, got entity type {raw['type']!r}"
        raise ValueError(msg)
    if kind == "dbt_source" and raw[kind].count(".") != 1:
        msg = f"{path}: dbt_source must be <source_name>.<table_name>, got {raw[kind]!r}"
        raise ValueError(msg)
    return EntityBinding(entity_type=raw["type"], kind=kind, target=raw[kind])


def load_contracts(contracts_dir: Path) -> list[DataContract]:
    """Load every ``*.yaml`` / ``*.yml`` contract in *contracts_dir*, sorted by path.

    :param contracts_dir: directory holding one contract file per entity.
    :returns: the parsed contracts; empty when the directory does not exist.
    :rtype: list[DataContract]
    :raises ValueError: when a file's ``entity`` binding is malformed, its
        ``contract`` lacks a ``name``, or it sets ``contract.entity`` (sync owns it).
    """
    if not contracts_dir.is_dir():
        return []
    contracts = []
    for path in sorted(p for p in contracts_dir.iterdir() if p.suffix in {".yaml", ".yml"}):
        raw = yaml.safe_load(path.read_text())
        if not isinstance(raw, dict) or not isinstance(raw.get("contract"), dict):
            msg = f"{path}: a contract file needs top-level `entity` and `contract` mappings"
            raise ValueError(msg)
        binding = _parse_binding(path, raw.get("entity"))
        body = raw["contract"]
        if "name" not in body:
            msg = f"{path}: `contract` needs a `name`"
            raise ValueError(msg)
        if "entity" in body:
            msg = f"{path}: `contract.entity` is resolved by `ol-dbt contracts sync`; remove it"
            raise ValueError(msg)
        if body.get("schema") and binding.entity_type not in SCHEMA_TYPED_ENTITIES:
            msg = (
                f"{path}: a {binding.entity_type} contract can't carry a `schema`: OpenMetadata only checks "
                f"column types for {', '.join(sorted(SCHEMA_TYPED_ENTITIES))}. Use semantics rules instead."
            )
            raise ValueError(msg)
        contracts.append(DataContract(path=path, entity=binding, body=body))
    return contracts


def manifest_node(binding: EntityBinding, manifest: ManifestRegistry) -> ManifestModel | None:
    """Return the manifest node a dbt binding names, or ``None`` if it is missing or not a dbt binding."""
    if binding.kind == "dbt_model":
        return manifest.get_model(binding.target)
    if binding.kind == "dbt_source":
        return manifest.get_source(binding.target)
    return None


def om_fqn(binding: EntityBinding, manifest: ManifestRegistry, service: str) -> str:
    """OpenMetadata's FQN for the entity *binding* names.

    A dbt binding becomes ``service.database.schema.table`` from the manifest,
    so the manifest must come from the target OpenMetadata catalogs (production).

    :raises ValueError: when a dbt binding names a node the manifest lacks.
    """
    if binding.kind == "fqn":
        return binding.target
    node = manifest_node(binding, manifest)
    if node is None:
        msg = f"{binding.kind} {binding.target!r} is not in the manifest"
        raise ValueError(msg)
    return ".".join((service, node.database, node.schema, node.identifier or node.name))


def create_request(contract: DataContract, entity_id: str, owners: list[dict[str, str]] | None) -> dict[str, Any]:
    """Build the ``CreateDataContract`` body for PUT /v1/dataContracts.

    :param entity_id: the resolved OpenMetadata id of the contracted entity.
    :param owners: resolved owner references (``{"id", "type"}``), replacing
        the name-only ``owners`` in the file; ``None`` when the file has none.
    """
    body = copy.deepcopy(contract.body)
    body["entity"] = {"id": entity_id, "type": contract.entity.entity_type}
    if owners is not None:
        body["owners"] = owners
    return body


def om_data_type(warehouse_type: str) -> str | None:
    """Map a dbt ``data_type`` (e.g. ``varchar``, ``array(bigint)``) to an OpenMetadata type name."""
    match = _TYPE_HEAD.match(warehouse_type)
    if match is None:
        return None
    head = match.group(1).upper()
    return _WAREHOUSE_TYPE_ALIASES.get(head, head)


def types_compatible(contract_type: str, om_type: str) -> bool:
    contract_type = contract_type.upper()
    return contract_type == om_type or any(contract_type in f and om_type in f for f in _TYPE_FAMILIES)


def check_data_contracts(
    contracts: list[DataContract],
    manifest: ManifestRegistry,
    sql_models_by_name: dict[str, ParsedModel],
    report: ValidationReport,
) -> None:
    """Fail when a dbt model or source drops or retypes a column its contract lists.

    A contracted column must be declared in the model or source YAML with a
    ``data_type``, since the manifest carries no other type for it. For a model
    it must also be selected by the SQL, when the SQL resolved to a column list.
    """
    for contract in contracts:
        binding = contract.entity
        if binding.kind == "fqn":
            continue
        node = manifest_node(binding, manifest)
        if node is None:
            report.add(
                DATA_CONTRACT_CHECK,
                Severity.ERROR,
                binding.target,
                f"Contract {contract.path.name} names a {binding.kind} that does not exist",
                "Rename the contract's binding, or delete the contract with the model and retire it in OpenMetadata.",
            )
            continue
        parsed = sql_models_by_name.get(binding.target) if binding.kind == "dbt_model" else None
        sql_columns = (
            parsed.output_columns
            if parsed is not None and not parsed.parse_error and not parsed.has_star and parsed.output_columns
            else None
        )
        for column in contract.columns:
            _check_column(contract, column, node, sql_columns, report)


def _check_column(
    contract: DataContract,
    column: ContractColumn,
    node: ManifestModel,
    sql_columns: set[str] | None,
    report: ValidationReport,
) -> None:
    label = contract.entity.target
    declared = node.columns.get(column.name)
    if declared is None:
        report.add(
            DATA_CONTRACT_CHECK,
            Severity.ERROR,
            label,
            f"Contracted column '{column.name}' is not declared in the YAML",
            f"{contract.path.name} promises this column to consumers. Restore it, or change the "
            "contract in the same PR so the removal is reviewed as a contract change.",
        )
        return
    if sql_columns is not None and column.name not in {c.lower() for c in sql_columns}:
        report.add(
            DATA_CONTRACT_CHECK,
            Severity.ERROR,
            label,
            f"Contracted column '{column.name}' is not selected by the model SQL",
            f"{contract.path.name} promises this column to consumers.",
        )
        return
    if not declared.data_type:
        report.add(
            DATA_CONTRACT_CHECK,
            Severity.ERROR,
            label,
            f"Contracted column '{column.name}' has no data_type in the YAML",
            f"Declare `data_type` for it so a retype is visible to this check (contract: {column.data_type}).",
        )
        return
    om_type = om_data_type(declared.data_type)
    if om_type is None or not types_compatible(column.data_type, om_type):
        report.add(
            DATA_CONTRACT_CHECK,
            Severity.ERROR,
            label,
            f"Contracted column '{column.name}' is {declared.data_type}, contract says {column.data_type}",
            f"Revert the type, or change {contract.path.name} in the same PR.",
        )
