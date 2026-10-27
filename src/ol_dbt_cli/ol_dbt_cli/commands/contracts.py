"""`ol-dbt contracts` — publish and validate the OpenMetadata data contracts in ``contracts/``.

The contract files are the source of truth. ``sync`` makes OpenMetadata match
them and ``validate`` asks OpenMetadata to evaluate them now instead of waiting
for its daily run. The credential-free schema check is ``ol-dbt validate --only
data_contract``; see ``ol_dbt_cli.lib.data_contracts``.

Both commands read ``OM_SERVER_URL`` and ``OM_BOT_JWT_TOKEN`` from the
environment; see ``ol_dbt_cli.lib.openmetadata``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated, Any

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.data_contracts import (
    DEFAULT_CONTRACTS_DIR,
    DataContract,
    create_request,
    load_contracts,
    om_fqn,
)
from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.manifest import ManifestRegistry
from ol_dbt_cli.lib.openmetadata import OpenMetadataClient, load_catalog_manifest

console = Console()
err_console = Console(stderr=True)

contracts_app = App(
    name="contracts",
    help="Publish and validate OpenMetadata data contracts from contracts/.",
)

_FAILED_STATUSES = {"Failed", "Aborted"}


def _type_mismatches(schema_validation: dict[str, Any] | None) -> list[str]:
    """Retyped columns OpenMetadata found but does not count as failures.

    DataContractRepository.validateSchemaFieldsAgainstEntity (2.0.2) records a
    type mismatch "for informational purposes" only, so a contract whose column
    changed type still validates as Success. These commands fail on it instead.
    """
    return (schema_validation or {}).get("typeMismatchFields") or []


def _report_unresolved(contract: DataContract, exc: Exception) -> None:
    """Report a contract whose entity or owners did not resolve, so the rest still run.

    A dbt binding missing from the manifest raises ValueError; an entity or
    owner OpenMetadata does not have raises RuntimeError (a 404).
    """
    console.print(f"[bold red]unresolved[/] {escape(contract.body['name'])}: {escape(str(exc))}")


def _resolve_dirs(dbt_dir_path: str | None, contracts_dir_path: str | None) -> tuple[Path, Path]:
    repo_root = get_repo_root()
    dbt_dir = Path(dbt_dir_path).resolve() if dbt_dir_path else repo_root / "src" / "ol_dbt"
    contracts_dir = Path(contracts_dir_path).resolve() if contracts_dir_path else repo_root / DEFAULT_CONTRACTS_DIR
    return dbt_dir, contracts_dir


def _load(
    dbt_dir_path: str | None, contracts_dir_path: str | None, manifest_path: str | None, names: tuple[str, ...]
) -> tuple[list[DataContract], ManifestRegistry]:
    dbt_dir, contracts_dir = _resolve_dirs(dbt_dir_path, contracts_dir_path)
    contracts = load_contracts(contracts_dir)
    if names:
        unknown = set(names) - {c.body["name"] for c in contracts}
        if unknown:
            err_console.print(f"[red]Error:[/] no contract named {sorted(unknown)} in {contracts_dir}")
            sys.exit(1)
        contracts = [c for c in contracts if c.body["name"] in names]
    manifest = ManifestRegistry()
    if any(c.entity.kind != "fqn" for c in contracts):
        try:
            manifest = load_catalog_manifest(dbt_dir, manifest_path)
        except ValueError as exc:
            err_console.print(f"[red]Error:[/] {escape(str(exc))}")
            sys.exit(1)
    return contracts, manifest


_DBT_DIR = Parameter(name=["--dbt-dir", "-d"], help="dbt project root. Defaults to <repo>/src/ol_dbt.")
_CONTRACTS_DIR = Parameter(name=["--contracts-dir"], help="Contracts directory. Defaults to <repo>/contracts.")
_MANIFEST = Parameter(
    name=["--manifest"],
    help=(
        "manifest.json used to resolve dbt_model/dbt_source bindings to relations. It must describe "
        "the catalogs OpenMetadata ingests (production). Defaults to <dbt-dir>/target/manifest.json."
    ),
)
_SERVICE = Parameter(
    name=["--service"],
    help='OpenMetadata database service that dbt relations live under, e.g. "Starburst Galaxy".',
)
_NAMES = Parameter(name=["--contract", "-c"], help="Only these contracts (by contract name). Repeatable.")


@contracts_app.command
def sync(
    *,
    service: Annotated[str, _SERVICE],
    dry_run: Annotated[
        bool,
        Parameter(
            name=["--dry-run"],
            help="Only validate each contract against the live entity (POST /v1/dataContracts/validate); save nothing.",
        ),
    ] = False,
    names: Annotated[tuple[str, ...], _NAMES] = (),
    dbt_dir_path: Annotated[str | None, _DBT_DIR] = None,
    contracts_dir_path: Annotated[str | None, _CONTRACTS_DIR] = None,
    manifest_path: Annotated[str | None, _MANIFEST] = None,
) -> None:
    """Create or update each contract in OpenMetadata so it matches its file.

    PUT /v1/dataContracts is an upsert keyed on the contract name and entity, so
    re-running with unchanged files is a no-op apart from OpenMetadata's
    updatedAt. Each contract is validated against the live entity first and not
    written if a column is missing or retyped; --dry-run stops after that step.
    """
    contracts, manifest = _load(dbt_dir_path, contracts_dir_path, manifest_path, names)
    client = OpenMetadataClient.from_env()
    failures = 0
    for contract in contracts:
        name = escape(contract.body["name"])
        try:
            fqn = om_fqn(contract.entity, manifest, service)
            owners = contract.body.get("owners")
            body = create_request(
                contract,
                client.entity_id(contract.entity.entity_type, fqn),
                client.named_refs(owners) if owners else None,
            )
        except (ValueError, RuntimeError) as exc:
            failures += 1
            _report_unresolved(contract, exc)
            continue
        # PUT accepts a retyped column (OpenMetadata counts it as informational),
        # so every sync validates first and publishes only a clean contract.
        result = client.request("POST", "/v1/dataContracts/validate", body)
        if not result["valid"] or _type_mismatches(result.get("schemaValidation")):
            failures += 1
            console.print(f"[bold red]invalid[/] {name} -> {escape(fqn)}")
            console.print_json(data=result)
            continue
        if dry_run:
            console.print(f"[green]valid[/]   {name} -> {escape(fqn)}")
            continue
        saved = client.request("PUT", "/v1/dataContracts", body)
        console.print(f"[green]synced[/]  {name} -> {escape(saved['fullyQualifiedName'])} (v{saved['version']})")
    if failures:
        sys.exit(1)


@contracts_app.command
def validate(
    *,
    service: Annotated[str, _SERVICE],
    names: Annotated[tuple[str, ...], _NAMES] = (),
    dbt_dir_path: Annotated[str | None, _DBT_DIR] = None,
    contracts_dir_path: Annotated[str | None, _CONTRACTS_DIR] = None,
    manifest_path: Annotated[str | None, _MANIFEST] = None,
) -> None:
    """Run OpenMetadata's validation of each synced contract now and report the result.

    Calls POST /v1/dataContracts/entity/validate, which records a
    DataContractResult (and raises an incident on failure) exactly as the daily
    run would. Exits non-zero when any contract is Failed or Aborted, or has a
    retyped column, which OpenMetadata reports without failing the contract.
    """
    contracts, manifest = _load(dbt_dir_path, contracts_dir_path, manifest_path, names)
    client = OpenMetadataClient.from_env()
    failures = 0
    for contract in contracts:
        try:
            fqn = om_fqn(contract.entity, manifest, service)
            entity_id = client.entity_id(contract.entity.entity_type, fqn)
        except (ValueError, RuntimeError) as exc:
            failures += 1
            _report_unresolved(contract, exc)
            continue
        result = client.request(
            "POST",
            "/v1/dataContracts/entity/validate",
            query={"entityId": entity_id, "entityType": contract.entity.entity_type},
        )
        status = result["contractExecutionStatus"]
        mismatches = _type_mismatches(result.get("schemaValidation"))
        failed = status in _FAILED_STATUSES or bool(mismatches)
        style = "bold red" if failed else "green"
        console.print(f"[{style}]{status}[/] {escape(contract.body['name'])} -> {escape(fqn)}")
        for mismatch in mismatches:
            console.print(f"  [bold red]type mismatch[/] {escape(mismatch)}")
        for section in ("schemaValidation", "semanticsValidation"):
            detail = result.get(section)
            if detail and detail.get("failed"):
                console.print(f"  {section}:")
                console.print_json(data=detail)
        if failed:
            failures += 1
    if failures:
        sys.exit(1)
