"""`ol-dbt contracts` — publish and validate the OpenMetadata data contracts in ``contracts/``.

The contract files are the source of truth. ``sync`` makes OpenMetadata match
them and ``validate`` asks OpenMetadata to evaluate them now instead of waiting
for its daily run. The credential-free schema check is ``ol-dbt validate --only
data_contract``; see ``ol_dbt_cli.lib.data_contracts``.

Both commands read ``OM_SERVER_URL`` (the API root, e.g.
``https://data.ol.mit.edu/api``) and ``OM_BOT_JWT_TOKEN`` from the environment,
the same pair the OpenMetadata bootstrap Job in ol-infrastructure uses.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
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
from ol_dbt_cli.lib.manifest import ManifestRegistry, find_manifest, load_manifest

console = Console()
err_console = Console(stderr=True)

contracts_app = App(
    name="contracts",
    help="Publish and validate OpenMetadata data contracts from contracts/.",
)

HTTP_TIMEOUT_SECONDS = 60

# OpenMetadata's collection path for each entity type a contract may name.
_ENTITY_COLLECTIONS = {
    "table": "tables",
    "topic": "topics",
    "apiEndpoint": "apiEndpoints",
    "dashboardDataModel": "dashboard/datamodels",
    "container": "containers",
}
_OWNER_COLLECTIONS = {"team": "teams", "user": "users"}
_FAILED_STATUSES = {"Failed", "Aborted"}


def _type_mismatches(schema_validation: dict[str, Any] | None) -> list[str]:
    """Retyped columns OpenMetadata found but does not count as failures.

    DataContractRepository.validateSchemaFieldsAgainstEntity (2.0.2) records a
    type mismatch "for informational purposes" only, so a contract whose column
    changed type still validates as Success. These commands fail on it instead.
    """
    return (schema_validation or {}).get("typeMismatchFields") or []


class OpenMetadataClient:
    def __init__(self, server_url: str, token: str) -> None:
        """Talk to the OpenMetadata API at *server_url* (the ``/api`` root) with a bot JWT."""
        self.server_url = server_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    @classmethod
    def from_env(cls) -> OpenMetadataClient:
        return cls(os.environ["OM_SERVER_URL"], os.environ["OM_BOT_JWT_TOKEN"])

    def request(self, method: str, path: str, body: Any = None, query: dict[str, str] | None = None) -> Any:
        url = f"{self.server_url}{path}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=self._headers)  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:  # noqa: S310
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            msg = f"{method} {path} -> {exc.code}: {exc.read().decode(errors='replace')[:500]}"
            raise RuntimeError(msg) from exc

    def entity_id(self, entity_type: str, fqn: str) -> str:
        collection = _ENTITY_COLLECTIONS[entity_type]
        return self.request("GET", f"/v1/{collection}/name/{urllib.parse.quote(fqn, safe='')}")["id"]

    def owner_refs(self, owners: list[dict[str, str]]) -> list[dict[str, str]]:
        refs = []
        for owner in owners:
            collection = _OWNER_COLLECTIONS[owner["type"]]
            found = self.request("GET", f"/v1/{collection}/name/{urllib.parse.quote(owner['name'], safe='')}")
            refs.append({"id": found["id"], "type": owner["type"]})
        return refs


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
        path = Path(manifest_path) if manifest_path else find_manifest(dbt_dir)
        if path is None:
            err_console.print(
                "[red]Error:[/] dbt-bound contracts need a manifest built for the target OpenMetadata "
                "catalogs (production). Pass --manifest."
            )
            sys.exit(1)
        manifest = load_manifest(path)
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
            help="Validate each contract against the live entity without saving it (POST /v1/dataContracts/validate).",
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
    updatedAt. OpenMetadata rejects a contract whose schema names a column the
    entity lacks; --dry-run reports that without writing.
    """
    contracts, manifest = _load(dbt_dir_path, contracts_dir_path, manifest_path, names)
    client = OpenMetadataClient.from_env()
    failures = 0
    for contract in contracts:
        fqn = om_fqn(contract.entity, manifest, service)
        owners = contract.body.get("owners")
        body = create_request(
            contract,
            client.entity_id(contract.entity.entity_type, fqn),
            client.owner_refs(owners) if owners else None,
        )
        name = escape(contract.body["name"])
        if dry_run:
            result = client.request("POST", "/v1/dataContracts/validate", body)
            if result["valid"] and not _type_mismatches(result.get("schemaValidation")):
                console.print(f"[green]valid[/]   {name} -> {escape(fqn)}")
            else:
                failures += 1
                console.print(f"[bold red]invalid[/] {name} -> {escape(fqn)}")
                console.print_json(data=result)
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
        fqn = om_fqn(contract.entity, manifest, service)
        result = client.request(
            "POST",
            "/v1/dataContracts/entity/validate",
            query={
                "entityId": client.entity_id(contract.entity.entity_type, fqn),
                "entityType": contract.entity.entity_type,
            },
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
