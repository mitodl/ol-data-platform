"""`ol-dbt metrics` — maintain the business metric registry in ``metrics/``.

The credential-free check is ``ol-dbt validate --only metric_registry``; see
``ol_dbt_cli.lib.metric_registry``. It validates each metric body against a
committed snapshot of OpenMetadata's request schema, and ``refresh-schema``
rewrites that snapshot from a server. ``sync`` publishes the files to
OpenMetadata as Metric entities; see ``ol_dbt_cli.lib.metric_sync`` for why it
PUTs and PATCHes what it does.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path
from typing import Annotated, Any

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.manifest import ManifestRegistry
from ol_dbt_cli.lib.metric_registry import (
    DEFAULT_METRICS_DIR,
    EXCLUDED_FIELDS,
    SCHEMA_SNAPSHOT,
    SCHEMA_VERSION_KEY,
    MetricDefinition,
    build_schema_snapshot,
    file_status_problem,
    load_metrics,
)
from ol_dbt_cli.lib.metric_sync import (
    IN_REVIEW_STATUS,
    METRIC_FIELDS,
    STATUS_FIELD,
    TAG_COLLECTIONS,
    after_put,
    create_body,
    drift,
    last_synced_version,
    lineage_edge,
    lineage_sources,
    patch_ops,
    same_description,
    structural_changes,
)
from ol_dbt_cli.lib.openmetadata import (
    JSON_PATCH_CONTENT_TYPE,
    OpenMetadataClient,
    load_catalog_manifest,
    quote_fqn,
)

console = Console()
err_console = Console(stderr=True)

metrics_app = App(
    name="metrics",
    help="Maintain the business metric registry in metrics/.",
)

HTTP_TIMEOUT_SECONDS = 60


@metrics_app.command(name="refresh-schema")
def refresh_schema(
    server_url: Annotated[
        str | None,
        Parameter(
            name="--server-url",
            help="OpenMetadata API root, e.g. https://data.ol.mit.edu/api. Defaults to OM_SERVER_URL.",
        ),
    ] = None,
) -> None:
    """Rewrite the committed CreateMetric schema snapshot from an OpenMetadata server.

    Run this after the server is upgraded and commit the result, so the change
    in what a metric file may contain is reviewed. The server publishes its
    OpenAPI document without authentication, so no token is needed.
    """
    api_root = (server_url or os.environ["OM_SERVER_URL"]).rstrip("/")
    # The OpenAPI document is served beside the API root, not under it.
    url = f"{api_root.removesuffix('/api')}/swagger.json"
    if not url.startswith("https://"):
        msg = f"--server-url must be an https URL, got {api_root!r}"
        raise ValueError(msg)
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as resp:  # noqa: S310
        snapshot = build_schema_snapshot(json.load(resp))
    SCHEMA_SNAPSHOT.write_text(json.dumps(snapshot, indent=2) + "\n")
    console.print(f"Wrote {SCHEMA_SNAPSHOT} from OpenMetadata {snapshot[SCHEMA_VERSION_KEY]} ({url})")


_SERVICE = Parameter(
    name=["--service"],
    help='OpenMetadata database service that dbt relations live under, e.g. "Starburst Galaxy".',
)


def _load(
    dbt_dir_path: str | None, metrics_dir_path: str | None, manifest_path: str | None, names: tuple[str, ...]
) -> tuple[list[MetricDefinition], ManifestRegistry]:
    repo_root = get_repo_root()
    dbt_dir = Path(dbt_dir_path).resolve() if dbt_dir_path else repo_root / "src" / "ol_dbt"
    metrics_dir = Path(metrics_dir_path).resolve() if metrics_dir_path else repo_root / DEFAULT_METRICS_DIR
    metrics = load_metrics(metrics_dir)
    if names:
        unknown = set(names) - {m.name for m in metrics}
        if unknown:
            err_console.print(f"[red]Error:[/] no metric named {sorted(unknown)} in {metrics_dir}")
            sys.exit(1)
        metrics = [m for m in metrics if m.name in names]
    manifest = ManifestRegistry()
    if any(i.binding.kind != "fqn" for m in metrics for i in m.implemented_by):
        try:
            manifest = load_catalog_manifest(dbt_dir, manifest_path)
        except ValueError as exc:
            err_console.print(f"[red]Error:[/] {escape(str(exc))}")
            sys.exit(1)
    return metrics, manifest


def _check_server_version(client: OpenMetadataClient) -> None:
    """Stop when the server is not the version the metric files were validated against."""
    server = client.request("GET", "/v1/system/version")["version"]
    snapshot = json.loads(SCHEMA_SNAPSHOT.read_text())[SCHEMA_VERSION_KEY]
    if server != snapshot:
        err_console.print(
            f"[red]Error:[/] OpenMetadata is {escape(server)} but the CreateMetric snapshot is from "
            f"{escape(snapshot)}. Run `ol-dbt metrics refresh-schema`, check `ol-dbt validate --only "
            "metric_registry` and commit the snapshot first."
        )
        sys.exit(1)


def _metric_path(name: str) -> str:
    return f"/v1/metrics/name/{quote_fqn(name)}"


def _sync_one(
    client: OpenMetadataClient,
    metric: MetricDefinition,
    manifest: ManifestRegistry,
    service: str,
    bot: str,
    *,
    dry_run: bool,
    force: bool,
) -> bool:
    """Make OpenMetadata's copy of one metric match its file. Returns False when it did not."""
    name = escape(metric.name)
    # Resolve everything before reading the live metric, so nothing is written
    # for a file that names a team, term or table the catalog lacks.
    try:
        excluded = sorted(EXCLUDED_FIELDS.keys() & metric.body.keys())
        if excluded:
            msg = " ".join(f"`metric.{f}` is not a field the file may set. {EXCLUDED_FIELDS[f]}" for f in excluded)
            raise ValueError(msg)
        status_problem = file_status_problem(metric.status)
        if status_problem is not None:
            raise ValueError(". ".join(status_problem))
        owners = metric.body.get("owners")
        reviewers = metric.body.get("reviewers")
        # An empty list clears the field; only an absent key leaves it to OpenMetadata.
        owner_refs = client.named_refs(owners) if owners is not None else None
        reviewer_refs = client.named_refs(reviewers) if reviewers is not None else None
        for tag in metric.body.get("tags", []):
            collection = TAG_COLLECTIONS.get(tag["source"])
            if collection is None:
                msg = f"tag {tag['tagFQN']} has source {tag['source']!r}, not {' or '.join(TAG_COLLECTIONS)}"
                raise ValueError(msg)
            client.request("GET", f"/v1/{collection}/name/{quote_fqn(tag['tagFQN'])}")
        sources, skipped = lineage_sources(metric, manifest, service)
        source_ids = [client.entity_id(s.entity_type, s.fqn) for s in sources]
    except (ValueError, RuntimeError) as exc:
        console.print(f"[bold red]unresolved[/] {name}: {escape(str(exc))}")
        return False
    for model in skipped:
        console.print(
            f"[yellow]skipped[/]   {name}: lineage from {escape(model)}, a StarRocks view "
            "OpenMetadata has no service for"
        )

    try:
        live: dict[str, Any] | None = client.get_or_none(_metric_path(metric.name), query={"fields": METRIC_FIELDS})
        synced = live
        if live is not None and live.get("updatedBy") != bot:
            synced = last_synced_version(client.request("GET", f"/v1/metrics/{live['id']}/versions"), bot)
    except RuntimeError as exc:
        console.print(f"[bold red]failed[/]    {name}: {escape(str(exc))}")
        return False
    if live is not None and live.get(STATUS_FIELD) == IN_REVIEW_STATUS:
        # Even a structural PUT would end the review by resetting the status to
        # Unprocessed, so --force does not reach past this.
        console.print(
            f"[bold red]in review[/] {name}: In Review in OpenMetadata, and sync does not override a review "
            "in progress, even with --force. A reviewer approves or rejects it in OpenMetadata, or someone moves "
            "it out of In Review; then sync again."
        )
        return False
    body = create_body(metric, owner_refs, reviewer_refs, live)
    # Before any write: a PUT records the bot as updatedBy, after which a human
    # edit would no longer show as drift.
    found = drift(metric, body, live, synced)
    if found:
        verb = "overriding" if force else "drift"
        console.print(
            f"[bold red]{verb}[/]  {name}: {escape(', '.join(found))} changed in OpenMetadata since the last "
            f"sync (last updated by {escape(str(live and live.get('updatedBy')))})"
        )
        if not force:
            return False

    changes = structural_changes(body, live) if live is not None else []
    needs_put = live is None or bool(changes)
    status = metric.status or (live or {}).get(STATUS_FIELD)
    action = "create" if live is None else f"update {', '.join(changes)}" if changes else "no field changes"
    if dry_run:
        ops = patch_ops(metric, body, after_put(body, live) if needs_put else live or {}, status)
        console.print(f"[green]would[/]     {name}: {escape(action)}")
        if ops:
            console.print(f"  then PATCH {', '.join(op['path'].lstrip('/') for op in ops)}")
        for source in sources:
            console.print(f"  lineage {escape(source.fqn)} -> {name}")
        return True

    try:
        if needs_put:
            client.request("PUT", "/v1/metrics", body)
            current = client.request("GET", _metric_path(metric.name), query={"fields": METRIC_FIELDS})
        else:
            current = live
        ops = patch_ops(metric, body, current, status)
        if ops:
            current = client.request("PATCH", f"/v1/metrics/{current['id']}", ops, content_type=JSON_PATCH_CONTENT_TYPE)
        for source, source_id in zip(sources, source_ids, strict=True):
            client.request("PUT", "/v1/lineage", lineage_edge(source_id, source, current["id"], metric.path.name))
    except RuntimeError as exc:
        console.print(f"[bold red]failed[/]    {name}: {escape(str(exc))}")
        return False
    patched = f"; patched {', '.join(op['path'].lstrip('/') for op in ops)}" if ops else ""
    console.print(f"[green]synced[/]    {name} (v{current['version']}): {escape(action + patched)}")
    if "description" in body and not same_description(current.get("description"), body["description"]):
        console.print(
            f"[yellow]rewritten[/] {name}: OpenMetadata's sanitizer changed the description (it drops markup "
            "and control characters it does not allow), so every sync will PATCH it again. Fix the file."
        )
    return True


@metrics_app.command
def sync(
    *,
    service: Annotated[str, _SERVICE],
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry-run"], help="Resolve every file and report what would change; write nothing."),
    ] = False,
    force: Annotated[
        bool,
        Parameter(
            name=["--force"],
            help=(
                "Overwrite description, owners, reviewers or status that were edited in OpenMetadata since the "
                "last sync. Never overrides a metric that is In Review: sync writes nothing for it."
            ),
        ),
    ] = False,
    names: Annotated[
        tuple[str, ...], Parameter(name=["--metric", "-m"], help="Only these metrics (by name). Repeatable.")
    ] = (),
    dbt_dir_path: Annotated[
        str | None,
        Parameter(name=["--dbt-dir", "-d"], help="dbt project root. Defaults to src/ol_dbt in the repository."),
    ] = None,
    metrics_dir_path: Annotated[
        str | None, Parameter(name=["--metrics-dir"], help="Metrics directory. Defaults to metrics/ in the repository.")
    ] = None,
    manifest_path: Annotated[
        str | None,
        Parameter(
            name=["--manifest"],
            help=(
                "manifest.json that resolves dbt_model bindings to relations. It must describe the catalogs "
                "OpenMetadata ingests (production). Defaults to target/manifest.json in the dbt project."
            ),
        ),
    ] = None,
) -> None:
    """Create or update each metric in OpenMetadata so it matches its file, with lineage from its tables.

    Reads OM_SERVER_URL and OM_BOT_JWT_TOKEN. Stops before the first file when
    the server's version differs from the CreateMetric snapshot's. A metric
    whose description, owners, reviewers or status someone changed in OpenMetadata
    since the last sync is reported and left alone unless --force, so the edit
    can become a pull request. A metric that is In Review in OpenMetadata is
    reported and left alone even with --force. Exits non-zero when any metric
    did not sync.
    """
    metrics, manifest = _load(dbt_dir_path, metrics_dir_path, manifest_path, names)
    client = OpenMetadataClient.from_env()
    _check_server_version(client)
    bot = client.request("GET", "/v1/users/loggedInUser")["name"]
    failures = sum(
        not _sync_one(client, metric, manifest, service, bot, dry_run=dry_run, force=force) for metric in metrics
    )
    if failures:
        sys.exit(1)
