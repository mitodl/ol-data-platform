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
    FILE_DIGEST_PROPERTY,
    METRIC_ENTITY_TYPE,
    METRIC_FIELDS,
    REVIEW_HOLD_STATUSES,
    STATUS_FIELD,
    TAG_COLLECTIONS,
    LineageSource,
    after_put,
    create_body,
    display_name_conflict,
    drift,
    last_synced_version,
    lineage_edge,
    lineage_sources,
    patch_ops,
    same_description,
    stale_edges,
    structural_changes,
)
from ol_dbt_cli.lib.openmetadata import (
    JSON_PATCH_CONTENT_TYPE,
    OpenMetadataClient,
    OpenMetadataError,
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
_STARROCKS_SERVICE = Parameter(
    name=["--starrocks-service"],
    help="OpenMetadata database service that the StarRocks views (dbt models tagged starrocks) live under.",
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
    """Stop when the server's major or minor version is not the snapshot's.

    A patch release only warns. Stopping on one cost three failed nightly syncs
    when the server moved from 2.0.5 to 2.0.6 with an identical CreateMetric.
    """
    server = client.request("GET", "/v1/system/version")["version"]
    snapshot = json.loads(SCHEMA_SNAPSHOT.read_text())[SCHEMA_VERSION_KEY]
    if server == snapshot:
        return
    refresh = (
        "Run `ol-dbt metrics refresh-schema`, check `ol-dbt validate --only metric_registry` and commit the snapshot"
    )
    if server.split(".")[:2] != snapshot.split(".")[:2]:
        err_console.print(
            f"[red]Error:[/] OpenMetadata is {escape(server)} but the CreateMetric snapshot is from "
            f"{escape(snapshot)}. {refresh} first."
        )
        sys.exit(1)
    console.print(
        f"[yellow]{'version'.ljust(_LABEL_WIDTH)}[/]OpenMetadata is {escape(server)} but the CreateMetric snapshot "
        f"is from {escape(snapshot)}, the same major and minor version, so sync goes on. {refresh}."
    )


# Sync output labels are padded to this many columns.
_LABEL_WIDTH = 10


def _metric_path(name: str) -> str:
    return f"/v1/metrics/name/{quote_fqn(name)}"


def _write_failure(exc: RuntimeError) -> str:
    # CatalogExceptionMessage.unknownCustomField, answered with a 400.
    if (
        isinstance(exc, OpenMetadataError)
        and exc.status == 400
        and f"Unknown custom field {FILE_DIGEST_PROPERTY}" in str(exc)
    ):
        return (
            f"OpenMetadata has no `{FILE_DIGEST_PROPERTY}` custom property on the metric type, where sync records "
            "the digest of each metric file. It must be defined there (ol-infrastructure om_server_config) before "
            f"metrics can sync. {exc}"
        )
    return str(exc)


def _report_display_name(name: str, file_value: str | None, stored: str) -> None:
    wanted = repr(file_value) if file_value else "none"
    console.print(
        f"[bold red]{'stale'.ljust(_LABEL_WIDTH)}[/]{name}: OpenMetadata has displayName {escape(repr(stored))} and "
        f"the file has {escape(wanted)}. OpenMetadata does not let a bot change a stored displayName "
        "(DefaultBotPolicy denies EditDisplayName), so set it in OpenMetadata to match the file."
    )


def _stale_edges(client: OpenMetadataClient, metric_id: str, declared_ids: list[str], bot: str) -> list[dict[str, Any]]:
    lineage = client.request(
        "GET", f"/v1/lineage/{METRIC_ENTITY_TYPE}/{metric_id}", query={"upstreamDepth": "1", "downstreamDepth": "0"}
    )
    return stale_edges(lineage, metric_id, set(declared_ids), bot)


def _sync_one(
    client: OpenMetadataClient,
    metric: MetricDefinition,
    manifest: ManifestRegistry,
    service: str,
    starrocks_service: str,
    bot: str,
    *,
    dry_run: bool,
    force: bool,
    prune: bool,
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
            raise ValueError(f"{metric.path.name}: " + ". ".join(status_problem))
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
        sources: list[LineageSource] = []
        source_ids: list[str] = []
        skipped: list[LineageSource] = []
        for source in lineage_sources(metric, manifest, service, starrocks_service):
            if source.starrocks:
                source_id = client.entity_id_or_none(source.entity_type, source.fqn)
                if source_id is None:
                    skipped.append(source)
                    continue
            else:
                source_id = client.entity_id(source.entity_type, source.fqn)
            sources.append(source)
            source_ids.append(source_id)
    except (ValueError, RuntimeError) as exc:
        console.print(f"[bold red]unresolved[/] {name}: {escape(str(exc))}")
        return False
    for source in skipped:
        console.print(
            f"[yellow]skipped[/]   {name}: lineage from {escape(source.fqn)}, a StarRocks view OpenMetadata "
            "does not have (404). The edge lands on the first sync after OpenMetadata's StarRocks ingestion "
            "catalogs it."
        )

    try:
        live: dict[str, Any] | None = client.get_or_none(_metric_path(metric.name), query={"fields": METRIC_FIELDS})
        synced = live
        if live is not None and live.get("updatedBy") != bot:
            synced = last_synced_version(client.request("GET", f"/v1/metrics/{live['id']}/versions"), bot)
    except RuntimeError as exc:
        console.print(f"[bold red]failed[/]    {name}: {escape(str(exc))}")
        return False
    hold = REVIEW_HOLD_STATUSES.get(live[STATUS_FIELD]) if live is not None else None
    if hold is not None:
        # Even a structural PUT would reset the status to Unprocessed, so
        # --force does not reach past this.
        label, message = hold
        console.print(f"[bold red]{label.ljust(_LABEL_WIDTH)}[/]{name}: {message}")
        return False
    body = create_body(metric, owner_refs, reviewer_refs, live, bot)
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

    stored_name = display_name_conflict(body, live)
    changes = structural_changes(body, live) if live is not None else []
    needs_put = live is None or bool(changes)
    action = "create" if live is None else f"update {', '.join(changes)}" if changes else "no field changes"
    # A metric this run creates has no edge sync did not just write.
    prune_edges = prune and live is not None and not skipped
    if prune and live is not None and skipped:
        # A view that is not catalogued yet and a wrong --starrocks-service both
        # answer 404, and the second would make every view's edge look stale.
        console.print(
            f"[yellow]{'kept'.ljust(_LABEL_WIDTH)}[/]{name}: removes no lineage edge this run, because OpenMetadata "
            f"did not have {len(skipped)} of its StarRocks views (skipped above), and sync cannot tell a view not "
            "catalogued yet from a --starrocks-service that names the wrong service."
        )
    if dry_run:
        try:
            stale = _stale_edges(client, live["id"], source_ids, bot) if live is not None and prune_edges else []
        except RuntimeError as exc:
            console.print(f"[bold red]failed[/]    {name}: {escape(str(exc))}")
            return False
        ops = patch_ops(metric, body, after_put(body, live) if needs_put else live or {}, bot)
        console.print(f"[green]would[/]     {name}: {escape(action)}")
        if ops:
            console.print(f"  then PATCH {', '.join(op['path'].lstrip('/') for op in ops)}")
        for source in sources:
            console.print(f"  lineage {escape(source.fqn)} -> {name}")
        for node in stale:
            console.print(f"  remove lineage {escape(node['fullyQualifiedName'])} -> {name}")
        if stored_name is not None:
            _report_display_name(name, body.get("displayName"), stored_name)
        return stored_name is None

    try:
        if needs_put:
            client.request("PUT", "/v1/metrics", body)
            current = client.request("GET", _metric_path(metric.name), query={"fields": METRIC_FIELDS})
        else:
            current = live
        ops = patch_ops(metric, body, current, bot)
        if ops:
            current = client.request("PATCH", f"/v1/metrics/{current['id']}", ops, content_type=JSON_PATCH_CONTENT_TYPE)
        for source, source_id in zip(sources, source_ids, strict=True):
            client.request("PUT", "/v1/lineage", lineage_edge(source_id, source, current["id"], metric.path.name))
        for node in _stale_edges(client, current["id"], source_ids, bot) if prune_edges else []:
            client.request("DELETE", f"/v1/lineage/{node['type']}/{node['id']}/{METRIC_ENTITY_TYPE}/{current['id']}")
            # Reported as each lands, so a later failure does not hide it.
            console.print(
                f"[green]{'removed'.ljust(_LABEL_WIDTH)}[/]{name}: lineage from {escape(node['fullyQualifiedName'])}, "
                "which sync created and the file no longer lists"
            )
    except RuntimeError as exc:
        console.print(f"[bold red]failed[/]    {name}: {escape(_write_failure(exc))}")
        return False
    patched = f"; patched {', '.join(op['path'].lstrip('/') for op in ops)}" if ops else ""
    console.print(f"[green]synced[/]    {name} (v{current['version']}): {escape(action + patched)}")
    if "description" in body and not same_description(current.get("description"), body["description"]):
        console.print(
            f"[yellow]rewritten[/] {name}: OpenMetadata stored the description as "
            f"{escape(repr(current.get('description')))}, which differs from the file's, so every sync will PATCH "
            "it again. Its sanitizer runs on any description with a `<` outside its `<#E::...>` entity links: it "
            "drops markup it does not allow and control characters, and rewrites some markup it keeps, e.g. `<br>` "
            "becomes `<br />`, an `<https://...>` autolink is dropped, and `x<y` loses everything from `<y`. Change "
            "the file to what OpenMetadata stored, or write a literal `<` as `&lt;`."
        )
    if stored_name is not None:
        _report_display_name(name, body.get("displayName"), stored_name)
    return stored_name is None


@metrics_app.command
def sync(
    *,
    service: Annotated[str, _SERVICE],
    starrocks_service: Annotated[str, _STARROCKS_SERVICE] = "StarRocks",
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
                "last sync. Never overrides a metric that is In Review or Rejected: sync writes nothing for it."
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
    can become a pull request. A metric that is In Review or Rejected in
    OpenMetadata is reported and left alone even with --force. Each file's
    digest is recorded in the metric's metricFileSha256 custom property, which
    ol-infrastructure defines on the metric type. A stored displayName that
    differs from the file is reported as stale, because OpenMetadata keeps it
    against a bot's change; the rest of that metric still syncs. A lineage edge
    sync created from a table the file no longer lists is deleted; one a person
    added stays. Exits non-zero
    when any metric did not sync or has a stale displayName.
    """
    metrics, manifest = _load(dbt_dir_path, metrics_dir_path, manifest_path, names)
    client = OpenMetadataClient.from_env()
    _check_server_version(client)
    caller = client.request("GET", "/v1/users/loggedInUser")
    bot = caller["name"]
    # The only check that the token is a bot's: edges sync created carry its name.
    prune = caller["isBot"]
    if not prune:
        console.print(
            f"[yellow]no prune[/]  OM_BOT_JWT_TOKEN belongs to {escape(bot)}, who is not a bot, so sync removes no "
            "lineage edge this run: an edge that user drew in OpenMetadata would read as one sync created."
        )
    failures = sum(
        not _sync_one(
            client, metric, manifest, service, starrocks_service, bot, dry_run=dry_run, force=force, prune=prune
        )
        for metric in metrics
    )
    if failures:
        sys.exit(1)
