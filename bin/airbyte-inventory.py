#!/usr/bin/env python3
# ruff: noqa: T201
"""Snapshot the live Airbyte workspace and derive the ingestion-inventory findings.

Implements step 2 of ``docs/specs/INGESTION_INVENTORY_SPEC.md``: read the Airbyte
public API over the basic-auth route, write a redacted JSON snapshot, and turn it
into (a) the findings the spec and its open tasks need and (b) a draft inventory
under ``ingestion/inventory/units/``.

Nothing here writes to Airbyte. Every call is a GET.

Usage::

    export AIRBYTE_PASSWORD='...'          # or pass --password
    uv run python bin/airbyte-inventory.py all --environment qa

    # or one step at a time
    uv run python bin/airbyte-inventory.py dump --environment qa
    uv run python bin/airbyte-inventory.py report --output findings.md
    uv run python bin/airbyte-inventory.py render --output-dir ingestion/inventory/units

Each environment runs its OWN Airbyte deployment with its own connections, so a
snapshot is per-environment: ``--environment`` picks the host from
``AIRBYTE_SERVERS`` and writes ``airbyte-snapshot-<environment>.json``.
``render`` merges however many exist into one inventory, tagging each connection
with the environment it came from.

**One environment per run.** Each deployment's basic-auth credential comes from
its own Vault service, so a single ``--username``/``--password`` cannot reach
two of them. Run the whole thing once per environment with that environment's
credential; ``render`` picks up whatever snapshots are on disk, so the merged
inventory accumulates across runs.

That merge is what makes the QA side legible. Outside production most
connections are ``inactive`` — configured once, then disabled as they fell out
of use — and an inactive connection produces no Dagster asset, so reading the
asset graph makes QA look nearly empty when it is merely switched off. The
distinction matters twice over: it is the evidence ``strategies.qa`` is meant to
be decided on (RFC 12711 §3), and it is the scope of what a replacement dlt
pipeline has to cover, since a disabled connection still records the streams
someone once wanted in QA.

The password is the same Vault KV v1 secret Dagster uses::

    vault kv get -mount=secret-data -field=dagster_unhashed_password \\
        dagster-http-auth-password

Findings produced (see the spec section in parentheses):

  A. Replication method per source — which connections use xmin (§3.4, task
     tk-determine-per-source-incremental-cursor-viabilit-51f299).
  B. Cursor fields and primary keys per stream, and which incremental streams
     carry no explicit cursor (so they ride the source-defined one).
  C. Dagster coupling — connection name → group name → the interval map in
     lakehouse/definitions.py; reports connections the selector drops, groups
     that silently fall back to 24h, and dead interval-map entries (§1.3).
  D. Schedules — connections carrying their own Airbyte cron, i.e. double
     scheduled against Dagster (§6.4).
  E. dbt reconcile — streams with no dbt source table and vice versa (§1.4).
     Heuristic: matches on the predicted raw table name.
  F. Table prefixes — the authoritative `prefix` per connection, which is what
     actually produces `raw__<deployment>__<layer>__…` names (§1.1).

Secrets: source and destination configuration values are redacted by default
(any key whose name looks credential-bearing), on top of the masking Airbyte
already does server-side. Pass --full-config to keep them, and mind where the
snapshot lands if you do.
"""

from __future__ import annotations

import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import yaml
from cyclopts import App, Parameter

app = App(
    name="airbyte-inventory",
    help="Snapshot Airbyte and derive the ingestion inventory + findings.",
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_API_PATH = "/api/public/v1"

# One Airbyte deployment per environment, each with its own connections. Hosts
# come from ol-infrastructure's `airbyte:api_host_domain` (applications/airbyte/
# Pulumi.<Env>.yaml). Exhaustive on purpose, matching the no-fallback rule the
# dbt target maps follow: an unknown environment raises rather than quietly
# snapshotting production and filing the result under another name.
#
# `ci` is deliberately absent. ol-infrastructure defines an api-airbyte-ci host,
# but nothing runs there -- the CI code location sets SKIP_AIRBYTE and loads an
# empty workspace (definitions.py). Listing it would offer a snapshot that can
# only ever come back empty, and an empty snapshot is indistinguishable from a
# workspace whose connections were all deleted.
AIRBYTE_SERVERS: dict[str, str] = {
    "production": "https://api-airbyte.odl.mit.edu",
    "qa": "https://api-airbyte-qa.odl.mit.edu",
}
DEFAULT_ENVIRONMENT = "production"


def snapshot_path_for(environment: str) -> Path:
    """Default snapshot filename for an environment.

    Environment-qualified so a QA dump cannot silently overwrite the production
    snapshot -- `render` reads several at once and the two are not
    interchangeable.
    """
    return Path(f"airbyte-snapshot-{environment}.json")


def _resolve_server_url(environment: str, server_url: str | None) -> str:
    if server_url:
        return server_url
    try:
        return AIRBYTE_SERVERS[environment]
    except KeyError:
        msg = (
            f"No Airbyte server known for environment {environment!r} "
            f"(known: {sorted(AIRBYTE_SERVERS)}). Add it to AIRBYTE_SERVERS or "
            f"pass --server-url."
        )
        raise SystemExit(msg) from None


DEFINITIONS_PY = (
    REPO_ROOT / "dg_projects" / "lakehouse" / "lakehouse" / "definitions.py"
)
DBT_MODELS_DIR = REPO_ROOT / "src" / "ol_dbt" / "models"

# Keys whose values are redacted in the snapshot. Substring match, lowercased.
SENSITIVE_KEY_MARKERS = (
    "password",
    "secret",
    "token",
    "credential",
    "private_key",
    "api_key",
    "apikey",
    "passphrase",
    "ssh_key",
    "service_account",
    "auth",
)

# Layer names RFC 12711 §3 allows, and how prefix segments map onto them.
KNOWN_LAYERS = ("mysql", "mongodb", "api", "tracking_logs", "fastly", "app_postgres")

# Anything slower than this gets a progress line, so a stall is attributable.
SLOW_REQUEST_SECONDS = 5.0

INCREMENTAL_SYNC_MODES = (
    "incremental_append",
    "incremental_deduped_history",
    "incremental_update",
    "incremental_soft_delete",
)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def _progress(message: str) -> None:
    """Write progress to stderr, flushed — a `\\r` line never flushes itself."""
    print(message, file=sys.stderr, flush=True)


def _resolve_password(password: str | None) -> str:
    if password:
        return password
    msg = (
        "No password given. Pass --password or set AIRBYTE_PASSWORD.\n"
        "  vault kv get -mount=secret-data -field=dagster_unhashed_password "
        "dagster-http-auth-password"
    )
    raise SystemExit(msg)


def _paginated_get(
    client: httpx.Client,
    path: str,
    params: dict[str, Any],
    id_key: str,
) -> list[dict[str, Any]]:
    """Page through a list endpoint by explicit offset, de-duplicating by id.

    Deliberately does NOT follow the response's `next` link. A self-hosted
    Airbyte builds `next` against localhost, and it emits one even on the final
    page, so following it re-reads page 1 forever. Offset paging is the same
    number of requests and depends on nothing the server gets wrong.

    Advances `offset` by the number of records the server actually returned,
    never by the number requested — the server is free to cap `limit`, and
    assuming otherwise stops paging after the first short page.

    Stops when a page comes back empty or carries no id we have not already
    seen, so a server that ignores `offset` costs one extra request rather than
    looping forever.
    """
    limit = int(params.get("limit", 50))
    collected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    offset = 0
    while True:
        page_params = {**params, "limit": limit, "offset": offset}
        started = time.monotonic()
        response = client.get(path, params=page_params)
        if response.status_code == httpx.codes.UNAUTHORIZED:
            msg = f"401 from {response.url} — check --username/--password."
            raise SystemExit(msg)
        response.raise_for_status()
        page = response.json().get("data", [])
        fresh = [item for item in page if item.get(id_key) not in seen_ids]
        seen_ids.update(item[id_key] for item in fresh if id_key in item)
        collected.extend(fresh)
        elapsed = time.monotonic() - started
        if offset or elapsed > SLOW_REQUEST_SECONDS:
            _progress(
                f"    offset {offset}: +{len(fresh)} new of {len(page)} "
                f"({len(collected)} total, {elapsed:.1f}s)"
            )
        if not page or not fresh:
            return collected
        offset += len(page)


def _redact(value: Any, *, enabled: bool) -> Any:
    if not enabled:
        return value
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if any(marker in key.lower() for marker in SENSITIVE_KEY_MARKERS):
                redacted[key] = "<redacted>"
            else:
                redacted[key] = _redact(item, enabled=enabled)
        return redacted
    if isinstance(value, list):
        return [_redact(item, enabled=enabled) for item in value]
    return value


@app.command
def dump(  # noqa: PLR0913
    *,
    username: Annotated[str, Parameter(env_var="AIRBYTE_USERNAME")] = "dagster",
    password: Annotated[
        str | None, Parameter(env_var="AIRBYTE_PASSWORD", show_default=False)
    ] = None,
    environment: Annotated[
        str,
        Parameter(
            env_var="AIRBYTE_ENVIRONMENT",
            help=(
                "Which Airbyte deployment to snapshot: "
                f"{', '.join(sorted(AIRBYTE_SERVERS))}."
            ),
        ),
    ] = DEFAULT_ENVIRONMENT,
    server_url: Annotated[
        str | None,
        Parameter(
            env_var="AIRBYTE_SERVER_URL",
            help="Override the host for --environment.",
            show_default=False,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None, Parameter(help="Limit to one workspace.")
    ] = None,
    output: Annotated[
        Path | None,
        Parameter(
            help="Where to write the JSON snapshot.",
            show_default=False,
        ),
    ] = None,
    page_size: int = 50,
    timeout: float = 60.0,
    include_deleted: bool = False,
    stream_properties: Annotated[
        bool,
        Parameter(
            help=(
                "Also fetch per-source stream schemas (available sync modes, "
                "source-defined cursors). SLOW: one schema discovery per source, "
                "minutes each on a large database. No finding uses it yet."
            )
        ),
    ] = False,
    full_config: Annotated[
        bool,
        Parameter(
            help="Keep credential-shaped connector config values (default: redact)."
        ),
    ] = False,
) -> Path:
    """Fetch workspaces, sources, destinations and connections into a JSON snapshot."""
    secret = _resolve_password(password)
    resolved_url = _resolve_server_url(environment, server_url)
    output = output or snapshot_path_for(environment)
    base_url = resolved_url.rstrip("/") + PUBLIC_API_PATH

    with httpx.Client(
        base_url=base_url,
        auth=httpx.BasicAuth(username, secret),
        timeout=timeout,
        headers={"Accept": "application/json"},
        follow_redirects=True,
    ) as client:
        common: dict[str, Any] = {"limit": page_size, "includeDeleted": include_deleted}
        if workspace_id:
            common["workspaceIds"] = [workspace_id]

        _progress(f"Fetching {environment} from {base_url} as {username} …")
        workspaces = _paginated_get(client, "/workspaces", dict(common), "workspaceId")
        _progress(f"  workspaces:   {len(workspaces)}")
        sources = _paginated_get(client, "/sources", dict(common), "sourceId")
        _progress(f"  sources:      {len(sources)}")
        destinations = _paginated_get(
            client, "/destinations", dict(common), "destinationId"
        )
        _progress(f"  destinations: {len(destinations)}")
        connections = _paginated_get(
            client, "/connections", dict(common), "connectionId"
        )
        # Broken out by status because outside production most connections are
        # `inactive` -- disabled after falling out of use, not absent. A count
        # alone reads as "QA has almost nothing", which is the wrong conclusion
        # and the reason this command grew an --environment flag.
        by_status = Counter(c.get("status", "unknown") for c in connections)
        _progress(
            f"  connections:  {len(connections)} "
            f"({', '.join(f'{n} {s}' for s, n in sorted(by_status.items()))})"
        )

        # Some server versions omit stream configs from the list response.
        for connection in connections:
            if not (connection.get("configurations") or {}).get("streams"):
                _progress(f"  stream config for {connection['name']} …")
                detail = client.get(f"/connections/{connection['connectionId']}")
                if detail.is_success:
                    connection["configurations"] = detail.json().get(
                        "configurations", {}
                    )

        schemas: dict[str, Any] = {}
        if stream_properties:
            sources_by_id = {s["sourceId"]: s for s in sources}
            pairs = sorted({(c["sourceId"], c["destinationId"]) for c in connections})
            _progress(
                f"  fetching stream schemas for {len(pairs)} source/destination "
                "pair(s) — each one makes Airbyte discover the source schema, "
                "which can take minutes on a large database"
            )
            for index, (source_id, destination_id) in enumerate(pairs, start=1):
                name = sources_by_id.get(source_id, {}).get("name", source_id)
                _progress(f"    [{index}/{len(pairs)}] {name} …")
                started = time.monotonic()
                response = client.get(
                    "/streams",
                    params={"sourceId": source_id, "destinationId": destination_id},
                )
                if response.is_success:
                    # Keyed by the pair, not the source: Salesforce and Mailgun
                    # each feed two destinations, so a source-only key would keep
                    # whichever came last.
                    schemas[f"{source_id}:{destination_id}"] = response.json()
                _progress(
                    f"    [{index}/{len(pairs)}] {name} → {response.status_code} "
                    f"in {time.monotonic() - started:.1f}s"
                )

    for actor in (*sources, *destinations):
        actor["configuration"] = _redact(
            actor.get("configuration"), enabled=not full_config
        )

    snapshot = {
        "fetched_at": datetime.now(UTC).isoformat(),
        "environment": environment,
        "server_url": resolved_url,
        "redacted": not full_config,
        "workspaces": workspaces,
        "sources": sources,
        "destinations": destinations,
        "connections": connections,
        "stream_properties": schemas,
    }
    output.write_text(json.dumps(snapshot, indent=2, sort_keys=False))
    _progress(f"Wrote {output} ({output.stat().st_size / 1024:.0f} KiB)")
    return output


# ---------------------------------------------------------------------------
# Shared derivations
# ---------------------------------------------------------------------------


SNAPSHOT_ID_KEYS = {
    "workspaces": "workspaceId",
    "sources": "sourceId",
    "destinations": "destinationId",
    "connections": "connectionId",
}


def _load_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        msg = f"No snapshot at {path}. Run `airbyte-inventory dump` first."
        raise SystemExit(msg)
    snapshot = json.loads(path.read_text())

    # Snapshots taken before the offset-paging fix contain page 1 several times
    # over, which silently inflates every count downstream.
    duplicated = {
        collection: (len(items), len({i[key] for i in items if key in i}))
        for collection, key in SNAPSHOT_ID_KEYS.items()
        if len(items := snapshot.get(collection, []))
        != len({i[key] for i in items if key in i})
    }
    if duplicated:
        detail = ", ".join(
            f"{name} {total}→{distinct}"
            for name, (total, distinct) in duplicated.items()
        )
        msg = (
            f"{path} contains duplicate records ({detail}). It was taken with a "
            "broken pagination loop; re-run `airbyte-inventory dump`."
        )
        raise SystemExit(msg)

    # Snapshots predate --environment; they can only have come from the one
    # server the script used to hard-code.
    snapshot.setdefault("environment", DEFAULT_ENVIRONMENT)
    return snapshot


def dagster_group_name(connection_name: str) -> str:
    """Reproduce OLAirbyteTranslator's group-name derivation exactly.

    definitions.py:177-181 — collapse dashes/whitespace to underscores, drop
    everything else (including Airbyte's U+2192 arrow), strip, lowercase.
    """
    return (
        re.sub(r"[^A-Za-z0-9_]", "", re.sub(r"[-\s]+", "_", connection_name))
        .strip("_")
        .lower()
    )


def _parse_interval_map() -> dict[str, int]:
    """Read group_name_to_interval out of definitions.py rather than duplicating it."""
    if not DEFINITIONS_PY.exists():
        return {}
    text = DEFINITIONS_PY.read_text()
    block = re.search(r"group_name_to_interval\s*=\s*\{(.*?)\n\s*\}", text, re.DOTALL)
    if not block:
        return {}
    return {
        name: int(hours)
        for name, hours in re.findall(r'"([^"]+)":\s*(\d+)', block.group(1))
    }


def _parse_selector_suffix() -> str:
    if not DEFINITIONS_PY.exists():
        return "s3 data lake"
    match = re.search(r'endswith\(\s*"([^"]+)"\s*\)', DEFINITIONS_PY.read_text())
    return match.group(1) if match else "s3 data lake"


def _dbt_raw_tables() -> set[str]:
    if not DBT_MODELS_DIR.exists():
        return set()
    tables: set[str] = set()
    for path in DBT_MODELS_DIR.rglob("_*sources.yml"):
        tables.update(
            re.findall(r"^\s*- name:\s*(raw__\S+)\s*$", path.read_text(), re.MULTILINE)
        )
    return tables


# Airbyte's own spelling → the inventory's vocabulary. `STANDARD` is Airbyte's
# name for cursor-based replication, which is what the inventory calls `cursor`.
REPLICATION_METHODS = {
    "xmin": "xmin",
    "standard": "cursor",
    "cursor": "cursor",
    "cdc": "cdc",
}


def _normalized_replication_method(configuration: dict[str, Any] | None) -> str:
    raw = _replication_method(configuration)
    return REPLICATION_METHODS.get(raw.lower(), "n/a")


def _replication_method(configuration: dict[str, Any] | None) -> str:
    """Pull the replication method out of a source config, whatever shape it takes."""
    if not isinstance(configuration, dict):
        return "unknown"
    method = configuration.get("replication_method")
    if isinstance(method, dict):
        return str(method.get("method") or method.get("replication_slot") or "unknown")
    if isinstance(method, str):
        return method
    return "n/a"


def _streams_of(connection: dict[str, Any]) -> list[dict[str, Any]]:
    return (connection.get("configurations") or {}).get("streams") or []


def _predicted_raw_table(connection: dict[str, Any], stream: dict[str, Any]) -> str:
    return f"{connection.get('prefix') or ''}{stream.get('name', '')}"


def _effective_prefix(connection: dict[str, Any]) -> str:
    """Return the connection's prefix, or the shared head of its stream names.

    Two naming mechanisms are in use. Database connections set a `prefix` and
    carry short stream names (`users_user`). File/S3 connections set no prefix
    and carry the fully-qualified name in the stream itself
    (`raw__mitx__openedx__tracking_logs`). For the second kind the unit's
    table_prefix is the common `raw__…__` head of its stream names.
    """
    if prefix := connection.get("prefix"):
        return str(prefix)
    names = [s.get("name", "") for s in _streams_of(connection)]
    if not names or not all(name.startswith("raw__") for name in names):
        return ""
    segments = [name.split("__") for name in names]
    shared: list[str] = []
    for index in range(min(len(parts) for parts in segments) - 1):
        column = {parts[index] for parts in segments}
        if len(column) != 1:
            break
        shared.append(column.pop())
    return "__".join(shared) + "__" if shared else ""


# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------


def _finding_a_replication(snapshot: dict[str, Any]) -> list[str]:
    sources_by_id = {s["sourceId"]: s for s in snapshot["sources"]}
    streams_per_source: Counter[str] = Counter()
    connections_per_source: Counter[str] = Counter()
    for connection in snapshot["connections"]:
        connections_per_source[connection["sourceId"]] += 1
        streams_per_source[connection["sourceId"]] += len(_streams_of(connection))

    rows = []
    for source_id, source in sorted(
        sources_by_id.items(), key=lambda kv: kv[1]["name"].lower()
    ):
        rows.append(
            (
                source["name"],
                source.get("sourceType", "?"),
                _replication_method(source.get("configuration")),
                connections_per_source.get(source_id, 0),
                streams_per_source.get(source_id, 0),
            )
        )

    lines = [
        "## A. Replication method per source",
        "",
        "`xmin` is the finding that matters: it has no dlt equivalent, and",
        "source-postgres 3.8+ refuses it outright on any database that has",
        "ever wrapped around.",
        "",
        "| Source | Type | Replication method | Connections | Streams |",
        "|---|---|---|---:|---:|",
    ]
    lines += [
        f"| {name} | {stype} | **{method}** | {conns} | {strms} |"
        for name, stype, method, conns, strms in rows
    ]

    xmin = [r for r in rows if "xmin" in r[2].lower()]
    lines += [
        "",
        (
            f"**{len(xmin)} source(s) on xmin**, covering "
            f"{sum(r[4] for r in xmin)} stream(s)."
        ),
    ]
    if xmin:
        lines += [
            "",
            "Needs a replacement cursor column per table before dlt takes over:",
        ]
        lines += [f"- {r[0]} ({r[1]}, {r[4]} streams)" for r in xmin]
    return lines


def _finding_b_cursors(snapshot: dict[str, Any]) -> list[str]:
    cursor_names: Counter[str] = Counter()
    incremental_without_cursor: list[tuple[str, str]] = []
    dedup_without_pk: list[tuple[str, str]] = []
    mode_counts: Counter[str] = Counter()

    for connection in snapshot["connections"]:
        for stream in _streams_of(connection):
            mode = stream.get("syncMode") or "(unset)"
            mode_counts[mode] += 1
            cursor = stream.get("cursorField") or []
            if mode in INCREMENTAL_SYNC_MODES:
                if cursor:
                    cursor_names[".".join(cursor)] += 1
                else:
                    incremental_without_cursor.append(
                        (connection["name"], stream.get("name", "?"))
                    )
            if "dedup" in mode and not (stream.get("primaryKey") or []):
                dedup_without_pk.append((connection["name"], stream.get("name", "?")))

    lines = [
        "## B. Sync modes, cursor fields and primary keys",
        "",
        "| Sync mode | Streams |",
        "|---|---:|",
    ]
    lines += [f"| {mode} | {count} |" for mode, count in mode_counts.most_common()]
    lines += [
        "",
        f"**Explicit cursor fields in use ({len(cursor_names)} distinct):**",
        "",
        "| Cursor field | Streams |",
        "|---|---:|",
    ]
    lines += [f"| `{name}` | {count} |" for name, count in cursor_names.most_common()]
    lines += [
        "",
        (
            f"**{len(incremental_without_cursor)} incremental stream(s) carry no "
            "explicit cursor field** and so ride the source-defined cursor "
            "(for Postgres, that is xmin)."
        ),
    ]
    lines += [
        f"- {conn} → {stream}" for conn, stream in incremental_without_cursor[:40]
    ]
    if len(incremental_without_cursor) > 40:  # noqa: PLR2004
        lines += [f"- … and {len(incremental_without_cursor) - 40} more"]
    if dedup_without_pk:
        lines += [
            "",
            (
                f"**{len(dedup_without_pk)} dedup stream(s) with no primary key** "
                "— no dlt `merge` disposition is possible until one is chosen:"
            ),
        ]
        lines += [f"- {conn} → {stream}" for conn, stream in dedup_without_pk[:20]]
    return lines


def _finding_c_dagster(snapshot: dict[str, Any]) -> list[str]:
    interval_map = _parse_interval_map()
    suffix = _parse_selector_suffix()

    selected: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for connection in snapshot["connections"]:
        (selected if connection["name"].lower().endswith(suffix) else dropped).append(
            connection
        )

    # Several connections can derive the same group name — the derivation strips
    # every non-alphanumeric, so "Foo-Bar" and "Foo Bar" collide. That is a
    # production problem, not just an audit one: Dagster would collapse both into
    # one asset group. Collect names per group so a collision is reported rather
    # than silently overwritten.
    derived: dict[str, list[str]] = {}
    for connection in selected:
        derived.setdefault(dagster_group_name(connection["name"]), []).append(
            connection["name"]
        )
    collisions = {group: names for group, names in derived.items() if len(names) > 1}
    missing_from_map = sorted(set(derived) - set(interval_map))
    dead_entries = sorted(set(interval_map) - set(derived))

    lines = [
        "## C. Dagster coupling (connection name → group name → interval map)",
        "",
        (
            f"Parsed from `{DEFINITIONS_PY.relative_to(REPO_ROOT)}`: selector "
            f"suffix `{suffix}`, {len(interval_map)} interval-map entries."
        ),
        "",
        f"- {len(selected)} connection(s) match the selector, becoming assets.",
        f"- **{len(dropped)} connection(s) do NOT match** — invisible to Dagster.",
        (
            f"- **{len(missing_from_map)} derived group name(s) are missing from "
            "the interval map** → silently scheduled at the 24h default."
        ),
        (
            f"- **{len(dead_entries)} interval-map entry(ies) match no "
            "connection** → dead config."
        ),
        (
            f"- **{len(collisions)} group name(s) derived from more than one "
            "connection** → Dagster collapses them into one asset group."
        ),
    ]
    if collisions:
        lines += ["", "Group-name collisions:"]
        lines += [
            f"- `{group}` ← {', '.join(names)}"
            for group, names in sorted(collisions.items())
        ]
    if dropped:
        lines += ["", "Connections the selector drops:"]
        lines += [f"- {c['name']}" for c in dropped]
    if missing_from_map:
        lines += [
            "",
            "Derived group names absent from the interval map (defaulting to 24h):",
        ]
        lines += [
            f"- `{group}` ← {', '.join(derived[group])}" for group in missing_from_map
        ]
    if dead_entries:
        lines += ["", "Interval-map entries with no matching connection:"]
        lines += [f"- `{group}` ({interval_map[group]}h)" for group in dead_entries]
    lines += [
        "",
        "Any config-as-code import must reproduce these names byte-for-byte; a rename",
        "re-groups the assets and drops the cadence back to 24h with no error.",
    ]
    return lines


def _finding_d_schedules(snapshot: dict[str, Any]) -> list[str]:
    cron: list[tuple[str, str]] = []
    manual: list[dict[str, Any]] = []
    other: list[tuple[str, str]] = []
    inactive: list[tuple[str, str]] = []
    for connection in snapshot["connections"]:
        schedule = connection.get("schedule") or {}
        schedule_type = schedule.get("scheduleType", "(none)")
        if schedule_type == "manual":
            manual.append(connection)
        elif schedule_type == "cron":
            cron.append((connection["name"], schedule.get("cronExpression", "?")))
        else:
            other.append((connection["name"], schedule_type))
        if connection.get("status") != "active":
            inactive.append((connection["name"], connection.get("status")))

    lines = [
        "## D. Schedules and status",
        "",
        f"- {len(manual)} connection(s) are `manual` — Dagster is the only trigger.",
        (
            f"- **{len(cron)} connection(s) carry their own Airbyte cron** — "
            "double-scheduled today."
        ),
        f"- {len(other)} connection(s) have another schedule type.",
        f"- {len(inactive)} connection(s) are not `active`.",
    ]
    if cron:
        lines += [
            "",
            "Airbyte-side cron (reconcile with `sync_interval_hours` at import):",
        ]
        lines += [f"- {name}: `{expression}`" for name, expression in cron]
    if other:
        lines += ["", "Other schedule types:"]
        lines += [f"- {name}: {schedule_type}" for name, schedule_type in other]
    if inactive:
        lines += ["", "Not active:"]
        lines += [f"- {name}: {status}" for name, status in inactive]
    return lines


def _finding_e_dbt(snapshot: dict[str, Any]) -> list[str]:
    dbt_tables = _dbt_raw_tables()
    predicted: dict[str, tuple[str, str]] = {}
    for connection in snapshot["connections"]:
        for stream in _streams_of(connection):
            predicted[_predicted_raw_table(connection, stream)] = (
                connection["name"],
                stream.get("name", "?"),
            )

    lowered_dbt = {table.lower(): table for table in dbt_tables}
    loaded_unmodeled = sorted(
        name for name in predicted if name.lower() not in lowered_dbt
    )
    lowered_predicted = {name.lower() for name in predicted}
    modeled_unloaded = sorted(
        table for table in dbt_tables if table.lower() not in lowered_predicted
    )

    lines = [
        "## E. dbt reconcile (heuristic)",
        "",
        "Predicted raw table = connection `prefix` + stream name, matched",
        "case-insensitively against the `raw__*` tables declared in",
        "`src/ol_dbt/models/**/_*sources.yml`.",
        "",
        (
            f"- {len(predicted)} predicted raw tables from "
            f"{len(snapshot['connections'])} connections."
        ),
        f"- {len(dbt_tables)} raw tables declared in dbt.",
        f"- **{len(loaded_unmodeled)} loaded but not modeled** → `modeled: false`.",
        (
            f"- **{len(modeled_unloaded)} modeled but produced by no Airbyte "
            "stream** → expected for dlt-owned tables (edxorg, mitpe, oll, "
            "mit_climate); anything else is a gap."
        ),
    ]
    if modeled_unloaded:
        lines += ["", "Modeled with no matching Airbyte stream:"]
        lines += [f"- {table}" for table in modeled_unloaded[:60]]
        if len(modeled_unloaded) > 60:  # noqa: PLR2004
            lines += [f"- … and {len(modeled_unloaded) - 60} more"]
    return lines


def _finding_f_prefixes(snapshot: dict[str, Any]) -> list[str]:
    rows = [
        (
            connection["name"],
            connection.get("prefix") or "(none)",
            connection.get("namespaceDefinition", "?"),
            connection.get("namespaceFormat") or "",
            len(_streams_of(connection)),
        )
        for connection in sorted(
            snapshot["connections"], key=lambda c: c["name"].lower()
        )
    ]
    prefixes = Counter(row[1] for row in rows)
    duplicated = [
        prefix for prefix, count in prefixes.items() if count > 1 and prefix != "(none)"
    ]

    lines = [
        "## F. Table prefixes (the authoritative `table_prefix`)",
        "",
        "| Connection | Prefix | Namespace def | Namespace format | Streams |",
        "|---|---|---|---|---:|",
    ]
    lines += [
        f"| {name} | `{prefix}` | {ns_def} | `{ns_fmt}` | {count} |"
        for name, prefix, ns_def, ns_fmt, count in rows
    ]
    if duplicated:
        lines += [
            "",
            (
                f"**{len(duplicated)} prefix(es) are shared by more than one "
                "connection** — the inventory requires prefixes to be pairwise "
                "non-overlapping, so these units need splitting:"
            ),
        ]
        lines += [
            f"- `{prefix}` ({prefixes[prefix]} connections)" for prefix in duplicated
        ]
    missing = [row[0] for row in rows if row[1] == "(none)"]
    if missing:
        lines += [
            "",
            (
                f"**{len(missing)} connection(s) have no prefix** — raw table "
                "names come from the stream name alone:"
            ),
        ]
        lines += [f"- {name}" for name in missing]
    return lines


@app.command
def report(
    *,
    snapshot: Annotated[
        Path | None,
        Parameter(help="Snapshot written by `dump`.", show_default=False),
    ] = None,
    output: Annotated[
        Path | None, Parameter(help="Write markdown here instead of stdout.")
    ] = None,
) -> None:
    """Derive the findings from ONE snapshot and print them as markdown.

    Single-environment by design, unlike `render`. Findings C and E join against
    production-only facts -- ``group_name_to_interval`` (guarded by
    ``DAGSTER_ENV == "production"``) and the dbt sources, which describe the
    production warehouse -- so running them over a QA snapshot would report every
    QA connection as miswired. Point it at one environment at a time.
    """
    snapshot = snapshot or snapshot_path_for(DEFAULT_ENVIRONMENT)
    data = _load_snapshot(snapshot)
    lines = [
        "# Airbyte ingestion inventory — findings",
        "",
        f"Environment: `{data['environment']}`.",
        f"Source: `{data['server_url']}`, fetched {data['fetched_at']}.",
        f"- {len(data['sources'])} sources, {len(data['destinations'])} destinations",
        f"- {len(data['connections'])} connections",
        f"- {sum(len(_streams_of(c)) for c in data['connections'])} configured streams",
        "",
    ]
    for section in (
        _finding_a_replication,
        _finding_b_cursors,
        _finding_c_dagster,
        _finding_d_schedules,
        _finding_e_dbt,
        _finding_f_prefixes,
    ):
        lines += section(data)
        lines += ["", "---", ""]

    text = "\n".join(lines)
    if output:
        output.write_text(text)
        _progress(f"Wrote {output}")
    else:
        print(text)


# ---------------------------------------------------------------------------
# Draft inventory
# ---------------------------------------------------------------------------


def _infer_unit_key(prefix: str) -> tuple[str, str, bool]:  # noqa: PLR0911
    """Guess (deployment, layer) from a raw table prefix.

    Returns the guess plus whether it is confident. Raw table names are not
    positionally parseable (spec §1.1), so anything unconfident is emitted with
    a TODO for a human to resolve rather than silently invented.
    """
    parts = [part for part in prefix.split("__") if part]
    if not parts or parts[0] != "raw" or len(parts) < 3:  # noqa: PLR2004
        return ("TODO", "TODO", False)
    deployment, *rest = parts[1:]
    if "tracking_logs" in rest or "tracking" in rest:
        return (deployment, "tracking_logs", True)
    if "mysql" in rest:
        return (deployment, "mysql", True)
    if "mongodb" in rest or "mongo" in rest:
        return (deployment, "mongodb", True)
    if "app" in rest and "postgres" in rest:
        return (deployment, "app_postgres", True)
    if "fastly" in rest:
        return (deployment, "fastly", True)
    if "api" in rest:
        return (deployment, "api", True)
    if rest and rest[-1] in KNOWN_LAYERS:
        return (deployment, rest[-1], True)
    return (deployment, f"TODO_{'_'.join(rest)}", False)


@app.command
def render(  # noqa: C901, PLR0912, PLR0915
    *,
    snapshots: Annotated[
        list[Path] | None,
        Parameter(
            help=(
                "Snapshots written by `dump`, one per environment. Repeat the "
                "flag to merge several; defaults to whichever "
                "airbyte-snapshot-<env>.json files exist."
            ),
            show_default=False,
        ),
    ] = None,
    output_dir: Annotated[
        Path, Parameter(help="Directory for the draft unit files.")
    ] = Path("ingestion/inventory/units"),
) -> None:
    """Render a DRAFT inventory from the snapshots — for review, not for merging as-is.

    Every field the API cannot answer (`scope`, `strategies`, `modeled` for
    tables dbt does not declare) is emitted with a TODO so the reviewer has to
    decide it rather than inherit a guess.

    Merges across environments. A unit is (deployment, layer) regardless of where
    it runs, so the same unit's production and QA connections land in one
    `airbyte.connections` list, each tagged with its `environment`. That is what
    makes `strategies.qa` decidable from evidence instead of assumption: a unit
    with no QA connection at all, and a unit whose QA connection exists but is
    `inactive`, are different situations and used to look identical here.
    """
    paths = snapshots or [
        candidate
        for environment in sorted(AIRBYTE_SERVERS)
        if (candidate := snapshot_path_for(environment)).exists()
    ]
    if not paths:
        msg = (
            "No snapshots found. Run `airbyte-inventory dump --environment "
            f"<{'|'.join(sorted(AIRBYTE_SERVERS))}>` first, or pass --snapshots."
        )
        raise SystemExit(msg)

    dbt_tables = {table.lower() for table in _dbt_raw_tables()}
    interval_map = _parse_interval_map()

    sources_by_id: dict[str, dict[str, Any]] = {}
    grouped: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    unresolved: list[str] = []
    environments: list[str] = []
    for path in paths:
        data = _load_snapshot(path)
        environment = data["environment"]
        if environment in environments:
            msg = (
                f"Two snapshots claim environment {environment!r} "
                f"(latest: {path}). Renders would double-count its connections."
            )
            raise SystemExit(msg)
        environments.append(environment)
        _progress(
            f"  {path}: {environment}, {len(data['connections'])} connection(s), "
            f"fetched {data['fetched_at']}"
        )
        sources_by_id.update({s["sourceId"]: s for s in data["sources"]})
        for connection in sorted(data["connections"], key=lambda c: c["name"].lower()):
            prefix = _effective_prefix(connection)
            deployment, layer, confident = _infer_unit_key(prefix)
            if not confident:
                unresolved.append(
                    f"[{environment}] {connection['name']} "
                    f"(prefix {prefix or '(none)'})"
                )
            grouped[deployment, layer].append((environment, connection))

    units: dict[tuple[str, str], dict[str, Any]] = {}
    todos_by_key: dict[tuple[str, str], list[str]] = {}
    for key, tagged_connections in grouped.items():
        deployment, layer = key
        todos: list[str] = []
        actors = []
        for environment, connection in tagged_connections:
            source = sources_by_id.get(connection["sourceId"], {})
            group = dagster_group_name(connection["name"])
            actors.append(
                {
                    "environment": environment,
                    "connection_name": connection["name"],
                    "source_kind": f"source-{source.get('sourceType', 'unknown')}",
                    "replication_method": _normalized_replication_method(
                        source.get("configuration")
                    ),
                    "status": connection.get("status", "active"),
                    "streams": sorted(
                        str(stream.get("name", ""))
                        for stream in _streams_of(connection)
                    ),
                    "table_prefix": _effective_prefix(connection),
                    "dagster_group": group,
                    # group_name_to_interval in definitions.py is guarded by
                    # `if DAGSTER_ENV == "production"`, so it says nothing about
                    # any other environment. Reading it for a QA connection would
                    # invent a cadence -- and QA's schedules are all STOPPED
                    # anyway, so there is no cadence to report.
                    "sync_interval_hours": (
                        interval_map.get(group) if environment == "production" else None
                    ),
                }
            )

        intervals = {
            a["sync_interval_hours"] for a in actors if a["sync_interval_hours"]
        }
        if len(intervals) > 1:
            todos.append(
                f"connections disagree on cadence ({sorted(intervals)}h) — "
                "split the unit or pick one"
            )
        prefixes = {a["table_prefix"] for a in actors}
        if len(prefixes) > 1:
            todos.append(
                f"connections use different prefixes ({sorted(prefixes)}) — "
                "the unit key is wrong"
            )
        if deployment == "TODO" or layer.startswith("TODO"):
            todos.append(
                "(deployment, layer) could not be inferred — assign it by hand"
            )

        by_environment: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for actor in actors:
            by_environment[actor["environment"]].append(actor)
        reference = next(
            (a for a in actors if a["environment"] == DEFAULT_ENVIRONMENT), actors[0]
        )

        # The per-environment notes below are the whole point of merging
        # snapshots: they are the evidence `strategies.qa` is supposed to be
        # decided on, and equally the scoping input for replacing this unit with
        # a dlt pipeline -- what a QA loader would have to cover, whatever loads
        # it. Only emitted for environments actually snapshotted, so a
        # production-only render says nothing about QA rather than implying
        # absence.
        production_streams = {
            stream
            for actor in by_environment.get(DEFAULT_ENVIRONMENT, [])
            for stream in actor["streams"]
        }
        for environment in environments:
            if environment == DEFAULT_ENVIRONMENT:
                continue
            peers = by_environment.get(environment, [])
            if not peers:
                todos.append(
                    f"no {environment} connection exists for this unit — "
                    f"`strategies.{environment}` cannot be `ingest` without "
                    f"building one"
                )
                continue
            if all(actor["status"] != "active" for actor in peers):
                todos.append(
                    f"every {environment} connection is disabled "
                    f"({', '.join(sorted(a['connection_name'] for a in peers))}) — "
                    f"the unit is configured there but has gone dark"
                )
            peer_streams = {s for actor in peers for s in actor["streams"]}
            if missing := sorted(production_streams - peer_streams):
                shown = ", ".join(missing[:5])
                more = f", +{len(missing) - 5} more" if len(missing) > 5 else ""  # noqa: PLR2004
                todos.append(
                    f"{len(missing)} stream(s) in production are absent from "
                    f"{environment}: {shown}{more}"
                )

        # Structurally schema-valid (§3): every required key present and every
        # list in the right shape, so `ol-dbt inventory validate` reports only
        # the TODO values a human still has to decide — not a wall of shape
        # errors on top of them.
        unit: dict[str, Any] = {
            "schema_version": 1,
            "deployment": deployment,
            "layer": layer,
            "scope": "TODO",
            "strategies": {"qa": "TODO", "local": "TODO"},
            "loader": "airbyte",
            "table_prefix": sorted(prefixes)[0],
            "airbyte": {
                # Production is the reference for the unit-level facts when a
                # unit runs in several environments: QA's connector can lag a
                # version or sit on an older replication method without that
                # being what the unit means.
                "source_kind": reference["source_kind"],
                "replication_method": reference["replication_method"],
                "connections": [
                    {
                        "environment": actor["environment"],
                        "name": actor["connection_name"],
                        "status": actor["status"],
                        "sync_interval_hours": actor["sync_interval_hours"],
                        "streams": actor["streams"],
                    }
                    for actor in sorted(
                        actors,
                        key=lambda a: (a["environment"], a["connection_name"].lower()),
                    )
                ],
            },
        }
        kinds = {actor["source_kind"] for actor in actors}
        if len(kinds) > 1:
            todos.append(f"connections use different source kinds ({sorted(kinds)})")

        tables: dict[str, dict[str, Any]] = {}
        # Production first, so it wins every raw_table it defines. `tables` is
        # the unit's intended contract, not a description of any one
        # environment's current wiring -- and a QA connection frozen before a
        # schema change would otherwise overwrite the live definition with a
        # stale one purely on iteration order.
        for environment, connection in sorted(
            tagged_connections,
            key=lambda pair: (pair[0] != DEFAULT_ENVIRONMENT, pair[0]),
        ):
            for stream in sorted(
                _streams_of(connection), key=lambda s: s.get("name", "")
            ):
                raw_table = _predicted_raw_table(connection, stream)
                entry: dict[str, Any] = {
                    "name": stream.get("name"),
                    "raw_table": raw_table,
                    "sync_mode": stream.get("syncMode"),
                }
                # 790 of 1,518 streams carry a namespace (public, edxapp,
                # forum, …). Dropping it makes the rendered config unable to
                # reproduce the imported connection, so the empty-preview gate
                # would never pass.
                if stream.get("namespace"):
                    entry["namespace"] = stream["namespace"]
                if stream.get("cursorField"):
                    entry["cursor_field"] = stream["cursorField"]
                if stream.get("primaryKey"):
                    # Stored as Airbyte spells it: a list of paths, each path a
                    # list of segments. Joining into one dotted string per column
                    # would be lossy — a single segment containing a literal "."
                    # is indistinguishable from two nested segments once joined.
                    entry["primary_key"] = [list(part) for part in stream["primaryKey"]]
                entry["modeled"] = raw_table.lower() in dbt_tables
                held = tables.get(raw_table)
                # Compare on the stream config alone: `held` also carries the
                # `_environment` scratch key, so comparing whole dicts would
                # report every table as differing.
                if (
                    held is not None
                    and {k: v for k, v in held.items() if k != "_environment"} != entry
                ):
                    # Across environments this is drift, not a conflict to
                    # resolve: production defines the table and the other
                    # environment has fallen behind it. Only same-environment
                    # disagreement means two connections genuinely contradict.
                    if environment == tables[raw_table]["_environment"]:
                        todos.append(
                            f"{raw_table} is configured differently by two "
                            f"{environment} connections"
                        )
                    else:
                        todos.append(
                            f"{raw_table} differs between "
                            f"{tables[raw_table]['_environment']} and {environment} "
                            f"— keeping the "
                            f"{tables[raw_table]['_environment']} definition"
                        )
                tables.setdefault(raw_table, {**entry, "_environment": environment})
        # `_environment` is renderer scratch for the drift note above; the schema
        # forbids unknown keys, so it never reaches the file.
        unit["tables"] = sorted(
            (
                {k: v for k, v in entry.items() if k != "_environment"}
                for entry in tables.values()
            ),
            key=lambda t: t["raw_table"],
        )
        # Kept beside the unit rather than inside it: the schema forbids unknown
        # keys, and renderer scratch notes should not earn a slot in it.
        todos_by_key[key] = todos
        units[key] = unit

    output_dir.mkdir(parents=True, exist_ok=True)
    for (deployment, layer), unit in sorted(units.items()):
        path = output_dir / re.sub(
            r"[^a-z0-9_]+", "_", f"{deployment}__{layer}".lower()
        ).strip("_")
        path = path.with_suffix(".yml")
        header = (
            "---\n"
            "# DRAFT — generated by bin/airbyte-inventory.py from live Airbyte\n"
            f"# snapshots of: {', '.join(environments)}.\n"
            "# Every TODO is a decision the API cannot make: scope, per-environment\n"
            "# strategies, and any layer that could not be inferred from the prefix.\n"
            "# See docs/specs/INGESTION_INVENTORY_SPEC.md §3.\n"
        )
        for todo in todos_by_key.get((deployment, layer), []):
            header += f"# TODO: {todo}\n"
        path.write_text(
            header
            + yaml.safe_dump(unit, sort_keys=False, width=100, allow_unicode=True)
        )
        _progress(f"  {path}  ({len(unit['tables'])} tables)")

    _progress(f"\nWrote {len(units)} draft unit file(s) to {output_dir}")
    todo_units = [
        f"{deployment}__{layer}"
        for deployment, layer in units
        if deployment == "TODO" or layer.startswith("TODO")
    ]
    if todo_units:
        _progress(
            f"{len(todo_units)} unit(s) need a hand-assigned (deployment, layer): "
            f"{', '.join(sorted(todo_units))}"
        )
    if unresolved:
        _progress("Connections whose unit key could not be inferred:")
        for name in unresolved:
            _progress(f"  - {name}")


@app.command(name="all")
def run_all(  # noqa: PLR0913
    *,
    username: Annotated[str, Parameter(env_var="AIRBYTE_USERNAME")] = "dagster",
    password: Annotated[
        str | None, Parameter(env_var="AIRBYTE_PASSWORD", show_default=False)
    ] = None,
    environment: Annotated[
        str,
        Parameter(
            env_var="AIRBYTE_ENVIRONMENT",
            help=(
                "Which Airbyte deployment to snapshot: "
                f"{', '.join(sorted(AIRBYTE_SERVERS))}."
            ),
        ),
    ] = DEFAULT_ENVIRONMENT,
    server_url: Annotated[
        str | None,
        Parameter(
            env_var="AIRBYTE_SERVER_URL",
            help="Override the host for --environment.",
            show_default=False,
        ),
    ] = None,
    findings: Path = Path("airbyte-findings.md"),
    output_dir: Path = Path("ingestion/inventory/units"),
) -> None:
    """Dump ONE environment → report → render from every snapshot on disk.

    One environment per run, because each Airbyte deployment has its own
    credentials from its own Vault service -- a single set of
    --username/--password cannot reach two of them. Run it once per
    environment; the render step picks up whatever snapshots are present, so
    the merged inventory builds up across runs rather than needing one pass
    that could authenticate everywhere.
    """
    dump(
        username=username,
        password=password,
        environment=environment,
        server_url=server_url,
    )

    # Findings C and E join against production-only facts, so they are derived
    # from the production snapshot whenever one exists -- this run's, or an
    # earlier one's. Nothing to report from without it.
    production_snapshot = snapshot_path_for(DEFAULT_ENVIRONMENT)
    if production_snapshot.exists():
        report(snapshot=production_snapshot, output=findings)
    else:
        _progress(
            f"No {production_snapshot} yet — skipping findings, which are "
            f"production-only. Run `all --environment {DEFAULT_ENVIRONMENT}` "
            f"to produce them."
        )

    render(output_dir=output_dir)


if __name__ == "__main__":
    app()
