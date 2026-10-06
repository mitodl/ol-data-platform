"""`ol-dbt metrics` — maintain the business metric registry in ``metrics/``.

The credential-free check is ``ol-dbt validate --only metric_registry``; see
``ol_dbt_cli.lib.metric_registry``. It validates each metric body against a
committed snapshot of OpenMetadata's request schema, and ``refresh-schema``
rewrites that snapshot from a server.
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console

from ol_dbt_cli.lib.metric_registry import SCHEMA_SNAPSHOT, SCHEMA_VERSION_KEY, build_schema_snapshot

console = Console()

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
