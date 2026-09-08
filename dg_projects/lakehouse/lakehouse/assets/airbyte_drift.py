"""Report where the live Airbyte workspace has drifted from the ingestion inventory.

INGESTION_INVENTORY_SPEC steps 5 and 6 were struck (§6.0), so nothing applies
the inventory to Airbyte and its configuration is edited by hand. That makes
this the only thing that notices when the file stops describing reality —
§4 calls it "the only part of the Airbyte-as-code story that needs to run on a
timer", and the acceptance criterion is that a connection edited in the UI is
reported within a day.

It runs here rather than in Concourse because Dagster already holds the
Airbyte basic-auth credential it needs, and because a failing run already
routes to Sentry through the existing run-failure sensor — so an ERROR
finding needs no reporting plumbing of its own.

The comparison itself lives in `ol_dbt_cli.lib.inventory`, which imports
neither dbt nor duckdb precisely so a Dagster code location can read the
inventory (spec §5). This module only fetches and adapts.
"""

from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, Failure, MetadataValue, Output, asset
from ol_dbt_cli.lib.inventory import check_drift, load_units
from ol_dbt_cli.lib.validation import Severity, ValidationReport

from lakehouse.resources.airbyte import AirbyteOSSWorkspace

# The image copies the repo to /opt/dagster/code and sets it as WORKDIR
# (dockerfiles/orchestrate/Dockerfile.global), so the inventory ships with the
# code location and the relative path resolves. Anchored on this file rather
# than the process's cwd, which a run launcher is free to change.
INVENTORY_DIR = Path(__file__).resolve().parents[4] / "ingestion" / "inventory"


def _fetch_workspace(workspace: AirbyteOSSWorkspace) -> dict[str, list[dict[str, Any]]]:
    """Read the connections and sources the drift check compares against.

    Deliberately raw API responses rather than `fetch_airbyte_workspace_data`:
    dagster-airbyte's `AirbyteConnection` carries only id, name, stream_prefix
    and stream names. It has no status, no schedule, no `sourceId`, and no
    per-stream sync mode or cursor — which is most of what drift means here.
    """
    client = workspace.get_client()
    common = {
        "workspaceIds": workspace.workspace_id,
        "limit": workspace.request_page_size,
    }

    connections = list(
        client._paginated_request(  # noqa: SLF001
            method="GET",
            url=f"{client.rest_api_base_url}/connections",
            params=dict(common),
        )
    )
    sources = list(
        client._paginated_request(  # noqa: SLF001
            method="GET", url=f"{client.rest_api_base_url}/sources", params=dict(common)
        )
    )

    # Some server versions omit stream configs from the list response; the same
    # re-fetch `bin/airbyte-inventory.py` does. A connection left without one is
    # NOT treated as having no streams — `check_drift` reports the gap as an
    # unusable snapshot rather than as every declared stream being dropped.
    for connection in connections:
        if not (connection.get("configurations") or {}).get("streams"):
            detail = client._single_request(  # noqa: SLF001
                method="GET",
                url=f"{client.rest_api_base_url}/connections/{connection['connectionId']}",
            )
            if detail:
                connection["configurations"] = detail.get("configurations", {})

    return {"connections": connections, "sources": sources}


@asset(
    name="airbyte_inventory_drift",
    group_name="ingestion_inventory",
    description=(
        "Diff the live Airbyte workspace against ingestion/inventory. Fails the "
        "run when the inventory is wrong about something it declares."
    ),
    compute_kind="airbyte",
)
def airbyte_inventory_drift(
    context: AssetExecutionContext, airbyte: AirbyteOSSWorkspace
) -> Output[dict[str, int]]:
    """Fail when Airbyte no longer matches what the inventory declares."""
    workspace = _fetch_workspace(airbyte)
    units = load_units(INVENTORY_DIR)

    # An empty fetch is not a workspace with no connections — it is a read that
    # did not work, and reporting it as drift would claim every declared
    # connection had been deleted. Same refusal as an incomplete snapshot.
    if not workspace["connections"]:
        msg = (
            "Airbyte returned no connections. Refusing to report drift against an "
            "empty read, which would say every declared connection has been deleted."
        )
        raise Failure(description=msg)
    if not units:
        msg = f"No inventory units found under {INVENTORY_DIR}."
        raise Failure(description=msg)

    report = ValidationReport()
    check_drift(workspace, units, report)

    for issue in report.issues:
        line = f"{issue.model}: {issue.message}"
        if issue.severity is Severity.ERROR:
            context.log.error(line)
        else:
            context.log.warning(line)

    counts = {
        "live_connections": len(workspace["connections"]),
        "units": len(units),
        "errors": len(report.errors),
        "warnings": len(report.warnings),
    }
    metadata = {
        **{key: MetadataValue.int(value) for key, value in counts.items()},
        "findings": MetadataValue.md(
            "\n".join(
                f"- **{i.severity.value}** {i.model}: {i.message}"
                for i in report.issues
            )
            or "No drift."
        ),
    }

    if report.errors:
        # Failing the run is the report: the existing run-failure sensor routes
        # it to Sentry, so an ERROR needs no notification path of its own.
        # Warnings do not fail — a connection we have not declared yet costs
        # nothing until something depends on it, and paging on it would train
        # people to ignore this.
        msg = (
            f"{len(report.errors)} way(s) in which the inventory no longer describes "
            f"Airbyte. See the run logs, and INGESTION_INVENTORY_SPEC §4."
        )
        raise Failure(description=msg, metadata=metadata)

    return Output(counts, metadata=metadata)
