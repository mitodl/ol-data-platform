"""
MIT edX (MITx on edX.org) program ingestion via dlt.

Fetches programs from the edX.org Programs API using OAuth2 client
credentials. Only MIT-authored programs (``authoring_organizations`` key
contains a known MIT owner key) that are active and not MicroMasters are
included. MicroMasters programs are handled separately via Airbyte.

Data flow:
    edX Programs API (OAuth2)  ─► raw__mit_edx__programs (Iceberg)

Secrets required (via dlt secrets / environment):
  EDX_API_CLIENT_ID
  EDX_API_CLIENT_SECRET
  EDX_API_ACCESS_TOKEN_URL
  EDX_PROGRAMS_API_URL

Usage (standalone):
    python -m data_loading.defs.mit_edx_programs_ingest.loads
"""

import logging
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

logger = logging.getLogger(__name__)

_DLT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent
if _DLT_PROJECT_DIR.exists():
    os.environ.setdefault("DLT_PROJECT_DIR", str(_DLT_PROJECT_DIR))

# MIT owner keys used by MIT Learn to identify MIT-authored edX content.
# Kept in sync with learning_resources/etl/openedx.py MIT_OWNER_KEYS.
_MIT_OWNER_KEYS = frozenset(
    [
        "MITx",
        "MITx_PRO",
        "mitx",
        "mitxpro",
        "MITProfessionalX",
        "MITgcfx",
        "MITOCWx",
        "MITLinkedInDataScienceProf",
        "MITx_CMS",
    ]
)


def _is_mit_program(program: dict[str, Any]) -> bool:
    """Return True if ``program`` is a live, non-MicroMasters MIT program."""
    orgs = program.get("authoring_organizations") or []
    return (
        any(org.get("key") in _MIT_OWNER_KEYS for org in orgs)
        and "micromasters" not in (program.get("type") or "").lower()
        and program.get("status") == "active"
    )


@dlt.source(name="mit_edx_programs_ingest")
def mit_edx_programs_source(
    client_id: str | None = None,
    client_secret: str | None = None,
    access_token_url: str | None = None,
    programs_api_url: str | None = None,
) -> Generator[Any, None, None]:
    """
    Load active MIT-authored programs from the edX.org Programs API.

    Uses OAuth2 client credentials to authenticate. Programs are filtered
    to MIT-authored, active, non-MicroMasters entries before storage.

    Credentials are resolved at execution time from dlt secrets or environment
    variables (``SOURCES__MIT_EDX_PROGRAMS_INGEST__CLIENT_ID`` etc.) if not
    passed explicitly.

    Args:
        client_id: OAuth2 client ID. Resolved from secrets if not provided.
        client_secret: OAuth2 client secret. Resolved from secrets if not provided.
        access_token_url: Token endpoint URL. Resolved from secrets if not provided.
        programs_api_url: Full base URL of the Programs API. Resolved from secrets
            if not provided.
    """
    @dlt.resource(
        name="raw__mit_edx__programs",
        primary_key="uuid",
        write_disposition="replace",
    )
    def programs() -> Generator[dict[str, Any], None, None]:
        """Fetch and yield MIT-authored programs from the edX Programs API."""
        # Resolve credentials here (lazily, at execution time) so the module
        # loads cleanly when secrets are not set in local development.
        resolved_client_id = client_id or os.getenv("EDX_API_CLIENT_ID")
        resolved_client_secret = client_secret or os.getenv("EDX_API_CLIENT_SECRET")
        resolved_token_url = access_token_url or os.getenv("EDX_API_ACCESS_TOKEN_URL")
        resolved_programs_url = programs_api_url or os.getenv("EDX_PROGRAMS_API_URL")

        missing = [
            name
            for name, val in [
                ("client_id", resolved_client_id),
                ("client_secret", resolved_client_secret),
                ("access_token_url", resolved_token_url),
                ("programs_api_url", resolved_programs_url),
            ]
            if not val
        ]
        if missing:
            msg = (
                f"MIT edX programs source is missing required credentials: "
                f"{', '.join(missing)}. "
                "Set EDX_API_CLIENT_ID, EDX_API_CLIENT_SECRET, "
                "EDX_API_ACCESS_TOKEN_URL, EDX_PROGRAMS_API_URL."
            )
            raise ValueError(msg)

        auth = OAuth2ClientCredentials(
            access_token_url=resolved_token_url,  # type: ignore[arg-type]
            client_id=resolved_client_id,  # type: ignore[arg-type]
            client_secret=resolved_client_secret,  # type: ignore[arg-type]
        )
        rest_client = RESTClient(
            base_url=resolved_programs_url,  # type: ignore[arg-type]
            auth=auth,
            paginator=JSONLinkPaginator(next_url_path="next"),
        )
        total = 0
        included = 0
        for page in rest_client.paginate(""):
            for program in page:
                total += 1
                if _is_mit_program(program):
                    included += 1
                    yield program
        logger.info(
            "MIT edX programs: %d total fetched, %d MIT programs included",
            total,
            included,
        )

    yield programs


# ---------------------------------------------------------------------------
# Module-level instances referenced by defs.yaml
# ---------------------------------------------------------------------------
# Secrets are resolved lazily by dlt at pipeline run time, not at import.
# The `dlt.secrets.value` sentinel tells dlt to look them up in secrets.toml
# or environment variables when the source is actually executed.

_dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "dev")

if _dagster_env in ("qa", "production"):
    _destination_name = f"mit_edx_programs_{_dagster_env}"
    _dataset_name = f"ol_warehouse_{_dagster_env}_raw"
else:
    _destination_name = "mit_edx_programs_local"
    _dataset_name = "mit_edx_programs_local"

mit_edx_programs_load_source = mit_edx_programs_source()

mit_edx_programs_pipeline = dlt.pipeline(
    pipeline_name="mit_edx_programs",
    destination=_destination_name,
    dataset_name=_dataset_name,
    progress="log",
)


def _run_pipeline() -> None:
    """Execute the MIT edX programs pipeline (for standalone testing)."""
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "Running MIT edX programs pipeline: destination=%s dataset=%s",
        _destination_name,
        _dataset_name,
    )
    load_info = mit_edx_programs_pipeline.run(mit_edx_programs_load_source)
    logger.info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    _run_pipeline()
