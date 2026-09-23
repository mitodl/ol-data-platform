"""MIT edX (MITx on edX.org) program ingestion via dlt.

Fetches programs from the edX.org Programs API using JWT client credentials.
The edX API expects ``Authorization: JWT <token>`` (not Bearer). Only
MIT-authored programs that are active and not MicroMasters are included
(MicroMasters are handled separately via Airbyte).

Data flow:
    edX Programs API (JWT)  -> raw__edxorg__discovery__api__programs

Secrets are resolved lazily at run time. The qa and production profiles read
the edX.org OAuth client from Vault (``EDX_OAUTH_VAULT_PATH``), the same one the
edxorg code location uses against this API. Any other profile reads
EDX_API_CLIENT_ID, EDX_API_CLIENT_SECRET and EDX_API_ACCESS_TOKEN_URL from the
environment. EDX_PROGRAMS_API_URL overrides ``EDX_PROGRAMS_API_URL`` in both.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.mit_edx_programs
"""

import logging
from collections.abc import Generator
from typing import Any

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config, vault

logger = logging.getLogger(__name__)

# The Dagster deployment carries no EDX_API_* environment, so until this was
# read from Vault every deployed run failed on missing credentials and
# raw__edxorg__discovery__api__programs was never created.
EDX_OAUTH_VAULT_MOUNT = "secret-data"
EDX_OAUTH_VAULT_PATH = "pipelines/edx/edxorg/edx-oauth-client"
EDX_PROGRAMS_API_URL = "https://discovery.edx.org/api/v1/programs/"

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


def _resolve_credentials(
    client_id: str | None,
    client_secret: str | None,
    access_token_url: str | None,
    programs_api_url: str | None,
) -> dict[str, str]:
    """Return the OAuth client and endpoints for the active profile.

    Deployed profiles take the client from Vault, as ``ol_dlt.database`` does for
    its database credentials. Explicit arguments and the environment apply only
    to the other profiles.
    """
    programs_api_url = (
        config.resolve_secret(programs_api_url, "EDX_PROGRAMS_API_URL")
        or EDX_PROGRAMS_API_URL
    )
    if config.active_profile() in config.ICEBERG_PROFILES:
        oauth_client = vault.read_kv_secret(EDX_OAUTH_VAULT_MOUNT, EDX_OAUTH_VAULT_PATH)
        return {
            "client_id": oauth_client["id"],
            "client_secret": oauth_client["secret"],
            "access_token_url": oauth_client["token_url"],
            "programs_api_url": programs_api_url,
        }
    return config.require_secrets(
        client_id=config.resolve_secret(client_id, "EDX_API_CLIENT_ID"),
        client_secret=config.resolve_secret(client_secret, "EDX_API_CLIENT_SECRET"),
        access_token_url=config.resolve_secret(
            access_token_url, "EDX_API_ACCESS_TOKEN_URL"
        ),
        programs_api_url=programs_api_url,
    )


@dlt.source(name="mit_edx_programs_ingest")
def mit_edx_programs_source(
    client_id: str | None = None,
    client_secret: str | None = None,
    access_token_url: str | None = None,
    programs_api_url: str | None = None,
) -> Generator[Any]:
    """Load active MIT-authored programs from the edX.org Programs API.

    Credentials are resolved at execution time from environment variables if not
    passed explicitly, so the module imports cleanly without secrets present.

    Args:
        client_id: JWT client ID (else EDX_API_CLIENT_ID).
        client_secret: JWT client secret (else EDX_API_CLIENT_SECRET).
        access_token_url: Token endpoint URL (else EDX_API_ACCESS_TOKEN_URL).
        programs_api_url: Programs API base URL (else EDX_PROGRAMS_API_URL).
    """

    @dlt.resource(
        name="raw__edxorg__discovery__api__programs",
        # merge (not replace) so a paginated fetch that fails partway through
        # upserts what it got rather than truncating the table to a short page.
        primary_key="uuid",
        write_disposition="merge",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def programs() -> Generator[dict[str, Any]]:
        """Fetch and yield MIT-authored programs from the edX Programs API."""
        creds = _resolve_credentials(
            client_id, client_secret, access_token_url, programs_api_url
        )

        # edX uses JWT token type, not Bearer — fetch a token with the client
        # credentials grant and send it as "Authorization: JWT <token>".
        token_resp = requests.post(
            creds["access_token_url"],
            data={
                "grant_type": "client_credentials",
                "client_id": creds["client_id"],
                "client_secret": creds["client_secret"],
                "token_type": "jwt",
            },
            timeout=30,
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        headers = {"Authorization": f"JWT {token}"}

        next_url: str | None = creds["programs_api_url"]
        total = 0
        included = 0
        while next_url:
            resp = requests.get(next_url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for program in data.get("results", []):
                total += 1
                if _is_mit_program(program):
                    included += 1
                    yield program
            next_url = data.get("next")
        logger.info(
            "MIT edX programs: %d total fetched, %d MIT programs included",
            total,
            included,
        )

    yield programs


mit_edx_programs_pipeline = config.pipeline_for("mit_edx_programs")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for the Dagster wrapper)."""
    return config.with_nullable_load_id(mit_edx_programs_source())
