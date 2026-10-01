"""Sloan Executive Education (MIT Learn etl_source ``see``) ingestion via dlt.

Loads the Sloan unified-portal API's courses and course offerings as the API
returns them. Both endpoints return the whole catalog as one JSON array, so each
table is fully replaced on every run, behind
``config.guard_against_replace_truncation``.

Records are loaded whole. The delivery code location's ``sloan_course_metadata``
asset keeps a hand-picked subset of fields, and that subset drops the offering's
``Format``, which MIT Learn's Sloan ETL (learning_resources/etl/sloan.py) reads to
decide a run's availability, pace and format.

Data flow:
    SEE_API_URL/courses           -> raw__see__api__courses
    SEE_API_URL/course-offerings  -> raw__see__api__course_offerings

Run standalone:
    DLT_PROFILE=dev SEE_API_CLIENT_ID=... SEE_API_CLIENT_SECRET=... \
        SEE_API_ACCESS_TOKEN_URL=... python -m ol_dlt.sources.see
"""

import logging
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

import dlt
from dlt.sources.helpers import requests

from ol_dlt import config, oauth

logger = logging.getLogger(__name__)

# The OAuth client the delivery location's sloan_api resource already reads.
SEE_OAUTH_VAULT_PATH = "pipelines/sloan/oauth-client"
# The same in every environment; mit-learn's SEE_API_URL in ol-infrastructure.
SEE_API_URL = "https://mit-unified-portal-prod-78eeds.43d8q2.usa-e2.cloudhub.io/api/"

# The fields staging reads, declared so a field that is null in every record of a
# load (Currency nearly always is) still gets a column. Normalized names.
COURSE_COLUMNS = (
    "course_id",
    "title",
    "description",
    "url",
    "certification_type",
    "topics",
    "image_src",
    "source_create_date",
    "source_last_modified_date",
)
COURSE_OFFERING_COLUMNS = (
    "co_title",
    "course_id",
    "start_date",
    "end_date",
    "delivery",
    "format",
    "duration",
    "price",
    "continuing_ed_credits",
    "time_commitment",
    "location",
    "currency",
    "faculty_name",
)


def _columns(names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """Declare ``names`` as text, plus the two fields every record carries.

    Text rather than inferred: dlt's default iso_timestamp detection would load
    the dates and ``retrieved_at`` as timestamps.
    """
    return {
        **{name: {"data_type": "text", "nullable": True} for name in names},
        "retrieved_at": {"data_type": "text", "nullable": False},
        "api_position": {"data_type": "bigint", "nullable": False},
    }


@dlt.source(name="see_ingest")
def see_source(
    client_id: str | None = None,
    client_secret: str | None = None,
    access_token_url: str | None = None,
    api_url: str = SEE_API_URL,
) -> Generator[Any]:
    """Load Sloan Executive Education courses and course offerings.

    Credentials are resolved at execution time, so the module imports cleanly
    without secrets present. Under the qa and production profiles the OAuth
    client always comes from Vault and the first three arguments are ignored.

    Args:
        client_id: OAuth client ID (else SEE_API_CLIENT_ID). Local profiles only.
        client_secret: OAuth client secret (else SEE_API_CLIENT_SECRET). Local
            profiles only.
        access_token_url: Token endpoint URL (else SEE_API_ACCESS_TOKEN_URL).
            Local profiles only.
        api_url: Base URL of the Sloan API, with a trailing slash.
    """

    def _fetch(table_name: str, endpoint: str) -> Iterator[dict[str, Any]]:
        headers = oauth.jwt_auth_headers(
            oauth.resolve_client_credentials(
                vault_path=SEE_OAUTH_VAULT_PATH,
                env_prefix="SEE_API",
                client_id=client_id,
                client_secret=client_secret,
                access_token_url=access_token_url,
            )
        )
        url = urljoin(api_url, endpoint)
        logger.info("Fetching Sloan %s from %s", endpoint, url)
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        records: list[dict[str, Any]] = resp.json()
        config.guard_against_replace_truncation(table_name, len(records))
        # One timestamp per extraction, the time MIT Learn would have read it.
        retrieved_at = datetime.now(tz=UTC).isoformat()
        # api_position keeps the response order, which MIT Learn's ETL reads: a
        # course's continuing-ed credits are those of its first listed offering.
        for api_position, record in enumerate(records):
            yield {
                **record,
                "retrieved_at": retrieved_at,
                "api_position": api_position,
            }

    @dlt.resource(
        name="raw__see__api__courses",
        write_disposition="replace",
        columns=_columns(COURSE_COLUMNS),
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def courses() -> Iterator[dict[str, Any]]:
        yield from _fetch("raw__see__api__courses", "courses")

    @dlt.resource(
        name="raw__see__api__course_offerings",
        write_disposition="replace",
        columns=_columns(COURSE_OFFERING_COLUMNS),
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def course_offerings() -> Iterator[dict[str, Any]]:
        yield from _fetch("raw__see__api__course_offerings", "course-offerings")

    yield courses
    yield course_offerings


see_pipeline = config.pipeline_for("see")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for Dagster)."""
    return config.with_nullable_load_id(see_source())
