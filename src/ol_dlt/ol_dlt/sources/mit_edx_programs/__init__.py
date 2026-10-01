"""MIT edX (MITx on edX.org) programs and MITx catalog ingestion via dlt.

Reads two edX discovery API endpoints with JWT client credentials. The edX API
expects ``Authorization: JWT <token>`` (not Bearer).

Data flow:
    edX Programs API (JWT)
        -> raw__edxorg__discovery__api__programs        (MIT, active, not
           MicroMasters; nested, merged on uuid)
        -> raw__edxorg__discovery__api__program         (every program,
           flattened, appended per extraction)
        -> raw__edxorg__discovery__api__program_course  (each program's courses)
    edX catalog 10 courses API (JWT)
        -> raw__edxorg__discovery__api__mitx_course
        -> raw__edxorg__discovery__api__mitx_course_run

The four flattened tables replace the edxorg code location's
``edxorg_program_metadata`` / ``edxorg_mitx_course_metadata`` assets and the
Airbyte connection that loaded their files into ``raw__edxorg__s3__*``. That
connection inferred its schema from a sample of old files, so it dropped the
fields #2721 added, and it re-appended every file the assets overwrote. Each
row carries the ``retrieved_at`` of the extraction that wrote it, and staging
treats the rows sharing the latest ``retrieved_at`` as what the API currently
lists, so these tables only ever append.

Nested values (images, subjects, seats, staff, a course's runs) are stored as
JSON strings, which is what the Airbyte tables held and what staging parses.

Secrets are resolved lazily at run time. The qa and production profiles read
the edX.org OAuth client from Vault (``EDX_OAUTH_VAULT_PATH``), the same one the
edxorg code location uses against this API. Any other profile reads
EDX_API_CLIENT_ID, EDX_API_CLIENT_SECRET and EDX_API_ACCESS_TOKEN_URL from the
environment. EDX_PROGRAMS_API_URL and EDX_MITX_COURSES_API_URL override the
endpoints in both.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.mit_edx_programs
"""

import json
import logging
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
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
# Catalog 10 is edX's MITx catalog.
EDX_MITX_COURSES_API_URL = "https://discovery.edx.org/api/v1/catalogs/10/courses/"

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


def _columns(text: tuple[str, ...], **typed: str) -> dict[str, dict[str, Any]]:
    """Declare every column of a flattened table, text unless typed otherwise.

    These are the types the Airbyte tables held, which staging reads. Declaring
    all of them matters for three reasons. dlt's default iso_timestamp detection
    would otherwise load retrieved_at and the API's dates as timestamps, where
    staging and the edX programs delivery parse ISO strings. dlt creates no
    column that is null in every row of a load, so a day on which, say, no run
    has an announcement would drop a column staging selects. And the API sends
    estimated_hours as an int for whole hours and a float otherwise.
    """
    return {
        name: {"data_type": typed.get(name, "text"), "nullable": True}
        for name in (*text, *typed)
    }


_PROGRAM_COLUMNS = _columns(
    (
        "uuid",
        "title",
        "subtitle",
        "type",
        "status",
        "authoring_organizations",
        "data_modified_timestamp",
        "marketing_url",
        "banner_image_url",
        "level_type_override",
        "retrieved_at",
    )
)
_PROGRAM_COURSE_COLUMNS = _columns(
    (
        "program_uuid",
        "course_key",
        "course_title",
        "course_short_description",
        "course_type",
        "course_runs",
        "retrieved_at",
    ),
    course_position="bigint",
    excluded_from_search="bool",
)
_MITX_COURSE_COLUMNS = _columns(
    (
        "course_key",
        "title",
        "owner",
        "short_description",
        "full_description",
        "level_type",
        "marketing_url",
        "image",
        "course_type",
        "subjects",
        "prerequisites",
        "prerequisites_raw",
        "modified",
        "retrieved_at",
    )
)
_MITX_COURSE_RUN_COLUMNS = _columns(
    (
        "course_key",
        "run_key",
        "title",
        "short_description",
        "full_description",
        "marketing_url",
        "level_type",
        "languages",
        "start_on",
        "end_on",
        "enrollment_start",
        "enrollment_end",
        "announcement",
        "pacing_type",
        "enrollment_type",
        "availability",
        "status",
        "image",
        "seats",
        "staff",
        "modified",
        "retrieved_at",
    ),
    is_enrollable="bool",
    min_effort="bigint",
    max_effort="bigint",
    weeks_to_complete="bigint",
    estimated_hours="double",
)


def _is_mit_program(program: dict[str, Any]) -> bool:
    """Return True if ``program`` is a live, non-MicroMasters MIT program."""
    orgs = program.get("authoring_organizations") or []
    return (
        any(org.get("key") in _MIT_OWNER_KEYS for org in orgs)
        and "micromasters" not in (program.get("type") or "").lower()
        and program.get("status") == "active"
    )


def _json(value: Any) -> str:  # noqa: ANN401
    """Serialize a nested value compactly, as the Airbyte tables stored it."""
    return json.dumps(value, separators=(",", ":"))


def program_record(program: dict[str, Any], *, retrieved_at: str) -> dict[str, Any]:
    """Flatten one edX discovery API program into a program row.

    Marketing URL, banner image and level override are what MIT Learn's program
    records are built from, alongside the program's courses and their runs.
    """
    banner_image = program.get("banner_image") or {}
    return {
        "uuid": program["uuid"],
        "title": program["title"],
        "subtitle": program["subtitle"],
        "type": program["type"],
        "status": program["status"],
        "authoring_organizations": ", ".join(
            org["key"] for org in program["authoring_organizations"]
        ),
        "data_modified_timestamp": program["data_modified_timestamp"],
        "marketing_url": program.get("marketing_url"),
        "banner_image_url": (banner_image.get("medium") or {}).get("url"),
        "level_type_override": program.get("level_type_override"),
        "retrieved_at": retrieved_at,
    }


def program_course_records(
    program: dict[str, Any], *, retrieved_at: str
) -> list[dict[str, Any]]:
    """Flatten a program's courses into program_course rows.

    ``course_runs`` is kept whole as a JSON string: MIT Learn derives a program's
    dates, price, pace and availability from these runs as the program API reports
    them.

    The rows sharing the latest ``retrieved_at`` are the program's current
    courses; a course dropped from a program leaves no row there.
    ``course_position`` keeps the API's course order, which MIT Learn uses to
    order a program's courses.
    """
    return [
        {
            "program_uuid": program["uuid"],
            "course_key": course["key"],
            "course_position": position,
            "course_title": course["title"],
            "course_short_description": course["short_description"],
            "course_type": course["course_type"],
            "excluded_from_search": course["excluded_from_search"],
            "course_runs": json.dumps(course["course_runs"]),
            "retrieved_at": retrieved_at,
        }
        for position, course in enumerate(program["courses"], start=1)
    ]


def _image(image: dict[str, Any] | None) -> str | None:
    if not image or not image.get("src"):
        return None
    return _json({"url": image["src"], "description": image.get("description")})


def mitx_course_record(course: dict[str, Any], *, retrieved_at: str) -> dict[str, Any]:
    """Flatten one catalog course into a mitx_course row."""
    return {
        "course_key": course["key"],
        "title": course["title"],
        "owner": ", ".join(owner["key"] for owner in course["owners"]),
        "short_description": course["short_description"],
        "full_description": course["full_description"],
        "level_type": course["level_type"],
        "marketing_url": course["marketing_url"],
        "image": _image(course["image"]),
        "course_type": course["course_type"],
        "subjects": _json(
            [{"name": subject.get("name")} for subject in course.get("subjects", [])]
        ),
        "prerequisites": _json(course["prerequisites"]),
        "prerequisites_raw": course["prerequisites_raw"],
        "modified": course["modified"],
        "retrieved_at": retrieved_at,
    }


def mitx_course_run_records(
    course: dict[str, Any], *, retrieved_at: str
) -> list[dict[str, Any]]:
    """Flatten a catalog course's runs into mitx_course_run rows."""
    return [
        {
            "course_key": course["key"],
            "run_key": run["key"],
            "title": run["title"],
            "short_description": run["short_description"],
            "full_description": run["full_description"],
            "marketing_url": run["marketing_url"],
            "level_type": run["level_type"],
            "languages": run["content_language"],
            "start_on": run["start"],
            "end_on": run["end"],
            "enrollment_start": run["enrollment_start"],
            "enrollment_end": run["enrollment_end"],
            "announcement": run["announcement"],
            "pacing_type": run["pacing_type"],
            "enrollment_type": run["type"],
            "availability": run["availability"],
            "status": run["status"],
            "is_enrollable": run["is_enrollable"],
            "image": _image(run["image"]),
            "seats": _json(run["seats"]),
            "staff": _json(
                [
                    {
                        "first_name": staff.get("given_name"),
                        "last_name": staff.get("family_name"),
                    }
                    for staff in run["staff"]
                ]
            ),
            "weeks_to_complete": run["weeks_to_complete"],
            "min_effort": run["min_effort"],
            "max_effort": run["max_effort"],
            "estimated_hours": run["estimated_hours"],
            "modified": run["modified"],
            "retrieved_at": retrieved_at,
        }
        for run in course["course_runs"]
    ]


def _resolve_credentials(
    client_id: str | None,
    client_secret: str | None,
    access_token_url: str | None,
) -> dict[str, str]:
    """Return the OAuth client for the active profile.

    Deployed profiles take the client from Vault, as ``ol_dlt.database`` does for
    its database credentials. Explicit arguments and the environment apply only
    to the other profiles.
    """
    if config.active_profile() in config.ICEBERG_PROFILES:
        oauth_client = vault.read_kv_secret(EDX_OAUTH_VAULT_MOUNT, EDX_OAUTH_VAULT_PATH)
        return {
            "client_id": oauth_client["id"],
            "client_secret": oauth_client["secret"],
            "access_token_url": oauth_client["token_url"],
        }
    return config.require_secrets(
        client_id=config.resolve_secret(client_id, "EDX_API_CLIENT_ID"),
        client_secret=config.resolve_secret(client_secret, "EDX_API_CLIENT_SECRET"),
        access_token_url=config.resolve_secret(
            access_token_url, "EDX_API_ACCESS_TOKEN_URL"
        ),
    )


def _jwt_headers(creds: dict[str, str]) -> dict[str, str]:
    """Fetch a client-credentials token and return the edX JWT auth header."""
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
    return {"Authorization": f"JWT {token_resp.json()['access_token']}"}


def _paginate(url: str, headers: dict[str, str]) -> Iterator[dict[str, Any]]:
    """Yield every result of a paginated discovery API listing."""
    next_url: str | None = url
    while next_url:
        resp = requests.get(next_url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        yield from data["results"]
        next_url = data["next"]


@dlt.source(name="mit_edx_programs_ingest")
def mit_edx_programs_source(
    client_id: str | None = None,
    client_secret: str | None = None,
    access_token_url: str | None = None,
    programs_api_url: str | None = None,
    mitx_courses_api_url: str | None = None,
) -> Generator[Any]:
    """Load edX.org programs and the MITx course catalog.

    Credentials are resolved at execution time, so the module imports cleanly
    without secrets present. Under the qa and production profiles the OAuth
    client always comes from Vault and the first three arguments are ignored.

    Args:
        client_id: JWT client ID (else EDX_API_CLIENT_ID). Local profiles only.
        client_secret: JWT client secret (else EDX_API_CLIENT_SECRET). Local
            profiles only.
        access_token_url: Token endpoint URL (else EDX_API_ACCESS_TOKEN_URL).
            Local profiles only.
        programs_api_url: Programs API URL (else EDX_PROGRAMS_API_URL, else
            ``EDX_PROGRAMS_API_URL``).
        mitx_courses_api_url: MITx catalog courses API URL (else
            EDX_MITX_COURSES_API_URL, else ``EDX_MITX_COURSES_API_URL``).
    """

    def _extraction(url: str) -> Iterator[dict[str, Any]]:
        headers = _jwt_headers(
            _resolve_credentials(client_id, client_secret, access_token_url)
        )
        # One timestamp per extraction: staging finds what the API currently
        # lists by the latest retrieved_at.
        retrieved_at = datetime.now(tz=UTC).isoformat()
        for item in _paginate(url, headers):
            yield {"item": item, "retrieved_at": retrieved_at}

    # The parents are unselected so each endpoint is read once per run and
    # fanned out to the transformers below, which are the tables.
    @dlt.resource(selected=False)
    def edx_programs() -> Iterator[dict[str, Any]]:
        yield from _extraction(
            config.resolve_secret(programs_api_url, "EDX_PROGRAMS_API_URL")
            or EDX_PROGRAMS_API_URL
        )

    @dlt.resource(selected=False)
    def edx_mitx_courses() -> Iterator[dict[str, Any]]:
        yield from _extraction(
            config.resolve_secret(mitx_courses_api_url, "EDX_MITX_COURSES_API_URL")
            or EDX_MITX_COURSES_API_URL
        )

    @dlt.transformer(
        data_from=edx_programs,
        name="raw__edxorg__discovery__api__programs",
        # merge (not replace) so a paginated fetch that fails partway through
        # upserts what it got rather than truncating the table to a short page.
        primary_key="uuid",
        write_disposition="merge",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
    )
    def mit_programs(extracted: dict[str, Any]) -> Iterator[dict[str, Any]]:
        if _is_mit_program(extracted["item"]):
            yield extracted["item"]

    @dlt.transformer(
        data_from=edx_programs,
        name="raw__edxorg__discovery__api__program",
        write_disposition="append",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
        columns=_PROGRAM_COLUMNS,
        max_table_nesting=0,
    )
    def program(extracted: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield program_record(extracted["item"], retrieved_at=extracted["retrieved_at"])

    @dlt.transformer(
        data_from=edx_programs,
        name="raw__edxorg__discovery__api__program_course",
        write_disposition="append",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
        columns=_PROGRAM_COURSE_COLUMNS,
        max_table_nesting=0,
    )
    def program_course(extracted: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from program_course_records(
            extracted["item"], retrieved_at=extracted["retrieved_at"]
        )

    @dlt.transformer(
        data_from=edx_mitx_courses,
        name="raw__edxorg__discovery__api__mitx_course",
        write_disposition="append",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
        columns=_MITX_COURSE_COLUMNS,
        max_table_nesting=0,
    )
    def mitx_course(extracted: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield mitx_course_record(
            extracted["item"], retrieved_at=extracted["retrieved_at"]
        )

    @dlt.transformer(
        data_from=edx_mitx_courses,
        name="raw__edxorg__discovery__api__mitx_course_run",
        write_disposition="append",
        table_format=config.active_table_format(),
        schema_contract=config.JSON_API_SCHEMA_CONTRACT,
        columns=_MITX_COURSE_RUN_COLUMNS,
        max_table_nesting=0,
    )
    def mitx_course_run(extracted: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield from mitx_course_run_records(
            extracted["item"], retrieved_at=extracted["retrieved_at"]
        )

    yield edx_programs
    yield edx_mitx_courses
    yield mit_programs
    yield program
    yield program_course
    yield mitx_course
    yield mitx_course_run


mit_edx_programs_pipeline = config.pipeline_for("mit_edx_programs")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source (uniform entrypoint for the Dagster wrapper)."""
    return config.with_nullable_load_id(mit_edx_programs_source())
