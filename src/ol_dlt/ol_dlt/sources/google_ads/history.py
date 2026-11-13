"""Google Ads change history from ad_opt_v2's files, loaded once.

The API's ``change_event`` keeps 30 days, so the history before
``raw__google_ads__api__change_events`` began (2026-10-14) exists only in two
files per course in mitodl/ad_opt_v2, read from GitHub at ``AD_OPT_V2_REF``:

    <course>/data/Change history report.csv
        -> raw__google_ads__github__change_history_report
    <course>/data/change-history-api.jsonl
        -> raw__google_ads__github__change_history_api

The report is the Google Ads UI export (UTF-16, tab-separated, two title lines,
then ``Date & time, User, Campaign, Ad group, Changes``), from October 2024 to
when it was downloaded. The JSONL is what ad_opt_v2's pull_change_events saved
from the API between that download and 2026-10-20, already in the report's
terms (a header line and detail lines per change). Staging combines both with
the API table, as ad_opt_v2's parse_change_history does.

The report's ``User`` column is dropped and email addresses in its text are
masked: they name the people who made a change or were given account access,
and the API load leaves ``user_email`` out for the same reason. Each table is
replaced on every load, so a rerun reloads the files rather than adding rows.

Credentials: the qa and production profiles read the data platform's GitHub
App from Vault ``secret-data/pipelines/github-app``, whose installation must
include mitodl/ad_opt_v2. Any other profile reads a local ad_opt_v2 checkout
from the path in AD_OPT_V2_PATH.
"""

import base64
import csv
import functools
import io
import json
import logging
import re
from collections.abc import Callable, Generator, Iterator
from pathlib import Path
from typing import Any

import dlt
from github import Auth, Github

from ol_dlt import config, vault

logger = logging.getLogger(__name__)

AD_OPT_V2_REPO = "mitodl/ad_opt_v2"
# The history stops changing once the daily API load took over, so pin the
# commit the files were last updated at rather than following master.
# pragma: allowlist nextline secret
AD_OPT_V2_REF = "e2da8e23047e22bf693aef40e6043fe7af599b28"
GITHUB_APP_VAULT_MOUNT = "secret-data"
GITHUB_APP_VAULT_PATH = "pipelines/github-app"

REPORT_FILE = "Change history report.csv"
API_FILE = "change-history-api.jsonl"
_FILE_PATH = re.compile(r"^(?P<course>[^/]+)/data/(?P<name>[^/]+)$")

REPORT_TABLE = "raw__google_ads__github__change_history_report"
API_TABLE = "raw__google_ads__github__change_history_api"

REPORT_COLUMNS = ("Date & time", "User", "Campaign", "Ad group", "Changes")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
MASKED_EMAIL = "<email>"

# (path in the repo, content) of every change history file.
Files = Callable[[], Iterator[tuple[str, bytes]]]


class ChangeHistoryReportHeaderError(Exception):
    """A change history report's header is not ``REPORT_COLUMNS``."""


def mask_emails(text: str) -> str:
    """Replace every email address in ``text`` with ``MASKED_EMAIL``."""
    return _EMAIL.sub(MASKED_EMAIL, text)


def read_report(content: bytes, path: str) -> Iterator[dict[str, Any]]:
    """Yield one row per change in a UI change history report, without ``User``."""
    rows = csv.reader(io.StringIO(content.decode("utf-16")), delimiter="\t")
    for _ in range(2):
        next(rows)  # the report's title and date range
    header = tuple(next(rows))
    if header != REPORT_COLUMNS:
        msg = f"{path} has the header {list(header)}, not {list(REPORT_COLUMNS)}"
        raise ChangeHistoryReportHeaderError(msg)
    for row_number, (date_time, _user, campaign, ad_group, changes) in enumerate(
        rows, start=1
    ):
        yield {
            "row_number": row_number,
            "date_time": date_time,
            "campaign": campaign,
            "ad_group": ad_group,
            "changes": mask_emails(changes),
        }


def read_api_changes(content: bytes, _path: str) -> Iterator[dict[str, Any]]:
    """Yield one row per change group in ad_opt_v2's change-history-api.jsonl."""
    lines = (line for line in content.decode("utf-8").splitlines() if line.strip())
    for line_number, line in enumerate(lines, start=1):
        group = json.loads(line)
        yield {
            "line_number": line_number,
            "timestamp": group["timestamp"],
            "campaign": group["campaign"],
            "ad_group": group["ad_group"],
            "header": mask_emails(group["header"]),
            "details": json.dumps([mask_emails(d) for d in group["details"]]),
        }


def github_files(repo: Any, ref: str = AD_OPT_V2_REF) -> Iterator[tuple[str, bytes]]:  # noqa: ANN401
    """Yield every change history file in ``repo`` at ``ref``.

    Read as blobs: the contents API omits the body of files over 1 MB.
    """
    for element in repo.get_git_tree(ref, recursive=True).tree:
        match = _FILE_PATH.match(element.path)
        if (
            element.type == "blob"
            and match
            and match["name"] in (REPORT_FILE, API_FILE)
        ):
            yield element.path, base64.b64decode(repo.get_git_blob(element.sha).content)


def local_files(root: Path) -> Iterator[tuple[str, bytes]]:
    """Yield every change history file in a local ad_opt_v2 checkout."""
    for name in (REPORT_FILE, API_FILE):
        for file in sorted(root.glob(f"*/data/{name}")):
            yield file.relative_to(root).as_posix(), file.read_bytes()


def default_files() -> Iterator[tuple[str, bytes]]:
    """Read the files from GitHub when deployed, else from AD_OPT_V2_PATH."""
    if config.active_profile() not in config.DEPLOYED_PROFILES:
        root = config.require_secrets(
            AD_OPT_V2_PATH=config.resolve_secret(None, "AD_OPT_V2_PATH")
        )["AD_OPT_V2_PATH"]
        yield from local_files(Path(root).expanduser())
        return
    secret = vault.read_kv_secret(GITHUB_APP_VAULT_MOUNT, GITHUB_APP_VAULT_PATH)
    auth = Auth.AppAuth(secret["app_id"], secret["private_key"])
    client = Github(
        auth=auth.get_installation_auth(
            int(secret["installation_id"]), token_permissions={"contents": "read"}
        )
    )
    yield from github_files(client.get_repo(AD_OPT_V2_REPO))


@dlt.source(name="google_ads_history")
def google_ads_history_source(
    files: Files = default_files,
    table_format: config.TableFormat | None = None,
) -> Generator[Any]:
    """Load ad_opt_v2's change history files.

    Args:
        files: Returns ``(path, content)`` for each file; GitHub or a local
            checkout by default (see ``default_files``).
        table_format: ``native`` or ``iceberg``; defaults to the active profile's.
    """
    resolved_format = table_format or config.active_table_format()
    # Both tables read the same 24 files, so fetch them from GitHub once.
    all_files = functools.cache(lambda: list(files()))

    def _table(name: str, file_name: str, read: Any, key: str) -> Any:  # noqa: ANN401
        @dlt.resource(
            name=name,
            write_disposition="replace",
            primary_key=["course", key],
            table_format=resolved_format,
            columns={
                **config.DLT_LOAD_ID_COLUMN,
                "_source_file": {"data_type": "text"},
            },
            schema_contract=config.JSON_API_SCHEMA_CONTRACT,
        )
        def rows() -> Iterator[dict[str, Any]]:
            for path, content in all_files():
                match = _FILE_PATH.match(path)
                if not match or match["name"] != file_name:
                    continue
                count = 0
                for row in read(content, path):
                    count += 1
                    yield {
                        "course": match["course"],
                        **row,
                        "_source_file": (
                            f"https://github.com/{AD_OPT_V2_REPO}/blob/"
                            f"{AD_OPT_V2_REF}/{path}"
                        ),
                    }
                logger.info("%s: %d rows", path, count)

        return rows

    yield _table(REPORT_TABLE, REPORT_FILE, read_report, "row_number")
    yield _table(API_TABLE, API_FILE, read_api_changes, "line_number")


google_ads_history_pipeline = config.pipeline_for(
    "google_ads", pipeline_name="google_ads_history"
)


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the Dagster wrapper."""
    return google_ads_history_source()
