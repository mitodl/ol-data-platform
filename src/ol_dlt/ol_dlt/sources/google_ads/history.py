"""Google Ads change history from ad_opt_v2's files, loaded once.

The API's ``change_event`` keeps 30 days, so the history before
``raw__google_ads__api__change_events`` began (2026-10-14) exists only in two
files per course in mitodl/ad_opt_v2 (``<course>/data/``), copied as they are
into the landing zone:

    thirdparty/google_ads/change_history/<course>/Change history report.csv
        -> raw__google_ads__s3__change_history_report
    thirdparty/google_ads/change_history/<course>/change-history-api.jsonl
        -> raw__google_ads__s3__change_history_api

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
"""

import csv
import functools
import io
import json
import logging
import re
from collections.abc import Callable, Generator, Iterator
from typing import Any

import dlt
import s3fs

from ol_dlt import config

logger = logging.getLogger(__name__)

# Always the production landing zone, whatever the profile: it is where the
# files are read from, not where rows are written to.
LANDING_ZONE = (
    "ol-data-lake-landing-zone-production/thirdparty/google_ads/change_history"
)
REPORT_FILE = "Change history report.csv"
API_FILE = "change-history-api.jsonl"

REPORT_TABLE = "raw__google_ads__s3__change_history_report"
API_TABLE = "raw__google_ads__s3__change_history_api"

REPORT_COLUMNS = ("Date & time", "User", "Campaign", "Ad group", "Changes")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
MASKED_EMAIL = "<email>"

# (course, file name, source URL, content) of every change history file.
Files = Callable[[], Iterator[tuple[str, str, str, bytes]]]


class ChangeHistoryReportHeaderError(Exception):
    """A change history report's header is not ``REPORT_COLUMNS``."""


def mask_emails(text: str) -> str:
    """Replace every email address in ``text`` with ``MASKED_EMAIL``."""
    return _EMAIL.sub(MASKED_EMAIL, text)


def read_report(content: bytes, url: str) -> Iterator[dict[str, Any]]:
    """Yield one row per change in a UI change history report, without ``User``."""
    rows = csv.reader(io.StringIO(content.decode("utf-16")), delimiter="\t")
    for _ in range(2):
        next(rows)  # the report's title and date range
    header = tuple(next(rows))
    if header != REPORT_COLUMNS:
        msg = f"{url} has the header {list(header)}, not {list(REPORT_COLUMNS)}"
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


def read_api_changes(content: bytes, _url: str) -> Iterator[dict[str, Any]]:
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


def landing_zone_files(
    fs: Any = None,  # noqa: ANN401
    root: str = LANDING_ZONE,
) -> Iterator[tuple[str, str, str, bytes]]:
    """Yield every change history file under ``root``, one folder per course.

    Args:
        fs: An fsspec filesystem; defaults to ``s3fs``, which refreshes IRSA
            credentials itself (see ol_dlt.sources.edxorg_s3).
        root: The folder holding one subfolder per course.
    """
    fs = fs or s3fs.S3FileSystem()
    for name in (REPORT_FILE, API_FILE):
        for path in sorted(fs.glob(f"{root}/*/{name}")):
            course = path.rstrip("/").split("/")[-2]
            yield course, name, f"s3://{path}", fs.cat_file(path)


@dlt.source(name="google_ads_history")
def google_ads_history_source(
    files: Files = landing_zone_files,
    table_format: config.TableFormat | None = None,
) -> Generator[Any]:
    """Load ad_opt_v2's change history files.

    Args:
        files: Returns ``(course, file name, source URL, content)`` for each
            file; the landing zone by default.
        table_format: ``native`` or ``iceberg``; defaults to the active profile's.
    """
    resolved_format = table_format or config.active_table_format()
    # Both tables read from the same listing, so read the files once.
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
            for course, found, url, content in all_files():
                if found != file_name:
                    continue
                count = 0
                for row in read(content, url):
                    count += 1
                    yield {"course": course, **row, "_source_file": url}
                logger.info("%s: %d rows", url, count)

        return rows

    yield _table(REPORT_TABLE, REPORT_FILE, read_report, "row_number")
    yield _table(API_TABLE, API_FILE, read_api_changes, "line_number")


google_ads_history_pipeline = config.pipeline_for(
    "google_ads", pipeline_name="google_ads_history"
)


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the Dagster wrapper."""
    return google_ads_history_source()
