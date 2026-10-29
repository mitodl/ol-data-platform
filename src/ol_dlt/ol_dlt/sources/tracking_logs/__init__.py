"""Open edX tracking log ingestion via dlt.

Each Open edX deployment ships its tracking log to its own bucket as gzipped
JSON Lines, one prefix per UTC day:

    s3://{mitx,mitxonline,xpro}-production-edxapp-tracking/valid/
        <YYYY-MM-DD>/<file>.{log.gz,json.gz,gz}

One Airbyte source-s3 connection per deployment appends these files into
raw__<deployment>__openedx__tracking_logs. This source appends them into the
tables in ``TABLES``, stamped with the file they came from. Staging dedups
tracking events on their business columns, so the loader's own columns do not
matter to it.

edX.org's tracking logs have no table here. MIT's courses moved off edX.org,
and the Airbyte table raw__edxorg__s3__tracking_logs is frozen as their final
copy (ingestion/inventory/retired.yml).

On 2026-10-27 the prefixes held 473,863 files (124.5 GB) for mitx, 437,607
(118.3 GB) for mitxonline and 264,950 (44.3 GB) for xpro. The gzip ratio
measured across these and the edX.org feed is 13x to 22x.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.tracking_logs <raw_table>
"""

import gzip
import io
import json
import logging
import zlib
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dlt
import pyarrow as pa
import s3fs
from dlt.sources.filesystem import FileItemDict

from ol_dlt import config
from ol_dlt.file_metadata import FILE_METADATA_COLUMNS, add_file_metadata
from ol_dlt.json_text import as_text
from ol_dlt.sources.edxorg_s3 import FileListing, edxorg_files

logger = logging.getLogger(__name__)

# How much gzip one load may cover. dlt's Iceberg writer holds a whole load in
# memory (see ol_dlt.sources.edxorg_s3), so the load step's peak follows the
# batch's JSON bytes. Loading recent mitx and mitxonline files into a local
# Iceberg table peaked at 2.26-2.34 GB for 128 MiB of gzip (1.3-1.5 GB of JSON)
# and 3.93-4.21 GB for 256 MiB (2.5-3.0 GB), about 0.5 GB + 1.3x the JSON. The
# worst gzip ratio measured is 21.6x, where 128 MiB is ~2.9 GB of JSON and
# should peak near 4.3 GB, and 256 MiB would pass the data_loading run pod's
# 8Gi default limit. A run also holds its listing (see FileListing).
BUDGET_BYTES = 128 * 1024**2

BATCH_ROWS = 10_000

# The columns of the Airbyte tables, in their order. Every value is text, as
# Airbyte landed it: event and context are already JSON text in the files, and
# labels, the one list, becomes its JSON text. Older files carry keys outside
# this list (minion_id, roles, and about 90 pipelined_beta_* in 2018 mitx), and
# they are dropped rather than becoming columns. A key a line lacks loads null.
FIELDS = (
    "ip",
    "host",
    "name",
    "page",
    "time",
    "agent",
    "event",
    "labels",
    "context",
    "referer",
    "session",
    "log_file",
    "username",
    "timestamp",
    "event_type",
    "environment",
    "source_type",
    "event_source",
    "accept_language",
)
SCHEMA = pa.schema([pa.field(name, pa.string()) for name in FIELDS])

# A cursor at most this many days old lists one prefix per day from the day
# before the cursor's, and an older one lists the whole bucket. Every file whose
# mtime is after 2023-08-20 sits under the prefix of its mtime's day or the day
# before (measured 2026-10-27 over full listings); the files before that were
# copied in bulk in 2023, long after their days. One daily prefix lists in about
# 0.1 s, against 50 s (xpro) to 89 s (mitx) for the whole bucket.
DAILY_PREFIX_MAX_DAYS = 31


@dataclass(frozen=True)
class TrackingLogTable:
    """One raw table and where its files land.

    :param raw_table: Raw warehouse table name.
    :param bucket_url: Production URL the file globs are relative to.
    :param pipeline_name: dlt pipeline name, which keys the cursor.
    :param qa_bucket_url: The QA deployment's URL, where it has one.
    """

    raw_table: str
    bucket_url: str
    pipeline_name: str
    qa_bucket_url: str | None = None

    def bucket_url_for(self, profile: str) -> str:
        """Return the URL a profile reads this table's files from.

        QA reads its own deployment's bucket, where one exists, and every other
        profile reads production. QA ingests the Open edX units rather than
        mirroring them (their inventory units are ``qa: ingest``) because QA
        users are not production users.
        """
        if profile == "qa" and self.qa_bucket_url:
            return self.qa_bucket_url
        return self.bucket_url

    def file_globs(
        self, cursor: datetime | None, today: date | None = None
    ) -> list[str]:
        """Return the globs that cover every file the cursor has not passed.

        :param cursor: The latest mtime loaded, or None before the first load.
        :param today: Today's UTC date; defaults to now.
        :returns: One glob per daily prefix from the day before the cursor's,
            or one glob for the whole bucket.
        :rtype: list[str]
        """
        today = today or datetime.now(UTC).date()
        if cursor is None or (today - cursor.date()).days > DAILY_PREFIX_MAX_DAYS:
            return ["**/*.gz"]
        first = cursor.date() - timedelta(days=1)
        return [
            f"{first + timedelta(days=offset):%Y-%m-%d}/*.gz"
            for offset in range((today - first).days + 1)
        ]


def _openedx(deployment: str) -> TrackingLogTable:
    return TrackingLogTable(
        raw_table=f"raw__{deployment}__openedx__tracking_log_events",
        bucket_url=f"s3://{deployment}-production-edxapp-tracking/valid/",
        pipeline_name=f"tracking_logs__{deployment}",
        qa_bucket_url=f"s3://{deployment}-qa-edxapp-tracking/valid/",
    )


TABLES = {
    table.raw_table: table
    for table in (_openedx(deployment) for deployment in ("mitx", "mitxonline", "xpro"))
}


class TrackingLogLineError(ValueError):
    """A tracking log line is not a JSON object."""


class TrackingLogUnreadableError(Exception):
    """A tracking log file could not be decompressed or decoded."""


def _row(line: str) -> dict[str, str | None]:
    """Project one JSON line onto ``FIELDS``, every value as text.

    :raises TrackingLogLineError: The line is not a JSON object.
    """
    try:
        record = json.loads(line)
    except json.JSONDecodeError as error:
        raise TrackingLogLineError(str(error)) from error
    if not isinstance(record, dict):
        msg = f"a JSON {type(record).__name__}, not an object"
        raise TrackingLogLineError(msg)
    return {name: as_text(record.get(name)) for name in FIELDS}


@dlt.transformer(standalone=True)
def read_tracking_logs(
    items: Iterable[FileItemDict],
    batch_rows: int = BATCH_ROWS,
) -> Iterator[pa.Table]:
    """Stream each gzipped JSON Lines file as Arrow tables stamped with its provenance.

    :raises TrackingLogLineError: A line does not parse as a JSON object, named
        by file and line number.
    :raises TrackingLogUnreadableError: A file is not gzip, is truncated, has a
        corrupt compressed stream, or holds invalid UTF-8, named by file. dlt's
        own error would name only the resource.
    """
    for item in items:
        rows: list[dict[str, str | None]] = []
        line_number = 0
        try:
            # Every file is gzip, whatever its name says; dlt's default "auto"
            # decides from the name.
            with (
                item.open(compression="enable") as raw,
                io.TextIOWrapper(raw, encoding="utf-8") as text,
            ):
                for line_number, line in enumerate(text, start=1):
                    if not line.strip():
                        continue
                    try:
                        rows.append(_row(line))
                    except TrackingLogLineError as error:
                        msg = f"{item['file_url']} line {line_number}: {error}"
                        raise TrackingLogLineError(msg) from error
                    if len(rows) == batch_rows:
                        yield _stamp(rows, item)
                        rows = []
        except (UnicodeDecodeError, EOFError, gzip.BadGzipFile, zlib.error) as error:
            msg = f"{item['file_url']} after line {line_number}: {error}"
            raise TrackingLogUnreadableError(msg) from error
        if rows:
            yield _stamp(rows, item)


def _stamp(rows: list[dict[str, str | None]], item: FileItemDict) -> pa.Table:
    return add_file_metadata(
        pa.Table.from_pylist(rows, schema=SCHEMA),
        source_file=item["file_url"],
        modified_at=item.get("modification_date"),
    )


@dlt.source(name="tracking_logs")
def tracking_logs_source(
    raw_table: str,
    bucket_url: str | None = None,
    table_format: config.TableFormat | None = None,
    budget_bytes: int = BUDGET_BYTES,
    listing: FileListing | None = None,
) -> Generator[Any]:
    """Load one tracking log table, one gzip budget at a time.

    The caller re-runs the source until a batch reads nothing, as for edxorg_s3,
    and passes every batch of a run the same ``listing`` so the run lists the
    bucket once.

    :param raw_table: A key of ``TABLES``.
    :param bucket_url: Overrides the table's ``bucket_url_for`` the active
        profile.
    :param table_format: ``native`` or ``iceberg``; defaults to the active
        profile's.
    :param budget_bytes: How much gzip one load may cover.
    :param listing: Shared by the batches of one run; None lists every batch.
    """
    table = TABLES[raw_table]
    # No explicit credentials, so s3fs refreshes IRSA credentials itself (see
    # ol_dlt.sources.edxorg_s3 for why dlt's own credentials expire mid-run).
    files = edxorg_files(
        bucket_url=bucket_url or table.bucket_url_for(config.active_profile()),
        file_globs=table.file_globs,
        credentials=s3fs.S3FileSystem(),
        budget_bytes=budget_bytes,
        listing=listing,
    )
    yield (
        (files | read_tracking_logs())
        .with_name(table.raw_table)
        .apply_hints(
            table_name=table.raw_table,
            # Append: a file is read once, and its events never change.
            write_disposition="append",
            table_format=table_format or config.active_table_format(),
            columns={**config.DLT_LOAD_ID_COLUMN, **FILE_METADATA_COLUMNS},
        )
    )


def tracking_logs_pipeline_for(raw_table: str) -> dlt.Pipeline:
    """Return the pipeline for one table.

    The pipeline name keys the modification_date cursor, so it must stay
    stable: renaming it re-reads the bucket and, under append, inserts every
    row again.
    """
    table = TABLES[raw_table]
    return config.pipeline_for("openedx", pipeline_name=table.pipeline_name)
