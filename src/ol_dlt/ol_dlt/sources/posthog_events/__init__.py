"""PostHog event export ingestion via dlt.

PostHog batch-exports every raw event for the MIT Learn project to the
production landing zone, one Parquet object per closed hour. This source lands
those events unmodified in ``raw__posthog__learn__s3__events``; dbt unpacks the
JSON property blobs downstream.

S3 layout (verified 2026-09-04 against the live bucket)::

    s3://ol-data-lake-landing-zone-production/thirdparty/posthog/learn/events/
        YYYYMMDD/<window-start-iso>-<window-end-iso>.parquet.zst

The objects are plain Parquet with ZSTD *column* compression, not a Parquet
file wrapped in a zstd stream, so pyarrow opens them directly and the ``.zst``
suffix is only a name. Columns: ``uuid``, ``created_at``, ``distinct_id``,
``elements_chain``, ``event``, ``person_id``, ``_inserted_at``, ``timestamp``,
``person_properties``, ``properties``. The last two are JSON *strings* carrying
the full PostHog client context per event; they are preserved verbatim rather
than flattened, because the property set is open-ended.

Incrementality is keyed on the hour window parsed out of the object name, not
on ``LastModified``: the January-March 2025 partitions were backfilled on
2025-09-15, so modification time does not order the export.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.posthog_events
"""

import logging
import re
from collections.abc import Generator, Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from ol_dlt import config

logger = logging.getLogger(__name__)

POSTHOG_LANDING_BUCKET = "ol-data-lake-landing-zone-production"
POSTHOG_EVENTS_PREFIX = "thirdparty/posthog/learn/events"

RESOURCE_NAME = "raw__posthog__learn__s3__events"

# The first day partition PostHog wrote. Only used when a run explicitly asks
# for the whole history; a normal run resumes from the state cursor.
EXPORT_EPOCH = date(2025, 1, 1)

# Rows per Arrow batch handed to dlt. `properties` and `person_properties`
# together run to a few KB per event, so this is the knob that keeps peak RSS
# proportional to a batch rather than to a whole 60-120 MB object.
BATCH_SIZE = 10_000

# Cold start window when nothing has been loaded and no explicit start date was
# given. A first run should produce a usable table without pulling 600+ day
# partitions; the full history is a deliberate `start_date=EXPORT_EPOCH` run.
DEFAULT_COLD_START_DAYS = 7

# Events are immutable and each hour object is read once, so rows are appended.
# A run that fails after committing part of a load package and before dlt
# persists the cursor will re-read the hours it already wrote, which duplicates
# those events rather than losing them. `s3_object_key` is on every row so a
# duplicated hour can be identified and deleted.
WRITE_DISPOSITION = "append"

# New columns land in the raw table; an incompatible type flip on an existing
# column fails the load instead of silently retyping a column dbt casts.
SCHEMA_CONTRACT = config.JSON_API_SCHEMA_CONTRACT

_STATE_KEY = "last_window_end"

_ISO_HOUR = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}"
# The start/end separator is a bare `-`, which also appears inside both ISO
# timestamps, so the two halves are matched positionally rather than split on.
_OBJECT_NAME_RE = re.compile(
    rf"(?P<start>{_ISO_HOUR})-(?P<end>{_ISO_HOUR})\.parquet(\.[A-Za-z0-9]+)?$"
)


class PostHogObjectNameError(ValueError):
    """An export object name did not carry a parseable hour window."""


def parse_hour_window(object_key: str) -> tuple[datetime, datetime]:
    """Return the (start, end) UTC datetimes encoded in an export object name."""
    match = _OBJECT_NAME_RE.search(object_key)
    if not match:
        msg = f"Cannot parse an hour window out of PostHog export key {object_key}"
        raise PostHogObjectNameError(msg)
    return (
        datetime.fromisoformat(match.group("start")).astimezone(UTC),
        datetime.fromisoformat(match.group("end")).astimezone(UTC),
    )


def day_partitions(start: date, end: date) -> Iterator[str]:
    """Yield the ``YYYYMMDD`` partition names from ``start`` to ``end`` inclusive."""
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def list_export_objects(
    filesystem: s3fs.S3FileSystem,
    *,
    after: datetime,
    until: datetime,
    bucket: str = POSTHOG_LANDING_BUCKET,
    prefix: str = POSTHOG_EVENTS_PREFIX,
) -> list[tuple[datetime, str]]:
    """Return ``(window_end, key)`` for export objects with ``after < end <= until``.

    Listing is bounded to the day partitions the window touches instead of
    globbing the whole prefix: a resumed run reads one or two partitions where a
    recursive listing would enumerate every object written since January 2025.

    The scan starts one day before ``after`` and ends one day after ``until``
    because a partition is named for the hour window's *end*, so a window can sit
    in the neighbouring day's directory. Objects are filtered on the parsed
    window afterwards, so the extra listing cannot pull in extra data.
    """
    found: list[tuple[datetime, str]] = []
    for partition in day_partitions(
        (after - timedelta(days=1)).date(), (until + timedelta(days=1)).date()
    ):
        partition_prefix = f"{bucket}/{prefix}/{partition}"
        try:
            keys = filesystem.ls(partition_prefix, detail=False)
        except FileNotFoundError:
            # Days before the export started, and the day after the current one.
            continue
        for key in keys:
            try:
                _, window_end = parse_hour_window(key)
            except PostHogObjectNameError:
                logger.warning("Skipping unrecognised PostHog export object %s", key)
                continue
            if after < window_end <= until:
                found.append((window_end, key))
    found.sort()
    return found


def _read_object(
    filesystem: s3fs.S3FileSystem, key: str, batch_size: int
) -> Iterator[pa.RecordBatch]:
    """Yield Arrow batches from one export object, tagged with its S3 key.

    ``ParquetFile.iter_batches`` reads row group by row group over the open
    fsspec handle, so an object is never held in memory whole. Decompressed, one
    hour is several times its 60-120 MB on-disk size.
    """
    with filesystem.open(key, "rb") as handle:
        parquet_file = pq.ParquetFile(handle)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            yield batch.append_column(
                "s3_object_key",
                pa.array([key] * batch.num_rows, type=pa.string()),
            )


@dlt.source(name="posthog_events_ingest")
def posthog_events_source(
    start_date: date | None = None,
    end_date: date | None = None,
    max_objects: int | None = None,
    bucket: str = POSTHOG_LANDING_BUCKET,
    prefix: str = POSTHOG_EVENTS_PREFIX,
    batch_size: int = BATCH_SIZE,
    table_format: config.TableFormat | None = None,
) -> Generator[Any]:
    """Load the PostHog hourly event export from S3.

    Uses AWS IAM auth automatically (``~/.aws/credentials`` locally, IRSA in
    K8s).

    Args:
        start_date: Ingest hours from this UTC date onward, ignoring the stored
            cursor entirely. This is how a backfill is requested;
            ``EXPORT_EPOCH`` reaches the start of the export. A normal run
            leaves it unset and resumes from the cursor.
        end_date: Stop at the end of this UTC date. Unset means "up to now".
        max_objects: Cap on hour objects read in one run, so a backfill can be
            walked forward in bounded chunks instead of one run that has to
            survive 600 days of history.
        bucket: Landing-zone bucket holding the export.
        prefix: Key prefix of the event export within that bucket.
        batch_size: Rows per Arrow batch (see ``BATCH_SIZE``).
        table_format: ``native`` (parquet) or ``iceberg``; defaults to the active
            profile's table format.
    """
    resolved_format = table_format or config.active_table_format()

    @dlt.resource(
        name=RESOURCE_NAME,
        write_disposition=WRITE_DISPOSITION,
        table_format=resolved_format,
        schema_contract=SCHEMA_CONTRACT,
    )
    def events() -> Generator[pa.RecordBatch]:
        """Yield Arrow batches for every unread hour, oldest first."""
        state = dlt.current.resource_state()
        cursor = state.get(_STATE_KEY)
        after = datetime.fromisoformat(cursor) if cursor else None

        if start_date is not None:
            # An explicit start date is an instruction, not a hint: it wins over
            # the cursor so a backfill can reach behind what is already loaded.
            after = datetime.combine(start_date, time.min, tzinfo=UTC)
        elif after is None:
            after = datetime.now(UTC) - timedelta(days=DEFAULT_COLD_START_DAYS)

        # `end_date` is inclusive, and the hour ending at midnight belongs to the
        # day it covers, so the bound is the following midnight.
        until = (
            datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=UTC)
            if end_date is not None
            else datetime.now(UTC)
        )

        # `s3fs` is constructed without explicit credentials so aiobotocore keeps
        # its own refreshable IRSA chain. Handing dlt's snapshotted static
        # credentials to s3fs strands a long run on an expired STS token.
        filesystem = s3fs.S3FileSystem()

        objects = list_export_objects(
            filesystem, after=after, until=until, bucket=bucket, prefix=prefix
        )
        if max_objects is not None:
            objects = objects[:max_objects]

        logger.info(
            "PostHog export: %d hour object(s) to read after %s", len(objects), after
        )

        for window_end, key in objects:
            logger.info("Reading PostHog export object %s", key)
            yield from _read_object(filesystem, key, batch_size)
            # Advance only once an object is fully yielded. dlt persists resource
            # state alongside a completed load, so a failed run leaves the cursor
            # where the last successful one left it.
            state[_STATE_KEY] = window_end.isoformat()

    yield events


posthog_events_pipeline = config.pipeline_for("posthog", pipeline_name="posthog_events")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the Dagster wrapper.

    Takes no environment overrides: a scheduled run always resumes from the
    cursor. Backfills go through ``__main__`` or a direct
    ``posthog_events_source(start_date=...)`` call, so a stray environment
    variable cannot turn every hourly run into a 600-day replay.
    """
    return posthog_events_source()
