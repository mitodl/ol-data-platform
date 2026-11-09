"""Load an OCW course soon after it is published, not at the next daily sweep.

ocw-studio publishes a course by syncing its Hugo build into the public
``ocw-content-live-production`` bucket, ``courses/<slug>/data.json`` included.
The sensor reads when each course's ``data.json`` was last written
(``ocw_content.published_at``) and requests one partition of the course load per
course written since its last tick. The run key names the digest of the course's
objects, so a publish that rewrote identical bytes costs one listing of the
course. A course that goes back to bytes it had before is not requested again
either; the sweep reads it. The daily sweep stays the backstop, and is the only
load that unpublishes a course.

Why a sensor on the bucket and not ocw-studio's publish webhook: see
docs/design/contentfile_scoped_pull_contract.md, section 8.
"""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import dagster as dg
import s3fs
from ol_dlt.sources import ocw_content

from data_loading.defs.ingestion.assets import (
    OCW_COURSE_SLUGS_NAME,
    ocw_course_load_assets,
)
from data_loading.defs.ingestion.schedules import (
    OCW_CONTENT_RUN_TAGS,
    OCW_CONTENT_SENSOR_NAME,
    ocw_content_run_in_flight,
)

# A tick costs one HEAD per course (2,603 prefixes on 2026-10-19), so polling
# every five minutes is ~750,000 HEAD requests a day against the bucket.
POLL_INTERVAL_SECONDS = 5 * 60

# A course is loaded once its data.json is this old, so the load does not
# start while the rest of the publish is still landing. The last object of an
# online build landed at most 3 s after data.json for the 39 courses published
# 2026-10-16 to 2026-10-19.
SETTLE_SECONDS = 2 * 60

# More courses than this written per poll interval is a mass rebuild (138
# courses per five minutes at the median during the 2026-10-14 one; the busiest
# day of single publishes since had 30). Those rarely change what the source
# reads, so they are left to the sweep, which checks every course anyway. Per
# interval rather than per tick: a tick that waited an hour on a running load
# covers twelve intervals of ordinary publishes.
MAX_COURSES_PER_POLL = 50
# However long the window, since each course is a run and a listing in this tick.
# Runs go one at a time and hold the sweep back until they finish.
MAX_COURSES_PER_TICK = 100


def courses_published_between(
    published: Mapping[str, datetime], since: datetime, until: datetime
) -> list[str]:
    """Return the slugs whose ``data.json`` was written in ``(since, until]``.

    :param published: When each course's ``data.json`` was last written.
    :param since: The end of the window the previous tick covered.
    :param until: The newest write old enough to load.
    :returns: The slugs, sorted.
    """
    return sorted(slug for slug, at in published.items() if since < at <= until)


def course_digest(fs: s3fs.S3FileSystem, slug: str) -> str:
    """Return the version of a course as its objects stand now."""
    objects = ocw_content.list_course(fs, ocw_content.BUCKET, slug)
    return ocw_content.course_version(slug, objects)


@dg.sensor(
    name=OCW_CONTENT_SENSOR_NAME,
    target=dg.AssetSelection.assets(ocw_course_load_assets),
    minimum_interval_seconds=POLL_INTERVAL_SECONDS,
    # Stopped everywhere until the first scoped loads have been watched in
    # production. The source always reads the production OCW bucket, as the
    # daily schedule does.
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Loads the OCW courses published since the last tick into "
        "raw__ocw__s3__course_content, ahead of the daily sweep."
    ),
)
def ocw_content_publish_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    """Request a partition of each course published since the last tick.

    Every course in the bucket gets a partition, so any of them can be retried
    or backfilled from the UI. The cursor is the end of the window the last tick
    covered. It does not move while a load is in flight, so the courses published
    meanwhile are requested once it ends; the load skips any the sweep already
    read. It does move once a run is requested, so the courses of a run that
    fails wait for the next sweep.
    """
    until = datetime.now(tz=UTC) - timedelta(seconds=SETTLE_SECONDS)
    if context.cursor is None:
        return dg.SensorResult(
            skip_reason=f"Watching for courses published after {until.isoformat()}.",
            cursor=until.isoformat(),
        )
    if ocw_content_run_in_flight(context.instance):
        return dg.SensorResult(
            skip_reason="An OCW content load is still running; waiting for it."
        )

    since = datetime.fromisoformat(context.cursor)
    fs = s3fs.S3FileSystem(anon=True, use_listings_cache=False)
    published = ocw_content.published_at(fs, ocw_content.BUCKET)
    new_courses = sorted(
        set(published)
        - set(context.instance.get_dynamic_partitions(OCW_COURSE_SLUGS_NAME))
    )
    new_partitions = (
        [dg.AddDynamicPartitionsRequest(OCW_COURSE_SLUGS_NAME, new_courses)]
        if new_courses
        else []
    )
    courses = courses_published_between(published, since, until)
    cursor = until.isoformat()
    if not courses:
        return dg.SensorResult(
            skip_reason=f"No course published between {since} and {until}.",
            cursor=cursor,
            dynamic_partitions_requests=new_partitions,
        )
    intervals = round((until - since) / timedelta(seconds=POLL_INTERVAL_SECONDS))
    limit = min(MAX_COURSES_PER_POLL * max(intervals, 1), MAX_COURSES_PER_TICK)
    if len(courses) > limit:
        context.log.warning(
            "%s courses were written between %s and %s, more than publishes "
            "write; leaving them to the daily sweep.",
            len(courses),
            since,
            until,
        )
        return dg.SensorResult(
            skip_reason=(
                f"{len(courses)} courses written at once is a rebuild; "
                "the daily sweep reads them."
            ),
            cursor=cursor,
            dynamic_partitions_requests=new_partitions,
        )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                partition_key=slug,
                run_key=f"{slug}:{course_digest(fs, slug)}",
                tags=OCW_CONTENT_RUN_TAGS,
            )
            for slug in courses
        ],
        cursor=cursor,
        dynamic_partitions_requests=new_partitions,
    )


defs = dg.Definitions(sensors=[ocw_content_publish_sensor])
