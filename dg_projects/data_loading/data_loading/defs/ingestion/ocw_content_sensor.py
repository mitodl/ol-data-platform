"""Load an OCW course soon after it is published, not at the next daily sweep.

ocw-studio publishes a course by syncing its Hugo build into the public
``ocw-content-live-production`` bucket, ``courses/<slug>/data.json`` included.
The sensor reads when each course's ``data.json`` was last written
(``ocw_content.published_at``) and requests one load scoped to the courses
written since its last tick. The load reads a named course only when the ETags
of its objects changed, so a publish that rewrote identical bytes costs one
listing of the course. The daily sweep stays the backstop, and is the only
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
    OcwContentLoadConfig,
    ocw_content_assets,
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


@dg.sensor(
    name=OCW_CONTENT_SENSOR_NAME,
    target=dg.AssetSelection.assets(ocw_content_assets),
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
    """Request a load of the courses published since the last tick.

    The cursor is the end of the window the last tick covered. It does not
    move while a load is in flight, so the courses published meanwhile are
    requested once it ends; the load skips any the sweep already read. It does
    move once a run is requested, so the courses of a run that fails wait for
    the next sweep.
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
    courses = courses_published_between(
        ocw_content.published_at(fs, ocw_content.BUCKET), since, until
    )
    cursor = until.isoformat()
    if not courses:
        return dg.SensorResult(
            skip_reason=f"No course published between {since} and {until}.",
            cursor=cursor,
        )
    intervals = round((until - since) / timedelta(seconds=POLL_INTERVAL_SECONDS))
    if len(courses) > MAX_COURSES_PER_POLL * max(intervals, 1):
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
        )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"{OCW_CONTENT_SENSOR_NAME}:{cursor}",
                tags=OCW_CONTENT_RUN_TAGS,
                run_config=dg.RunConfig(
                    ops={
                        ocw_content_assets.op.name: OcwContentLoadConfig(
                            courses=courses
                        )
                    }
                ),
            )
        ],
        cursor=cursor,
    )


defs = dg.Definitions(sensors=[ocw_content_publish_sensor])
