"""@dlt_assets wrappers that expose ol_dlt pipelines as Dagster assets.

Six simple sources are wrapped uniformly via ``build_ingest_assets``. edxorg_s3
keeps custom asset-building because it adds upstream deps and materializes each
table as its OWN ``@dlt_assets`` op (see the edxorg_s3 section below) so tables
of wildly different sizes can load concurrently instead of one giant table
blocking the rest of the run behind it.
"""

from collections.abc import Callable, Iterable, Iterator
from functools import partial
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetRecordsFilter,
    AssetsDefinition,
    BackfillPolicy,
    DagsterEventType,
    DataVersion,
    Definitions,
    DynamicPartitionsDefinition,
    MaterializeResult,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from ol_dlt.sources import (
    canvas_content,
    course_xml_blocks,
    edxorg_program_reports,
    edxorg_s3,
    google_ads,
    keycloak,
    medium,
    mit_climate,
    mit_edx_programs,
    mitpe,
    mitxonline_app,
    ocw_content,
    oll,
    openlearning,
    podcast_rss,
    posthog_events,
    see,
    tracking_logs,
    xpro_app,
    youtube,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, EDXORG_DB_TABLES
from ol_orchestrate.lib.failures import with_failure_hooks

from data_loading.defs.ingestion.translators import (
    EdxorgDltTranslator,
    RawDataDltTranslator,
)
from ol_dlt import keyset


def build_ingest_assets(
    *,
    name: str,
    source: object,
    pipeline: object,
    translator: RawDataDltTranslator | None = None,
) -> AssetsDefinition:
    """Wrap an ol_dlt source/pipeline as a single @dlt_assets definition."""

    @dlt_assets(
        dlt_source=source,
        dlt_pipeline=pipeline,
        name=name,
        # group_name is set per-asset by the translator (scoped by source system).
        dagster_dlt_translator=translator or RawDataDltTranslator(),
    )
    def _assets(
        context: AssetExecutionContext, dlt: DagsterDltResource
    ) -> Iterable[Any]:
        yield from dlt.run(context=context, dlt_source=source)

    return _assets


oll_assets = build_ingest_assets(
    name="oll_ingest", source=oll.build_source(), pipeline=oll.oll_pipeline
)
mitpe_assets = build_ingest_assets(
    name="mitpe_ingest", source=mitpe.build_source(), pipeline=mitpe.mitpe_pipeline
)
mit_climate_assets = build_ingest_assets(
    name="mit_climate_ingest",
    source=mit_climate.build_source(),
    pipeline=mit_climate.mit_climate_pipeline,
)
mit_edx_programs_assets = build_ingest_assets(
    name="mit_edx_programs_ingest",
    source=mit_edx_programs.build_source(),
    pipeline=mit_edx_programs.mit_edx_programs_pipeline,
)
podcast_rss_assets = build_ingest_assets(
    name="podcast_rss_ingest",
    source=podcast_rss.build_source(),
    pipeline=podcast_rss.podcast_rss_pipeline,
)
google_ads_assets = build_ingest_assets(
    name="google_ads_ingest",
    source=google_ads.build_source(),
    pipeline=google_ads.google_ads_pipeline,
)
keycloak_assets = build_ingest_assets(
    name="keycloak_ingest",
    source=keycloak.build_source(),
    pipeline=keycloak.keycloak_pipeline,
)
# Environments where dlt owns the MITx Online app-database load.
#
# Production is deliberately absent, and it is not a preference. The Airbyte
# connection "MITx Online Production App DB → S3 Data Lake" still loads that
# unit there, and the lakehouse code location builds one asset per stream keyed
# ol_warehouse_raw_data/raw__mitxonline__app__postgres__<table> -- byte-for-byte
# the keys these dlt assets produce (definitions.py:182, and dagster_airbyte
# keys on stream_prefix + stream_name). Registering both is a duplicate asset
# key across two code locations, and two loaders writing one Iceberg table.
#
# QA has nothing to collide with: per the 2026-08-28 Airbyte snapshot its
# connection is named "MITx Online QA Application DB → OL S3 Glue Data Lake -
# QA", which the lakehouse selector (endswith "s3 data lake") drops, and it
# still points at the legacy Glue destination that was never migrated to
# Iceberg. That is why QA raw for this unit is frozen at 2025-01-19 despite the
# connection being enabled, and it is what RFC 12711 step 8 exists to fix.
#
# Add "production" here in the SAME change that disables the Airbyte connection
# and flips the inventory unit to `loader: dlt`. Never before.
MITXONLINE_APP_DLT_ENVIRONMENTS = frozenset({"dev", "ci", "qa"})

mitxonline_app_assets = (
    build_ingest_assets(
        name="mitxonline_app_ingest",
        source=mitxonline_app.build_source(),
        pipeline=mitxonline_app.mitxonline_app_pipeline,
    )
    if DAGSTER_ENV in MITXONLINE_APP_DLT_ENVIRONMENTS
    else None
)
# Environments where dlt owns the xPro app-database load. Production is absent
# for the reason MITXONLINE_APP_DLT_ENVIRONMENTS gives: the Airbyte connection
# "xPro Production App DB → S3 Data Lake" still loads the unit there under the
# same asset keys.
#
# QA is different from MITx Online in one respect. QA_DATA_TOPOLOGY_SPEC.md §7
# measured the QA xPro connection on 2026-09-17 as active on the Iceberg
# destination with no jobs, so the lakehouse selector (endswith "s3 data lake")
# may match it. That connection has to be deleted in QA Airbyte before this
# deploys there, or the lakehouse code location claims the same keys.
#
# Add "production" in the SAME change that disables the Airbyte connection and
# flips the inventory unit to `loader: dlt`.
XPRO_APP_DLT_ENVIRONMENTS = frozenset({"dev", "ci", "qa"})

xpro_app_assets = (
    build_ingest_assets(
        name="xpro_app_ingest",
        source=xpro_app.build_source(),
        pipeline=xpro_app.xpro_app_pipeline,
    )
    if DAGSTER_ENV in XPRO_APP_DLT_ENVIRONMENTS
    else None
)
# Keys-only reads of the MITx Online app database (ol_dlt.keyset), for the
# history snapshots and staging to tell deleted keys from live ones. Run by hand
# for now: the run after every load comes with the keyset automation (witan task
# tk-run-each-unit-s-keysets-after-every-load-and-sta-4caeb1).
#
# Production is absent because its keysets read the RDS read replica, and the
# MITx Online stack does not export the replica's address yet
# (tk-export-production-read-replica-addresses-for-the-d2d989).
# MITXONLINE_APP_DB_HOST in production is the primary.
MITXONLINE_APP_KEYSET_ENVIRONMENTS = frozenset({"dev", "ci", "qa"})
MITXONLINE_APP_KEYSET_UNIT = "mitxonline/app_postgres"

mitxonline_app_keyset_assets = (
    build_ingest_assets(
        name="mitxonline_app_keyset",
        source=keyset.build_unit_keyset_source(MITXONLINE_APP_KEYSET_UNIT),
        pipeline=keyset.unit_keyset_pipeline(MITXONLINE_APP_KEYSET_UNIT),
    )
    if DAGSTER_ENV in MITXONLINE_APP_KEYSET_ENVIRONMENTS
    else None
)
see_assets = build_ingest_assets(
    name="see_ingest", source=see.build_source(), pipeline=see.see_pipeline
)
openlearning_assets = build_ingest_assets(
    name="openlearning_ingest",
    source=openlearning.build_source(),
    pipeline=openlearning.openlearning_pipeline,
)
medium_assets = build_ingest_assets(
    name="medium_ingest",
    source=medium.build_source(),
    pipeline=medium.medium_pipeline,
)
youtube_assets = build_ingest_assets(
    name="youtube_ingest",
    source=youtube.build_source(),
    pipeline=youtube.youtube_pipeline,
)


# --- edxorg_s3: custom upstream deps + one op per table ---------------------

# Bounds how many tables load concurrently. Dagster's K8sRunLauncher gives each
# RUN its own pod (not each step); with no executor_def override, steps within
# that pod run via the default multiprocess executor -- genuinely separate OS
# processes (safe: dlt's non-thread-safe injectable-context dict is
# process-local, never shared across them), but still sharing that one pod's
# fixed CPU/memory budget. Tune the slot count in the Dagster instance UI.
_EDXORG_S3_POOL = "edxorg_s3"

# Each batch is one dlt load of at most `budget_bytes` of source TSV (see
# ol_dlt.sources.edxorg_s3), so one Dagster run walks the backlog a batch at a
# time instead of holding a whole table's load in memory. The cap keeps a run
# from spinning forever on a source that never drains; the cursor is saved per
# batch, so the next run picks up where this one stopped. 11.1 TB of
# courseware_studentmodule at 4 GiB per batch is ~2,800 batches, so the
# backlog spans many runs by design.
_MAX_BATCHES_PER_RUN = 200


def _normalized_row_count(pipeline: Any, table_name: str) -> int:
    """Rows this pipeline's last load normalized into ``table_name``.

    Zero means the batch found no files the cursor had not already covered,
    which is how the loop learns the backlog is drained. Read from the trace
    rather than the load info because dagster-dlt hands back materializations,
    not the LoadInfo.
    """
    trace = pipeline.last_trace
    if trace is None or trace.last_normalize_info is None:
        return 0
    return trace.last_normalize_info.row_counts.get(table_name, 0)


def load_in_batches(
    *,
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    build_source: Callable[[], Any],
    pipeline: Any,
    resource_name: str,
) -> tuple[list[Any], int, int]:
    """Run one dlt load per byte budget until the table's backlog is drained.

    ``build_source`` returns a budgeted source for the one table. Returns the
    last batch's materializations, how many batches ran, and the rows they
    loaded between them.

    A batch that normalizes zero rows means the cursor already covers every
    file in the landing zone, which is the only stop condition that does not
    need a second listing of the bucket. Each batch commits its own cursor, so
    a pod killed mid-run costs one batch rather than the run.
    """
    results: list[Any] = []
    rows_loaded = 0
    batches = 0

    for batch in range(1, _MAX_BATCHES_PER_RUN + 1):
        # A fresh source per batch: a DltSource's resources are generators,
        # spent once the batch that consumed them ends. Whatever the batches
        # of a run share, such as a listing, build_source closes over (see
        # sources_for_run).
        results = list(
            dlt.run(
                context=context,
                dlt_source=build_source(),
                loader_file_format="parquet",
            )
        )
        batches = batch
        batch_rows = _normalized_row_count(pipeline, resource_name)
        rows_loaded += batch_rows
        context.log.info(
            "Batch %s of %s loaded %s rows (%s total).",
            batch,
            resource_name,
            batch_rows,
            rows_loaded,
        )
        if batch_rows == 0:
            break
    else:
        context.log.warning(
            "%s hit the %s batch cap with rows still loading; the next run "
            "resumes from the saved cursor.",
            resource_name,
            _MAX_BATCHES_PER_RUN,
        )

    return results, batches, rows_loaded


def unreported_attempt(
    context: AssetExecutionContext,
    asset_key: AssetKey,
    partitions: list[str] | None = None,
) -> bool:
    """Tell whether an earlier attempt could have loaded rows nothing reports.

    Either another run planned the asset after its last materialization and has
    not materialized it, or this step already started once in this run (a step
    retry, or the daemon resuming a run whose pod died). Each batch commits its
    own cursor, so this attempt starts past those rows and reads as empty.

    The order of the events decides it, not which run wrote them, because a
    materialization can come from a run that did not plan the asset (the OCW
    course load reports the raw table's).

    :param context: The running step's context.
    :param asset_key: The asset to check.
    :param partitions: The partitions to check. A partition's run says nothing
        about another's, and the sensor queues them side by side.
    """
    step_key = context.get_step_execution_context().step.key
    step_starts = context.instance.get_records_for_run(
        context.run.run_id,
        of_type={DagsterEventType.STEP_START, DagsterEventType.STEP_RESTARTED},
    ).records
    if sum(record.event_log_entry.step_key == step_key for record in step_starts) > 1:
        return True
    records_filter = AssetRecordsFilter(asset_key, asset_partitions=partitions)
    # One planned event per partition of this run, then the one that matters.
    planned = [
        record
        for record in context.instance.fetch_planned_materializations(
            records_filter, limit=len(partitions or [asset_key]) + 1
        ).records
        if record.event_log_entry.run_id != context.run.run_id
    ]
    if not planned:
        return False
    latest = context.instance.fetch_materializations(records_filter, limit=1).records
    return not latest or planned[0].storage_id > latest[0].storage_id


def _previous_data_version(
    context: AssetExecutionContext,
    asset_key: AssetKey,
    partitions: list[str] | None = None,
) -> DataVersion | None:
    """Return the data version the asset's latest materialization recorded.

    None when ``unreported_attempt`` says this load may not be as empty as it
    looks, or when the asset has no materialization to take one from.
    """
    if unreported_attempt(context, asset_key, partitions):
        return None
    records = context.instance.fetch_materializations(
        AssetRecordsFilter(asset_key, asset_partitions=partitions), limit=1
    ).records
    if not records:
        return None
    event = records[0].event_log_entry.asset_materialization
    value = event.tags.get(DATA_VERSION_TAG) if event else None
    return None if value is None else DataVersion(value)


def drain(
    *,
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    build_source: Callable[[], Any],
    pipeline: Any,
) -> Iterator[MaterializeResult[Any]]:
    """Run ``load_in_batches`` and yield one materialization per asset.

    One, not one per batch: Dagster rejects a step that materializes the same
    asset twice. The last batch's metadata describes an empty catch-up load, so
    the counts that describe the whole run are added here.

    A run that loaded no rows keeps the previous data version. Every source
    drained here appends, so the table is unchanged, but these assets have no
    code version and Dagster would otherwise derive a new data version from the
    run id. The lakehouse dbt models rebuild on a parent's data version change,
    so each empty load (e.g. an OCW course republished with identical bytes)
    would rebuild everything below the raw table.
    """
    (resource_name,) = build_source().resources
    partitions = (
        list(context.partition_keys)
        if context.has_partition_key or context.has_partition_key_range
        else None
    )
    results, batches, rows_loaded = load_in_batches(
        context=context,
        dlt=dlt,
        build_source=build_source,
        pipeline=pipeline,
        resource_name=resource_name,
    )
    for result in results:
        yield MaterializeResult(
            asset_key=result.asset_key,
            metadata={
                **dict(result.metadata or {}),
                "batches": batches,
                "rows_loaded": rows_loaded,
            },
            data_version=(
                None
                if rows_loaded
                else _previous_data_version(context, result.asset_key, partitions)
            ),
        )


def sources_for_run(
    build_source: Callable[..., Any], *, list_once: bool
) -> Callable[[], Any]:
    """Return the per-batch source factory for one run.

    With ``list_once``, every batch's source gets the same new
    ``edxorg_s3.FileListing``, so the run lists its files once and carves each
    batch from that listing instead of listing again per batch.
    """
    if list_once:
        return partial(build_source, listing=edxorg_s3.FileListing())
    return build_source


def build_batched_assets(  # noqa: PLR0913
    *,
    name: str,
    build_source: Callable[..., Any],
    pipeline: Any,
    translator: RawDataDltTranslator,
    pool: str | None = None,
    list_once: bool = False,
) -> AssetsDefinition:
    """Wrap a one-table budgeted source as an ``@dlt_assets`` op that drains it.

    ``build_source`` is called once for the asset definition, once per run for
    the table's name and once per batch (see ``drain``). Its single resource
    names the table. With ``list_once`` it also takes a ``listing`` keyword,
    one per run (see ``sources_for_run``).
    """

    @dlt_assets(
        dlt_source=build_source(),
        dlt_pipeline=pipeline,
        name=name,
        # group_name is set per-asset by the translator (scoped by source system).
        dagster_dlt_translator=translator,
        pool=pool,
    )
    def _asset(
        context: AssetExecutionContext, dlt: DagsterDltResource
    ) -> Iterable[Any]:
        yield from drain(
            context=context,
            dlt=dlt,
            build_source=sources_for_run(build_source, list_once=list_once),
            pipeline=pipeline,
        )

    return _asset


def _build_edxorg_s3_table_asset(table_name: str) -> AssetsDefinition:
    """Wrap one edxorg_s3 table as its own ``@dlt_assets`` op.

    One op per table (rather than one op looping over every table) lets
    Dagster's step executor run tables concurrently instead of a single huge
    table head-of-line-blocking every smaller table behind it in one
    sequential Python loop. Each table gets its own dlt pipeline_name (see
    ``edxorg_s3_pipeline_for``) so concurrent table loads never share a local
    working directory.
    """
    return build_batched_assets(
        name=f"edxorg_s3_{table_name}",
        build_source=lambda: edxorg_s3.edxorg_s3_source(tables=[table_name]),
        pipeline=edxorg_s3.edxorg_s3_pipeline_for(table_name),
        translator=EdxorgDltTranslator(),
        pool=_EDXORG_S3_POOL,
    )


edxorg_s3_table_assets = [
    _build_edxorg_s3_table_asset(table_name) for table_name in EDXORG_DB_TABLES
]

# The course archive assets in the edxorg and openedx code locations land one
# JSON Lines file of parsed XML blocks per course version; nothing else loads
# them into raw. The edxorg and openedx structure blocks are the same layout.
# One op per table, like edxorg_s3, so they drain independently.
course_xml_blocks_assets = [
    build_batched_assets(
        # course_xml_blocks_openedx etc. for the block tables, as before.
        name=table.pipeline_name.replace("__", "_"),
        build_source=partial(
            course_xml_blocks.course_xml_blocks_source, raw_table=raw_table
        ),
        pipeline=course_xml_blocks.course_xml_blocks_pipeline_for(raw_table),
        translator=RawDataDltTranslator(),
    )
    for raw_table, table in course_xml_blocks.TABLES.items()
]


# edX.org's daily program learner reports, which the edxorg code location copies
# into the landing zone. Replaced the Airbyte-loaded
# raw__edxorg__program_learner_report (ingestion/inventory/retired.yml).
edxorg_program_reports_assets = build_batched_assets(
    name="edxorg_program_learner_report",
    build_source=edxorg_program_reports.edxorg_program_reports_source,
    pipeline=edxorg_program_reports.edxorg_program_reports_pipeline(),
    translator=RawDataDltTranslator(),
)


# The Open edX tracking logs, one asset per deployment. xPro's replaced the
# Airbyte-loaded raw__xpro__openedx__tracking_logs, which is kept as the
# archive. MITx Online's and MITx's load beside their Airbyte tables until
# staging is repointed. One listing per run: a full listing of a production
# bucket (264,950 to 473,863 files on 2026-10-27) takes 50 to 89 s, and the
# first runs walk ~331 (xpro), ~884 (mitxonline) and ~932 (mitx) batches.
TRACKING_LOGS_DEPLOYMENTS = ("xpro", "mitxonline", "mitx")


def tracking_logs_table(deployment: str) -> str:
    """Return the raw table ol_dlt.sources.tracking_logs loads for a deployment."""
    return f"raw__{deployment}__openedx__tracking_log_events"


tracking_logs_assets = {
    deployment: build_batched_assets(
        name=f"{deployment}_tracking_logs",
        build_source=partial(
            tracking_logs.tracking_logs_source,
            raw_table=tracking_logs_table(deployment),
        ),
        pipeline=tracking_logs.tracking_logs_pipeline_for(
            tracking_logs_table(deployment)
        ),
        translator=RawDataDltTranslator(),
        list_once=True,
    )
    for deployment in TRACKING_LOGS_DEPLOYMENTS
}


# Canvas course files and text for MIT Learn's ContentFiles. One load covers at
# most canvas_content.BUDGET_BYTES of archive members, and the first run extracts
# every course (~9,100 files through Tika).
canvas_content_assets = build_batched_assets(
    name="canvas_content_ingest",
    build_source=canvas_content.build_source,
    pipeline=canvas_content.canvas_content_pipeline,
    translator=RawDataDltTranslator(),
)


# Resumes from the dlt cursor every run. A backfill is a deliberate
# `posthog_events_source(start_date=...)` invocation (see the source's
# __main__), not something a scheduled run can fall into.
#
# Batched because a single load holds all of its data in memory: the 7-day cold
# start (168 hourly objects) as one load OOMKilled every hourly run from
# 2026-09-22 on, so the cursor never moved. Each load now covers at most
# posthog_events.BUDGET_BYTES of export, and the loop stops at the first load
# that reads nothing. An hour object with no rows ends a run's loop early if it
# forms a load of its own; the next tick resumes from the cursor.
posthog_events_assets = build_batched_assets(
    name="posthog_events_ingest",
    build_source=posthog_events.build_source,
    pipeline=posthog_events.posthog_events_pipeline,
    translator=RawDataDltTranslator(),
)


# One load covers at most ocw_content.BUDGET_BYTES of course files, and each
# load saves which courses it finished, so a run cut off mid-backlog resumes
# there. The first run extracts every course (~70,000 files through Tika);
# later runs read only the courses whose objects changed.
@dlt_assets(
    dlt_source=ocw_content.build_source(),
    dlt_pipeline=ocw_content.ocw_content_pipeline,
    name="ocw_content_ingest",
    dagster_dlt_translator=RawDataDltTranslator(),
)
def ocw_content_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
) -> Iterable[Any]:
    """Sweep the OCW live bucket, the only load that unpublishes a course."""
    yield from drain(
        context=context,
        dlt=dlt,
        build_source=ocw_content.build_source,
        pipeline=ocw_content.ocw_content_pipeline,
    )


OCW_COURSE_SLUGS_NAME = "ocw_course_slugs"
ocw_course_slugs = DynamicPartitionsDefinition(name=OCW_COURSE_SLUGS_NAME)
OCW_COURSE_LOAD_KEY = AssetKey(["ocw", "course_content_load"])
OCW_RAW_TABLE_KEY = AssetKey(["ol_warehouse_raw_data", ocw_content.RAW_TABLE])


class OcwCourseLoadTranslator(RawDataDltTranslator):
    """Key the per-course load apart from the table it writes.

    The table's asset stays unpartitioned. With 100 or more upstream partitions
    Dagster drops a dependency from data versioning
    (SKIP_PARTITION_DATA_VERSION_DEPENDENCY_THRESHOLD), and the five OCW models
    below the table rebuild on its data version.
    """

    def get_asset_spec(self, data: DltResourceTranslatorData) -> Any:
        """Return the raw-data spec under the per-course load's key."""
        return super().get_asset_spec(data).replace_attributes(key=OCW_COURSE_LOAD_KEY)


def report_raw_table(
    context: AssetExecutionContext,
    results: Iterable[MaterializeResult],
    courses: list[str],
) -> Iterator[MaterializeResult]:
    """Report the raw table for a course load, which is what its models watch.

    An empty load reports it too when an earlier attempt, of this partition or
    of a sweep, may have committed rows it never reported. Nothing else would
    rebuild the models for them before the next sweep. The report comes first,
    so a worker that dies after the result leaves no rows unreported.

    :param context: The running step's context.
    :param results: The load's results, from ``drain``.
    :param courses: The run's partitions.
    """
    for result in results:
        if (
            result.metadata["rows_loaded"]
            or unreported_attempt(context, OCW_COURSE_LOAD_KEY, courses)
            or unreported_attempt(context, OCW_RAW_TABLE_KEY)
        ):
            context.log_event(
                AssetMaterialization(
                    asset_key=OCW_RAW_TABLE_KEY,
                    metadata={"courses": courses},
                    tags={DATA_VERSION_TAG: context.run_id},
                )
            )
        yield result


# One partition per course: its load history, and a retry or backfill of it from
# the UI. A backfill of a contiguous range is one run, so a re-read of hundreds of
# courses does not start a pod for each. ocw_content_publish_sensor requests these.
# Launch one by hand with the OCW_CONTENT_RUN_TAGS run tag (ocw/content_load: true).
# The run queue's one-at-a-time cap and ocw_content_run_in_flight match on that run
# tag, and an asset's own tags do not become run tags, so an untagged run starts
# beside a sensor's load and the two contend to commit to the raw table.
@dlt_assets(
    dlt_source=ocw_content.build_source(),
    dlt_pipeline=ocw_content.ocw_course_pipeline,
    name="ocw_course_content_load",
    dagster_dlt_translator=OcwCourseLoadTranslator(),
    partitions_def=ocw_course_slugs,
    backfill_policy=BackfillPolicy.single_run(),
)
def ocw_course_load_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
) -> Iterable[Any]:
    """Load the courses of the run's partitions, each only if it changed."""
    courses = list(context.partition_keys)
    context.log.info("Loading %s OCW courses: %s", len(courses), courses)
    yield from report_raw_table(
        context,
        drain(
            context=context,
            dlt=dlt,
            build_source=partial(ocw_content.build_source, courses),
            pipeline=ocw_content.ocw_course_pipeline,
        ),
        courses,
    )


defs = Definitions(
    assets=with_failure_hooks(
        [
            oll_assets,
            mitpe_assets,
            google_ads_assets,
            mit_climate_assets,
            mit_edx_programs_assets,
            podcast_rss_assets,
            keycloak_assets,
            *([mitxonline_app_assets] if mitxonline_app_assets else []),
            *([mitxonline_app_keyset_assets] if mitxonline_app_keyset_assets else []),
            *([xpro_app_assets] if xpro_app_assets else []),
            youtube_assets,
            see_assets,
            openlearning_assets,
            medium_assets,
            posthog_events_assets,
            ocw_content_assets,
            canvas_content_assets,
            ocw_course_load_assets,
            *edxorg_s3_table_assets,
            *course_xml_blocks_assets,
            edxorg_program_reports_assets,
            *tracking_logs_assets.values(),
        ]
    ),
)
