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
    AssetsDefinition,
    Config,
    DagsterEventType,
    DataVersion,
    Definitions,
    MaterializeResult,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from dagster_dlt import DagsterDltResource, dlt_assets
from ol_dlt.sources import (
    course_xml_blocks,
    edxorg_program_reports,
    edxorg_s3,
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
from pydantic import Field

from data_loading.defs.ingestion.translators import (
    EdxorgDltTranslator,
    RawDataDltTranslator,
)


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


def _previous_data_version(
    context: AssetExecutionContext, asset_key: AssetKey
) -> DataVersion | None:
    """Return the data version the asset's latest materialization recorded.

    None when an earlier attempt could have loaded rows that no
    materialization reports: the run before this one planned the asset and did
    not materialize it, or this step already started once in this run (a step
    retry, or the daemon resuming a run whose pod died). Each batch commits its
    own cursor, so this attempt starts past those rows and reads as empty.
    """
    step_key = context.get_step_execution_context().step.key
    step_starts = context.instance.get_records_for_run(
        context.run.run_id,
        of_type={DagsterEventType.STEP_START, DagsterEventType.STEP_RESTARTED},
    ).records
    if sum(record.event_log_entry.step_key == step_key for record in step_starts) > 1:
        return None
    event = context.instance.get_latest_materialization_event(asset_key)
    if event is None or event.asset_materialization is None:
        return None
    planned_runs = [
        record.event_log_entry.run_id
        for record in context.instance.fetch_planned_materializations(
            asset_key, limit=2
        ).records
        if record.event_log_entry.run_id != context.run.run_id
    ]
    if planned_runs and planned_runs[0] != event.run_id:
        return None
    value = event.asset_materialization.tags.get(DATA_VERSION_TAG)
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
                else _previous_data_version(context, result.asset_key)
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
# into the landing zone. Loads beside the Airbyte-loaded
# raw__edxorg__program_learner_report until staging is repointed.
edxorg_program_reports_assets = build_batched_assets(
    name="edxorg_program_learner_report",
    build_source=edxorg_program_reports.edxorg_program_reports_source,
    pipeline=edxorg_program_reports.edxorg_program_reports_pipeline(),
    translator=RawDataDltTranslator(),
)


# The xPro tracking log, loaded in full beside the Airbyte-loaded
# raw__xpro__openedx__tracking_logs until staging is repointed at it. One
# listing per run: a full listing of the production bucket (264,950 files on
# 2026-10-27) takes about 50 s, and the first runs walk ~331 batches.
XPRO_TRACKING_LOGS_TABLE = "raw__xpro__openedx__tracking_log_events"
xpro_tracking_logs_assets = build_batched_assets(
    name="xpro_tracking_logs",
    build_source=partial(
        tracking_logs.tracking_logs_source, raw_table=XPRO_TRACKING_LOGS_TABLE
    ),
    pipeline=tracking_logs.tracking_logs_pipeline_for(XPRO_TRACKING_LOGS_TABLE),
    translator=RawDataDltTranslator(),
    list_once=True,
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


class OcwContentLoadConfig(Config):
    """Run config for the OCW content load."""

    courses: list[str] | None = Field(
        default=None,
        description=(
            "Course slugs to load on their own, each only if it changed. "
            "Sweeps every course when unset."
        ),
    )


# One load covers at most ocw_content.BUDGET_BYTES of course files, and each
# load saves which courses it finished, so a run cut off mid-backlog resumes
# there. The first run extracts every course (~70,000 files through Tika);
# later runs read only the courses whose objects changed.
#
# Run config `courses` scopes the run to those courses, read only if changed and
# drained a budget at a time the same way. That is how
# ocw_content_publish_sensor loads a course soon after it is published, and how
# a named set is loaded from the launchpad:
#   ops: {ocw_content_ingest: {config: {courses: [<slug>, ...]}}}
@dlt_assets(
    dlt_source=ocw_content.build_source(),
    dlt_pipeline=ocw_content.ocw_content_pipeline,
    name="ocw_content_ingest",
    dagster_dlt_translator=RawDataDltTranslator(),
)
def ocw_content_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    config: OcwContentLoadConfig,
) -> Iterable[Any]:
    """Sweep the OCW live bucket, or load the courses named in the run config."""
    if config.courses is not None:
        context.log.info(
            "Loading %s named OCW courses: %s", len(config.courses), config.courses
        )
    yield from drain(
        context=context,
        dlt=dlt,
        build_source=partial(ocw_content.build_source, config.courses),
        pipeline=ocw_content.ocw_content_pipeline,
    )


defs = Definitions(
    assets=with_failure_hooks(
        [
            oll_assets,
            mitpe_assets,
            mit_climate_assets,
            mit_edx_programs_assets,
            podcast_rss_assets,
            keycloak_assets,
            *([mitxonline_app_assets] if mitxonline_app_assets else []),
            *([xpro_app_assets] if xpro_app_assets else []),
            youtube_assets,
            see_assets,
            openlearning_assets,
            medium_assets,
            posthog_events_assets,
            ocw_content_assets,
            *edxorg_s3_table_assets,
            *course_xml_blocks_assets,
            edxorg_program_reports_assets,
            xpro_tracking_logs_assets,
        ]
    ),
)
