"""@dlt_assets wrappers that expose ol_dlt pipelines as Dagster assets.

Six simple sources are wrapped uniformly via ``build_ingest_assets``. edxorg_s3
keeps custom asset-building because it adds upstream deps and materializes each
table as its OWN ``@dlt_assets`` op (see the edxorg_s3 section below) so tables
of wildly different sizes can load concurrently instead of one giant table
blocking the rest of the run behind it.
"""

from collections.abc import Callable, Iterable
from functools import partial
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Definitions,
    MaterializeResult,
)
from dagster_dlt import DagsterDltResource, dlt_assets
from ol_dlt.sources import (
    course_xml_blocks,
    edxorg_s3,
    keycloak,
    mit_climate,
    mit_edx_programs,
    mitpe,
    mitxonline_app,
    oll,
    podcast_rss,
    posthog_events,
    youtube,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, EDXORG_DB_TABLES
from ol_orchestrate.lib.failures import with_failure_hooks

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
youtube_assets = build_ingest_assets(
    name="youtube_ingest",
    source=youtube.build_source(),
    pipeline=youtube.youtube_pipeline,
)
# Resumes from the dlt cursor every run. A backfill is a deliberate
# `posthog_events_source(start_date=...)` invocation (see the source's
# __main__), not something a scheduled run can fall into.
posthog_events_assets = build_ingest_assets(
    name="posthog_events_ingest",
    source=posthog_events.build_source(),
    pipeline=posthog_events.posthog_events_pipeline,
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
        # spent once the batch that consumed them ends.
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


def build_batched_assets(
    *,
    name: str,
    build_source: Callable[[], Any],
    pipeline: Any,
    translator: RawDataDltTranslator,
    pool: str | None = None,
) -> AssetsDefinition:
    """Wrap a one-table budgeted source as an ``@dlt_assets`` op that drains it.

    ``build_source`` is called once for the asset definition and once per
    batch (see ``load_in_batches``). Its single resource names the table.
    """
    source = build_source()
    (resource_name,) = source.resources

    @dlt_assets(
        dlt_source=source,
        dlt_pipeline=pipeline,
        name=name,
        # group_name is set per-asset by the translator (scoped by source system).
        dagster_dlt_translator=translator,
        pool=pool,
    )
    def _asset(
        context: AssetExecutionContext, dlt: DagsterDltResource
    ) -> Iterable[Any]:
        results, batches, rows_loaded = load_in_batches(
            context=context,
            dlt=dlt,
            build_source=build_source,
            pipeline=pipeline,
            resource_name=resource_name,
        )

        # One materialization per asset, not one per batch: Dagster rejects a
        # step that materializes the same asset twice. The last batch's
        # metadata describes an empty catch-up load, so the counts that
        # describe the whole run are added here.
        for result in results:
            yield MaterializeResult(
                asset_key=result.asset_key,
                metadata={
                    **dict(result.metadata or {}),
                    "batches": batches,
                    "rows_loaded": rows_loaded,
                },
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
# them into raw. One op per table, like edxorg_s3, so the two drain
# independently.
course_xml_blocks_assets = [
    build_batched_assets(
        name=f"course_xml_blocks_{table.pipeline_prefix}",
        build_source=partial(
            course_xml_blocks.course_xml_blocks_source, raw_table=raw_table
        ),
        pipeline=course_xml_blocks.course_xml_blocks_pipeline_for(raw_table),
        translator=RawDataDltTranslator(),
    )
    for raw_table, table in course_xml_blocks.TABLES.items()
]


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
            youtube_assets,
            posthog_events_assets,
            *edxorg_s3_table_assets,
            *course_xml_blocks_assets,
        ]
    ),
)
