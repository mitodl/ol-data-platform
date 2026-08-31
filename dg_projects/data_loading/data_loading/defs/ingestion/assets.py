"""@dlt_assets wrappers that expose ol_dlt pipelines as Dagster assets.

Six simple sources are wrapped uniformly via ``build_ingest_assets``. edxorg_s3
keeps custom asset-building because it adds upstream deps and materializes each
table as its OWN ``@dlt_assets`` op (see the edxorg_s3 section below) so tables
of wildly different sizes can load concurrently instead of one giant table
blocking the rest of the run behind it.
"""

from collections.abc import Iterable
from typing import Any

from dagster import AssetExecutionContext, AssetsDefinition, Definitions
from dagster_dlt import DagsterDltResource, dlt_assets
from ol_dlt.sources import (
    edxorg_s3,
    keycloak,
    mit_climate,
    mit_edx_programs,
    mitpe,
    mitxonline_app,
    oll,
    podcast_rss,
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


# --- edxorg_s3: custom upstream deps + one op per table ---------------------

# Bounds how many tables load concurrently. Dagster's K8sRunLauncher gives each
# RUN its own pod (not each step); with no executor_def override, steps within
# that pod run via the default multiprocess executor -- genuinely separate OS
# processes (safe: dlt's non-thread-safe injectable-context dict is
# process-local, never shared across them), but still sharing that one pod's
# fixed CPU/memory budget. Tune the slot count in the Dagster instance UI.
_EDXORG_S3_POOL = "edxorg_s3"


def _build_edxorg_s3_table_asset(table_name: str) -> AssetsDefinition:
    """Wrap one edxorg_s3 table as its own ``@dlt_assets`` op.

    One op per table (rather than one op looping over every table) lets
    Dagster's step executor run tables concurrently instead of a single huge
    table head-of-line-blocking every smaller table behind it in one
    sequential Python loop. Each table gets its own dlt pipeline_name (see
    ``edxorg_s3_pipeline_for``) so concurrent table loads never share a local
    working directory.
    """
    source = edxorg_s3.edxorg_s3_source(tables=[table_name])
    pipeline = edxorg_s3.edxorg_s3_pipeline_for(table_name)

    @dlt_assets(
        dlt_source=source,
        dlt_pipeline=pipeline,
        name=f"edxorg_s3_{table_name}",
        # group_name is set per-asset by the translator (scoped by source system).
        dagster_dlt_translator=EdxorgDltTranslator(),
        pool=_EDXORG_S3_POOL,
    )
    def _asset(
        context: AssetExecutionContext, dlt: DagsterDltResource
    ) -> Iterable[Any]:
        yield from dlt.run(
            context=context,
            dlt_source=source,
            loader_file_format="parquet",
        )

    return _asset


edxorg_s3_table_assets = [
    _build_edxorg_s3_table_asset(table_name) for table_name in EDXORG_DB_TABLES
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
            *edxorg_s3_table_assets,
        ]
    ),
)
