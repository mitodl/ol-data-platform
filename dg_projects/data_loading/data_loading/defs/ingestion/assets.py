"""@dlt_assets wrappers that expose ol_dlt pipelines as Dagster assets.

Six simple sources are wrapped uniformly via ``build_ingest_assets``. edxorg_s3
keeps a custom asset because it adds upstream deps and materializes one table per
``dlt.run()`` call (so STS credentials refresh between tables).
"""

from collections.abc import Iterable
from typing import Any

from dagster import AssetExecutionContext, AssetKey, AssetsDefinition, Definitions
from dagster_dlt import DagsterDltResource, dlt_assets
from ol_dlt.sources import (
    edxorg_s3,
    mit_climate,
    mit_edx_programs,
    mitpe,
    oll,
    podcast_rss,
    youtube,
)
from ol_orchestrate.lib.constants import EDXORG_DB_TABLES

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
youtube_assets = build_ingest_assets(
    name="youtube_ingest",
    source=youtube.build_source(),
    pipeline=youtube.youtube_pipeline,
)


# --- edxorg_s3: custom upstream deps + per-table materialization ------------
_EDXORG_PREFIX = "raw__edxorg__s3__tables__"


def _edxorg_asset_key(table_name: str) -> AssetKey:
    return AssetKey(["ol_warehouse_raw_data", f"{_EDXORG_PREFIX}{table_name}"])


# The full-tables instance lets @dlt_assets discover the complete set of asset
# specs at import time; the execution function below overrides the source
# per-table.
@dlt_assets(
    dlt_source=edxorg_s3.edxorg_s3_source(tables=list(EDXORG_DB_TABLES)),
    dlt_pipeline=edxorg_s3.edxorg_s3_pipeline,
    name="edxorg_s3_tables",
    # group_name is set per-asset by the translator (scoped by source system).
    dagster_dlt_translator=EdxorgDltTranslator(),
)
def edxorg_s3_consolidated_tables(
    context: AssetExecutionContext, dlt: DagsterDltResource
) -> Iterable[Any]:
    """Load and consolidate EdX.org database tables from S3.

    Tables are processed sequentially, one per ``dlt.run()`` call, so that STS
    tokens refresh between tables, large tables do not exhaust a single token's
    TTL, and dlt's non-thread-safe injectable-context dict is never accessed
    concurrently (see also [extract] workers=1 in ol_dlt/.dlt/config.toml).
    """
    if context.is_subset:
        selected_tables = [
            t
            for t in EDXORG_DB_TABLES
            if _edxorg_asset_key(t) in context.selected_asset_keys
        ]
    else:
        selected_tables = list(EDXORG_DB_TABLES)

    for table_name in selected_tables:
        table_source = edxorg_s3.edxorg_s3_source(tables=[table_name])
        yield from dlt.run(
            context=context,
            dlt_source=table_source,
            loader_file_format="parquet",
        )


defs = Definitions(
    assets=[
        oll_assets,
        mitpe_assets,
        mit_climate_assets,
        mit_edx_programs_assets,
        podcast_rss_assets,
        youtube_assets,
        edxorg_s3_consolidated_tables,
    ],
)
