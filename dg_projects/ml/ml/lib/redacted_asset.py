"""Build an asset that masks PII in a dbt model's text columns via Presidio."""

import contextlib

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Config,
    MetadataValue,
    asset,
)
from ml.lib.redact import (
    REDACT_CHECKPOINT_BATCH_SIZE,
    REDACTION_VERSION,
    filter_unredacted,
    redact_and_checkpoint,
)
from ml.lib.warehouse import warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError


class RedactedAssetConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description=(
            "Re-redact every row not yet redacted with the current rules "
            "(REDACTION_VERSION in ml.lib.redact), not only rows missing from the "
            "table. Each batch is saved as it finishes, so a rerun after a crash "
            "continues where it stopped."
        ),
    )
    batch_size: int = Field(
        default=REDACT_CHECKPOINT_BATCH_SIZE,
        ge=1,
        description="Rows redacted and written together.",
    )
    sample_limit: int | None = Field(
        default=None,
        description="Cap the number of upstream rows read, for fast local testing.",
    )
    keys: dict[str, list[str]] | None = Field(
        default=None,
        description=(
            "Only process rows whose columns match these values, for spot-checking "
            'known cases, e.g. {"source_record_ref": ["123"], "source_slug": '
            '["zendesk"]}. Values are compared as strings.'
        ),
    )


def build_redacted_asset(  # noqa: PLR0913
    *,
    name: str,
    source: AssetKey,
    key_cols: list[str],
    text_cols: list[str],
    code_version: str,
    group_name: str,
    pool: str,
    source_filter: pl.Expr | None = None,
    description: str | None = None,
) -> AssetsDefinition:
    """Build an asset that writes <name> with each text column's PII masked.

    The output has the key columns, one <column>_redacted per text column, and
    redaction_version. A run redacts only rows missing from the output, and writes
    each batch as it finishes.

    Args:
        name: the asset and output table name, in the intermediate schema.
        source: the dbt model's asset key, [<layer>, <model>]; the asset reads the
            model from that layer's warehouse database.
        key_cols: the columns that identify a row.
        text_cols: the columns to redact.
        code_version: the asset's code version.
        group_name: the Dagster asset group.
        pool: the Dagster concurrency pool.
        source_filter: keeps only the source rows to redact.
        description: the asset description.

    Returns:
        AssetsDefinition: the redaction asset.
    """
    source_layer, source_table = source.path
    database_name = warehouse_database("intermediate")

    @asset(
        name=name,
        key_prefix=["intermediate"],
        code_version=code_version,
        group_name=group_name,
        description=description,
        deps=[source],
        automation_condition=upstream_or_code_changes(),
        io_manager_key="io_manager",
        pool=pool,
        metadata={
            "schema": database_name,
            "write_mode": "upsert",
            "upsert_options": {"join_cols": key_cols},
            "schema_update_mode": "update",
        },
    )
    def _redacted(
        context: AssetExecutionContext, config: RedactedAssetConfig
    ) -> pl.DataFrame:
        source_lazy = get_dbt_model_as_dataframe(
            database_name=warehouse_database(source_layer), table_name=source_table
        )
        if source_filter is not None:
            source_lazy = source_lazy.filter(source_filter)
        for column, values in (config.keys or {}).items():
            source_lazy = source_lazy.filter(
                pl.col(column).cast(pl.String).is_in(values)
            )
        if config.sample_limit is not None:
            source_lazy = source_lazy.limit(config.sample_limit)
        source_df = source_lazy.collect()

        already_redacted_df = source_df.select(key_cols).clear()
        with contextlib.suppress(NoSuchTableError):
            existing_lazy = get_dbt_model_as_dataframe(
                database_name=database_name, table_name=name
            )
            if config.full_refresh:
                # Rows from before redaction_version existed have no version, so a
                # full refresh redoes them.
                current = (
                    pl.col("redaction_version") == REDACTION_VERSION
                    if "redaction_version" in existing_lazy.collect_schema().names()
                    else pl.lit(False)  # noqa: FBT003
                )
                existing_lazy = existing_lazy.filter(current)
            already_redacted_df = existing_lazy.select(key_cols).collect()

        unredacted_df = filter_unredacted(source_df, already_redacted_df, key_cols)
        redacted_count = redact_and_checkpoint(
            unredacted_df,
            (get_glue_catalog(), f"{database_name}.{name}"),
            batch_size=config.batch_size,
            key_cols=key_cols,
            text_cols=text_cols,
        )

        context.log.info(
            "Redacted %d rows (%d already redacted, %d total upstream)",
            redacted_count,
            already_redacted_df.height,
            source_df.height,
        )
        context.add_output_metadata(
            {
                "redacted_count": MetadataValue.int(redacted_count),
                "already_redacted_count": MetadataValue.int(already_redacted_df.height),
                "redaction_version": MetadataValue.text(REDACTION_VERSION),
                "full_refresh": MetadataValue.bool(config.full_refresh),
            }
        )
        # Every row is already written chunk by chunk. Returning them would make the
        # IO manager upsert all of them again in one write, the step that failed at
        # about 594k rows.
        return pl.DataFrame(
            schema={
                **source_df.select(key_cols).schema,
                **{f"{column}_redacted": pl.String for column in text_cols},
                "redaction_version": pl.String,
            }
        )

    return _redacted
