import contextlib
import os

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    asset,
)
from ml.lib.redact import (
    JOIN_COLS,
    REDACT_CHECKPOINT_BATCH_SIZE,
    REDACTED_SCHEMA,
    REDACTION_VERSION,
    filter_unredacted,
    redact_and_checkpoint,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

if DAGSTER_ENV == "dev":
    # dev intentionally targets the production catalog under a personal schema
    # suffix, matching the dev_production dbt target other assets build against.
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
elif DAGSTER_ENV == "qa":
    database_name = "ol_warehouse_qa_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class FeedbackRedactedConfig(Config):
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
    record_refs: list[str] | None = Field(
        default=None,
        description=(
            "Only process rows with this source_record_ref, for spot-checking "
            "specific known cases locally without redacting the whole table. "
        ),
    )
    source_slug: str | None = Field(
        default=None,
        description="Paired with record_refs to disambiguate across sources.",
    )


@asset(
    code_version="feedback_redacted_v4",
    group_name="feedback",
    key=AssetKey(["intermediate", "feedback_redacted"]),
    deps=[AssetKey(["intermediate", "int__feedback__unioned"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_redacted",
    metadata={
        "schema": database_name,
        "write_mode": "upsert",
        "upsert_options": {"join_cols": JOIN_COLS},
        "schema_update_mode": "update",
    },
)
def feedback_redacted(
    context: AssetExecutionContext, config: FeedbackRedactedConfig
) -> pl.DataFrame:
    """
    Mask PII in raw feedback title/text via Presidio.

    """
    source_lazy = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="int__feedback__unioned",
    )
    if config.record_refs is not None:
        record_filter = pl.col("source_record_ref").is_in(config.record_refs)
        if config.source_slug is not None:
            record_filter = record_filter & (
                pl.col("source_slug") == config.source_slug
            )
        source_lazy = source_lazy.filter(record_filter)
    if config.sample_limit is not None:
        source_lazy = source_lazy.limit(config.sample_limit)
    source_df = source_lazy.collect()

    already_redacted_df = pl.DataFrame(schema=dict.fromkeys(JOIN_COLS, pl.String))
    with contextlib.suppress(NoSuchTableError):
        existing_lazy = get_dbt_model_as_dataframe(
            database_name=database_name,
            table_name="feedback_redacted",
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
        already_redacted_df = existing_lazy.select(JOIN_COLS).collect()

    unredacted_df = filter_unredacted(source_df, already_redacted_df)
    redacted_count = redact_and_checkpoint(
        unredacted_df,
        (get_glue_catalog(), f"{database_name}.feedback_redacted"),
        batch_size=config.batch_size,
    )

    context.log.info(
        "Redacted %d feedback rows (%d already redacted, %d total upstream)",
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
    return pl.DataFrame(schema=REDACTED_SCHEMA)
