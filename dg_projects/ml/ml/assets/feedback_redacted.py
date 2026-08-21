import contextlib
import os

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    asset,
)
from ml.lib.redact import (
    JOIN_COLS,
    filter_unredacted,
    redact_titles_and_text,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class FeedbackRedactedConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description=(
            "Re-redact every row instead of only rows missing from the table "
        ),
    )
    sample_limit: int | None = Field(
        default=None,
        description="Cap the number of upstream rows read, for fast local testing.",
    )


@asset(
    code_version="feedback_redacted_v2",
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
    if config.sample_limit is not None:
        source_lazy = source_lazy.limit(config.sample_limit)
    source_df = source_lazy.collect()

    already_redacted_df = pl.DataFrame(schema=dict.fromkeys(JOIN_COLS, pl.String))
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            already_redacted_df = (
                get_dbt_model_as_dataframe(
                    database_name=database_name,
                    table_name="feedback_redacted",
                )
                .select(JOIN_COLS)
                .collect()
            )

    unredacted_df = filter_unredacted(source_df, already_redacted_df)
    redacted_df = redact_titles_and_text(unredacted_df)

    context.log.info(
        "Redacted %d new feedback rows (%d already redacted, %d total upstream)",
        redacted_df.height,
        already_redacted_df.height,
        source_df.height,
    )

    return redacted_df
