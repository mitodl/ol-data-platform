import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    asset,
)
from feedback_clustering.lib.redact import redact_titles_and_text
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)


@asset(
    code_version="feedback_redacted_v1",
    group_name="feedback",
    key=AssetKey(["feedback_redacted"]),
    deps=[AssetKey(["int__feedback__unioned"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_redacted",
)
def feedback_redacted(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Mask PII in raw feedback title/text via Presidio.

    Phase-1 asset, upstream of tfact_feedback - not part of the ML pipeline.
    Raw title/text never propagate past this step (feedback_zendesk_mvp_spec.md
    Section 3): only title_redacted/text_redacted flow to the fact and, later,
    to embedding.
    """
    # get_dbt_model_as_dataframe returns a LazyFrame; redact needs it eager
    df = get_dbt_model_as_dataframe(
        database_name="ol_warehouse_production_reporting",
        table_name="int__feedback__unioned",
    ).collect()

    redacted_df = redact_titles_and_text(df)

    context.log.info(
        "Redacted %d feedback rows for tfact_feedback to left-join",
        redacted_df.height,
    )

    return redacted_df
