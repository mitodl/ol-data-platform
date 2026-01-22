import os
from datetime import UTC, datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Output,
    asset,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
    get_or_create_iceberg_table,
)
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import DoubleType, LongType, StringType, TimestamptzType
from student_risk_probability.lib.helper import (
    risk_probability,
    scaled_features,
)


@asset(
    code_version="student_risk_probability_feature_v1",
    group_name="student_risk_probability",
    deps=[AssetKey(["reporting", "cheating_detection_report"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    key=AssetKey(["student_risk_probability", "risk_scores"]),
)
def student_risk_probability(context: AssetExecutionContext):
    """
    Feature engineering for student risk probability (per course, per student).
    scale numeric features, calculate risk probabilities, and write to Iceberg table.
    """
    if DAGSTER_ENV == "dev":
        database_name = (
            f"ol_warehouse_production_{os.environ.get('DBT_SCHEMA_SUFFIX')}_reporting"
        )
    else:
        database_name = "ol_warehouse_production_reporting"

    source_table_name = "cheating_detection_report"
    output_table_name = "student_risk_probability"

    df = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name=source_table_name,
    )

    id_cols = ["courserun_readable_id", "openedx_user_id"]

    feature_numeric_cols = [
        "user_exam_median_solving_time",
        "user_non_exam_problem_count",
        "user_exam_time_flags",
        "user_avg_exam_grade",
    ]

    # Lazy selection and cleanup
    features_lazy = (
        df.filter(pl.col("user_taken_final_exam").eq(True))  # noqa: FBT003
        .select(id_cols + feature_numeric_cols)
        .unique(subset=id_cols)
        .drop_nulls(subset=feature_numeric_cols)
    )

    # Scale numeric features using RobustScaler
    features_eager = features_lazy.collect()
    scaled_df = scaled_features(features_eager, feature_numeric_cols)

    # Calculate risk probabilities using logistic regression weights
    probabilities = risk_probability(
        weights_path="student_risk_probability/resources/weights.json",
        features=scaled_df,
    )

    # build results dataframe
    results_df = pl.concat(
        [
            scaled_df.select(id_cols),
            probabilities.rename("risk_probability").to_frame(),
        ],
        how="horizontal",
    ).with_columns(pl.lit(datetime.now(UTC)).alias("created_timestamp"))

    iceberg_schema = Schema(
        NestedField(1, "courserun_readable_id", StringType(), required=False),
        NestedField(2, "openedx_user_id", LongType(), required=False),
        NestedField(3, "risk_probability", DoubleType(), required=False),
        NestedField(4, "created_timestamp", TimestamptzType(), required=False),
    )

    context.log.info(
        "Appending %d rows to Iceberg table student_risk_probability",
        results_df.height,
    )

    table = get_or_create_iceberg_table(
        database_name=database_name,
        table_name=output_table_name,
        schema=iceberg_schema,
    )

    arrow_table = results_df.to_arrow()
    # full overwrite for now
    table.overwrite(arrow_table)

    return Output(
        value={"table_name": {output_table_name}},
        metadata={
            "row_count": results_df.height,
            "table_name": f"{database_name}.{output_table_name}",
        },
    )
