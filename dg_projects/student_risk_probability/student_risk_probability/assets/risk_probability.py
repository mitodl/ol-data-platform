from datetime import UTC, datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    asset,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from student_risk_probability.lib.helper import (
    risk_probability,
    scaled_features,
)


@asset(
    code_version="student_risk_probability_v1",
    group_name="student_risk_probability",
    deps=[AssetKey(["reporting", "cheating_detection_report"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    key=AssetKey(["student_risk_probability"]),
)
def student_risk_probability(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Calculate student risk probabilities based on cheating_detection_report.
    Scales numeric features and applies logistic regression weights to compute
    risk probabilities.
    """

    df = get_dbt_model_as_dataframe(
        database_name="ol_warehouse_production_reporting",
        table_name="cheating_detection_report",
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

    context.log.info(
        "Appending %d rows to Iceberg table student_risk_probability",
        results_df.height,
    )

    return results_df
