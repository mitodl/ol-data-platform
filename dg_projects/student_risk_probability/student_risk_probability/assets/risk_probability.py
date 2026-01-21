import hashlib
from pathlib import Path

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Output,
    asset,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from student_risk_probability.lib.helper import (
    risk_probability,
    scaled_features,
)


@asset(
    code_version="student_risk_probability_feature_v1",
    group_name="student_risk_probability",
    deps=[AssetKey(["reporting", "cheating_detection_report"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="s3file_io_manager",
    key=AssetKey(["student_risk_probability", "feature"]),
)
def student_risk_probability(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Feature engineering for student risk probability (per course, per student).
    scale numeric features, calculate risk probabilities, and write to CSV.
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
            probabilities.rename(
                "risk_probability"
            ).to_frame(),  # Polars DataFrame of probabilities
        ],
        how="horizontal",
    )

    data_version = hashlib.sha256(
        results_df.write_csv(file=None).encode("utf-8")
    ).hexdigest()
    target_path = f"{'/'.join(context.asset_key.path)}/{data_version}.csv"
    data_file = Path(f"student_risk_probability/{data_version}.csv")

    results_df.lazy().sink_csv(str(data_file))

    context.log.info("Exported student risk probabilities to %s", data_file)

    yield Output(
        value=(data_file, target_path),
        data_version=DataVersion(data_version),
    )
