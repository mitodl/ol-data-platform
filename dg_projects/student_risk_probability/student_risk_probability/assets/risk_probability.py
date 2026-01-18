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
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from student_risk_probability.lib.helper import (
    risk_probability,
    scaled_features,
)


@asset(
    code_version="student_risk_probability_feature_v1",
    group_name="student_risk_probability",
    deps=[AssetKey(["reporting", "cheating_detection_report"])],
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

    features = df.select(id_cols + feature_numeric_cols).unique(subset=id_cols)
    features = features.drop_nulls(subset=feature_numeric_cols)

    # Scale numeric features using RobustScaler
    scaled_df = scaled_features(features)

    # Calculate risk probabilities using logistic regression weights
    probabilities = risk_probability(
        weights_path="student_risk_probability/resources/weights.json",
        features=scaled_df,
    )

    # build results dataframe
    results_df = pl.concat(
        [scaled_df.select(id_cols), probabilities.to_frame()], how="horizontal"
    )
    # Compute data version hash
    data_version = hashlib.sha256(
        results_df.to_csv(index=False).encode("utf-8")
    ).hexdigest()

    # Define output paths
    target_path = f"{'/'.join(context.asset_key_for_output('student_risk_probability').path)}/{data_version}.csv"  # noqa: E501
    data_file = Path(f"student_risk_probability/{data_version}.csv")

    results_df.sink_csv(str(data_file))

    with data_file.open("rb") as f:
        csv_bytes = f.read()
    data_version = hashlib.sha256(csv_bytes).hexdigest()

    context.log.info("Exported student risk probabilities to %s", data_file)

    yield Output(
        value=(data_file, target_path),
        data_version=DataVersion(data_version),
    )
