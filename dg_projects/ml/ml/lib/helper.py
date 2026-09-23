import json
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.preprocessing import RobustScaler


def scaled_features(
    features: pl.DataFrame | pl.LazyFrame,
    feature_cols: list[str],
) -> pl.DataFrame:
    """
    Apply RobustScaler to feature columns in a DataFrame and restore intercept.
    """

    numeric_data = features.select(feature_cols).to_numpy()

    scaler = RobustScaler()
    scaled = scaler.fit_transform(numeric_data)

    scaled_df = pl.DataFrame({col: scaled[:, i] for i, col in enumerate(feature_cols)})

    non_feature_cols = [c for c in features.columns if c not in feature_cols]

    result = pl.concat(
        [
            features.select(non_feature_cols),
            scaled_df,
        ],
        how="horizontal",
    )

    return result.with_columns(pl.lit(1.0).alias("intercept"))


def risk_probability(weights_path: Path, features: pl.DataFrame) -> pl.Series:
    """
    Calculate student risk probabilities using logistic regression weights.
    """
    weights_file = Path(weights_path)
    # Load pre-trained weights from JSON
    with weights_file.open("r") as f:
        weights_dict = json.load(f)

    weights_cols = list(weights_dict.keys())
    weights_vals = np.array(list(weights_dict.values()), dtype=float)

    # Ensure feature order matches weights
    x = features.select(weights_cols).to_numpy()

    # Clip extreme values to prevent numerical overflow
    z = np.clip(x.dot(weights_vals), -1000, 1000)

    # Apply sigmoid function to get probabilities
    probability = 1 / (1 + np.exp(-z))

    return pl.Series("risk_probability", probability)
