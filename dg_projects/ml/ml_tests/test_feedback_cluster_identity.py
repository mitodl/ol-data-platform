"""Tests for feedback_cluster_identity's cross-embedding-config retirement."""

from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
from ml.assets.feedback_cluster_identity import _other_config_active_keys


def _lazyframe(data: dict[str, list[Any]]) -> pl.LazyFrame:
    return pl.DataFrame(data).lazy()


def test_other_config_active_keys_none_when_table_missing() -> None:
    with patch("ml.assets.feedback_cluster_identity.table_exists", return_value=False):
        result = _other_config_active_keys(MagicMock(), "text-embedding-3-small", 1024)
    assert result == set()


def test_other_config_active_keys_excludes_matching_config() -> None:
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a", "b", "c"],
            "cluster_status": ["active", "active", "active"],
            "embedding_model_version": [
                "text-embedding-3-small",
                "text-embedding-3-large",
                "text-embedding-3-small",
            ],
            "embedding_dim": [1024, 1024, 512],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _other_config_active_keys(MagicMock(), "text-embedding-3-small", 1024)
    # "a" matches the run's config exactly, so it's excluded. "b" (different
    # model) and "c" (different dim) both count as a different config.
    assert result == {"b", "c"}


def test_other_config_active_keys_excludes_retired_rows() -> None:
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a"],
            "cluster_status": ["retired"],
            "embedding_model_version": ["text-embedding-3-large"],
            "embedding_dim": [1024],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _other_config_active_keys(MagicMock(), "text-embedding-3-small", 1024)
    # Already retired -- not this function's job to retire it again.
    assert result == set()
