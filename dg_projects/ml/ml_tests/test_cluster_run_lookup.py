"""Tests for ml.lib.cluster_run_lookup."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
from ml.lib import cluster_run_lookup


def _lazyframe(data: dict[str, list[Any]]) -> pl.LazyFrame:
    return pl.DataFrame(data).lazy()


def test_returns_none_when_lineage_table_missing() -> None:
    with patch(
        "ml.lib.cluster_run_lookup.table_exists",
        side_effect=lambda _catalog, table_id: "feedback_cluster_run" in table_id,
    ):
        result = cluster_run_lookup.latest_identity_processed_run(MagicMock(), "db")
    assert result is None


def test_returns_none_when_no_lineage_rows_yet() -> None:
    with (
        patch("ml.lib.cluster_run_lookup.table_exists", return_value=True),
        patch(
            "ml.lib.cluster_run_lookup.get_dbt_model_as_dataframe",
            return_value=_lazyframe({"cluster_run_id": []}),
        ),
    ):
        result = cluster_run_lookup.latest_identity_processed_run(MagicMock(), "db")
    assert result is None


def test_returns_most_recent_completed_processed_run() -> None:
    lineage_lf = _lazyframe({"cluster_run_id": ["run-1", "run-2"]})
    runs_lf = _lazyframe(
        {
            "cluster_run_id": ["run-1", "run-2"],
            "run_status": ["completed", "completed"],
            "run_at": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 2, tzinfo=UTC),
            ],
        }
    )

    def _fake_get_dbt_model_as_dataframe(*, database_name, table_name):  # noqa: ARG001
        return lineage_lf if table_name == "feedback_cluster_lineage" else runs_lf

    with (
        patch("ml.lib.cluster_run_lookup.table_exists", return_value=True),
        patch(
            "ml.lib.cluster_run_lookup.get_dbt_model_as_dataframe",
            side_effect=_fake_get_dbt_model_as_dataframe,
        ),
    ):
        result = cluster_run_lookup.latest_identity_processed_run(MagicMock(), "db")
    assert result == "run-2"
