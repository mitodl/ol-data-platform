"""Tests for feedback_cluster_assignment and its ml.lib.cluster_run_lookup helper."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_cluster_assignment import feedback_cluster_assignment
from ml.lib import cluster_run_lookup


class _CapturingIOManager(IOManager):
    def __init__(self) -> None:
        self.written: pl.DataFrame | None = None

    def handle_output(self, _context, obj: pl.DataFrame) -> None:
        self.written = obj

    def load_input(self, context) -> None:
        raise NotImplementedError


def test_bootstrap_with_no_upstream_tables_produces_empty_membership() -> None:
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_cluster_assignment.table_exists",
            return_value=False,
        ),
        patch(
            "ml.lib.cluster_run_lookup.table_exists",
            return_value=False,
        ),
        patch(
            "ml.assets.feedback_cluster_assignment.get_glue_catalog",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [feedback_cluster_assignment],
            resources={"io_manager": io_manager},
        )

    assert result.success
    assert io_manager.written is not None
    assert io_manager.written.height == 0


def _lazyframe(data: dict[str, list[Any]]) -> pl.LazyFrame:
    return pl.DataFrame(data).lazy()


def test_latest_identity_processed_run_none_when_lineage_table_missing() -> None:
    with patch(
        "ml.lib.cluster_run_lookup.table_exists",
        side_effect=lambda _catalog, table_id: "feedback_cluster_run" in table_id,
    ):
        result = cluster_run_lookup.latest_identity_processed_run(MagicMock(), "db")
    assert result is None


def test_latest_identity_processed_run_none_when_no_lineage_rows_yet() -> None:
    with (
        patch("ml.lib.cluster_run_lookup.table_exists", return_value=True),
        patch(
            "ml.lib.cluster_run_lookup.get_dbt_model_as_dataframe",
            return_value=_lazyframe({"cluster_run_id": []}),
        ),
    ):
        result = cluster_run_lookup.latest_identity_processed_run(MagicMock(), "db")
    assert result is None


def test_latest_identity_processed_run_returns_most_recent_completed() -> None:
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
