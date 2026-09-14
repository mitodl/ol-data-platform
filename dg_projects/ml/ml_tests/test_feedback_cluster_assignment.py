"""Tests for feedback_cluster_assignment and its ml.lib.cluster_run_lookup helper."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_cluster_assignment import (
    _active_clusters,
    _current_active_embedding_config,
    _run_embedding_config,
    feedback_cluster_assignment,
)
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


def test_run_embedding_config_reads_from_feedback_cluster_run() -> None:
    runs_lf = _lazyframe(
        {
            "cluster_run_id": ["run-1"],
            "embedding_model_version": ["text-embedding-3-small"],
            "embedding_dim": [1024],
        }
    )
    with patch(
        "ml.assets.feedback_cluster_assignment.get_dbt_model_as_dataframe",
        return_value=runs_lf,
    ):
        result = _run_embedding_config("run-1")
    assert result == ("text-embedding-3-small", 1024)


def test_current_active_embedding_config_none_when_table_missing() -> None:
    with patch(
        "ml.assets.feedback_cluster_assignment.table_exists", return_value=False
    ):
        result = _current_active_embedding_config(MagicMock())
    assert result is None


def test_current_active_embedding_config_returns_single_active_config() -> None:
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a", "b"],
            "cluster_status": ["active", "active"],
            "embedding_model_version": ["text-embedding-3-small"] * 2,
            "embedding_dim": [1024, 1024],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_assignment.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_assignment.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _current_active_embedding_config(MagicMock())
    assert result == ("text-embedding-3-small", 1024)


def test_current_active_embedding_config_none_when_two_configs_active() -> None:
    # A transitional window before feedback_cluster_identity retires the old
    # config's actives -- placement should skip rather than guess.
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a", "b"],
            "cluster_status": ["active", "active"],
            "embedding_model_version": [
                "text-embedding-3-small",
                "text-embedding-3-large",
            ],
            "embedding_dim": [1024, 1024],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_assignment.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_assignment.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _current_active_embedding_config(MagicMock())
    assert result is None


def test_active_clusters_scoped_to_config_and_active_status() -> None:
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a", "b", "c"],
            "cluster_status": ["active", "active", "retired"],
            "embedding_model_version": ["text-embedding-3-small"] * 3,
            "embedding_dim": [1024, 512, 1024],
            "centroid": [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]],
            "radius": [0.5, 0.5, 0.5],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_assignment.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_assignment.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _active_clusters(MagicMock(), "text-embedding-3-small", 1024)
    # "b" is the wrong dim and "c" is retired -- only "a" qualifies.
    assert [c["cluster_key"] for c in result] == ["a"]


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
