"""Smoke test for feedback_cluster_assignment's bootstrap path (no upstream tables)."""

from unittest.mock import MagicMock, patch

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_cluster_assignment import feedback_cluster_assignment


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
