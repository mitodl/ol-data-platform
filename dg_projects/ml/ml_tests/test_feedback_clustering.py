"""Tests for the feedback_clustering asset's small-N failure path."""

from unittest.mock import MagicMock, patch

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_clustering import FeedbackClusteringConfig, feedback_clustering


class _CapturingIOManager(IOManager):
    """Records handled outputs by name; enough to assert what got written."""

    def __init__(self) -> None:
        self.written: dict[str, pl.DataFrame] = {}

    def handle_output(self, context, obj) -> None:
        self.written[context.name] = obj

    def load_input(self, context) -> None:
        raise NotImplementedError


def _small_embeddings_lazyframe(n: int) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "feedback_conversation_pk": [str(i) for i in range(n)],
            "source_slug": ["zendesk"] * n,
            "conversation_ref": [str(i) for i in range(n)],
            "embedding_input": ["summary"] * n,
            "embedding_vector": [[0.1] * 8 for _ in range(n)],
            "embedding_model_version": ["text-embedding-3-small"] * n,
            "embedding_dim": [8] * n,
        }
    ).lazy()


def test_small_run_writes_a_failed_run_row_with_no_candidates() -> None:
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_clustering.get_dbt_model_as_dataframe",
            return_value=_small_embeddings_lazyframe(3),
        ),
        patch(
            "ml.assets.feedback_clustering.get_glue_catalog",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [feedback_clustering],
            resources={"io_manager": io_manager},
            run_config={
                "ops": {
                    "feedback_clustering": {
                        "config": FeedbackClusteringConfig(
                            min_cluster_size=5,
                            umap_n_components=5,
                            embedding_model_version="text-embedding-3-small",
                            embedding_dim=8,
                        ).model_dump()
                    }
                }
            },
            raise_on_error=False,
        )

    assert result.success is False
    assert list(io_manager.written.keys()) == ["feedback_cluster_run"]
    row = io_manager.written["feedback_cluster_run"].to_dicts()[0]
    assert row["run_status"] == "failed"
    assert row["cluster_count"] == 0
    assert row["noise_count"] == 0
    assert row["silhouette_score"] is None
    assert row["total_conversations"] == 3
