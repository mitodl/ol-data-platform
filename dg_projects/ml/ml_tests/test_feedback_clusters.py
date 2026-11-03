"""Tests for the feedback_clusters asset's small-N failure path."""

from unittest.mock import MagicMock, patch

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_clusters import FeedbackClustersConfig, feedback_clusters


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
            # Doubles as the int__feedback__conversation mock, which the scope
            # filter reads for each source's minimum length.
            "conversation_text_chars": [600] * n,
            "conversation_text": ["x" * 600] * n,
        }
    ).lazy()


def test_small_run_writes_a_failed_run_row_with_no_candidates() -> None:
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_clusters.get_dbt_model_as_dataframe",
            return_value=_small_embeddings_lazyframe(3),
        ),
        patch(
            "ml.assets.feedback_clusters.get_glue_catalog",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [feedback_clusters],
            resources={"io_manager": io_manager},
            run_config={
                "ops": {
                    "feedback_clusters": {
                        "config": FeedbackClustersConfig(
                            full_refresh=True,
                            min_cluster_size=5,
                            umap_n_components=5,
                            embedding_model="openai:text-embedding-3-small",
                            embedding_dim=8,
                            feedback_since=None,
                            platforms=None,
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


def _run_feedback_clusters(config: FeedbackClustersConfig, io_manager):
    return materialize(
        [feedback_clusters],
        resources={"io_manager": io_manager},
        run_config={"ops": {"feedback_clusters": {"config": config.model_dump()}}},
        raise_on_error=False,
    )


def test_promoted_full_refresh_is_rejected_without_a_review() -> None:
    # Valid input, so only the review gate can stop this run
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_clusters.get_dbt_model_as_dataframe",
            return_value=_small_embeddings_lazyframe(3),
        ),
        patch("ml.assets.feedback_clusters.get_glue_catalog", return_value=MagicMock()),
        patch(
            "ml.assets.feedback_clusters._live_cluster_keys",
            return_value=("live-run", {"0": "key-a"}),
        ),
    ):
        result = _run_feedback_clusters(
            FeedbackClustersConfig(
                is_promoted=True,
                full_refresh=True,
                embedding_model="openai:text-embedding-3-small",
                embedding_dim=8,
                feedback_since=None,
                platforms=None,
            ),
            io_manager,
        )

    assert result.success is False
    assert io_manager.written == {}


def test_incremental_run_with_no_live_members_in_its_input_writes_nothing() -> None:
    # The live cluster's member is missing from the embeddings, as in an outage
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_clusters.get_dbt_model_as_dataframe",
            return_value=_small_embeddings_lazyframe(3),
        ),
        patch("ml.assets.feedback_clusters.get_glue_catalog", return_value=MagicMock()),
        patch(
            "ml.assets.feedback_clusters._live_cluster_keys",
            return_value=("live-run", {"missing": "key-a"}),
        ),
    ):
        result = _run_feedback_clusters(
            FeedbackClustersConfig(
                embedding_model="openai:text-embedding-3-small",
                embedding_dim=8,
                feedback_since=None,
                platforms=None,
            ),
            io_manager,
        )

    assert result.success is False
    assert io_manager.written == {}


def test_run_clusters_embeddings_in_conversation_pk_order() -> None:
    # Iceberg returns rows in any order; the run must not depend on it
    io_manager = _CapturingIOManager()
    reversed_embeddings = _small_embeddings_lazyframe(5).reverse()
    with (
        patch(
            "ml.assets.feedback_clusters.get_dbt_model_as_dataframe",
            return_value=reversed_embeddings,
        ),
        patch("ml.assets.feedback_clusters.get_glue_catalog", return_value=MagicMock()),
        patch(
            "ml.assets.feedback_clusters._live_cluster_keys",
            return_value=("live-run", {"0": "key-a", "1": "key-a"}),
        ),
    ):
        result = _run_feedback_clusters(
            FeedbackClustersConfig(
                embedding_model="openai:text-embedding-3-small",
                embedding_dim=8,
                feedback_since=None,
                platforms=None,
            ),
            io_manager,
        )

    assert result.success is True
    written_pks = io_manager.written["feedback_cluster_candidate"][
        "feedback_conversation_pk"
    ]
    assert written_pks.to_list() == ["0", "1", "2", "3", "4"]
