"""Tests for the feedback_embeddings asset's run accounting."""

from unittest.mock import MagicMock, patch

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_embeddings import (
    FeedbackEmbeddingsConfig,
    feedback_embeddings,
)
from ml.resources.llm import LLMClientFactory


class _CapturingIOManager(IOManager):
    def __init__(self) -> None:
        self.written: pl.DataFrame | None = None

    def handle_output(self, context, obj) -> None:  # noqa: ARG002
        self.written = obj

    def load_input(self, context) -> None:
        raise NotImplementedError


class _FakeEmbeddingClient:
    model_version = "test-model"
    dim = 3
    max_request_batch_size = 250
    max_input_chars = None

    def embed_batch(
        self,
        texts: list[str],
        *,
        trace_metadata: dict[str, object] | None = None,  # noqa: ARG002
    ) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


def _upstream(*, table_name: str, **_: object) -> pl.LazyFrame:
    refs = ["1", "2", "3", "4"]
    if table_name == "feedback_summaries":
        return pl.DataFrame(
            {
                "feedback_conversation_pk": [f"pk-{ref}" for ref in refs],
                "source_slug": ["zendesk"] * 4,
                "conversation_ref": refs,
                "turn_count": [3] * 4,
                "conversation_summary": ["hello", "", " \n ", None],
                "embedding_input": ["summary"] * 4,
            }
        ).lazy()
    return pl.DataFrame(
        {
            "feedback_conversation_pk": [f"pk-{ref}" for ref in refs],
            "conversation_text": ["turns"] * 4,
        }
    ).lazy()


def test_blank_and_null_text_count_as_no_text_not_failed() -> None:
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_embeddings.get_dbt_model_as_dataframe",
            side_effect=_upstream,
        ),
        patch(
            "ml.assets.feedback_embeddings.build_embedding_client",
            return_value=_FakeEmbeddingClient(),
        ),
        patch("ml.assets.feedback_embeddings.get_glue_catalog", return_value=None),
        patch("ml.lib.embed.checkpoint_embedding_chunk", MagicMock()),
    ):
        result = materialize(
            [feedback_embeddings],
            resources={"io_manager": io_manager, "embedding_llm": LLMClientFactory()},
            run_config={
                "ops": {
                    "intermediate__feedback_embeddings": {
                        "config": FeedbackEmbeddingsConfig(
                            full_refresh=True
                        ).model_dump()
                    }
                }
            },
        )

    assert io_manager.written is not None
    assert io_manager.written["conversation_ref"].to_list() == ["1"]
    metadata = result.asset_materializations_for_node(
        "intermediate__feedback_embeddings"
    )[0].metadata
    assert metadata["embedded_count"].value == 1
    assert metadata["no_text_count"].value == 3
    assert metadata["failed_count"].value == 0
