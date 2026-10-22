"""Tests for the feedback_embeddings asset's run accounting."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import Failure, IOManager, materialize
from ml.assets.feedback_embeddings import (
    FeedbackEmbeddingsConfig,
    feedback_embeddings,
)
from ml.lib import embed
from ol_llm.testing import override_embedding_models
from pydantic_ai.embeddings import TestEmbeddingModel
from pydantic_ai.exceptions import ModelHTTPError

MODEL = "openai:text-embedding-3-large"


class _CapturingIOManager(IOManager):
    def __init__(self) -> None:
        self.written: pl.DataFrame | None = None

    def handle_output(self, context, obj) -> None:  # noqa: ARG002
        self.written = obj

    def load_input(self, context) -> None:
        raise NotImplementedError


def _upstream(stored: pl.DataFrame | None = None):
    refs = ["1", "2", "3", "4", "5"]

    def frame(*, table_name: str, **_: object) -> pl.LazyFrame:
        if table_name == "feedback_summaries":
            return pl.DataFrame(
                {
                    "feedback_conversation_pk": [f"pk-{ref}" for ref in refs],
                    "source_slug": ["zendesk"] * 5,
                    "conversation_ref": refs,
                    "turn_count": [3] * 5,
                    "conversation_summary": ["hello", "", " \n ", None, "world"],
                    "embedding_input": ["summary"] * 5,
                }
            ).lazy()
        if table_name == "feedback_embeddings":
            assert stored is not None
            return stored.lazy()
        return pl.DataFrame(
            {
                "feedback_conversation_pk": [f"pk-{ref}" for ref in refs],
                "conversation_text": ["turns"] * 5,
            }
        ).lazy()

    return frame


def _materialize(
    io_manager: _CapturingIOManager,
    config: FeedbackEmbeddingsConfig,
    stored: pl.DataFrame | None = None,
):
    with (
        patch(
            "ml.assets.feedback_embeddings.get_dbt_model_as_dataframe",
            side_effect=_upstream(stored),
        ),
        patch("ml.assets.feedback_embeddings.get_glue_catalog", return_value=None),
        patch(
            "ml.lib.embed.checkpoint_embedding_chunk",
            MagicMock(side_effect=lambda _catalog, _table, chunk_df: chunk_df),
        ),
        # The TestEmbeddingModel override is a context variable, which
        # process_batch's worker threads would not see.
        override_embedding_models(TestEmbeddingModel()),
    ):
        return materialize(
            [feedback_embeddings],
            resources={"io_manager": io_manager},
            run_config={
                "ops": {
                    "intermediate__feedback_embeddings": {"config": config.model_dump()}
                }
            },
        )


def test_blank_and_null_text_count_as_no_text_not_failed() -> None:
    io_manager = _CapturingIOManager()

    result = _materialize(
        io_manager,
        FeedbackEmbeddingsConfig(
            full_refresh=True,
            embedding_model=MODEL,
            embedding_dim=3,
            max_concurrency=1,
        ),
    )

    assert io_manager.written is not None
    assert io_manager.written["conversation_ref"].to_list() == ["1", "5"]
    assert (
        io_manager.written["embedding_model_version"].to_list()
        == ["text-embedding-3-large"] * 2
    )
    metadata = result.asset_materializations_for_node(
        "intermediate__feedback_embeddings"
    )[0].metadata
    assert metadata["embedded_count"].value == 2
    assert metadata["no_text_count"].value == 3
    assert metadata["failed_count"].value == 0
    assert metadata["embedding_model"].value == MODEL


def test_a_stored_vector_is_kept_for_the_same_text_and_redone_for_a_new_one() -> None:
    model = embed.resolve_embedding_model(MODEL, 3)
    stored = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1", "pk-5", "pk-5"],
            "embedding_model_version": ["text-embedding-3-large"] * 3,
            "embedding_dim": [3, 3, 1024],
            "idempotency_key": [
                model.key("hello"),
                model.key("an older summary"),
                model.key("world"),
            ],
        }
    )
    io_manager = _CapturingIOManager()

    result = _materialize(
        io_manager,
        FeedbackEmbeddingsConfig(
            embedding_model=MODEL, embedding_dim=3, max_concurrency=1
        ),
        stored,
    )

    assert io_manager.written is not None
    assert io_manager.written["conversation_ref"].to_list() == ["5"]
    metadata = result.asset_materializations_for_node(
        "intermediate__feedback_embeddings"
    )[0].metadata
    assert metadata["already_embedded_count"].value == 2


def test_a_table_without_idempotency_key_reembeds_every_conversation() -> None:
    stored = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1", "pk-5"],
            "embedding_model_version": ["text-embedding-3-large"] * 2,
            "embedding_dim": [3, 3],
        }
    )
    io_manager = _CapturingIOManager()

    _materialize(
        io_manager,
        FeedbackEmbeddingsConfig(
            embedding_model=MODEL, embedding_dim=3, max_concurrency=1
        ),
        stored,
    )

    assert io_manager.written is not None
    assert io_manager.written["conversation_ref"].to_list() == ["1", "5"]


def test_a_run_that_stops_raises_failure() -> None:
    class _Denied(TestEmbeddingModel):
        async def embed(self, inputs, *, input_type, settings=None):  # noqa: ARG002
            raise ModelHTTPError(status_code=401, model_name="test")

    with (
        patch(
            "ml.assets.feedback_embeddings.get_dbt_model_as_dataframe",
            side_effect=_upstream(),
        ),
        patch("ml.assets.feedback_embeddings.get_glue_catalog", return_value=None),
        patch("ml.lib.embed.checkpoint_embedding_chunk", MagicMock()),
        override_embedding_models(_Denied()),
        pytest.raises(Failure, match="stopped with model"),
    ):
        materialize(
            [feedback_embeddings],
            resources={"io_manager": _CapturingIOManager()},
            run_config={
                "ops": {
                    "intermediate__feedback_embeddings": {
                        "config": FeedbackEmbeddingsConfig(
                            full_refresh=True,
                            embedding_model=MODEL,
                            embedding_dim=3,
                            max_concurrency=1,
                        ).model_dump()
                    }
                }
            },
        )
