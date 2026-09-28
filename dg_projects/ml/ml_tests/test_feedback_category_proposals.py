"""Smoke test for feedback_category_proposals' bootstrap path (no cluster runs yet)."""

from unittest.mock import MagicMock, patch

from dagster import IOManager, materialize
from ml.assets.feedback_category_proposals import feedback_category_proposals
from ml.resources.llm import LLMClientFactory


class _CapturingIOManager(IOManager):
    def __init__(self) -> None:
        self.written = None

    def handle_output(self, _context, obj) -> None:
        self.written = obj

    def load_input(self, _context) -> None:
        raise NotImplementedError


def test_bootstrap_with_no_identity_processed_run_produces_empty_output() -> None:
    io_manager = _CapturingIOManager()
    with (
        patch(
            "ml.assets.feedback_category_proposals.get_glue_catalog",
            return_value=MagicMock(),
        ),
        patch(
            "ml.lib.cluster_run_lookup.table_exists",
            return_value=False,
        ),
    ):
        result = materialize(
            [feedback_category_proposals],
            resources={
                "io_manager": io_manager,
                "llm": LLMClientFactory(client_class="anthropic"),
            },
        )

    assert result.success
    assert io_manager.written is not None
    assert io_manager.written.height == 0
