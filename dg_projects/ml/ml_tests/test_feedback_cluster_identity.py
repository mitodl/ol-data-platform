"""Tests for feedback_cluster_identity's cross-embedding-config retirement."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
from ml.assets.feedback_cluster_identity import (
    FeedbackClusterIdentityConfig,
    _active_cluster_members,
    _other_config_active_keys,
    _select_run_to_process,
)


def _lazyframe(data: dict[str, list[Any]]) -> pl.LazyFrame:
    return pl.DataFrame(data).lazy()


def test_other_config_active_keys_none_when_table_missing() -> None:
    with patch("ml.assets.feedback_cluster_identity.table_exists", return_value=False):
        result = _other_config_active_keys(
            MagicMock(), "text-embedding-3-small", 1024, "summary"
        )
    assert result == set()


def test_other_config_active_keys_excludes_matching_config() -> None:
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a", "b", "c", "d"],
            "cluster_status": ["active", "active", "active", "active"],
            "embedding_model_version": [
                "text-embedding-3-small",
                "text-embedding-3-large",
                "text-embedding-3-small",
                "text-embedding-3-small",
            ],
            "embedding_dim": [1024, 1024, 512, 1024],
            "embedding_input_filter": [
                "summary",
                "summary",
                "summary",
                "concatenated_turns",
            ],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _other_config_active_keys(
            MagicMock(), "text-embedding-3-small", 1024, "summary"
        )
    # "a" matches the run's config exactly, so it's excluded. "b" (different
    # model), "c" (different dim), and "d" (different arm) all count as a
    # different config.
    assert result == {"b", "c", "d"}


def test_other_config_active_keys_matches_null_input_filter() -> None:
    # A run with embedding_input_filter=None (every arm clustered together)
    # should match an active cluster that also has a null filter, not treat
    # null as "different" from every other value.
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a"],
            "cluster_status": ["active"],
            "embedding_model_version": ["text-embedding-3-small"],
            "embedding_dim": [1024],
            "embedding_input_filter": [None],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _other_config_active_keys(
            MagicMock(), "text-embedding-3-small", 1024, None
        )
    assert result == set()


def test_other_config_active_keys_excludes_retired_rows() -> None:
    clusters_lf = _lazyframe(
        {
            "cluster_key": ["a"],
            "cluster_status": ["retired"],
            "embedding_model_version": ["text-embedding-3-large"],
            "embedding_dim": [1024],
            "embedding_input_filter": ["summary"],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            return_value=clusters_lf,
        ),
    ):
        result = _other_config_active_keys(
            MagicMock(), "text-embedding-3-small", 1024, "summary"
        )
    # Already retired -- not this function's job to retire it again.
    assert result == set()


def _runs_and_identity_frames(
    runs_lf: pl.LazyFrame, identity_lf: pl.LazyFrame | None = None
):
    def _fake_get_dbt_model_as_dataframe(*, database_name, table_name):  # noqa: ARG001
        if table_name == "feedback_cluster_run":
            return runs_lf
        return (
            identity_lf
            if identity_lf is not None
            else _lazyframe({"cluster_run_id": []})
        )

    return _fake_get_dbt_model_as_dataframe


def test_select_run_to_process_skips_unpromoted_same_model_sweep() -> None:
    # Same model/dim/arm as production (e.g. a min_cluster_size sweep) but not
    # promoted -- embedding provenance alone can't tell these apart (#2543).
    runs_lf = _lazyframe(
        {
            "cluster_run_id": ["sweep-run", "production-run"],
            "run_status": ["completed", "completed"],
            "run_at": [
                datetime(2026, 1, 2, tzinfo=UTC),
                datetime(2026, 1, 1, tzinfo=UTC),
            ],
            "embedding_model_version": [
                "text-embedding-3-large",
                "text-embedding-3-large",
            ],
            "embedding_dim": [1024, 1024],
            "embedding_input_filter": ["summary", "summary"],
            "is_promoted": [False, True],
        }
    )
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            side_effect=_runs_and_identity_frames(runs_lf),
        ),
        patch(
            "ml.assets.feedback_cluster_identity.default_embedding_model_version",
            return_value="text-embedding-3-large",
        ),
        patch("ml.assets.feedback_cluster_identity.EMBEDDING_DIM", 1024),
    ):
        result = _select_run_to_process(MagicMock(), FeedbackClusterIdentityConfig())
    assert result == "production-run"


def test_select_run_to_process_explicit_override_bypasses_filter() -> None:
    # An operator-supplied cluster_run_id (e.g. to process a bake-off run on
    # purpose) is a deliberate choice, not auto-selection -- skips the
    # production-config filter entirely.
    result = _select_run_to_process(
        MagicMock(),
        FeedbackClusterIdentityConfig(cluster_run_id="bake-off-run"),
    )
    assert result == "bake-off-run"


def _active_members_frames(table_name: str, **_: Any) -> pl.LazyFrame:
    frames = {
        "feedback_cluster": _lazyframe(
            {
                "cluster_key": ["mixed", "old-only"],
                "cluster_status": ["active", "active"],
                "embedding_model_version": ["model", "model"],
                "embedding_dim": [8, 8],
                "embedding_input_filter": ["summary", "summary"],
            }
        ),
        "feedback_cluster_membership": _lazyframe(
            {
                "feedback_conversation_pk": ["old-1", "new-1", "old-2"],
                "cluster_key": ["mixed", "mixed", "old-only"],
            }
        ),
        "int__feedback__conversation": _lazyframe(
            {
                "feedback_conversation_pk": ["old-1", "new-1", "old-2"],
                "conversation_opened_at": [
                    "2025-06-01T00:00:00.000",
                    "2026-02-01T00:00:00.000",
                    "2025-07-01T00:00:00.000",
                ],
                "source_slug": ["zendesk"] * 3,
                "conversation_text_chars": [600] * 3,
            }
        ),
    }
    return frames[table_name]


def test_active_cluster_members_keeps_only_members_in_the_run_date_range() -> None:
    with (
        patch("ml.assets.feedback_cluster_identity.table_exists", return_value=True),
        patch(
            "ml.assets.feedback_cluster_identity.get_dbt_model_as_dataframe",
            side_effect=_active_members_frames,
        ),
    ):
        full_history = _active_cluster_members(MagicMock(), "model", 8, "summary")
        since_2026 = _active_cluster_members(
            MagicMock(), "model", 8, "summary", "2026-01-01"
        )

    assert full_history == {
        "mixed": frozenset({"old-1", "new-1"}),
        "old-only": frozenset({"old-2"}),
    }
    # old-only stays, empty, so match_clusters retires it rather than dropping it
    assert since_2026 == {"mixed": frozenset({"new-1"}), "old-only": frozenset()}
