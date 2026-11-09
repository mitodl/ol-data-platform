"""Tests for feedback_clusters_growth_sensor's decision to start a run."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import polars as pl
from dagster import (
    DagsterInstance,
    DagsterRunStatus,
    RunRequest,
    SkipReason,
    build_sensor_context,
)
from dagster._core.test_utils import create_run_for_test
from ml.lib.cluster import should_trigger_early_recluster
from ml.sensors.feedback_clusters import feedback_clusters_growth_sensor

MODEL = "text-embedding-3-large"


def _tables(*, active_clusters: int, run_platforms: str, total_conversations: int):
    n = 10
    tables = {
        "feedback_embeddings": pl.DataFrame(
            {
                "feedback_conversation_pk": [str(i) for i in range(n)],
                "embedding_input": ["summary"] * n,
                "embedding_model_version": [MODEL] * n,
                "embedding_dim": [1024] * n,
            }
        ),
        "int__feedback__conversation": pl.DataFrame(
            {
                "feedback_conversation_pk": [str(i) for i in range(n)],
                "source_slug": ["zendesk"] * n,
                "platform": ["mitlearn"] * n,
                "conversation_text_chars": [600] * n,
                "conversation_text": ["x" * 600] * n,
            }
        ),
        "feedback_cluster": pl.DataFrame(
            {
                "cluster_key": [f"k{i}" for i in range(active_clusters)],
                "cluster_status": ["active"] * active_clusters,
                "embedding_model_version": [MODEL] * active_clusters,
                "embedding_dim": [1024] * active_clusters,
                "embedding_input_filter": ["summary"] * active_clusters,
            },
            schema={
                "cluster_key": pl.String,
                "cluster_status": pl.String,
                "embedding_model_version": pl.String,
                "embedding_dim": pl.Int64,
                "embedding_input_filter": pl.String,
            },
        ),
        "feedback_cluster_identity_run": pl.DataFrame({"cluster_run_id": ["live"]}),
        "feedback_cluster_run": pl.DataFrame(
            {
                "cluster_run_id": ["live"],
                "run_status": ["completed"],
                "is_promoted": [False],
                "run_at": [datetime(2026, 1, 1, tzinfo=UTC)],
                "total_conversations": [total_conversations],
                "embedding_model_version": [MODEL],
                "embedding_dim": [1024],
                "embedding_input_filter": ["summary"],
                "feedback_since": pl.Series([None], dtype=pl.String),
                "platforms": [run_platforms],
            }
        ),
    }
    return lambda *, database_name, table_name: tables[table_name].lazy()  # noqa: ARG005


def _evaluate(instance: DagsterInstance | None = None, **table_args):
    instance = instance or DagsterInstance.ephemeral()
    with (
        patch(
            "ml.sensors.feedback_clusters.get_glue_catalog", return_value=MagicMock()
        ),
        patch("ml.sensors.feedback_clusters.table_exists", return_value=True),
        patch(
            "ml.sensors.feedback_clusters.get_dbt_model_as_dataframe",
            side_effect=_tables(**table_args),
        ),
        patch("ml.lib.embed.EMBEDDING_MODEL", f"openai:{MODEL}"),
        patch("ml.lib.embed.EMBEDDING_DIM", 1024),
        # 10 embedded conversations against a baseline of 2: a small trigger
        patch(
            "ml.sensors.feedback_clusters.should_trigger_early_recluster",
            side_effect=lambda count, last: should_trigger_early_recluster(
                count, last, growth_trigger=5
            ),
        ),
    ):
        return feedback_clusters_growth_sensor(build_sensor_context(instance=instance))


def test_growth_sensor_triggers_when_the_live_run_is_far_behind() -> None:
    result = _evaluate(
        active_clusters=2, run_platforms="mitlearn", total_conversations=2
    )
    assert isinstance(result, RunRequest)
    assert result.run_config["ops"]["feedback_clusters"]["config"] == {
        "is_promoted": True,
        "add_new_clusters": True,
    }


def test_growth_sensor_skips_without_an_active_cluster() -> None:
    # An all-noise first run is processed but leaves no live cluster to extend
    result = _evaluate(
        active_clusters=0, run_platforms="mitlearn", total_conversations=2
    )
    assert isinstance(result, SkipReason)


def test_growth_sensor_skips_when_the_live_run_has_another_scope() -> None:
    # The incremental run it would start would be rejected for its scope
    result = _evaluate(
        active_clusters=2, run_platforms="mitxonline", total_conversations=2
    )
    assert isinstance(result, SkipReason)


def test_growth_sensor_skips_while_a_run_is_in_progress() -> None:
    instance = DagsterInstance.ephemeral()
    create_run_for_test(
        instance, job_name="feedback_clusters_job", status=DagsterRunStatus.STARTED
    )
    result = _evaluate(
        instance, active_clusters=2, run_platforms="mitlearn", total_conversations=2
    )
    assert isinstance(result, SkipReason)
