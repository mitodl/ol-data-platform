"""Retrieval metrics, exact search, and the Opik experiment they are logged as."""

import os
import subprocess
import sys
import textwrap
from collections.abc import Sequence
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from ol_orchestrate.lib.embeddings.chunking import WholeDocumentChunker
from ol_orchestrate.lib.embeddings.evaluation import (
    EvalQuery,
    eval_set_dataset_name,
    evaluate_retrieval,
    exact_search,
    log_opik_experiment,
    ndcg_at_k,
    recall_at_k,
    reciprocal_rank,
)
from ol_orchestrate.lib.embeddings.models import (
    EmbeddingBatch,
    EmbeddingModelSpec,
)
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy
from pydantic_ai.embeddings.result import EmbedInputType

STRATEGY = EmbeddingStrategy(
    strategy_id="exp_whole",
    doc_kind="page",
    chunker=WholeDocumentChunker(encoding="cl100k_base", max_tokens=100),
    model=EmbeddingModelSpec(model="openai:text-embedding-3-large", dimensions=2),
)
CORPUS_KEYS = ["b", "a", "c", "a"]
CORPUS_VECTORS = np.asarray(
    [[0.7, 0.7], [1.0, 0.0], [-1.0, 0.0], [0.0, 1.0]], dtype=np.float32
)
CORPUS = (CORPUS_KEYS, CORPUS_VECTORS)
QUERY_VECTORS = {"up": [0.0, 2.0], "right": [3.0, 0.0]}
QUERIES = [
    EvalQuery(query_id="q1", text="up", relevant_doc_keys=frozenset({"b"})),
    EvalQuery(query_id="q2", text="right", relevant_doc_keys=frozenset({"a", "c"})),
]


class QueryEmbedder:
    """Looks each query's vector up by its text."""

    spec = STRATEGY.model

    def __init__(self) -> None:
        self.input_types: list[EmbedInputType] = []

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        self.input_types.append(input_type)
        return EmbeddingBatch(
            vectors=[QUERY_VECTORS[text] for text in texts], input_tokens=len(texts)
        )


def test_metrics() -> None:
    relevant = frozenset({"a", "c"})
    assert recall_at_k(["x", "a", "y", "c"], relevant, 2) == 0.5
    assert recall_at_k(["x", "a", "y", "c"], relevant, 4) == 1.0
    assert reciprocal_rank(["x", "a"], relevant) == 0.5
    assert reciprocal_rank(["x", "y"], relevant) == 0.0
    assert ndcg_at_k(["a", "c", "x"], relevant, 3) == pytest.approx(1.0)
    assert ndcg_at_k(["x", "a"], relevant, 2) == pytest.approx(
        (1 / np.log2(3)) / (1 + 1 / np.log2(3))
    )


def test_a_document_scores_by_its_best_chunk() -> None:
    corpus = CORPUS_VECTORS.copy()
    rankings = exact_search(
        np.asarray([[0.0, 2.0], [3.0, 0.0]], dtype=np.float32),
        corpus,
        CORPUS_KEYS,
        depth=10,
    )
    assert rankings == [["a", "b", "c"], ["a", "b", "c"]]
    np.testing.assert_array_equal(corpus, CORPUS_VECTORS)


def test_search_returns_at_most_depth_documents() -> None:
    rankings = exact_search(
        np.asarray([[-1.0, 0.0]], dtype=np.float32),
        CORPUS_VECTORS,
        CORPUS_KEYS,
        depth=1,
    )
    assert rankings == [["c"]]


def test_search_rejects_empty_inputs() -> None:
    query = np.asarray([[1.0, 0.0]], dtype=np.float32)
    with pytest.raises(ValueError, match="at least one query and one chunk"):
        exact_search(query, np.empty((0, 2), dtype=np.float32), [], depth=1)
    with pytest.raises(ValueError, match="doc keys for"):
        exact_search(query, CORPUS_VECTORS, ["a"], depth=1)
    with pytest.raises(ValueError, match="depth"):
        exact_search(query, CORPUS_VECTORS, CORPUS_KEYS, depth=0)
    with pytest.raises(ValueError, match="at least one query"):
        evaluate_retrieval(STRATEGY, [], CORPUS, QueryEmbedder())
    with pytest.raises(ValueError, match="cutoffs"):
        evaluate_retrieval(STRATEGY, QUERIES, CORPUS, QueryEmbedder(), cutoffs=(0,))


def test_evaluate_retrieval_embeds_queries_as_queries() -> None:
    embedder = QueryEmbedder()

    report = evaluate_retrieval(STRATEGY, QUERIES, CORPUS, embedder, cutoffs=(1, 3))

    assert embedder.input_types == ["query"]
    assert [scores.retrieved_doc_keys for scores in report.queries] == [
        ["a", "b", "c"],
        ["a", "b", "c"],
    ]
    assert report.queries[0].scores["recall@1"] == 0.0
    assert report.queries[0].scores["mrr@3"] == 0.5
    assert report.queries[1].scores["recall@3"] == 1.0
    assert report.means["recall@3"] == 1.0
    assert report.means["mrr@3"] == 0.75
    assert report.query_input_tokens == len(QUERIES)


def test_report_is_logged_as_an_opik_experiment() -> None:
    report = evaluate_retrieval(
        STRATEGY, QUERIES, CORPUS, QueryEmbedder(), cutoffs=(1,)
    )
    client = MagicMock()
    dataset = client.get_or_create_dataset.return_value

    with patch("opik.evaluate") as evaluate:
        evaluate.return_value.experiment_id = "exp-1"
        experiment_id = log_opik_experiment(
            report,
            QUERIES,
            eval_set_name="content-files-synthetic",
            project_name="ol-data-platform",
            client_factory=lambda: client,
        )

    assert experiment_id == "exp-1"
    dataset_name = eval_set_dataset_name("content-files-synthetic", QUERIES)
    client.get_or_create_dataset.assert_called_once_with(name=dataset_name)
    dataset.insert.assert_called_once_with(
        [
            {"query_id": "q1", "query": "up", "relevant_doc_keys": ["b"]},
            {"query_id": "q2", "query": "right", "relevant_doc_keys": ["a", "c"]},
        ]
    )
    kwargs = evaluate.call_args.kwargs
    assert kwargs["dataset"] is dataset
    assert kwargs["experiment_name_prefix"] == "exp_whole"
    assert kwargs["project_name"] == "ol-data-platform"
    assert kwargs["experiment_config"]["model_id"] == "openai:text-embedding-3-large@2"
    assert kwargs["experiment_config"]["chunker_id"] == "whole:cl100k_base:100"
    output = kwargs["task"]({"query_id": "q2", "query": "right"})
    assert output["retrieved_doc_keys"] == ["a"]
    scores = {
        metric.name: metric.score(**output).value
        for metric in kwargs["scoring_metrics"]
    }
    assert scores == {"mrr@1": 1.0, "recall@1": 0.5, "ndcg@1": 1.0}


def test_each_version_of_an_eval_set_gets_its_own_dataset() -> None:
    name = eval_set_dataset_name("set", QUERIES)
    assert name == eval_set_dataset_name("set", list(reversed(QUERIES)))
    assert name.startswith("set-")
    changed = [
        QUERIES[0],
        EvalQuery(query_id="q2", text="right", relevant_doc_keys=frozenset({"a"})),
    ]
    assert eval_set_dataset_name("set", changed) != name
    assert eval_set_dataset_name("set", QUERIES[:1]) != name


def test_a_report_on_other_queries_is_not_logged() -> None:
    report = evaluate_retrieval(STRATEGY, QUERIES[:1], CORPUS, QueryEmbedder())
    with pytest.raises(ValueError, match="not the evaluation set's"):
        log_opik_experiment(
            report,
            QUERIES,
            eval_set_name="set",
            project_name="ol-data-platform",
            client_factory=MagicMock,
        )


def test_a_report_on_an_older_version_of_the_set_is_not_logged() -> None:
    report = evaluate_retrieval(STRATEGY, QUERIES, CORPUS, QueryEmbedder())
    relabeled = [
        QUERIES[0],
        EvalQuery(query_id="q2", text="right", relevant_doc_keys=frozenset({"a"})),
    ]
    with pytest.raises(ValueError, match="not the evaluation set's"):
        log_opik_experiment(
            report,
            relabeled,
            eval_set_name="set",
            project_name="ol-data-platform",
            client_factory=MagicMock,
        )


def test_query_ids_must_be_unique() -> None:
    duplicated = [
        QUERIES[0],
        EvalQuery(query_id="q1", text="right", relevant_doc_keys=frozenset({"a"})),
    ]
    with pytest.raises(ValueError, match="unique"):
        evaluate_retrieval(STRATEGY, duplicated, CORPUS, QueryEmbedder())
    with pytest.raises(ValueError, match="unique"):
        eval_set_dataset_name("set", duplicated)


def test_import_keeps_the_host_sentry_client() -> None:
    """Importing the module leaves the process's Sentry client in place.

    In a subprocess, because opik skips its Sentry setup when pytest is imported,
    and without the OPIK_SENTRY_ENABLE this process already got from ol_llm.
    """
    probe = textwrap.dedent(
        """
        import sentry_sdk

        sentry_sdk.init(dsn="https://public@example.invalid/1")
        host = sentry_sdk.get_client()

        import ol_orchestrate.lib.embeddings.evaluation

        assert sentry_sdk.get_client() is host, sentry_sdk.get_client().dsn
        """
    )
    env = {k: v for k, v in os.environ.items() if k != "OPIK_SENTRY_ENABLE"}
    # So a regression's client reports nowhere instead of to Comet.
    env["OPIK_SENTRY_DSN"] = "https://public@example.invalid/2"
    subprocess.run([sys.executable, "-c", probe], check=True, env=env)  # noqa: S603
