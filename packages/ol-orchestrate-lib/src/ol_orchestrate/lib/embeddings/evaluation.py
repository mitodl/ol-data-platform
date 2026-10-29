"""Score a strategy's retrieval with exact search, and record it in Opik.

The search here is exact cosine similarity in memory, with a document scored
by its best chunk: no index approximation, quantization or fusion, so two
strategies compared this way differ only in chunking, context and model. What a
serving engine adds on top (Qdrant's hybrid fusion, boosts) is measured
separately against a candidate collection.
"""

import json
from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import numpy.typing as npt

# Imported for its side effect, ahead of opik: ol_llm sets OPIK_SENTRY_ENABLE=false,
# without which importing opik replaces the process's Sentry client with Comet's.
import ol_llm  # noqa: F401
import opik
from opik.evaluation.metrics import base_metric, score_result
from pydantic import BaseModel, ConfigDict, Field

from ol_orchestrate.lib.embeddings.documents import md5_hex
from ol_orchestrate.lib.embeddings.models import Embedder
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy

DEFAULT_CUTOFFS = (1, 5, 10)
# Queries scored against the whole corpus at once: 64 x 1M chunk scores is 256 MiB.
QUERY_BLOCK_SIZE = 64


class EvalQuery(BaseModel):
    """A query and the documents that answer it.

    :param query_id: stable across runs, so results line up between strategies
    :param text: the query
    :param relevant_doc_keys: the documents that count as a hit
    """

    model_config = ConfigDict(frozen=True)

    query_id: str
    text: str
    relevant_doc_keys: frozenset[str] = Field(min_length=1)


def recall_at_k(retrieved: Sequence[str], relevant: frozenset[str], k: int) -> float:
    """Return the share of the relevant documents in the first ``k`` retrieved.

    :param retrieved: document keys, best first
    :param relevant: the relevant document keys
    :param k: the cutoff
    :rtype: float
    """
    return len(relevant.intersection(retrieved[:k])) / len(relevant)


def reciprocal_rank(retrieved: Sequence[str], relevant: frozenset[str]) -> float:
    """Return 1 / the rank of the first relevant document, 0 without one.

    :param retrieved: document keys, best first
    :param relevant: the relevant document keys
    :rtype: float
    """
    for rank, doc_key in enumerate(retrieved, start=1):
        if doc_key in relevant:
            return 1 / rank
    return 0.0


def ndcg_at_k(retrieved: Sequence[str], relevant: frozenset[str], k: int) -> float:
    """Return nDCG at ``k`` with binary relevance.

    :param retrieved: document keys, best first
    :param relevant: the relevant document keys
    :param k: the cutoff
    :rtype: float
    """
    gain = sum(
        1 / np.log2(rank + 1)
        for rank, doc_key in enumerate(retrieved[:k], start=1)
        if doc_key in relevant
    )
    ideal = sum(1 / np.log2(rank + 1) for rank in range(1, min(len(relevant), k) + 1))
    return float(gain / ideal)


def exact_search(
    query_vectors: npt.NDArray[np.float32],
    corpus_vectors: npt.NDArray[np.float32],
    corpus_doc_keys: Sequence[str],
    depth: int,
) -> list[list[str]]:
    """Rank documents for each query by the cosine similarity of their best chunk.

    :param query_vectors: one row per query
    :param corpus_vectors: one row per chunk
    :param corpus_doc_keys: the document of each chunk row
    :param depth: documents to return per query
    :returns: per query, up to ``depth`` document keys, best first
    :rtype: list[list[str]]
    :raises ValueError: on no queries, no chunks, a key count that does not
        match the chunk rows, or a depth below 1
    """
    if not len(query_vectors) or not len(corpus_vectors):
        msg = "exact_search needs at least one query and one chunk"
        raise ValueError(msg)
    if len(corpus_doc_keys) != len(corpus_vectors):
        msg = f"{len(corpus_doc_keys)} doc keys for {len(corpus_vectors)} chunks"
        raise ValueError(msg)
    if depth < 1:
        msg = "depth must be at least 1"
        raise ValueError(msg)
    order = np.argsort(np.asarray(corpus_doc_keys), kind="stable")
    keys = np.asarray(corpus_doc_keys)[order]
    # Indexing copies the corpus once; it is normalized in that copy.
    corpus = _normalize_rows(corpus_vectors[order].astype(np.float32, copy=False))
    starts = np.flatnonzero(np.r_[True, keys[1:] != keys[:-1]])
    doc_keys = keys[starts]
    depth = min(depth, len(doc_keys))
    ranked: list[list[str]] = []
    queries = _normalize_rows(query_vectors.astype(np.float32))
    for block_start in range(0, len(queries), QUERY_BLOCK_SIZE):
        block = queries[block_start : block_start + QUERY_BLOCK_SIZE]
        doc_scores = np.maximum.reduceat(block @ corpus.T, starts, axis=1)
        top = np.argpartition(-doc_scores, depth - 1, axis=1)[:, :depth]
        for row, candidates in zip(doc_scores, top, strict=True):
            best_first = candidates[np.argsort(-row[candidates], kind="stable")]
            ranked.append([str(key) for key in doc_keys[best_first]])
    return ranked


def _normalize_rows(vectors: npt.NDArray[np.float32]) -> npt.NDArray[np.float32]:
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1
    vectors /= norms
    return vectors


class QueryScores(BaseModel):
    """One query's ranking and its scores."""

    model_config = ConfigDict(frozen=True)

    query_id: str
    retrieved_doc_keys: list[str]
    scores: dict[str, float]


class RetrievalReport(BaseModel):
    """A strategy's scores on an evaluation set.

    :param eval_set_hash: :func:`eval_set_hash` of the queries the report scored
    :param means: each metric's mean over the queries
    :param query_input_tokens: tokens spent embedding the queries
    """

    model_config = ConfigDict(frozen=True)

    strategy: EmbeddingStrategy
    eval_set_hash: str
    queries: list[QueryScores]
    means: dict[str, float]
    query_input_tokens: int


def score_ranking(
    retrieved: Sequence[str], relevant: frozenset[str], cutoffs: Sequence[int]
) -> dict[str, float]:
    """Score one ranking: recall and nDCG at each cutoff, and MRR at the deepest.

    :param retrieved: document keys, best first
    :param relevant: the relevant document keys
    :param cutoffs: the ``k`` values
    :returns: scores named ``recall@k``, ``ndcg@k`` and ``mrr@k``
    :rtype: dict[str, float]
    """
    depth = max(cutoffs)
    scores = {f"mrr@{depth}": reciprocal_rank(retrieved[:depth], relevant)}
    for k in cutoffs:
        scores[f"recall@{k}"] = recall_at_k(retrieved, relevant, k)
        scores[f"ndcg@{k}"] = ndcg_at_k(retrieved, relevant, k)
    return scores


def evaluate_retrieval(
    strategy: EmbeddingStrategy,
    queries: Sequence[EvalQuery],
    corpus: tuple[Sequence[str], npt.NDArray[np.float32]],
    embedder: Embedder,
    *,
    cutoffs: Sequence[int] = DEFAULT_CUTOFFS,
) -> RetrievalReport:
    """Embed the queries with the strategy's model and score exact search.

    :param strategy: the strategy whose vectors ``corpus`` holds
    :param queries: the evaluation set
    :param corpus: the document key of each chunk, and the chunks' vectors as
        one row each. The search copies the vectors once, so 1M chunks of
        1,024 float32 take about 8 GB in all.
    :param embedder: an embedder for the strategy's model
    :param cutoffs: the ``k`` values to score
    :returns: per-query rankings and scores, and their means
    :rtype: RetrievalReport
    :raises ValueError: when the embedder is not for the strategy's model, there
        are no queries, two queries share an id, there are no cutoffs, or a cutoff
        is below 1
    """
    if embedder.spec != strategy.model:
        msg = f"embedder is for {embedder.spec.model_id}, not {strategy.model.model_id}"
        raise ValueError(msg)
    if not queries:
        msg = "evaluate_retrieval needs at least one query"
        raise ValueError(msg)
    if not cutoffs or min(cutoffs) < 1:
        msg = "cutoffs must be one or more values of at least 1"
        raise ValueError(msg)
    queries_hash = eval_set_hash(queries)
    batch = embedder.embed([query.text for query in queries], input_type="query")
    doc_keys, vectors = corpus
    rankings = exact_search(
        np.asarray(batch.vectors, dtype=np.float32),
        vectors,
        doc_keys,
        depth=max(cutoffs),
    )
    query_scores = [
        QueryScores(
            query_id=query.query_id,
            retrieved_doc_keys=ranking,
            scores=score_ranking(ranking, query.relevant_doc_keys, cutoffs),
        )
        for query, ranking in zip(queries, rankings, strict=True)
    ]
    means = {
        name: float(np.mean([scores.scores[name] for scores in query_scores]))
        for name in query_scores[0].scores
    }
    return RetrievalReport(
        strategy=strategy,
        eval_set_hash=queries_hash,
        queries=query_scores,
        means=means,
        query_input_tokens=batch.input_tokens,
    )


class _RankingMetric(base_metric.BaseMetric):
    """Reports one of a query's precomputed scores as an Opik metric."""

    def __init__(self, metric_name: str) -> None:
        super().__init__(name=metric_name, track=False)

    def score(
        self,
        scores: dict[str, float],
        **ignored: Any,  # noqa: ARG002
    ) -> score_result.ScoreResult:
        return score_result.ScoreResult(name=self.name, value=scores[self.name])


def eval_set_hash(queries: Sequence[EvalQuery]) -> str:
    """Hash one version of an evaluation set: its ids, texts and relevant documents.

    The order of the queries does not change it.

    :param queries: the evaluation set
    :returns: 12 hex digits
    :rtype: str
    :raises ValueError: when two queries share an id, since a query's results are
        looked up by its id
    """
    ids = [query.query_id for query in queries]
    if len(set(ids)) != len(ids):
        msg = "query ids must be unique within an evaluation set"
        raise ValueError(msg)
    items = sorted(
        json.dumps(_dataset_item(query), sort_keys=True) for query in queries
    )
    return md5_hex("\n".join(items))[:12]


def eval_set_dataset_name(eval_set_name: str, queries: Sequence[EvalQuery]) -> str:
    """Name the Opik dataset of one version of an evaluation set.

    ``opik.evaluate`` scores every item in a dataset, so a dataset must hold
    exactly one version of the set: a dropped query would have no ranking, and
    a changed one would be scored twice. The name ends with :func:`eval_set_hash`.

    :param eval_set_name: the evaluation set's name
    :param queries: the evaluation set
    :returns: ``{eval_set_name}-{12 hex digits}``
    :rtype: str
    :raises ValueError: when two queries share an id
    """
    return f"{eval_set_name}-{eval_set_hash(queries)}"


def _dataset_item(query: EvalQuery) -> dict[str, Any]:
    return {
        "query_id": query.query_id,
        "query": query.text,
        "relevant_doc_keys": sorted(query.relevant_doc_keys),
    }


def log_opik_experiment(
    report: RetrievalReport,
    queries: Sequence[EvalQuery],
    *,
    eval_set_name: str,
    project_name: str,
    client_factory: Callable[[], opik.Opik] = opik.Opik,
) -> str:
    """Record a report as an Opik experiment on the evaluation set's dataset.

    The dataset (:func:`eval_set_dataset_name`) holds one item per query, and
    the experiment carries the strategy as its configuration, so Opik's
    experiment comparison lines strategies up query by query.

    :param report: the strategy's report on ``queries``
    :param queries: the evaluation set
    :param eval_set_name: the evaluation set's name
    :param project_name: the Opik project
    :param client_factory: builds the Opik client
    :returns: the experiment's id
    :rtype: str
    :raises ValueError: when the report was not computed on this version of
        ``queries``, or two queries share an id
    """
    if report.eval_set_hash != eval_set_hash(queries):
        msg = "the report's queries are not the evaluation set's"
        raise ValueError(msg)
    by_query = {scores.query_id: scores for scores in report.queries}
    dataset = client_factory().get_or_create_dataset(
        name=eval_set_dataset_name(eval_set_name, queries)
    )
    dataset.insert([_dataset_item(query) for query in queries])
    result = opik.evaluate(
        dataset=dataset,
        task=lambda item: {
            "retrieved_doc_keys": by_query[item["query_id"]].retrieved_doc_keys,
            "scores": by_query[item["query_id"]].scores,
        },
        scoring_metrics=[_RankingMetric(name) for name in report.means],
        experiment_name_prefix=report.strategy.strategy_id,
        project_name=project_name,
        experiment_config={
            **report.strategy.model_dump(mode="json"),
            "model_id": report.strategy.model.model_id,
            "chunker_id": report.strategy.chunker.chunker_id,
            "query_input_tokens": report.query_input_tokens,
        },
        verbose=0,
    )
    return result.experiment_id
