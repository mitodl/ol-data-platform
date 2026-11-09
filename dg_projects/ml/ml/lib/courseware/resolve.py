"""Resolve extracted concept labels onto stable concept identities.

A concept is an opaque UUID minted once and never reused. Labels (normalized
`concept_label_key`s from int__courseware__concept_labels) are assigned to concepts,
and which label a concept displays is decided downstream from its members, so a
rename never changes an id. When two concepts turn out to be one, the loser keeps
its id with status `merged` and a redirect to the survivor. Nothing is deleted.

Each run reads the previous run's registry and assignments and resolves only the
labels that have no assignment yet:

1. Blocking. Every label is embedded (label, kind and description) and indexed
   by character trigrams. Each new label gets its nearest neighbours from both,
   among all labels, as its candidate list.
2. Adjudication. A model sees a few new labels at a time, each with its candidates,
   and gives a SKOS-style verdict per candidate: exact, close, broader, narrower,
   related or distinct. Small listwise batches, not pairwise yes/no calls and not
   one prompt over the whole corpus.
3. Decision. Only `exact` verdicts merge. Exact links are joined into components,
   and a component is accepted only if it is small and none of its members were
   judged distinct from each other, which stops one bad link from chaining
   unrelated labels together. Close, broader, narrower and related become concept
   relations for review.

`rebuild=True` re-resolves every label from scratch and then carries ids forward:
each new cluster takes the id of the previous concept it shares the most members
with (Jaccard at least CARRY_FORWARD_MIN_JACCARD), so a full re-run does not churn
the ids every downstream join depends on.

A label no current claim names keeps its assignment through incremental runs, so it
gets the same id back if it returns. A rebuild drops such labels and retires any
concept they leave with no members.

A label's concept is its primary sense. senses.py then assigns each label as each
course run uses it to one of the label's senses, splitting a new sense off the
primary one when a course means something else by the label; claims join to
concepts through those sense assignments, not through the label alone. Merges are
recorded as merged_into_concept_id and splits as split_from_concept_id.
"""

import hashlib
import json
import logging
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import batched
from typing import Any, Literal

import numpy as np
import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.schemas import Strict
from ml.lib.courseware.senses import (
    ALIAS_SCHEMA,
    SENSE_ASSIGNMENT_SCHEMA,
    SENSE_PROMPT_VERSION,
    assign_senses,
    label_aliases,
    sense_units,
)
from ml.lib.courseware.tracing import TRACE_TAGS, attach_call
from ml.lib.embed import resolve_embedding_model
from ml.resources.opik_auth import OPIK_PROJECT_NAME, traced
from ol_llm import Attribution, UseCase
from ol_orchestrate.lib.embeddings.models import Embedder, OlLlmEmbedder
from pydantic import ValidationError

log = logging.getLogger(__name__)

RESOLVER_ID = "courseware_resolution_v2"
# Adjudication batches are short and many, so the small model is the default.
ADJUDICATION_MODEL = "claude-haiku-5-5"
BEDROCK_ADJUDICATION_MODEL = "us.anthropic.claude-haiku-5-5"
EMBEDDING_DIM = 512
EMBEDDING_MODEL = "bedrock:amazon.titan-embed-text-v2:0"
# Candidates per new label from each blocker; the union goes to the model.
EMBEDDING_NEIGHBOURS = 8
LEXICAL_NEIGHBOURS = 4
# Below this character-trigram Jaccard a lexical neighbour is noise.
LEXICAL_MIN_JACCARD = 0.3
# New labels per adjudication call. Listwise LLM clustering degrades above roughly
# ten items per decision (LLM-CER, SIGMOD 2026); each label here brings up to twelve
# candidates, so a call stays at a few dozen short lines.
QUERIES_PER_CALL = 8
# An exact-match component that joins more than this many concepts and new labels
# is not merged automatically. An existing concept counts once, however many of its
# labels the component holds.
MAX_MERGE_COMPONENT = 6
CARRY_FORWARD_MIN_JACCARD = 0.5
ADJUDICATION_MAX_TOKENS = 16_000
ADJUDICATION_EFFORT = "medium"

Verdict = Literal["exact", "close", "broader", "narrower", "related", "distinct"]

ADJUDICATION_PROMPT = """\
You maintain the concept index for a catalog of university courses. Each query \
below is a concept label extracted from course material, with candidate labels \
that may name the same concept. Every label comes with its kind and a note on how \
one course used it.

For every candidate of every query, give one verdict describing the QUERY relative \
to the candidate:
- exact: the same concept; a learner searching for one wants the other (spelling, \
plural, abbreviation, word order, a qualifier that adds nothing, e.g. "k-means" and \
"k-means clustering")
- close: nearly the same and often used interchangeably, but not identical (e.g. \
"recall" and "true positive rate" in some fields)
- narrower: the query is a kind or part of the candidate (e.g. "Gini impurity" \
relative to "decision tree")
- broader: the candidate is a kind or part of the query
- related: clearly connected but neither the same nor a kind of the other \
(e.g. a method and the tool that implements it)
- distinct: unrelated, or only superficially similar

Do not call a tool, library or dataset exact to the method or concept it implements \
or illustrates. When unsure between exact and close, choose close.

Call record_verdicts once with a verdict for every candidate of every query.
"""


class CandidateVerdict(Strict):
    candidate_id: int
    verdict: Verdict


class QueryVerdicts(Strict):
    query_id: int
    verdicts: list[CandidateVerdict]


class AdjudicationBatch(Strict):
    queries: list[QueryVerdicts]


ADJUDICATION_TOOL = {
    "name": "record_verdicts",
    "description": "Record a verdict for every candidate of every query. Call once.",
    "input_schema": AdjudicationBatch.model_json_schema(),
}

PROMPT_VERSION = hashlib.sha256(
    json.dumps([ADJUDICATION_PROMPT, AdjudicationBatch.model_json_schema()]).encode()
).hexdigest()[:12]

REGISTRY_SCHEMA = {
    "resolution_run_id": pl.String,
    "concept_id": pl.String,
    "concept_status": pl.String,
    "merged_into_concept_id": pl.String,
    "minted_label": pl.String,
    "minted_run_id": pl.String,
    "minted_at": pl.Datetime(time_zone="UTC"),
    "split_from_concept_id": pl.String,
}
ASSIGNMENT_SCHEMA = {
    "resolution_run_id": pl.String,
    "concept_label_key": pl.String,
    "concept_id": pl.String,
    "assignment_source": pl.String,
    "assigned_run_id": pl.String,
}
RELATION_SCHEMA = {
    "resolution_run_id": pl.String,
    "from_concept_id": pl.String,
    "to_concept_id": pl.String,
    "relation": pl.String,
    "proposed_run_id": pl.String,
}
DECISION_SCHEMA = {
    "resolution_run_id": pl.String,
    "query_label_key": pl.String,
    "candidate_label_key": pl.String,
    "verdict": pl.String,
    "model_id": pl.String,
}
EMBEDDING_SCHEMA = {
    "concept_label_key": pl.String,
    "text_checksum": pl.String,
    "embedding_model_id": pl.String,
    "embedding": pl.List(pl.Float32),
}
RUN_SCHEMA = {
    "resolution_run_id": pl.String,
    "resolver_id": pl.String,
    "prompt_version": pl.String,
    "model_id": pl.String,
    "embedding_model_id": pl.String,
    "mode": pl.String,
    "label_count": pl.Int64,
    "resolved_label_count": pl.Int64,
    "adjudication_calls": pl.Int64,
    "failed_batch_count": pl.Int64,
    "failed_label_count": pl.Int64,
    "concepts_minted": pl.Int64,
    "concepts_merged": pl.Int64,
    "concepts_deprecated": pl.Int64,
    "concepts_carried_forward": pl.Int64,
    "rejected_components": pl.Int64,
    "started_at": pl.Datetime(time_zone="UTC"),
    "sense_prompt_version": pl.String,
    "sense_calls": pl.Int64,
    "senses_minted": pl.Int64,
    "sense_review_count": pl.Int64,
}


@dataclass
class ResolutionState:
    """The previous run's snapshot, or empty for the first run."""

    registry: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=REGISTRY_SCHEMA)
    )
    assignments: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=ASSIGNMENT_SCHEMA)
    )
    relations: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=RELATION_SCHEMA)
    )
    embeddings: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=EMBEDDING_SCHEMA)
    )
    aliases: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=ALIAS_SCHEMA)
    )
    sense_assignments: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=SENSE_ASSIGNMENT_SCHEMA)
    )


@dataclass(frozen=True)
class ResolutionResult:
    """Full snapshots of registry, label assignments, aliases, sense assignments and
    relations for this run, plus the run's new label and sense decisions, its new
    embeddings and its run record.
    """

    registry: pl.DataFrame
    assignments: pl.DataFrame
    aliases: pl.DataFrame
    sense_assignments: pl.DataFrame
    relations: pl.DataFrame
    decisions: pl.DataFrame
    sense_decisions: pl.DataFrame
    embeddings: pl.DataFrame
    run: pl.DataFrame


# --------------------------------------------------------------------------- blocking
def _label_text(row: dict[str, Any]) -> str:
    description = (row["description"] or "")[:240]
    return (
        f"{row['preferred_label']} ({row['concept_kind'] or 'concept'}): {description}"
    )


def _trigrams(key: str) -> set[str]:
    padded = f"  {key} "
    return {padded[index : index + 3] for index in range(len(padded) - 2)}


def lexical_neighbours(
    keys: list[str], queries: Iterable[str], k: int
) -> dict[str, list[str]]:
    """Top-k labels by character-trigram Jaccard, for each query key."""
    grams = {key: _trigrams(key) for key in keys}
    index: dict[str, list[str]] = defaultdict(list)
    for key, key_grams in grams.items():
        for gram in key_grams:
            index[gram].append(key)
    neighbours = {}
    for query in queries:
        shared: Counter[str] = Counter()
        for gram in grams[query]:
            shared.update(index[gram])
        shared.pop(query, None)
        scored = sorted(
            (
                (count / len(grams[query] | grams[other]), other)
                for other, count in shared.items()
            ),
            reverse=True,
        )
        neighbours[query] = [
            other for score, other in scored[:k] if score >= LEXICAL_MIN_JACCARD
        ]
    return neighbours


def embedding_neighbours(
    keys: list[str], vectors: np.ndarray, queries: list[str], k: int
) -> dict[str, list[str]]:
    """Top-k labels by cosine similarity, for each query key.

    Brute force over normalized vectors in chunks, which is fine to tens of
    thousands of labels; past that this becomes an ANN index (hnswlib, Qdrant).
    """
    position = {key: index for index, key in enumerate(keys)}
    neighbours = {}
    for chunk in batched(queries, 1024):
        rows = np.array([position[key] for key in chunk])
        scores = vectors[rows] @ vectors.T
        scores[np.arange(len(rows)), rows] = -np.inf
        top = np.argpartition(-scores, min(k, len(keys) - 1), axis=1)[:, :k]
        for row_index, key in enumerate(chunk):
            ordered = sorted(top[row_index], key=lambda col: -scores[row_index, col])
            # with k or fewer other labels the slice also holds the query's own column
            neighbours[key] = [keys[col] for col in ordered if col != rows[row_index]]
    return neighbours


def label_embedder(run_id: str | None = None) -> OlLlmEmbedder:
    """Return the ol-llm embedder for concept labels (Titan v2 at EMBEDDING_DIM).

    :param run_id: the Dagster run, for the traces; None off the asset path
    :rtype: OlLlmEmbedder
    """
    model = resolve_embedding_model(EMBEDDING_MODEL, dim=EMBEDDING_DIM)
    return OlLlmEmbedder(
        model.spec,
        UseCase(
            app=OPIK_PROJECT_NAME,
            name="courseware-concept-embedding",
            profile="batch",
            model=model.model,
        ),
        Attribution(run_id=run_id),
    )


def embed_labels(
    labels: pl.DataFrame,
    cached: pl.DataFrame,
    client: Embedder,
    max_workers: int = 16,
) -> tuple[np.ndarray, pl.DataFrame]:
    """Embed every label whose text changed since it was cached.

    :returns: an L2-normalized matrix in `labels` row order, and the new cache rows
    """
    if labels.is_empty():
        return np.zeros((0, 0), dtype=np.float32), pl.DataFrame(schema=EMBEDDING_SCHEMA)
    texts = {
        row["concept_label_key"]: _label_text(row)
        for row in labels.iter_rows(named=True)
    }
    checksums = {
        key: hashlib.sha256(text.encode()).hexdigest() for key, text in texts.items()
    }
    model_id = client.spec.model_id
    known = {
        row["concept_label_key"]: row["embedding"]
        for row in cached.filter(pl.col("embedding_model_id") == model_id).iter_rows(
            named=True
        )
        if checksums.get(row["concept_label_key"]) == row["text_checksum"]
    }
    missing = [key for key in texts if key not in known]
    # Titan embeds one text per request, so requests run in parallel slices.
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        slices = list(batched(missing, client.spec.batch_size))
        results = pool.map(
            lambda keys: (
                client.embed(
                    [texts[key] for key in keys], input_type="document"
                ).vectors
            ),
            slices,
        )
        for keys, vectors in zip(slices, results, strict=True):
            known.update(zip(keys, vectors, strict=True))
    new_rows = pl.DataFrame(
        [
            {
                "concept_label_key": key,
                "text_checksum": checksums[key],
                "embedding_model_id": model_id,
                "embedding": known[key],
            }
            for key in missing
        ],
        schema=EMBEDDING_SCHEMA,
    )
    matrix = np.array(
        [known[key] for key in labels["concept_label_key"]], dtype=np.float32
    )
    matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix, new_rows


# --------------------------------------------------------------------- adjudication
@traced(
    "courseware_adjudicate_batch",
    tags=TRACE_TAGS,
    ignore_arguments=["client", "rows", "trace_metadata"],
)
def _adjudicate_batch(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    batch: list[tuple[str, list[str]]],
    rows: dict[str, dict[str, Any]],
    *,
    trace_metadata: dict[str, Any],
) -> list[tuple[str, str, str]]:
    """Return (query_key, candidate_key, verdict) for one batch of queries."""

    def line(key: str) -> str:
        row = rows[key]
        description = (row["description"] or "")[:160]
        kind = row["concept_kind"] or "unknown"
        return f"{row['preferred_label']} | {kind} | {description}"

    listing = "\n\n".join(
        f"Q{query_index}: {line(query)}\n"
        + "\n".join(
            f"  c{candidate_index}: {line(candidate)}"
            for candidate_index, candidate in enumerate(candidates)
        )
        for query_index, (query, candidates) in enumerate(batch)
    )
    with client.messages.stream(
        model=model_id,
        max_tokens=ADJUDICATION_MAX_TOKENS,
        system=ADJUDICATION_PROMPT,
        tools=[ADJUDICATION_TOOL],
        tool_choice={"type": "auto"},
        messages=[{"role": "user", "content": listing}],
        output_config={"effort": ADJUDICATION_EFFORT},
    ) as stream:
        response = stream.get_final_message()
    attach_call(
        client,
        model_id,
        [response.usage],
        template=ADJUDICATION_PROMPT,
        metadata=trace_metadata,
    )
    tool_input = next(
        (
            block.input
            for block in response.content
            if block.type == "tool_use" and block.name == ADJUDICATION_TOOL["name"]
        ),
        None,
    )
    if tool_input is None:
        msg = f"no verdicts (stop_reason={response.stop_reason})"
        raise ValueError(msg)
    try:
        verdicts = AdjudicationBatch.model_validate(tool_input)
    except ValidationError as error:
        msg = f"invalid verdicts: {error}"
        raise ValueError(msg) from error
    # An omitted comparison would leave a label a singleton that incremental runs
    # never retry, so anything but one verdict per pair fails the batch.
    returned = sorted(query.query_id for query in verdicts.queries)
    if returned != list(range(len(batch))):
        msg = f"verdicts for queries {returned}, expected 0 to {len(batch) - 1}"
        raise ValueError(msg)
    decided: list[tuple[str, str, str]] = []
    for query in verdicts.queries:
        query_key, candidates = batch[query.query_id]
        judged = sorted(item.candidate_id for item in query.verdicts)
        if judged != list(range(len(candidates))):
            msg = (
                f"Q{query.query_id} verdicts for candidates {judged}, "
                f"expected 0 to {len(candidates) - 1}"
            )
            raise ValueError(msg)
        decided.extend(
            (query_key, candidates[item.candidate_id], item.verdict)
            for item in query.verdicts
        )
    return decided


def adjudicate(  # noqa: PLR0913
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    candidates: dict[str, list[str]],
    rows: dict[str, dict[str, Any]],
    max_workers: int = 6,
    *,
    run_id: str,
) -> tuple[list[tuple[str, str, str]], int, list[list[str]]]:
    """Adjudicate every query's candidates in small batches.

    :param run_id: the resolution run, recorded on each call's Opik span
    :returns: the decisions, how many calls were made, and the query keys of each
        batch that failed (left unassigned, so the next run retries them; a
        rebuild keeps their previous concept)
    """
    work = [(query, found) for query, found in candidates.items() if found]
    batches = [list(batch) for batch in batched(work, QUERIES_PER_CALL)]
    decisions: list[tuple[str, str, str]] = []
    failed: list[list[str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                _adjudicate_batch,
                client,
                model_id,
                batch,
                rows,
                trace_metadata={
                    "resolution_run_id": run_id,
                    "batch_ordinal": ordinal,
                    "query_count": len(batch),
                    "prompt_version": PROMPT_VERSION,
                    "effort": ADJUDICATION_EFFORT,
                },
            )
            for ordinal, batch in enumerate(batches)
        ]
        for batch, future in zip(batches, futures, strict=True):
            try:
                decisions.extend(future.result())
            except (ValueError, APIError):
                log.exception("Adjudication batch of %d labels failed", len(batch))
                failed.append([query for query, _ in batch])
    return decisions, len(batches), failed


# --------------------------------------------------------------------------- decisions
class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        self.parent.setdefault(item, item)
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: str, right: str) -> None:
        self.parent[self.find(left)] = self.find(right)

    def components(self, items: Iterable[str]) -> list[set[str]]:
        groups: dict[str, set[str]] = defaultdict(set)
        for item in items:
            groups[self.find(item)].add(item)
        return list(groups.values())


def accepted_components(
    keys: Iterable[str],
    decisions: list[tuple[str, str, str]],
    carried: dict[str, str] | None = None,
) -> tuple[list[set[str]], int]:
    """Join exact verdicts into components, keeping only components that are small
    and contain no pair judged distinct; a rejected component falls apart into
    singletons for review.

    A component joins the concepts its carried labels already belong to, so the
    distinct check covers every label that ends up in those concepts: their carried
    labels and every other component that joins one of them.

    :param keys: the labels to group; a decision naming any other label is ignored
    :param decisions: (query, candidate, verdict) triples
    :param carried: each carried label's current concept id
    :returns: the components covering every key, and how many were rejected
    """
    union_find = _UnionFind()
    keys = list(keys)
    for key in keys:
        union_find.find(key)
    known = set(keys)
    distinct: dict[str, set[str]] = defaultdict(set)
    for query, candidate, verdict in decisions:
        # a label left out of keys (its batch failed) must not come back in as
        # another label's candidate
        if query not in known or candidate not in known:
            continue
        if verdict == "exact":
            union_find.union(query, candidate)
        elif verdict == "distinct":
            distinct[query].add(candidate)
            distinct[candidate].add(query)
    carried = carried or {}
    found = union_find.components(keys)
    components: list[set[str]] = []
    rejected = 0
    for component, destination in zip(
        found, _destinations(found, carried), strict=True
    ):
        # a single label has nothing to merge, whatever its destination holds
        conflicted = len(component) > 1 and any(
            distinct[key] & destination for key in destination
        )
        # labels of one existing concept are one unit, not a chain of merges
        size = len({carried.get(key, key) for key in component})
        if size > MAX_MERGE_COMPONENT or conflicted:
            rejected += 1
            components.extend({key} for key in component)
        else:
            components.append(component)
    return components, rejected


def _destinations(
    components: list[set[str]], carried: dict[str, str]
) -> list[set[str]]:
    """Every label each component's concept will hold once all components are
    assigned: a component that joins concepts X and Y merges them, so it shares a
    destination with every component joining X or Y.
    """
    concept_groups = _UnionFind()
    joined = [
        [carried[key] for key in component if key in carried]
        for component in components
    ]
    for concepts in joined:
        for concept_id in concepts:
            concept_groups.union(concept_id, concepts[0])
    labels: dict[str, set[str]] = defaultdict(set)
    for key, concept_id in carried.items():
        labels[concept_groups.find(concept_id)].add(key)
    for component, concepts in zip(components, joined, strict=True):
        if concepts:
            labels[concept_groups.find(concepts[0])] |= component
    return [
        labels[concept_groups.find(concepts[0])] if concepts else component
        for component, concepts in zip(components, joined, strict=True)
    ]


def _resolve_redirects(registry: dict[str, dict[str, Any]]) -> None:
    """Point every merged concept straight at its active survivor."""
    for record in registry.values():
        target = record["merged_into_concept_id"]
        seen = set()
        while (
            target
            and registry[target]["concept_status"] == "merged"
            and target not in seen
        ):
            seen.add(target)
            target = registry[target]["merged_into_concept_id"]
        record["merged_into_concept_id"] = target


def resolve_concepts(  # noqa: PLR0912, PLR0913, PLR0915, C901
    labels: pl.DataFrame,
    state: ResolutionState,
    client: Anthropic | AnthropicBedrock,
    embedding_client: Embedder,
    *,
    mentions: pl.DataFrame,
    model_id: str,
    rebuild: bool = False,
) -> ResolutionResult:
    """Run one resolution over the current label list, then assign senses.

    :param labels: int__courseware__concept_labels rows (concept_label_key,
        preferred_label, concept_kind, description, mention_count)
    :param mentions: int__courseware__concept_sense_inputs rows, one per mention
        with its course run, course title and claim description
    :param state: the previous run's snapshots
    :param client: an Anthropic or AnthropicBedrock client for adjudication
    :param embedding_client: the label embedder
    :param model_id: the adjudication model
    :param rebuild: re-resolve every label, carrying ids forward by member overlap
    :returns: this run's snapshots and new rows
    :rtype: ResolutionResult
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(tz=UTC)
    labels = labels.sort("concept_label_key")
    rows = {row["concept_label_key"]: row for row in labels.iter_rows(named=True)}
    keys = list(rows)
    mention_counts = {key: row["mention_count"] for key, row in rows.items()}

    registry = {
        # registry snapshots written before senses have no split_from_concept_id
        row["concept_id"]: {
            "split_from_concept_id": None,
            **row,
            "resolution_run_id": run_id,
        }
        for row in state.registry.iter_rows(named=True)
    }
    previous_all = {
        row["concept_label_key"]: row for row in state.assignments.iter_rows(named=True)
    }
    previous = {key: row for key, row in previous_all.items() if key in rows}

    def active(concept_id: str) -> str:
        # follows merges made earlier in this run too, so a later component sees
        # two concepts an earlier one joined as one
        while registry[concept_id]["concept_status"] == "merged":
            concept_id = registry[concept_id]["merged_into_concept_id"]
        return concept_id

    carried = (
        {}
        if rebuild
        else {key: active(row["concept_id"]) for key, row in previous.items()}
    )
    pending = [key for key in keys if key not in carried]

    vectors, new_embeddings = embed_labels(labels, state.embeddings, embedding_client)
    by_embedding = embedding_neighbours(keys, vectors, pending, EMBEDDING_NEIGHBOURS)
    by_trigram = lexical_neighbours(keys, pending, LEXICAL_NEIGHBOURS)
    candidates = {
        key: list(dict.fromkeys(by_embedding[key] + by_trigram[key])) for key in pending
    }
    decisions, calls, failed_batches = adjudicate(
        client, model_id, candidates, rows, run_id=run_id
    )
    failed = {key for batch in failed_batches for key in batch}

    components, rejected = accepted_components(
        [key for key in keys if key not in failed or key in carried],
        decisions,
        carried,
    )
    assignments: dict[str, tuple[str, str]] = {
        key: (concept_id, "carried") for key, concept_id in carried.items()
    }
    minted = merged = carried_forward = 0

    def mint(component: set[str]) -> str:
        nonlocal minted
        concept_id = str(uuid.uuid4())
        label = rows[max(component, key=lambda key: (mention_counts[key], key))][
            "preferred_label"
        ]
        registry[concept_id] = {
            "resolution_run_id": run_id,
            "concept_id": concept_id,
            "concept_status": "active",
            "merged_into_concept_id": None,
            "minted_label": label,
            "minted_run_id": run_id,
            "minted_at": started_at,
            "split_from_concept_id": None,
        }
        minted += 1
        return concept_id

    previous_members: dict[str, set[str]] = defaultdict(set)
    for key, row in previous.items():
        previous_members[active(row["concept_id"])].add(key)
    taken: set[str] = set()

    for component in components:
        current = {key: active(carried[key]) for key in component if key in carried}
        existing = set(current.values())
        if existing:
            # the most-mentioned existing concept survives; the others redirect to it
            survivor = max(
                existing,
                key=lambda concept_id: (
                    sum(
                        mention_counts[key]
                        for key in component
                        if current.get(key) == concept_id
                    ),
                    concept_id,
                ),
            )
            for loser in existing - {survivor}:
                registry[loser]["concept_status"] = "merged"
                registry[loser]["merged_into_concept_id"] = survivor
                merged += 1
            for key in component:
                assignments[key] = (
                    survivor,
                    "carried" if current.get(key) == survivor else "exact_match",
                )
            continue
        concept_id = None
        if rebuild:
            # carry the id of the previous concept this cluster overlaps most
            best = max(
                (
                    (len(component & members) / len(component | members), previous_id)
                    for previous_id, members in previous_members.items()
                    if previous_id not in taken and component & members
                ),
                default=(0.0, None),
            )
            if best[1] and best[0] >= CARRY_FORWARD_MIN_JACCARD:
                concept_id = best[1]
                taken.add(concept_id)
                carried_forward += 1
        source = "rebuild_carried" if concept_id else "minted"
        concept_id = concept_id or mint(component)
        for key in component:
            assignments[key] = (
                concept_id,
                source if len(component) == 1 else "exact_match",
            )

    if not rebuild:
        # a label no current claim names keeps its concept, so it gets the same id
        # back if it returns; only a rebuild retires a concept left without labels
        for key, row in previous_all.items():
            if key not in rows:
                assignments[key] = (row["concept_id"], "carried")

    deprecated = 0
    if rebuild:
        # a label whose batch failed was not re-resolved, so it keeps its concept
        for key in failed - assignments.keys():
            if key in previous:
                assignments[key] = (active(previous[key]["concept_id"]), "carried")
        assigned_ids = {concept_id for concept_id, _ in assignments.values()}
        # a sense split off a label that still has claims keeps its concept
        assigned_ids.update(
            active(row["concept_id"])
            for row in state.aliases.filter(
                (pl.col("sense_role") == "split")
                & pl.col("concept_label_key").is_in(keys)
            ).iter_rows(named=True)
        )
        # every active concept, including one whose labels are all gone from `labels`
        for previous_id, record in registry.items():
            if record["concept_status"] != "active" or previous_id in assigned_ids:
                continue
            # retire it into whichever concept took most of its members
            successor = Counter(
                assignments[key][0] for key in previous_members[previous_id]
            ).most_common(1)
            registry[previous_id]["concept_status"] = (
                "merged" if successor else "deprecated"
            )
            registry[previous_id]["merged_into_concept_id"] = (
                successor[0][0] if successor else None
            )
            if successor:
                merged += 1
            else:
                deprecated += 1
    _resolve_redirects(registry)

    def resolved(concept_id: str) -> str:
        return active(concept_id)

    def live(concept_id: str) -> str | None:
        concept_id = active(concept_id)
        return (
            None
            if registry[concept_id]["concept_status"] == "deprecated"
            else concept_id
        )

    def mint_sense(label_key: str, primary: str) -> str:
        concept_id = str(uuid.uuid4())
        registry[concept_id] = {
            "resolution_run_id": run_id,
            "concept_id": concept_id,
            "concept_status": "active",
            "merged_into_concept_id": None,
            "minted_label": rows[label_key]["preferred_label"],
            "minted_run_id": run_id,
            "minted_at": started_at,
            "split_from_concept_id": primary,
        }
        return concept_id

    primaries = {
        key: resolved(concept_id) for key, (concept_id, _) in assignments.items()
    }
    aliases = label_aliases(
        primaries,
        state.aliases,
        {
            key: _label_text(rows[key])
            if key in rows
            else registry[primary]["minted_label"]
            for key, primary in primaries.items()
        },
        run_id=run_id,
        current=live,
    )
    senses = assign_senses(
        sense_units(mentions.filter(pl.col("concept_label_key").is_in(keys))),
        aliases,
        state.sense_assignments,
        client,
        model_id=model_id,
        run_id=run_id,
        active=active,
        mint_sense=mint_sense,
    )

    def relation_key(left: str, right: str, relation: str) -> tuple[str, str, str]:
        # only broader has a direction; the others are keyed in id order, which a
        # merge can reverse for a carried edge
        if relation != "broader":
            left, right = sorted((left, right))
        return left, right, relation

    relations: dict[tuple[str, str, str], str] = {}
    for row in state.relations.iter_rows(named=True):
        relations.setdefault(
            relation_key(
                resolved(row["from_concept_id"]),
                resolved(row["to_concept_id"]),
                row["relation"],
            ),
            row["proposed_run_id"],
        )
    # A pair is often judged from both sides, and the two answers can disagree on
    # direction, which would put a cycle in the hierarchy. Direction is decided by
    # votes over (narrower, broader), with carried edges voting too so a later run
    # cannot flip one silently; a tie keeps the pair as related.
    direction_votes: Counter[tuple[str, str]] = Counter()
    for edge in [edge for edge in relations if edge[2] == "broader"]:
        direction_votes[edge[0], edge[1]] += 1
    for query, candidate, verdict in decisions:
        if verdict in ("exact", "distinct"):
            continue
        if query not in assignments or candidate not in assignments:
            continue
        # an assignment made before a later component merged its concept still
        # holds the losing id
        left = resolved(assignments[query][0])
        right = resolved(assignments[candidate][0])
        if left == right:
            continue
        if verdict == "broader":
            direction_votes[right, left] += 1
        elif verdict == "narrower":
            direction_votes[left, right] += 1
        else:
            relations.setdefault(relation_key(left, right, verdict), run_id)
    for narrower, broader in list(direction_votes):
        forward = direction_votes[narrower, broader]
        backward = direction_votes[broader, narrower]
        if forward > backward:
            relations.setdefault((narrower, broader, "broader"), run_id)
            relations.pop((broader, narrower, "broader"), None)
        elif forward == backward:
            relations.pop((narrower, broader, "broader"), None)
            relations.setdefault(relation_key(narrower, broader, "related"), run_id)

    def frame(
        records: Iterable[dict[str, Any]], schema: dict[str, Any]
    ) -> pl.DataFrame:
        return pl.DataFrame(list(records), schema=schema)

    return ResolutionResult(
        registry=frame(registry.values(), REGISTRY_SCHEMA),
        aliases=senses.aliases,
        sense_assignments=senses.assignments,
        sense_decisions=senses.decisions,
        assignments=frame(
            (
                {
                    "resolution_run_id": run_id,
                    "concept_label_key": key,
                    "concept_id": resolved(concept_id),
                    "assignment_source": source,
                    "assigned_run_id": previous_all[key]["assigned_run_id"]
                    if source == "carried" and key in previous_all
                    else run_id,
                }
                for key, (concept_id, source) in sorted(assignments.items())
            ),
            ASSIGNMENT_SCHEMA,
        ),
        relations=frame(
            (
                {
                    "resolution_run_id": run_id,
                    "from_concept_id": left,
                    "to_concept_id": right,
                    "relation": relation,
                    "proposed_run_id": proposed,
                }
                for (left, right, relation), proposed in sorted(relations.items())
                # a deprecated concept has no labels left to relate
                if left != right
                and registry[left]["concept_status"] == "active"
                and registry[right]["concept_status"] == "active"
            ),
            RELATION_SCHEMA,
        ),
        decisions=frame(
            (
                {
                    "resolution_run_id": run_id,
                    "query_label_key": query,
                    "candidate_label_key": candidate,
                    "verdict": verdict,
                    "model_id": model_id,
                }
                for query, candidate, verdict in decisions
            ),
            DECISION_SCHEMA,
        ),
        embeddings=new_embeddings,
        run=frame(
            [
                {
                    "resolution_run_id": run_id,
                    "resolver_id": RESOLVER_ID,
                    "prompt_version": PROMPT_VERSION,
                    "model_id": model_id,
                    "embedding_model_id": embedding_client.spec.model_id,
                    "mode": "rebuild" if rebuild else "incremental",
                    "label_count": len(keys),
                    "resolved_label_count": len(pending),
                    "adjudication_calls": calls,
                    "failed_batch_count": len(failed_batches),
                    "failed_label_count": len(failed),
                    "concepts_minted": minted,
                    "concepts_merged": merged,
                    "concepts_deprecated": deprecated,
                    "concepts_carried_forward": carried_forward,
                    "rejected_components": rejected,
                    "started_at": started_at,
                    "sense_prompt_version": SENSE_PROMPT_VERSION,
                    "sense_calls": senses.calls,
                    "senses_minted": senses.senses_minted,
                    "sense_review_count": senses.review_count,
                }
            ],
            RUN_SCHEMA,
        ),
    )
