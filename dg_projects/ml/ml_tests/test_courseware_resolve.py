import itertools
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
from ml.lib.courseware import resolve
from ml.lib.courseware.resolve import (
    ResolutionState,
    accepted_components,
    embedding_neighbours,
    lexical_neighbours,
    resolve_concepts,
)
from ol_orchestrate.lib.embeddings.models import EmbeddingBatch, EmbeddingModelSpec


class FakeEmbedder:
    spec = EmbeddingModelSpec(model="fake:embed", dimensions=26, batch_size=16)

    def embed(self, texts, *, input_type):
        assert input_type == "document"
        # one-hot on the first letter keeps the vectors deterministic and distinct
        return EmbeddingBatch(
            vectors=[
                list(np.eye(26, dtype=np.float32)[ord(text[0].lower()) - 97] + 0.01)
                for text in texts
            ],
            input_tokens=0,
        )


def labels(*keys, mentions=None):
    return pl.DataFrame(
        {
            "concept_label_key": list(keys),
            "preferred_label": list(keys),
            "concept_kind": ["concept"] * len(keys),
            "description": [f"about {key}" for key in keys],
            "mention_count": [(mentions or {}).get(key, 1) for key in keys],
        }
    )


@pytest.fixture
def verdicts(monkeypatch):
    """Replace adjudication with a verdict table, {(query, candidate): verdict}."""
    table: dict[tuple[str, str], str] = {}
    inverse = {"broader": "narrower", "narrower": "broader"}

    def verdict(query: str, candidate: str) -> str:
        if (query, candidate) in table:
            return table[query, candidate]
        reverse = table.get((candidate, query), "distinct")
        return inverse.get(reverse, reverse)

    def fake_adjudicate(_client, _model_id, candidates, _rows):
        decided = [
            (query, candidate, verdict(query, candidate))
            for query, found in candidates.items()
            for candidate in found
        ]
        return decided, 1, set()

    monkeypatch.setattr(resolve, "adjudicate", fake_adjudicate)
    return table


def mentions_of(input_labels, courserun="course-v1:X+Y+1T2026"):
    """One mention of every label in one course run, so each has a single sense."""
    keys = input_labels["concept_label_key"].to_list()
    return pl.DataFrame(
        {
            "concept_label_key": keys,
            "platform": ["mitxonline"] * len(keys),
            "courserun_readable_id": [courserun] * len(keys),
            "course_title": ["Data Analysis"] * len(keys),
            "label": keys,
            "concept_kind": ["concept"] * len(keys),
            "description": [f"about {key}" for key in keys],
            "teaching_status": ["taught"] * len(keys),
        },
        schema={
            "concept_label_key": pl.String,
            "platform": pl.String,
            "courserun_readable_id": pl.String,
            "course_title": pl.String,
            "label": pl.String,
            "concept_kind": pl.String,
            "description": pl.String,
            "teaching_status": pl.String,
        },
    )


def run(input_labels, state=None, mentions=None, **kwargs):
    return resolve_concepts(
        input_labels,
        state or ResolutionState(),
        client=None,
        embedding_client=FakeEmbedder(),
        mentions=mentions_of(input_labels) if mentions is None else mentions,
        model_id="fake-model",
        **kwargs,
    )


def state_from(result):
    return ResolutionState(
        registry=result.registry,
        assignments=result.assignments,
        relations=result.relations,
        embeddings=result.embeddings,
        aliases=result.aliases,
        sense_assignments=result.sense_assignments,
    )


def concept_of(result, key):
    return result.assignments.filter(pl.col("concept_label_key") == key)["concept_id"][
        0
    ]


def test_exact_components_merge_and_distinct_pairs_block_chains():
    decisions = [
        ("auc", "area under the curve", "exact"),
        ("k means", "k means clustering", "exact"),
        ("k means clustering", "clustering", "exact"),
        ("k means", "clustering", "distinct"),
    ]
    components, rejected = accepted_components(
        ["auc", "area under the curve", "k means", "k means clustering", "clustering"],
        decisions,
    )
    assert {"auc", "area under the curve"} in components
    # the k-means chain contains a distinct pair, so it falls apart into singletons
    assert {"k means"} in components
    assert {"clustering"} in components
    assert rejected == 1


def test_lexical_neighbours_find_spelling_variants():
    found = lexical_neighbours(
        ["numpy array", "numpy arrays", "pandas"], ["numpy array"], k=2
    )
    assert found["numpy array"] == ["numpy arrays"]


def test_first_run_mints_ids_and_merges_exact_labels(verdicts):
    verdicts[("auc", "area under the curve")] = "exact"
    result = run(labels("auc", "area under the curve", "pandas"))
    assert concept_of(result, "auc") == concept_of(result, "area under the curve")
    assert concept_of(result, "pandas") != concept_of(result, "auc")
    assert result.registry.height == 2
    assert result.run["concepts_minted"][0] == 2


def test_incremental_run_keeps_ids_and_resolves_only_new_labels(verdicts):
    first = run(labels("auc", "pandas"))
    verdicts[("area under the curve", "auc")] = "exact"
    second = run(labels("auc", "pandas", "area under the curve"), state_from(first))
    assert second.run["resolved_label_count"][0] == 1
    assert concept_of(second, "auc") == concept_of(first, "auc")
    assert concept_of(second, "area under the curve") == concept_of(first, "auc")
    assert concept_of(second, "pandas") == concept_of(first, "pandas")
    assert second.run["concepts_minted"][0] == 0


def test_new_label_exact_to_two_concepts_merges_them_with_a_redirect(verdicts):
    first = run(labels("auc", "area under the curve", mentions={"auc": 5}))
    loser = concept_of(first, "area under the curve")
    survivor = concept_of(first, "auc")
    assert loser != survivor
    verdicts[("auc roc", "auc")] = "exact"
    verdicts[("auc roc", "area under the curve")] = "exact"
    second = run(
        labels("auc", "area under the curve", "auc roc", mentions={"auc": 5}),
        state_from(first),
    )
    registry = {row["concept_id"]: row for row in second.registry.iter_rows(named=True)}
    assert registry[loser]["concept_status"] == "merged"
    assert registry[loser]["merged_into_concept_id"] == survivor
    assert {
        concept_of(second, key) for key in ("auc", "area under the curve", "auc roc")
    } == {survivor}


def test_rebuild_carries_ids_forward_by_member_overlap(verdicts):
    verdicts[("auc", "area under the curve")] = "exact"
    first = run(labels("auc", "area under the curve", "pandas"))
    rebuilt = run(
        labels("auc", "area under the curve", "pandas"), state_from(first), rebuild=True
    )
    assert concept_of(rebuilt, "auc") == concept_of(first, "auc")
    assert concept_of(rebuilt, "pandas") == concept_of(first, "pandas")
    assert rebuilt.run["concepts_minted"][0] == 0
    assert rebuilt.run["concepts_carried_forward"][0] == 2


def test_contradictory_directions_become_related_not_a_cycle(verdicts):
    # both sides claim to be the narrower one
    verdicts[("cross validation", "k fold cross validation")] = "narrower"
    verdicts[("k fold cross validation", "cross validation")] = "narrower"
    result = run(labels("cross validation", "k fold cross validation"))
    assert result.relations["relation"].to_list() == ["related"]


def test_broader_and_narrower_verdicts_become_directed_relations(verdicts):
    verdicts[("gini impurity", "decision tree")] = "narrower"
    result = run(labels("gini impurity", "decision tree"))
    relation = result.relations.row(0, named=True)
    assert relation["relation"] == "broader"
    assert relation["from_concept_id"] == concept_of(result, "gini impurity")
    assert relation["to_concept_id"] == concept_of(result, "decision tree")


def test_rebuild_keeps_a_failed_label_on_its_previous_concept(verdicts, monkeypatch):
    verdicts[("auc", "area under the curve")] = "exact"
    first = run(labels("auc", "area under the curve", "pandas"))

    def adjudicate_failing_pandas(_client, _model_id, candidates, _rows):
        decided = [
            (query, candidate, verdicts.get((query, candidate), "distinct"))
            for query, found in candidates.items()
            if query != "pandas"
            for candidate in found
        ]
        return decided, 1, {"pandas"}

    monkeypatch.setattr(resolve, "adjudicate", adjudicate_failing_pandas)
    rebuilt = run(
        labels("auc", "area under the curve", "pandas"), state_from(first), rebuild=True
    )
    pandas = concept_of(first, "pandas")
    assert concept_of(rebuilt, "pandas") == pandas
    registry = {
        row["concept_id"]: row for row in rebuilt.registry.iter_rows(named=True)
    }
    assert registry[pandas]["concept_status"] == "active"
    assert rebuilt.run["concepts_deprecated"][0] == 0


def test_concepts_bridged_twice_merge_one_way(verdicts):
    # s1 and s2 form one concept, l1 and l2 another; the mention counts make each
    # bridging component prefer a different survivor
    mentions = {"s1": 5, "s2": 1, "l1": 1, "l2": 5}
    verdicts[("s1", "s2")] = "exact"
    verdicts[("l1", "l2")] = "exact"
    first = run(labels("l1", "l2", "s1", "s2", mentions=mentions))
    verdicts[("n1", "s1")] = "exact"
    verdicts[("n1", "l1")] = "exact"
    verdicts[("o2", "s2")] = "exact"
    verdicts[("o2", "l2")] = "exact"
    # each new label joins both concepts whole, so it is not distinct from the other
    # member of either
    verdicts[("n1", "s2")] = "close"
    verdicts[("n1", "l2")] = "close"
    verdicts[("o2", "s1")] = "close"
    verdicts[("o2", "l1")] = "close"
    verdicts[("n1", "o2")] = "close"
    second = run(
        labels("l1", "l2", "n1", "o2", "s1", "s2", mentions=mentions),
        state_from(first),
    )
    registry = {row["concept_id"]: row for row in second.registry.iter_rows(named=True)}
    previous = {concept_of(first, "s1"), concept_of(first, "l1")}
    statuses = sorted(registry[concept_id]["concept_status"] for concept_id in previous)
    assert statuses == ["active", "merged"]
    resolved = {concept_of(second, key) for key in ("l1", "l2", "n1", "o2", "s1", "s2")}
    assert len(resolved) == 1
    assert registry[resolved.pop()]["concept_status"] == "active"


def test_embedding_neighbours_never_include_the_query():
    keys = ["auc", "bias", "cart"]
    vectors = np.eye(3, dtype=np.float32)
    found = embedding_neighbours(keys, vectors, keys, k=8)
    assert all(key not in found[key] for key in keys)
    assert set(found["auc"]) == {"bias", "cart"}


@pytest.mark.usefixtures("verdicts")
def test_rebuild_deprecates_a_concept_whose_labels_are_all_gone():
    first = run(labels("auc", "pandas"))
    pandas = concept_of(first, "pandas")
    rebuilt = run(labels("auc"), state_from(first), rebuild=True)
    registry = {
        row["concept_id"]: row for row in rebuilt.registry.iter_rows(named=True)
    }
    assert registry[pandas]["concept_status"] == "deprecated"
    assert registry[pandas]["merged_into_concept_id"] is None
    assert rebuilt.run["concepts_deprecated"][0] == 1


def test_relations_of_a_deprecated_concept_are_dropped(verdicts):
    verdicts[("gini impurity", "decision tree")] = "narrower"
    first = run(labels("gini impurity", "decision tree"))
    assert first.relations.height == 1
    rebuilt = run(labels("decision tree"), state_from(first), rebuild=True)
    assert rebuilt.relations.height == 0


@pytest.mark.usefixtures("verdicts")
def test_an_empty_label_list_resolves_to_nothing():
    first = run(labels("auc", "pandas"))
    empty = labels().cast({"mention_count": pl.Int64})
    second = run(empty, state_from(first))
    # an incremental run carries every assignment of a label it no longer sees
    kept = ["concept_label_key", "concept_id", "assigned_run_id"]
    assert second.assignments.select(kept).equals(first.assignments.select(kept))
    assert second.registry.height == first.registry.height
    assert second.run["concepts_minted"][0] == 0


def test_carried_related_edges_are_keyed_in_id_order(verdicts, monkeypatch):
    # ids rise in mint order, so bias < cart < zeta; merging bias into zeta turns
    # the carried (bias, cart) edge into (zeta, cart), the mirror of (cart, zeta)
    counter = itertools.count(1)
    monkeypatch.setattr(resolve.uuid, "uuid4", lambda: uuid.UUID(int=next(counter)))
    verdicts[("bias", "cart")] = "related"
    verdicts[("cart", "zeta")] = "related"
    mentions = {"zeta": 5}
    first = run(labels("bias", "cart", "zeta", mentions=mentions))
    assert first.relations.height == 2
    verdicts[("new", "bias")] = "exact"
    verdicts[("new", "zeta")] = "exact"
    second = run(
        labels("bias", "cart", "new", "zeta", mentions=mentions), state_from(first)
    )
    rows = list(second.relations.iter_rows(named=True))
    assert [row["relation"] for row in rows] == ["related"]
    assert rows[0]["from_concept_id"] < rows[0]["to_concept_id"]


@pytest.mark.usefixtures("verdicts")
def test_a_label_that_drops_out_keeps_its_id_until_a_rebuild():
    first = run(labels("auc", "pandas"))
    pandas = concept_of(first, "pandas")
    # pandas has no claims in this run, so its assignment is carried, not lost
    dropped = run(labels("auc"), state_from(first))
    assert concept_of(dropped, "pandas") == pandas
    returned = run(labels("auc", "pandas"), state_from(dropped))
    assert concept_of(returned, "pandas") == pandas
    assert returned.run["concepts_minted"][0] == 0
    assert returned.run["resolved_label_count"][0] == 0
    # a rebuild is what retires the concept a dropped label leaves without members
    rebuilt = run(labels("auc"), state_from(dropped), rebuild=True)
    assert "pandas" not in rebuilt.assignments["concept_label_key"].to_list()
    registry = {
        row["concept_id"]: row for row in rebuilt.registry.iter_rows(named=True)
    }
    assert registry[pandas]["concept_status"] == "deprecated"


def test_a_label_distinct_from_a_member_of_its_destination_is_not_merged(verdicts):
    verdicts[("auc", "area under the curve")] = "exact"
    first = run(labels("auc", "area under the curve"))
    # exact to one member of the concept, distinct from the other
    verdicts[("auc roc", "auc")] = "exact"
    verdicts[("auc roc", "area under the curve")] = "distinct"
    second = run(labels("auc", "area under the curve", "auc roc"), state_from(first))
    assert concept_of(second, "auc roc") != concept_of(first, "auc")
    assert second.run["rejected_components"][0] == 1


def test_a_failed_label_is_not_brought_back_as_a_candidate():
    components, _ = accepted_components(
        ["auc", "pandas"], [("auc", "area under the curve", "exact")]
    )
    assert sorted(map(sorted, components)) == [["auc"], ["pandas"]]


def test_incremental_run_leaves_a_failed_label_unassigned(verdicts, monkeypatch):
    first = run(labels("auc", "pandas"))
    verdicts[("auc roc", "auc")] = "exact"
    verdicts[("area under the curve", "auc")] = "exact"

    def adjudicate_failing_auc_roc(_client, _model_id, candidates, _rows):
        # auc roc's own batch failed, but another query still lists it as exact
        decided = [
            (query, candidate, verdicts.get((query, candidate), "distinct"))
            for query, found in candidates.items()
            if query != "auc roc"
            for candidate in found
        ]
        decided.append(("area under the curve", "auc roc", "exact"))
        return decided, 1, {"auc roc"}

    monkeypatch.setattr(resolve, "adjudicate", adjudicate_failing_auc_roc)
    second = run(
        labels("auc", "pandas", "area under the curve", "auc roc"), state_from(first)
    )
    assigned = second.assignments["concept_label_key"].to_list()
    assert "auc roc" not in assigned
    assert concept_of(second, "area under the curve") == concept_of(first, "auc")


def test_a_relation_follows_a_merge_made_later_in_the_run(verdicts):
    verdicts[("l1", "l2")] = "exact"
    mentions = {"s1": 5}
    first = run(labels("l1", "l2", "p", "s1", mentions=mentions))
    survivor = concept_of(first, "s1")
    # x joins l1's concept, then n merges that concept into s1's
    verdicts[("x", "l1")] = "exact"
    verdicts[("x", "l2")] = "close"
    verdicts[("x", "p")] = "related"
    verdicts[("n", "s1")] = "exact"
    verdicts[("n", "l2")] = "exact"
    verdicts[("n", "l1")] = "close"
    # n merges l1's concept into s1's, so x ends up beside s1 and n
    verdicts[("x", "s1")] = "close"
    verdicts[("x", "n")] = "close"
    second = run(
        labels("l1", "l2", "n", "p", "s1", "x", mentions=mentions), state_from(first)
    )
    assert concept_of(second, "x") == survivor
    rows = list(second.relations.iter_rows(named=True))
    assert [
        ({row["from_concept_id"], row["to_concept_id"]}, row["relation"])
        for row in rows
    ] == [({survivor, concept_of(first, "p")}, "related")]


class _VerdictClient:
    """Stands in for an Anthropic client, returning one fixed record_verdicts call."""

    def __init__(self, tool_input):
        self.messages = self
        self.tool_input = tool_input

    @contextmanager
    def stream(self, **_kwargs):
        response = SimpleNamespace(
            stop_reason="tool_use",
            content=[
                SimpleNamespace(
                    type="tool_use", name="record_verdicts", input=self.tool_input
                )
            ],
        )
        yield SimpleNamespace(get_final_message=lambda: response)


@pytest.mark.parametrize(
    "tool_input",
    [
        {"queries": []},
        {"queries": [{"query_id": 0, "verdicts": []}]},
        {
            "queries": [
                {"query_id": 0, "verdicts": [{"candidate_id": 0, "verdict": "exact"}]}
            ]
            * 2
        },
        {
            "queries": [
                {"query_id": 1, "verdicts": [{"candidate_id": 0, "verdict": "exact"}]}
            ]
        },
        {
            "queries": [
                {
                    "query_id": 0,
                    "verdicts": [
                        {"candidate_id": 0, "verdict": "exact"},
                        {"candidate_id": 0, "verdict": "close"},
                    ],
                }
            ]
        },
    ],
)
def test_an_incomplete_adjudication_fails_the_batch(tool_input):
    rows = {
        key: {"preferred_label": key, "concept_kind": "concept", "description": ""}
        for key in ("auc", "area under the curve")
    }
    with pytest.raises(ValueError, match="verdicts for"):
        resolve._adjudicate_batch(
            _VerdictClient(tool_input),
            "fake-model",
            [("auc", ["area under the curve"])],
            rows,
        )


def test_a_complete_adjudication_is_returned():
    rows = {
        key: {"preferred_label": key, "concept_kind": "concept", "description": ""}
        for key in ("auc", "area under the curve")
    }
    client = _VerdictClient(
        {
            "queries": [
                {"query_id": 0, "verdicts": [{"candidate_id": 0, "verdict": "exact"}]}
            ]
        }
    )
    assert resolve._adjudicate_batch(
        client, "fake-model", [("auc", ["area under the curve"])], rows
    ) == [("auc", "area under the curve", "exact")]


def test_two_new_labels_distinct_from_each_other_do_not_both_join_a_concept(verdicts):
    verdicts[("auc", "area under the curve")] = "exact"
    first = run(labels("auc", "area under the curve"))
    concept = concept_of(first, "auc")
    # each new label is exact to a different member of the concept, and the two are
    # distinct from each other
    verdicts[("auc roc", "auc")] = "exact"
    verdicts[("auc roc", "area under the curve")] = "close"
    verdicts[("pr auc", "area under the curve")] = "exact"
    verdicts[("pr auc", "auc")] = "close"
    verdicts[("auc roc", "pr auc")] = "distinct"
    second = run(
        labels("auc", "area under the curve", "auc roc", "pr auc"), state_from(first)
    )
    joined = [
        key for key in ("auc roc", "pr auc") if concept_of(second, key) == concept
    ]
    assert joined == []
    assert second.run["rejected_components"][0] == 2


def test_a_new_label_exact_to_many_labels_of_one_concept_joins_it(verdicts):
    members = [f"k means {suffix}" for suffix in "abcdefg"]
    for left, right in itertools.combinations(members, 2):
        verdicts[(left, right)] = "exact"
    # the concept grows to seven labels over two runs
    first = run(labels(*members[:4]))
    grown = run(labels(*members), state_from(first))
    concept = concept_of(first, members[0])
    assert {concept_of(grown, key) for key in members} == {concept}
    # a new label exact to all seven joins one concept, not a seven-way chain
    for member in members:
        verdicts[("kmeans", member)] = "exact"
    second = run(labels(*members, "kmeans"), state_from(grown))
    assert concept_of(second, "kmeans") == concept
    assert second.run["rejected_components"][0] == 0
