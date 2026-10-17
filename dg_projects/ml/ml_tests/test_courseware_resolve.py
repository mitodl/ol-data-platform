import numpy as np
import polars as pl
import pytest
from ml.lib.courseware import resolve
from ml.lib.courseware.resolve import (
    ResolutionState,
    accepted_components,
    lexical_neighbours,
    resolve_concepts,
)


class FakeEmbedder:
    model_version = "fake-embed"
    max_request_batch_size = 16

    def embed_batch(self, texts):
        # one-hot on the first letter keeps the vectors deterministic and distinct
        return [
            list(np.eye(26, dtype=np.float32)[ord(text[0].lower()) - 97] + 0.01)
            for text in texts
        ]


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


def run(input_labels, state=None, **kwargs):
    return resolve_concepts(
        input_labels,
        state or ResolutionState(),
        client=None,
        embedding_client=FakeEmbedder(),
        model_id="fake-model",
        **kwargs,
    )


def state_from(result):
    return ResolutionState(
        registry=result.registry,
        assignments=result.assignments,
        relations=result.relations,
        embeddings=result.embeddings,
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
