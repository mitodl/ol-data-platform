import json
from types import SimpleNamespace

import polars as pl
import pytest
from ml.lib.courseware import resolve, senses
from ml.lib.courseware.resolve import ResolutionState
from ml.lib.courseware.senses import Alias, SenseQuery, Unit, _adjudicate_senses_batch

from .test_courseware_resolve import labels, run, state_from

OS = "course-v1:MITx+6.033+1T2026"
ML = "course-v1:MITx+6.036+1T2026"
LINALG = "course-v1:MITx+18.06+1T2026"
TITLES = {
    OS: "Computer Systems Engineering",
    ML: "Introduction to Machine Learning",
    LINALG: "Linear Algebra",
}


def mentions(*uses):
    """One mention per (label, course run)."""
    rows = [
        {
            "concept_label_key": key,
            "platform": "mitxonline",
            "courserun_readable_id": courserun,
            "course_title": TITLES[courserun],
            "label": key,
            "concept_kind": "concept",
            "description": f"{key} as {TITLES[courserun]} uses it",
            "teaching_status": "taught",
        }
        for key, courserun in uses
    ]
    return pl.DataFrame(rows, schema=dict.fromkeys(rows[0], pl.String))


@pytest.fixture
def verdicts(monkeypatch):
    """Label adjudication finds no exact matches, so each label is its own concept."""

    def fake_adjudicate(_client, _model_id, _candidates, _rows, **_kwargs):
        return [], 1, set()

    monkeypatch.setattr(resolve, "adjudicate", fake_adjudicate)


@pytest.fixture
def choose(monkeypatch):
    """Replace sense adjudication with a table, {courserun: (decision, sense)}.

    A run missing from the table fails, as a failed batch does.
    """
    table: dict[str, tuple[str, int | None]] = {}
    asked: list[SenseQuery] = []

    def fake_adjudicate_senses(_client, _model_id, queries, **_kwargs):
        asked.extend(queries)
        decided = [
            (unit.key, *table[unit.key[2]])
            for query in queries
            for unit in query.units
            if unit.key[2] in table
        ]
        return decided, len(queries)

    monkeypatch.setattr(senses, "adjudicate_senses", fake_adjudicate_senses)
    return SimpleNamespace(table=table, asked=asked)


def sense_of(result, key, courserun):
    row = result.sense_assignments.filter(
        (pl.col("concept_label_key") == key)
        & (pl.col("courserun_readable_id") == courserun)
    )
    return row.row(0, named=True) if row.height else None


def registry_of(result):
    return {row["concept_id"]: row for row in result.registry.iter_rows(named=True)}


@pytest.mark.usefixtures("verdicts")
def test_a_label_in_one_course_run_takes_its_only_sense_without_a_call(choose):
    result = run(labels("kernel"), mentions=mentions(("kernel", OS)))
    assignment = sense_of(result, "kernel", OS)
    assert assignment["concept_id"] == result.assignments["concept_id"][0]
    assert assignment["assignment_source"] == "single_sense"
    assert choose.asked == []


@pytest.mark.usefixtures("verdicts")
def test_a_homonym_splits_into_a_new_sense_with_lineage(choose):
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("new", 1)
    result = run(labels("kernel"), mentions=mentions(("kernel", OS), ("kernel", ML)))
    primary = result.assignments["concept_id"][0]
    os_sense = sense_of(result, "kernel", OS)
    ml_sense = sense_of(result, "kernel", ML)
    assert os_sense["concept_id"] == primary
    assert ml_sense["concept_id"] != primary
    assert ml_sense["assignment_source"] == "new_sense"
    registry = registry_of(result)
    assert registry[ml_sense["concept_id"]]["split_from_concept_id"] == primary
    assert registry[ml_sense["concept_id"]]["concept_status"] == "active"
    aliases = result.aliases.filter(pl.col("concept_label_key") == "kernel")
    assert aliases["sense_role"].to_list() == ["primary", "split"]
    assert aliases["concept_id"].to_list() == [primary, ml_sense["concept_id"]]
    assert result.run["senses_minted"][0] == 1


@pytest.mark.usefixtures("verdicts")
def test_runs_sharing_a_new_sense_get_one_concept(choose):
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("new", 1)
    choose.table[LINALG] = ("new", 1)
    result = run(
        labels("kernel"),
        mentions=mentions(("kernel", OS), ("kernel", ML), ("kernel", LINALG)),
    )
    assert (
        sense_of(result, "kernel", ML)["concept_id"]
        == sense_of(result, "kernel", LINALG)["concept_id"]
    )
    assert result.run["senses_minted"][0] == 1


@pytest.mark.usefixtures("verdicts")
def test_a_run_the_model_cannot_place_goes_to_review_until_a_run_can(choose):
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("unsure", None)
    uses = mentions(("kernel", OS), ("kernel", ML))
    first = run(labels("kernel"), mentions=uses)
    queued = sense_of(first, "kernel", ML)
    assert queued["sense_status"] == "needs_review"
    assert queued["concept_id"] is None
    assert first.run["sense_review_count"][0] == 1
    # no reviewer decision is stored, so the next run asks again, and only about it
    choose.asked.clear()
    second = run(labels("kernel"), state_from(first), mentions=uses)
    assert [unit.key[2] for query in choose.asked for unit in query.units] == [ML]
    assert sense_of(second, "kernel", ML)["sense_status"] == "needs_review"
    choose.table[ML] = ("new", 1)
    third = run(labels("kernel"), state_from(second), mentions=uses)
    assert sense_of(third, "kernel", ML)["sense_status"] == "assigned"
    assert (
        sense_of(third, "kernel", ML)["concept_id"]
        != (sense_of(third, "kernel", OS)["concept_id"])
    )


@pytest.mark.usefixtures("verdicts")
def test_a_new_course_run_of_a_known_label_is_adjudicated_not_inherited(choose):
    first = run(labels("pandas"), mentions=mentions(("pandas", OS)))
    pandas = sense_of(first, "pandas", OS)["concept_id"]
    choose.table[ML] = ("existing", 0)
    second = run(
        labels("pandas"),
        state_from(first),
        mentions=mentions(("pandas", OS), ("pandas", ML)),
    )
    # the earlier run is carried, the new one asked about, and the id kept
    assert [unit.key[2] for query in choose.asked for unit in query.units] == [ML]
    assert sense_of(second, "pandas", OS)["concept_id"] == pandas
    assert sense_of(second, "pandas", ML)["concept_id"] == pandas
    assert sense_of(second, "pandas", ML)["assignment_source"] == "context_selected"
    assert (
        sense_of(second, "pandas", OS)["assigned_run_id"]
        == (first.run["resolution_run_id"][0])
    )


@pytest.mark.usefixtures("verdicts")
def test_a_new_run_can_select_a_split_sense(choose):
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("new", 1)
    first = run(labels("kernel"), mentions=mentions(("kernel", OS), ("kernel", ML)))
    ml_kernel = sense_of(first, "kernel", ML)["concept_id"]
    choose.asked.clear()
    choose.table[LINALG] = ("existing", 1)
    second = run(
        labels("kernel"),
        state_from(first),
        mentions=mentions(("kernel", OS), ("kernel", ML), ("kernel", LINALG)),
    )
    [query] = choose.asked
    assert [alias.concept_id for alias in query.senses][1] == ml_kernel
    assert sense_of(second, "kernel", LINALG)["concept_id"] == ml_kernel
    assert second.run["senses_minted"][0] == 0


@pytest.mark.usefixtures("verdicts")
def test_a_failed_sense_batch_leaves_the_runs_unassigned_for_the_next_run(choose):
    first = run(labels("kernel"), mentions=mentions(("kernel", OS), ("kernel", ML)))
    assert first.sense_assignments.height == 0
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("existing", 0)
    second = run(
        labels("kernel"),
        state_from(first),
        mentions=mentions(("kernel", OS), ("kernel", ML)),
    )
    assert second.sense_assignments.height == 2


@pytest.mark.usefixtures("verdicts")
def test_a_rebuild_keeps_split_senses(choose):
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("new", 1)
    uses = mentions(("kernel", OS), ("kernel", ML))
    first = run(labels("kernel"), mentions=uses)
    ml_kernel = sense_of(first, "kernel", ML)["concept_id"]
    rebuilt = run(labels("kernel"), state_from(first), mentions=uses, rebuild=True)
    assert registry_of(rebuilt)[ml_kernel]["concept_status"] == "active"
    assert sense_of(rebuilt, "kernel", ML)["concept_id"] == ml_kernel
    assert rebuilt.run["concepts_deprecated"][0] == 0


def test_a_rebuild_that_separates_labels_moves_their_runs_with_them(
    choose, monkeypatch
):
    def merge_all(_client, _model_id, candidates, _rows, **_kwargs):
        return (
            [
                (query, found, "exact")
                for query, items in candidates.items()
                for found in items
            ],
            1,
            set(),
        )

    monkeypatch.setattr(resolve, "adjudicate", merge_all)
    uses = mentions(("alpha", OS), ("beta", ML), ("gamma", LINALG))
    first = run(labels("alpha", "beta", "gamma"), mentions=uses)
    merged = {
        row["concept_id"] for row in first.sense_assignments.iter_rows(named=True)
    }
    assert len(merged) == 1

    def merge_nothing(_client, _model_id, _candidates, _rows, **_kwargs):
        return [], 1, set()

    monkeypatch.setattr(resolve, "adjudicate", merge_nothing)
    rebuilt = run(
        labels("alpha", "beta", "gamma"), state_from(first), mentions=uses, rebuild=True
    )
    primaries = {
        row["concept_label_key"]: row["concept_id"]
        for row in rebuilt.assignments.iter_rows(named=True)
    }
    assert len(set(primaries.values())) == 3
    for key, courserun in (("alpha", OS), ("beta", ML), ("gamma", LINALG)):
        assert sense_of(rebuilt, key, courserun)["concept_id"] == primaries[key]
    # a label's old concept is not kept as a sense of it
    assert rebuilt.aliases["sense_role"].to_list() == ["primary"] * 3
    assert choose.asked == []


@pytest.mark.usefixtures("verdicts")
def test_sense_assignments_follow_a_label_merge(choose, monkeypatch):
    first = run(
        labels("auc", "area under the curve", mentions={"auc": 5}),
        mentions=mentions(("auc", OS), ("area under the curve", ML)),
    )
    survivor = sense_of(first, "auc", OS)["concept_id"]
    loser = sense_of(first, "area under the curve", ML)["concept_id"]

    def merge_new_label(_client, _model_id, _candidates, _rows, **_kwargs):
        return (
            [("auc roc", "auc", "exact"), ("auc roc", "area under the curve", "exact")],
            1,
            set(),
        )

    monkeypatch.setattr(resolve, "adjudicate", merge_new_label)
    second = run(
        labels("auc", "area under the curve", "auc roc", mentions={"auc": 5}),
        state_from(first),
        mentions=mentions(
            ("auc", OS), ("area under the curve", ML), ("auc roc", LINALG)
        ),
    )
    assert registry_of(second)[loser]["merged_into_concept_id"] == survivor
    assert sense_of(second, "area under the curve", ML)["concept_id"] == survivor
    assert choose.asked == []


@pytest.mark.usefixtures("verdicts")
def test_registry_rows_from_before_senses_read_without_split_lineage():
    first = run(labels("pandas"), mentions=mentions(("pandas", OS)))
    old = state_from(first)
    old.registry = old.registry.drop("split_from_concept_id")
    second = run(labels("pandas"), old, mentions=mentions(("pandas", OS)))
    assert second.registry["split_from_concept_id"].to_list() == [None]


class FakeStream:
    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def get_final_message(self):
        return self.response


class FakeClient:
    """Answers every call with one record_senses tool call."""

    def __init__(self, tool_input):
        block = SimpleNamespace(type="tool_use", name="record_senses", input=tool_input)
        self.response = SimpleNamespace(
            content=[block],
            stop_reason="tool_use",
            usage=SimpleNamespace(
                input_tokens=100,
                output_tokens=20,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
        )
        self.calls = []
        self.messages = self

    def stream(self, **kwargs):
        self.calls.append(kwargs)
        return FakeStream(self.response)


def test_the_batch_parser_maps_decisions_and_rejects_unlisted_senses():
    query = SenseQuery(
        "kernel",
        [Alias("k-os", "primary", "kernel: the core of an operating system", "r")],
        [
            Unit(("kernel", "mitxonline", OS), "kernel", TITLES[OS], ("the OS core",)),
            Unit(("kernel", "mitxonline", ML), "kernel", TITLES[ML], ("a similarity",)),
            Unit(("kernel", "mitxonline", LINALG), "kernel", TITLES[LINALG], ()),
        ],
    )
    client = FakeClient(
        {
            "queries": [
                {
                    "query_id": 0,
                    "runs": [
                        {"run_id": 0, "decision": "existing", "sense": 0},
                        {"run_id": 1, "decision": "new", "sense": 1},
                        # s3 is not listed, so this cannot assign the run
                        {"run_id": 2, "decision": "existing", "sense": 3},
                    ],
                },
            ]
        }
    )
    decided = _adjudicate_senses_batch(client, "fake-model", [query], trace_metadata={})
    assert decided == [
        (("kernel", "mitxonline", OS), "existing", 0),
        (("kernel", "mitxonline", ML), "new", 1),
        (("kernel", "mitxonline", LINALG), "unsure", None),
    ]
    [call] = client.calls
    listing = call["messages"][0]["content"]
    assert "s0: kernel: the core of an operating system" in listing
    assert "r1: kernel in Introduction to Machine Learning: a similarity" in listing


def test_the_batch_parser_raises_when_the_model_records_nothing():
    client = FakeClient(None)
    client.response.content = []
    query = SenseQuery(
        "kernel",
        [Alias("k", "primary", "g", "r")],
        [Unit(("kernel", "mitxonline", OS), "kernel", TITLES[OS], ())],
    )
    with pytest.raises(ValueError, match="no sense decisions"):
        _adjudicate_senses_batch(client, "fake-model", [query], trace_metadata={})


def test_default_state_has_empty_sense_tables():
    state = ResolutionState()
    assert state.aliases.is_empty()
    assert state.sense_assignments.is_empty()


@pytest.mark.usefixtures("verdicts")
def test_a_label_whose_runs_were_queued_is_not_shortcut_when_one_drops_out(choose):
    choose.table[OS] = ("unsure", None)
    choose.table[ML] = ("unsure", None)
    first = run(labels("kernel"), mentions=mentions(("kernel", OS), ("kernel", ML)))
    assert first.run["sense_review_count"][0] == 2
    choose.asked.clear()
    # a later extraction drops the label from the ML run
    second = run(labels("kernel"), state_from(first), mentions=mentions(("kernel", OS)))
    assert [unit.key[2] for query in choose.asked for unit in query.units] == [OS]
    assert sense_of(second, "kernel", OS)["sense_status"] == "needs_review"


def test_a_synonym_of_an_ambiguous_label_is_judged_against_its_saved_sense(
    choose, monkeypatch
):
    monkeypatch.setattr(
        resolve,
        "adjudicate",
        lambda _client, _model_id, _candidates, _rows, **_kwargs: ([], 1, set()),
    )
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("new", 1)
    first = run(labels("kernel"), mentions=mentions(("kernel", OS), ("kernel", ML)))
    os_kernel = sense_of(first, "kernel", OS)["concept_id"]
    os_gloss = first.aliases.filter(pl.col("sense_role") == "primary")["sense_gloss"][0]

    # "kernel function" resolves onto kernel's primary, the operating systems sense
    monkeypatch.setattr(
        resolve,
        "adjudicate",
        lambda _client, _model_id, _candidates, _rows, **_kwargs: (
            [("kernel function", "kernel", "exact")],
            1,
            set(),
        ),
    )
    choose.asked.clear()
    choose.table[LINALG] = ("new", 1)
    second = run(
        labels("kernel", "kernel function"),
        state_from(first),
        mentions=mentions(("kernel", OS), ("kernel", ML), ("kernel function", LINALG)),
    )
    [query] = choose.asked
    assert query.label_key == "kernel function"
    assert [alias.sense_gloss for alias in query.senses] == [os_gloss]
    assert sense_of(second, "kernel function", LINALG)["concept_id"] != os_kernel


def test_the_batch_parser_rejects_two_decisions_for_one_run():
    query = SenseQuery(
        "kernel",
        [Alias("k-os", "primary", "kernel: the core of an operating system", "r")],
        [Unit(("kernel", "mitxonline", ML), "kernel", TITLES[ML], ("a similarity",))],
    )
    client = FakeClient(
        {
            "queries": [
                {
                    "query_id": 0,
                    "runs": [
                        {"run_id": 0, "decision": "new", "sense": 1},
                        {"run_id": 0, "decision": "new", "sense": 2},
                    ],
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="more than one sense decision"):
        _adjudicate_senses_batch(client, "fake-model", [query], trace_metadata={})


@pytest.mark.parametrize(
    ("queries", "error"),
    [
        # the model numbered the runs from 1: r1 would take r0's sense
        (
            [
                {
                    "query_id": 0,
                    "runs": [
                        {"run_id": 1, "decision": "existing", "sense": 0},
                        {"run_id": 2, "decision": "new", "sense": 1},
                    ],
                }
            ],
            r"Q0 runs \[1, 2\], expected 0 to 1",
        ),
        # one run left out
        (
            [
                {
                    "query_id": 0,
                    "runs": [{"run_id": 0, "decision": "existing", "sense": 0}],
                }
            ],
            r"Q0 runs \[0\], expected 0 to 1",
        ),
        # the whole query left out, and one that was not asked
        (
            [{"query_id": 1, "runs": []}],
            r"queries \[1\], expected 0 to 0",
        ),
    ],
)
def test_the_batch_parser_rejects_an_omitted_or_unlisted_run(queries, error):
    query = SenseQuery(
        "kernel",
        [Alias("k-os", "primary", "kernel: the core of an operating system", "r")],
        [
            Unit(("kernel", "mitxonline", OS), "kernel", TITLES[OS], ("the OS core",)),
            Unit(("kernel", "mitxonline", ML), "kernel", TITLES[ML], ("a similarity",)),
        ],
    )
    with pytest.raises(ValueError, match=error):
        _adjudicate_senses_batch(
            FakeClient({"queries": queries}), "fake-model", [query], trace_metadata={}
        )


def test_a_synonym_of_a_label_adjudicated_in_the_same_run_is_not_shortcut(
    choose, monkeypatch
):
    # "kernel function" resolves onto "kernel" in the run where kernel's two course
    # runs are adjudicated and may split it
    monkeypatch.setattr(
        resolve,
        "adjudicate",
        lambda _client, _model_id, _candidates, _rows, **_kwargs: (
            [("kernel function", "kernel", "exact")],
            1,
            set(),
        ),
    )
    choose.table[OS] = ("existing", 0)
    choose.table[ML] = ("new", 1)
    choose.table[LINALG] = ("new", 1)
    result = run(
        labels("kernel", "kernel function"),
        mentions=mentions(("kernel", OS), ("kernel", ML), ("kernel function", LINALG)),
    )
    asked = {query.label_key for query in choose.asked}
    assert asked == {"kernel", "kernel function"}
    assert (
        sense_of(result, "kernel function", LINALG)["assignment_source"] == "new_sense"
    )


def test_decisions_sent_as_json_strings_are_decoded():
    query = SenseQuery(
        "kernel",
        [Alias("k-os", "primary", "kernel: the core of an operating system", "r")],
        [
            Unit(("kernel", "mitxonline", OS), "kernel", TITLES[OS], ()),
            Unit(("kernel", "mitxonline", ML), "kernel", TITLES[ML], ()),
        ],
    )
    # the queries array, one query's runs and one run each arrive JSON-encoded
    runs = [
        {"run_id": 0, "decision": "existing", "sense": 0},
        json.dumps({"run_id": 1, "decision": "new", "sense": 1}),
    ]
    queries = [{"query_id": 0, "runs": json.dumps(runs)}]
    client = FakeClient({"queries": json.dumps(queries) + "}"})
    assert _adjudicate_senses_batch(
        client, "fake-model", [query], trace_metadata={}
    ) == [
        (("kernel", "mitxonline", OS), "existing", 0),
        (("kernel", "mitxonline", ML), "new", 1),
    ]
