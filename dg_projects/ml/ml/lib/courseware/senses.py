"""Assign each course run's use of a concept label to one sense of that label.

Label resolution (resolve.py) clusters normalized labels into concepts, so on its
own every claim labelled "kernel" lands on one concept whether the course is about
operating systems, linear algebra or support vector machines. This module adds the
sense layer on top of it.

A label's senses are its aliases: the concept label resolution put it in (its
primary sense) and any concepts split off it for another meaning. The unit that is
assigned a sense is the label within one course run: a course uses a label in one
sense throughout ("one sense per discourse"), and the run's title and claim
descriptions are the context that tells the senses apart.

- A label seen in one course run only, with one sense, takes that sense without a
  model call, unless a run of the label is queued for review or that sense is the
  primary of another label that has split senses or is adjudicated in the same run:
  label resolution matched it against that label's description across all of its
  senses, so it may mean one of the splits.
- Every other new (label, course run) goes to a model with the label's senses and
  their glosses. The model picks an existing sense, groups runs into a new sense
  (minted as a concept split from the primary one), or says it cannot tell, which
  leaves the run unassigned in the review queue, asked again on each later run until
  reviewer decisions have somewhere to be stored. Nothing falls back to the first
  sense.
- An assignment is carried to later runs while its concept (after merges) is still
  one of the label's senses, so existing ids do not move.
"""

import hashlib
import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import batched
from typing import Any, Literal

import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.schemas import Strict
from pydantic import ValidationError

log = logging.getLogger(__name__)

# Labels per call, and course runs per label per call. A label with more new runs
# is decided over several rounds, each seeing the senses earlier rounds minted, so
# one new meaning is not minted once per chunk.
SENSE_LABELS_PER_CALL = 6
SENSE_RUNS_PER_LABEL = 10
# Claim descriptions shown per course run.
SENSE_DESCRIPTIONS_PER_RUN = 3
SENSE_MAX_TOKENS = 8_000

SenseDecision = Literal["existing", "new", "unsure"]

SENSE_PROMPT = """\
You keep apart the meanings of concept labels in a catalog of university courses. \
The same label can name different concepts in different courses, e.g. "kernel" in \
an operating systems course, in linear algebra and in support vector machines.

Each query is one label. It lists the senses the label already has (s0, s1, ...), \
each with a note on what it means, and course runs that use the label (r0, r1, \
...), each with its course title and how the course describes the label.

For every course run, decide which sense it uses:
- existing: one of the listed senses; give its number as sense
- new: none of the listed senses fits; give a new-sense number (1, 2, ...) as \
sense, the same number for every run of this query that uses that new meaning
- unsure: the context does not say which sense, or two senses fit equally

Prefer an existing sense when a course uses the label for the same thing with a \
different emphasis, depth or application. Choose new only for a different meaning.

Call record_senses once with a decision for every course run of every query.
"""


class RunSense(Strict):
    run_id: int
    decision: SenseDecision
    sense: int | None = None


class LabelSenses(Strict):
    query_id: int
    runs: list[RunSense]


class SenseBatch(Strict):
    queries: list[LabelSenses]


SENSE_TOOL = {
    "name": "record_senses",
    "description": "Record a sense decision for every course run of every query.",
    "input_schema": SenseBatch.model_json_schema(),
}

SENSE_PROMPT_VERSION = hashlib.sha256(
    json.dumps([SENSE_PROMPT, SenseBatch.model_json_schema()]).encode()
).hexdigest()[:12]

ALIAS_SCHEMA = {
    "resolution_run_id": pl.String,
    "concept_label_key": pl.String,
    "concept_id": pl.String,
    "sense_role": pl.String,
    "sense_gloss": pl.String,
    "added_run_id": pl.String,
}
SENSE_ASSIGNMENT_SCHEMA = {
    "resolution_run_id": pl.String,
    "concept_label_key": pl.String,
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "concept_id": pl.String,
    "sense_status": pl.String,
    "assignment_source": pl.String,
    "assigned_run_id": pl.String,
}
SENSE_DECISION_SCHEMA = {
    "resolution_run_id": pl.String,
    "concept_label_key": pl.String,
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "decision": pl.String,
    "concept_id": pl.String,
    "model_id": pl.String,
}

# A label as one course run uses it: concept label key, platform, course run.
UnitKey = tuple[str, str, str]


@dataclass(frozen=True)
class Alias:
    concept_id: str
    sense_role: Literal["primary", "split"]
    sense_gloss: str
    added_run_id: str


@dataclass(frozen=True)
class Unit:
    """One label as one course run uses it."""

    key: UnitKey
    label: str
    course_title: str
    descriptions: tuple[str, ...]

    @property
    def context(self) -> str:
        described = "; ".join(self.descriptions) or "no description"
        return (
            f"{self.label} in {self.course_title or 'an untitled course'}: {described}"
        )


@dataclass(frozen=True)
class SenseResult:
    aliases: pl.DataFrame
    assignments: pl.DataFrame
    decisions: pl.DataFrame
    calls: int
    senses_minted: int
    review_count: int


def sense_units(mentions: pl.DataFrame) -> dict[UnitKey, Unit]:
    """Group mentions into (label, course run) units with their context.

    :param mentions: int__courseware__concept_sense_inputs rows
    """
    grouped: dict[UnitKey, list[dict[str, Any]]] = defaultdict(list)
    for row in mentions.iter_rows(named=True):
        key = (
            row["concept_label_key"],
            row["platform"],
            row["courserun_readable_id"],
        )
        grouped[key].append(row)
    units = {}
    for key, rows in sorted(grouped.items()):
        label = Counter(row["label"] for row in rows).most_common(1)[0][0]
        # taught descriptions say what the label means better than used ones
        described = sorted(
            (row for row in rows if row["description"]),
            key=lambda row: (
                row["teaching_status"] != "taught",
                -len(row["description"]),
            ),
        )
        descriptions = tuple(
            dict.fromkeys(row["description"][:200] for row in described)
        )[:SENSE_DESCRIPTIONS_PER_RUN]
        units[key] = Unit(
            key=key,
            label=label,
            course_title=rows[0]["course_title"] or "",
            descriptions=descriptions,
        )
    return units


# ------------------------------------------------------------------------ adjudication
@dataclass(frozen=True)
class SenseQuery:
    label_key: str
    senses: list[Alias]
    units: list[Unit]


def _adjudicate_senses_batch(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    batch: list[SenseQuery],
) -> list[tuple[UnitKey, SenseDecision, int | None]]:
    """Return (unit key, decision, sense number) for every run the model decided."""
    listing = "\n\n".join(
        f"Q{query_index}: {query.units[0].label}\n"
        + "\n".join(
            f"  s{sense_index}: {alias.sense_gloss[:240]}"
            for sense_index, alias in enumerate(query.senses)
        )
        + "\n"
        + "\n".join(
            f"  r{run_index}: {unit.context[:400]}"
            for run_index, unit in enumerate(query.units)
        )
        for query_index, query in enumerate(batch)
    )
    with client.messages.stream(
        model=model_id,
        max_tokens=SENSE_MAX_TOKENS,
        system=SENSE_PROMPT,
        tools=[SENSE_TOOL],
        tool_choice={"type": "auto"},
        messages=[{"role": "user", "content": listing}],
        output_config={"effort": "medium"},
    ) as stream:
        response = stream.get_final_message()
    tool_input = next(
        (
            block.input
            for block in response.content
            if block.type == "tool_use" and block.name == SENSE_TOOL["name"]
        ),
        None,
    )
    if tool_input is None:
        msg = f"no sense decisions (stop_reason={response.stop_reason})"
        raise ValueError(msg)
    try:
        decided = SenseBatch.model_validate(tool_input)
    except ValidationError as error:
        msg = f"invalid sense decisions: {error}"
        raise ValueError(msg) from error
    # Two decisions for one run would mint two senses from the same context, so a
    # repeated query or run fails the batch and the next run retries it.
    query_ids = [query.query_id for query in decided.queries]
    run_ids = [
        (query.query_id, item.run_id)
        for query in decided.queries
        for item in query.runs
    ]
    if len(set(query_ids)) < len(query_ids) or len(set(run_ids)) < len(run_ids):
        msg = "a query or course run has more than one sense decision"
        raise ValueError(msg)
    # A query or run left out, or numbered past the listing, puts the numbering in
    # doubt (runs numbered from 1 would give each run its neighbour's sense), so the
    # batch fails and the next run asks about all of it again.
    if sorted(query_ids) != list(range(len(batch))):
        msg = (
            f"sense decisions for queries {sorted(query_ids)}, "
            f"expected 0 to {len(batch) - 1}"
        )
        raise ValueError(msg)
    for query in decided.queries:
        returned = sorted(item.run_id for item in query.runs)
        expected = len(batch[query.query_id].units)
        if returned != list(range(expected)):
            msg = (
                f"sense decisions for Q{query.query_id} runs {returned}, "
                f"expected 0 to {expected - 1}"
            )
            raise ValueError(msg)
    results: list[tuple[UnitKey, SenseDecision, int | None]] = []
    for query in decided.queries:
        sense_query = batch[query.query_id]
        for item in query.runs:
            decision, sense = item.decision, item.sense
            # an existing sense that is not listed, or a new one with no group, is
            # not a decision the run can be assigned by
            if (
                decision == "existing"
                and not (sense is not None and 0 <= sense < len(sense_query.senses))
            ) or (decision == "new" and sense is None):
                decision, sense = "unsure", None
            results.append((sense_query.units[item.run_id].key, decision, sense))
    return results


def adjudicate_senses(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    queries: list[SenseQuery],
    max_workers: int = 6,
) -> tuple[list[tuple[UnitKey, SenseDecision, int | None]], int]:
    """Decide the sense of every query's runs, in small batches of labels.

    A failed batch's runs get no decision, so they stay unassigned and the next run
    retries them.

    :returns: the decisions and how many calls were made
    """
    batches = [list(batch) for batch in batched(queries, SENSE_LABELS_PER_CALL)]
    decisions: list[tuple[UnitKey, SenseDecision, int | None]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_adjudicate_senses_batch, client, model_id, batch)
            for batch in batches
        ]
        for batch, future in zip(batches, futures, strict=True):
            try:
                decisions.extend(future.result())
            except (ValueError, APIError):
                log.exception("Sense batch of %d labels failed", len(batch))
    return decisions, len(batches)


# --------------------------------------------------------------------------- assignment
def _assignment(
    key: UnitKey, concept_id: str | None, source: str, run_id: str
) -> dict[str, Any]:
    return {
        "resolution_run_id": run_id,
        "concept_label_key": key[0],
        "platform": key[1],
        "courserun_readable_id": key[2],
        "concept_id": concept_id,
        "sense_status": "assigned" if concept_id else "needs_review",
        "assignment_source": source,
        "assigned_run_id": run_id,
    }


def carried_assignments(
    previous_assignments: pl.DataFrame,
    aliases: dict[str, list[Alias]],
    run_id: str,
    active: Callable[[str], str],
) -> dict[UnitKey, dict[str, Any]]:
    """Keep each earlier assignment whose concept is still one of the label's senses.

    A run in the review queue is not kept: until reviewer decisions are stored, each
    run asks again, as the run may have new claims or the label new senses since.
    A run whose concept is no longer one of the label's senses is decided again.
    """
    carried = {}
    for row in previous_assignments.iter_rows(named=True):
        key = (row["concept_label_key"], row["platform"], row["courserun_readable_id"])
        if key[0] not in aliases or row["sense_status"] == "needs_review":
            continue
        current = active(row["concept_id"])
        if current in {alias.concept_id for alias in aliases[key[0]]}:
            carried[key] = dict(row, resolution_run_id=run_id, concept_id=current)
    return carried


def _single_sense_labels(
    pending: dict[str, list[Unit]],
    aliases: dict[str, list[Alias]],
    assignments: dict[UnitKey, dict[str, Any]],
    previous_assignments: pl.DataFrame,
) -> list[str]:
    """Return the labels whose one new course run takes their one sense without a
    call: the label's first and only course run, where there is nothing to tell apart.

    A label with a run queued for review is excluded: the queued run is not
    carried, but the model could not tell the label's runs apart, so a run that
    survives is asked about again.

    So is a label whose sense is the primary of another label that has split senses
    or is adjudicated in this run (and may split one). Label resolution matched it
    against that label's description across all of its senses, so it may mean one
    of the splits.
    """
    seen_runs = Counter(key[0] for key in assignments)
    queued = set(
        previous_assignments.filter(pl.col("sense_status") == "needs_review")[
            "concept_label_key"
        ]
    )
    eligible = [
        label_key
        for label_key, label_units in pending.items()
        if len(aliases[label_key]) == 1
        and not seen_runs[label_key]
        and label_key not in queued
        and len(label_units) == 1
    ]
    adjudicated = pending.keys() - set(eligible)
    ambiguous = {
        senses[0].concept_id
        for label_key, senses in aliases.items()
        if len(senses) > 1 or label_key in adjudicated
    }
    return [
        label_key
        for label_key in eligible
        if aliases[label_key][0].concept_id not in ambiguous
    ]


def assign_senses(  # noqa: PLR0913
    units: dict[UnitKey, Unit],
    aliases: dict[str, list[Alias]],
    previous_assignments: pl.DataFrame,
    client: Anthropic | AnthropicBedrock,
    *,
    model_id: str,
    run_id: str,
    active: Callable[[str], str],
    mint_sense: Callable[[str, str], str],
) -> SenseResult:
    """Assign every (label, course run) unit to one of its label's senses.

    :param units: this run's units, from sense_units
    :param aliases: each label's senses, primary first; extended in place with the
        senses this run splits off
    :param previous_assignments: the previous run's sense assignments
    :param client: an Anthropic or AnthropicBedrock client
    :param model_id: the sense adjudication model
    :param run_id: this resolution run
    :param active: follows a concept id's merge redirects to the active concept
    :param mint_sense: mints a concept for a new sense of a label, given the label
        key and the primary concept it is split from, and returns its id
    """
    assignments = carried_assignments(previous_assignments, aliases, run_id, active)
    pending: dict[str, list[Unit]] = defaultdict(list)
    for key, unit in units.items():
        if key[0] in aliases and key not in assignments:
            pending[key[0]].append(unit)
    for label_key in _single_sense_labels(
        pending, aliases, assignments, previous_assignments
    ):
        key = pending.pop(label_key)[0].key
        assignments[key] = _assignment(
            key, aliases[label_key][0].concept_id, "single_sense", run_id
        )

    decision_rows: list[dict[str, Any]] = []
    calls = minted = 0
    chunks = {
        label_key: list(batched(label_units, SENSE_RUNS_PER_LABEL))
        for label_key, label_units in pending.items()
    }
    for round_index in range(max(map(len, chunks.values()), default=0)):
        queries = [
            SenseQuery(
                label_key, list(aliases[label_key]), list(label_chunks[round_index])
            )
            for label_key, label_chunks in chunks.items()
            if round_index < len(label_chunks)
        ]
        decided, round_calls = adjudicate_senses(client, model_id, queries)
        calls += round_calls
        # new-sense numbers are per call, and a call holds one chunk of a label
        new_senses: dict[tuple[str, int], str] = {}
        for key, decision, sense in sorted(decided, key=lambda item: item[0]):
            senses = aliases[key[0]]
            concept_id: str | None = None
            if decision == "existing" and sense is not None:
                concept_id = senses[sense].concept_id
            elif decision == "new" and sense is not None:
                concept_id = new_senses.get((key[0], sense))
                if concept_id is None:
                    concept_id = mint_sense(key[0], senses[0].concept_id)
                    new_senses[key[0], sense] = concept_id
                    senses.append(
                        Alias(concept_id, "split", units[key].context, run_id)
                    )
                    minted += 1
            source = {"existing": "context_selected", "new": "new_sense"}.get(
                decision, "unsure"
            )
            assignments[key] = _assignment(key, concept_id, source, run_id)
            decision_rows.append(
                {
                    "resolution_run_id": run_id,
                    "concept_label_key": key[0],
                    "platform": key[1],
                    "courserun_readable_id": key[2],
                    "decision": decision,
                    "concept_id": concept_id,
                    "model_id": model_id,
                }
            )

    return SenseResult(
        aliases=alias_frame(aliases, run_id),
        assignments=pl.DataFrame(
            [assignments[key] for key in sorted(assignments)],
            schema=SENSE_ASSIGNMENT_SCHEMA,
        ),
        decisions=pl.DataFrame(decision_rows, schema=SENSE_DECISION_SCHEMA),
        calls=calls,
        senses_minted=minted,
        review_count=sum(
            1 for row in assignments.values() if row["sense_status"] == "needs_review"
        ),
    )


def label_aliases(
    primaries: dict[str, str],
    previous_aliases: pl.DataFrame,
    glosses: dict[str, str],
    *,
    run_id: str,
    current: Callable[[str], str | None],
) -> dict[str, list[Alias]]:
    """Return each label's senses: its primary concept first, then the senses split
    off it.

    :param primaries: each label's concept from label resolution, redirect-resolved
    :param previous_aliases: the previous run's aliases
    :param glosses: the gloss for a label whose primary sense no label had before
    :param run_id: this resolution run
    :param current: a concept id's active concept after merges, or None when it
        was deprecated
    """
    previous: dict[str, list[dict[str, Any]]] = defaultdict(list)
    # the saved gloss of each concept that is some label's primary sense
    primary_glosses: dict[str | None, str] = {}
    for row in previous_aliases.iter_rows(named=True):
        previous[row["concept_label_key"]].append(row)
        if row["sense_role"] == "primary":
            primary_glosses.setdefault(current(row["concept_id"]), row["sense_gloss"])
    aliases: dict[str, list[Alias]] = {}
    for label_key, primary in sorted(primaries.items()):
        # The previous primary carries its gloss only while it is still the label's
        # concept. When a rebuild moves the label to another concept, the old one is
        # not kept as a sense: the label's course runs are decided again instead of
        # staying on a concept that is now another label's.
        carried_primary = next(
            (
                row
                for row in previous[label_key]
                if row["sense_role"] == "primary"
                and current(row["concept_id"]) == primary
            ),
            None,
        )
        kept = {
            concept_id: row
            for row in previous[label_key]
            if row["sense_role"] == "split"
            and (concept_id := current(row["concept_id"])) not in {None, primary}
        }
        # A label new to an existing concept takes that concept's saved gloss, not
        # its own description, so its runs are judged against what the concept means.
        if carried_primary:
            gloss = carried_primary["sense_gloss"]
        else:
            gloss = primary_glosses.get(primary, glosses[label_key])
        senses = [
            Alias(
                primary,
                "primary",
                gloss,
                carried_primary["added_run_id"] if carried_primary else run_id,
            )
        ]
        senses.extend(
            Alias(concept_id, "split", row["sense_gloss"], row["added_run_id"])
            for concept_id, row in sorted(kept.items())
        )
        aliases[label_key] = senses
    return aliases


def alias_frame(aliases: dict[str, list[Alias]], run_id: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "resolution_run_id": run_id,
                "concept_label_key": label_key,
                "concept_id": alias.concept_id,
                "sense_role": alias.sense_role,
                "sense_gloss": alias.sense_gloss,
                "added_run_id": alias.added_run_id,
            }
            for label_key, senses in sorted(aliases.items())
            for alias in senses
        ],
        schema=ALIAS_SCHEMA,
    )
