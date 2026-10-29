"""Write a learner-facing description for every resolved concept.

Claim descriptions say how one scope treats a concept ("used here to compare two
models"), which is evidence, not something to show a learner. This step reads up to
five of them per concept, from different courses, and writes one short and one
longer description that hold for any course. It also flags labels that are not
concepts a learner would look up (a dataset, a course-specific exercise), so
user-facing surfaces can leave them out.

A concept is described again only when its inputs or the prompt or model differ
from its newest description; rows are added, never replaced, like the extractions.
"""

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from itertools import batched
from typing import Any

import polars as pl
from anthropic import Anthropic, AnthropicBedrock, APIError
from ml.lib.courseware.schemas import Strict
from pydantic import Field, ValidationError

log = logging.getLogger(__name__)

DESCRIPTION_PROMPT = """\
You write the short descriptions shown next to concepts in a course catalog, for \
someone deciding what to learn. Each entry below is one concept: an id, the labels \
courses used for it, its kind, and notes on how specific courses treat it.

For each concept write:
- short_description: at most 12 words, a phrase that completes "This is ...", \
without the label itself, e.g. "a measure of how well a binary classifier ranks \
positives above negatives".
- description: one or two plain sentences, at most 45 words, that define the \
concept in general terms and say what it is used for. Write for a capable newcomer. \
Do not mention any particular course, lecture, dataset or instructor, and do not \
start with "This concept".
- is_learner_facing: false when the entry is not something a learner would look up \
as a topic: a dataset, a single exercise or case study, a course-internal label, \
or a phrase that is not a concept. Still describe it.

Use the course notes to pick the right sense of an ambiguous label (e.g. "kernel" \
in density estimation vs. operating systems), but describe the general concept, \
not the notes. Do not state facts the notes and general knowledge do not support.

Call record_descriptions once with one entry per id.
"""

DESCRIPTION_BATCH_SIZE = 40
DESCRIPTION_MAX_TOKENS = 32_000


class ConceptDescription(Strict):
    concept_id: int
    short_description: str
    description: str
    is_learner_facing: bool


class DescriptionBatch(Strict):
    descriptions: list[ConceptDescription] = Field(
        description="One entry per concept id in the list"
    )


DESCRIPTION_TOOL = {
    "name": "record_descriptions",
    "description": "Record the descriptions for every concept. Call it once.",
    "input_schema": DescriptionBatch.model_json_schema(),
}

DESCRIPTION_SCHEMA = {
    "concept_id": pl.String,
    "short_description": pl.String,
    "description": pl.String,
    "is_learner_facing": pl.Boolean,
    "input_checksum": pl.String,
    "description_version": pl.String,
    "model_id": pl.String,
    "described_at": pl.Datetime(time_zone="UTC"),
}


def _concept_inputs(rows: pl.DataFrame) -> list[dict[str, Any]]:
    """Group int__courseware__concept_description_inputs rows by concept."""
    concepts = []
    for (concept_id,), group in rows.sort("concept_id", "context_rank").group_by(
        "concept_id", maintain_order=True
    ):
        notes = [
            f"{row['course_title'] or 'a course'}: {row['description']}"
            for row in group.iter_rows(named=True)
        ]
        kinds = [kind for kind in group["concept_kind"] if kind]
        entry = {
            "concept_id": concept_id,
            "labels": sorted(set(group["label"])),
            "kind": kinds[0] if kinds else "unknown",
            "notes": notes,
        }
        entry["input_checksum"] = hashlib.sha256(
            json.dumps(entry, sort_keys=True).encode()
        ).hexdigest()
        concepts.append(entry)
    return concepts


def _describe_batch(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    effort: str,
    batch: list[dict[str, Any]],
) -> list[ConceptDescription]:
    listing = "\n\n".join(
        f"{index} | labels: {' / '.join(entry['labels'])} | kind: {entry['kind']}\n"
        + "\n".join(f"  - {note}" for note in entry["notes"])
        for index, entry in enumerate(batch)
    )
    with client.messages.stream(
        model=model_id,
        max_tokens=DESCRIPTION_MAX_TOKENS,
        system=DESCRIPTION_PROMPT,
        tools=[DESCRIPTION_TOOL],
        tool_choice={"type": "auto"},
        messages=[{"role": "user", "content": listing}],
        output_config={"effort": effort},
    ) as stream:
        response = stream.get_final_message()
    tool_input = next(
        (
            block.input
            for block in response.content
            if block.type == "tool_use" and block.name == DESCRIPTION_TOOL["name"]
        ),
        None,
    )
    if tool_input is None:
        msg = f"no {DESCRIPTION_TOOL['name']} call (stop_reason={response.stop_reason})"
        raise ValueError(msg)
    try:
        return DescriptionBatch.model_validate(tool_input).descriptions
    except ValidationError as error:
        msg = f"invalid descriptions: {error}"
        raise ValueError(msg) from error


def describe_concepts(  # noqa: PLR0913
    rows: pl.DataFrame,
    client: Anthropic | AnthropicBedrock,
    *,
    model_id: str,
    effort: str = "medium",
    existing: pl.DataFrame | None = None,
    max_workers: int = 4,
) -> pl.DataFrame:
    """Describe every concept whose inputs have no description yet.

    :param rows: int__courseware__concept_description_inputs rows
    :param client: an Anthropic or AnthropicBedrock client
    :param model_id: the model id the client expects
    :param effort: output_config.effort
    :param existing: descriptions already stored, to skip unchanged concepts
    :param max_workers: concurrent calls
    :returns: the new description rows
    :rtype: pl.DataFrame
    """
    version = hashlib.sha256(
        json.dumps([DESCRIPTION_PROMPT, model_id, effort]).encode()
    ).hexdigest()[:12]
    done: set[tuple[str, str]] = set()
    if existing is not None:
        # only a concept's newest row is what dim_concept shows, so an older row that
        # matches (inputs changed A, B, then back to A) is described again
        done = set(
            existing.sort("described_at")
            .group_by("concept_id")
            .last()
            .filter(pl.col("description_version") == version)
            .select("concept_id", "input_checksum")
            .iter_rows()
        )
    pending = [
        entry
        for entry in _concept_inputs(rows)
        if (entry["concept_id"], entry["input_checksum"]) not in done
    ]
    batches = [list(batch) for batch in batched(pending, DESCRIPTION_BATCH_SIZE)]
    described_at = datetime.now(tz=UTC)
    output: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_describe_batch, client, model_id, effort, batch)
            for batch in batches
        ]
        for batch, future in zip(batches, futures, strict=True):
            # a failed batch is left out; the next run describes only what is missing
            try:
                descriptions = future.result()
            except (ValueError, APIError):
                log.exception("Description batch of %d concepts failed", len(batch))
                continue
            by_id = {item.concept_id: item for item in descriptions}
            missing = [
                entry["concept_id"]
                for index, entry in enumerate(batch)
                if index not in by_id
            ]
            if missing:
                log.warning("No description returned for %d concepts", len(missing))
            output.extend(
                {
                    "concept_id": entry["concept_id"],
                    "short_description": by_id[index].short_description,
                    "description": by_id[index].description,
                    "is_learner_facing": by_id[index].is_learner_facing,
                    "input_checksum": entry["input_checksum"],
                    "description_version": version,
                    "model_id": model_id,
                    "described_at": described_at,
                }
                for index, entry in enumerate(batch)
                if index in by_id
            )
    return pl.DataFrame(output, schema=DESCRIPTION_SCHEMA)
