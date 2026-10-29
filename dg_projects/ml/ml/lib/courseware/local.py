"""Run courseware claim extraction outside Dagster, from and to local parquet files.

For proof-of-concept runs against the local DuckDB lake: export
int__courseware__extraction_sources to parquet, run this, then expose the three
output files to dbt as the courseware_intermediate sources. Re-running skips every
scope whose extraction key is already in the output directory.

    python -m ml.lib.courseware.local extract sources.parquet out/ --dry-run
"""

import logging
from pathlib import Path

import cyclopts
import polars as pl
from anthropic import AnthropicBedrock
from ml.lib.courseware.answer_check import (
    DEFAULT_SOURCE_KINDS,
    check_sources,
    check_version,
    pending_sources,
)
from ml.lib.courseware.describe import describe_concepts
from ml.lib.courseware.evaluation.cli import gold
from ml.lib.courseware.extract import (
    BEDROCK_DEFAULT_MODEL,
    DEFAULT_EFFORT,
    Effort,
    extract_scopes,
    extraction_key,
)
from ml.lib.courseware.graph import compute_graph, previous_membership
from ml.lib.courseware.prompts import SYSTEM_PROMPT, scope_message
from ml.lib.courseware.resolve import EMBEDDING_DIM as RESOLUTION_EMBEDDING_DIM
from ml.lib.courseware.resolve import ResolutionState, resolve_concepts
from ml.lib.courseware.scopes import CHARS_PER_TOKEN, build_scopes
from ml.lib.courseware.tagging import ConceptDictionary, tag_sources, tagging_input_key
from ml.lib.embed import resolve_embedding_model
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import OPIK_PROJECT_NAME
from ol_llm import Attribution, UseCase
from ol_orchestrate.lib.embeddings.models import OlLlmEmbedder

app = cyclopts.App(help=__doc__)
app.command(gold)
# Descriptions are short, batched and many; the small model is the default.
BEDROCK_DESCRIPTION_MODEL = "us.anthropic.claude-haiku-5-5"
# Answer checks are short and many; the small model is the default.
BEDROCK_ANSWER_CHECK_MODEL = "us.anthropic.claude-haiku-5-5"
EXTRACTION_TABLE = "courseware_claim_extraction"
RESOLUTION_EMBEDDING_MODEL = "bedrock:amazon.titan-embed-text-v2:0"


def _existing(output_dir: Path, table: str) -> pl.DataFrame | None:
    path = output_dir / f"{table}.parquet"
    return pl.read_parquet(path) if path.exists() else None


def _write(frame: pl.DataFrame, output_dir: Path, table: str) -> None:
    """Replace a table's file whole, so an interrupted write leaves the old one."""
    path = output_dir / f"{table}.parquet"
    staged = path.with_suffix(".parquet.tmp")
    frame.write_parquet(staged)
    staged.replace(path)


def _append(output_dir: Path, table: str, new_rows: pl.DataFrame) -> None:
    previous = _existing(output_dir, table)
    _write(
        # diagonal: a column added since the file was written is null in older rows
        new_rows
        if previous is None
        else pl.concat([previous, new_rows], how="diagonal_relaxed"),
        output_dir,
        table,
    )


@app.command
def extract(  # noqa: PLR0913
    sources: Path,
    output_dir: Path,
    *,
    courserun: list[str] | None = None,
    scope_title: list[str] | None = None,
    model_id: str = BEDROCK_DEFAULT_MODEL,
    effort: Effort = DEFAULT_EFFORT,
    aws_region: str = "us-east-1",
    max_workers: int = 6,
    dry_run: bool = False,
) -> None:
    """Extract claims for every scope of the runs in a sources parquet file.

    :param sources: int__courseware__extraction_sources exported to parquet
    :param output_dir: where the three output tables are read from and written to
    :param courserun: limit to these courserun_readable_ids
    :param scope_title: limit to scopes with these titles, e.g. for a smoke test
    :param model_id: Bedrock model or inference profile id
    :param effort: output_config.effort
    :param aws_region: Bedrock region
    :param max_workers: concurrent model calls
    :param dry_run: print the scopes and their estimated input size, call nothing
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    rows = pl.read_parquet(sources)
    if courserun:
        rows = rows.filter(pl.col("courserun_readable_id").is_in(courserun))
    scopes = [
        scope
        for _, run_rows in rows.group_by("platform", "courserun_readable_id")
        for scope in build_scopes(run_rows)
        if not scope_title or scope.scope_title in scope_title
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    stored = _existing(output_dir, EXTRACTION_TABLE)
    existing_keys = set(stored["extraction_key"]) if stored is not None else set()

    if dry_run:
        system_tokens = len(SYSTEM_PROMPT) // CHARS_PER_TOKEN
        total = 0
        for scope in scopes:
            tokens = system_tokens + len(scope_message(scope)) // CHARS_PER_TOKEN
            stored_marker = (
                "stored"
                if extraction_key(scope, model_id, effort) in existing_keys
                else ""
            )
            total += 0 if stored_marker else tokens
            print(  # noqa: T201
                f"{scope.course.courserun_readable_id} {scope.scope_key:<70.70} "
                f"{len(scope.sources):>3} sources ~{tokens:>7,} tokens {stored_marker}"
            )
        print(f"{len(scopes)} scopes, ~{total:,} input tokens to send")  # noqa: T201
        return

    client = LLMClientFactory(
        client_class="bedrock", aws_region=aws_region
    ).get_client()
    if not isinstance(client, AnthropicBedrock):
        msg = f"bedrock client_class built a {type(client).__name__}"
        raise TypeError(msg)
    tables = extract_scopes(
        scopes,
        client,
        model_id=model_id,
        effort=effort,
        existing_keys=existing_keys,
        max_workers=max_workers,
    )
    # A stored extraction key is what makes a re-run skip a scope, so the extraction
    # table is written last. A run cut off before it leaves claim and evidence rows
    # with no extraction, which are dropped here and written again.
    for table, new_rows in (
        ("courseware_claim_evidence", tables.evidence),
        ("courseware_claim", tables.claims),
        (EXTRACTION_TABLE, tables.extractions),
    ):
        previous = _existing(output_dir, table)
        if previous is not None:
            previous = previous.filter(pl.col("extraction_key").is_in(existing_keys))
        _write(
            new_rows if previous is None else pl.concat([previous, new_rows]),
            output_dir,
            table,
        )
    print(  # noqa: T201
        f"{tables.extractions.height} extractions, {tables.claims.height} claims, "
        f"{tables.evidence.height} evidence rows; "
        f"{tables.extractions['input_tokens'].sum():,} in / "
        f"{tables.extractions['output_tokens'].sum():,} out tokens"
    )


def _latest_run(
    output_dir: Path, table: str, run_id: str | None
) -> pl.DataFrame | None:
    rows = _existing(output_dir, table)
    if rows is None or run_id is None:
        return None
    return rows.filter(pl.col("resolution_run_id") == run_id)


@app.command
def resolve(  # noqa: PLR0913
    concept_labels: Path,
    sense_inputs: Path,
    output_dir: Path,
    *,
    model_id: str = BEDROCK_DESCRIPTION_MODEL,
    aws_region: str = "us-east-1",
    rebuild: bool = False,
) -> None:
    """Assign concept labels to stable concept ids, resolving only new labels, then
    assign each course run's use of a label to one of the label's senses.

    :param concept_labels: int__courseware__concept_labels exported to parquet
    :param sense_inputs: int__courseware__concept_sense_inputs exported to parquet
    :param output_dir: where the resolution tables are read and appended
    :param model_id: Bedrock model or inference profile id for label and sense
        adjudication
    :param aws_region: Bedrock region for adjudication; the label embeddings go
        through ol-llm, which reads AWS_DEFAULT_REGION
    :param rebuild: re-resolve every label, carrying ids forward by overlap
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    client = LLMClientFactory(
        client_class="bedrock", aws_region=aws_region
    ).get_client()
    if not isinstance(client, AnthropicBedrock):
        msg = f"bedrock client_class built a {type(client).__name__}"
        raise TypeError(msg)
    embedding_model = resolve_embedding_model(
        RESOLUTION_EMBEDDING_MODEL, dim=RESOLUTION_EMBEDDING_DIM
    )
    embedding_client = OlLlmEmbedder(
        embedding_model.spec,
        UseCase(
            app=OPIK_PROJECT_NAME,
            name="courseware-concept-embedding",
            profile="batch",
            model=embedding_model.model,
        ),
        Attribution(),
    )

    runs = _existing(output_dir, "courseware_resolution_run")
    last_run = (
        runs.sort("started_at")["resolution_run_id"][-1] if runs is not None else None
    )
    state = ResolutionState()
    for attribute, table in (
        ("registry", "courseware_concept_registry"),
        ("assignments", "courseware_concept_assignment"),
        ("relations", "courseware_concept_relation"),
        ("aliases", "courseware_concept_alias"),
        ("sense_assignments", "courseware_concept_sense_assignment"),
    ):
        snapshot = _latest_run(output_dir, table, last_run)
        if snapshot is not None:
            setattr(state, attribute, snapshot)
    cached = _existing(output_dir, "courseware_concept_label_embedding")
    if cached is not None:
        state.embeddings = cached

    result = resolve_concepts(
        pl.read_parquet(concept_labels),
        state,
        client,
        embedding_client,
        mentions=pl.read_parquet(sense_inputs),
        model_id=model_id,
        rebuild=rebuild,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, new_rows in (
        ("courseware_concept_registry", result.registry),
        ("courseware_concept_assignment", result.assignments),
        ("courseware_concept_relation", result.relations),
        ("courseware_concept_alias", result.aliases),
        ("courseware_concept_sense_assignment", result.sense_assignments),
        ("courseware_resolution_decision", result.decisions),
        ("courseware_sense_decision", result.sense_decisions),
        ("courseware_concept_label_embedding", result.embeddings),
        ("courseware_resolution_run", result.run),
    ):
        _append(output_dir, table, new_rows)
    print(result.run.to_dicts()[0])  # noqa: T201


@app.command
def graph(concepts: Path, relations: Path, output_dir: Path) -> None:
    """Compute the concept graph's topology and append it to the graph tables.

    :param concepts: dim_concept exported to parquet
    :param relations: bridge_concept_relation exported to parquet
    :param output_dir: where the courseware_concept_graph_* tables are appended
    """
    runs = _existing(output_dir, "courseware_concept_graph_run")
    last_run = (
        runs.sort("computed_at")["graph_run_id"][-1] if runs is not None else None
    )
    result = compute_graph(
        pl.read_parquet(concepts),
        pl.read_parquet(relations),
        previous_nodes=previous_membership(
            _existing(output_dir, "courseware_concept_graph_node"),
            _existing(output_dir, "courseware_concept_graph_community"),
            last_run,
        ),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, new_rows in (
        ("courseware_concept_graph_node", result.nodes),
        ("courseware_concept_graph_edge", result.edges),
        ("courseware_concept_graph_community", result.communities),
        ("courseware_concept_graph_prerequisite_closure", result.prerequisite_closure),
        ("courseware_concept_graph_run", result.run),
    ):
        _append(output_dir, table, new_rows)
    print(result.run.to_dicts()[0])  # noqa: T201


@app.command
def describe(
    description_inputs: Path,
    output_dir: Path,
    *,
    model_id: str = BEDROCK_DESCRIPTION_MODEL,
    effort: Effort = DEFAULT_EFFORT,
    aws_region: str = "us-east-1",
) -> None:
    """Write learner-facing descriptions for concepts that have none yet.

    :param description_inputs: int__courseware__concept_description_inputs as parquet
    :param output_dir: where courseware_concept_description is appended
    :param model_id: Bedrock model or inference profile id
    :param effort: output_config.effort
    :param aws_region: Bedrock region
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    client = LLMClientFactory(
        client_class="bedrock", aws_region=aws_region
    ).get_client()
    if not isinstance(client, AnthropicBedrock):
        msg = f"bedrock client_class built a {type(client).__name__}"
        raise TypeError(msg)
    table = "courseware_concept_description"
    previous = _existing(output_dir, table)
    new_rows = describe_concepts(
        pl.read_parquet(description_inputs),
        client,
        model_id=model_id,
        effort=effort,
        existing=previous,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    _append(output_dir, table, new_rows)
    print(f"{new_rows.height} concepts described")  # noqa: T201


@app.command
def tag(sources: Path, aliases: Path, output_dir: Path) -> None:
    """Tag every source with the known concepts its text mentions.

    :param sources: int__courseware__extraction_sources exported to parquet
    :param aliases: dim_concept_alias exported to parquet
    :param output_dir: where the courseware_concept_tagging_* and
        courseware_concept_mention tables are appended
    """
    source_rows = pl.read_parquet(sources)
    alias_rows = pl.read_parquet(aliases)
    runs = _existing(output_dir, "courseware_concept_tagging_run")
    input_key = tagging_input_key(ConceptDictionary(alias_rows), source_rows)
    # only the newest run counts: after A, B, A the warehouse must move back to A
    if runs is not None and runs.sort("tagged_at")["input_key"][-1] == input_key:
        print("the newest tagging run already has these inputs")  # noqa: T201
        return
    result = tag_sources(source_rows, alias_rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, new_rows in (
        ("courseware_concept_mention", result.mentions),
        ("courseware_concept_tagging_alias", result.aliases),
        ("courseware_concept_tagging_run", result.run),
    ):
        _append(output_dir, table, new_rows)
    print(result.run.to_dicts()[0])  # noqa: T201


@app.command(name="answer-check")
def answer_check(  # noqa: PLR0913
    sources: Path,
    output_dir: Path,
    *,
    courserun: list[str] | None = None,
    source_kind: list[str] | None = None,
    model_id: str = BEDROCK_ANSWER_CHECK_MODEL,
    effort: Effort = "low",
    aws_region: str = "us-east-1",
    dry_run: bool = False,
) -> None:
    """Ask a small model which sources read as answer keys, for review.

    :param sources: int__courseware__extraction_sources exported to parquet
    :param output_dir: where courseware_answer_check is read and appended
    :param courserun: limit to these courserun_readable_ids
    :param source_kind: the source kinds to check (default: document)
    :param model_id: Bedrock model or inference profile id
    :param effort: output_config.effort
    :param aws_region: Bedrock region
    :param dry_run: print how many sources would be checked, call nothing
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    rows = pl.read_parquet(sources)
    if courserun:
        rows = rows.filter(pl.col("courserun_readable_id").is_in(courserun))
    kinds = tuple(source_kind) if source_kind else DEFAULT_SOURCE_KINDS
    table = "courseware_answer_check"
    previous = _existing(output_dir, table)
    if dry_run:
        pending, to_check, _ = pending_sources(
            rows, check_version(model_id, effort), previous, kinds
        )
        print(  # noqa: T201
            f"{len(pending)} sources of kind {', '.join(kinds)} need a verdict; "
            f"{len(to_check)} distinct texts would be sent, the rest reuse one"
        )
        return
    client = LLMClientFactory(
        client_class="bedrock", aws_region=aws_region
    ).get_client()
    if not isinstance(client, AnthropicBedrock):
        msg = f"bedrock client_class built a {type(client).__name__}"
        raise TypeError(msg)
    new_rows = check_sources(
        rows,
        client,
        model_id=model_id,
        effort=effort,
        existing=previous,
        source_kinds=kinds,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    _append(output_dir, table, new_rows)
    flagged = new_rows.filter(pl.col("is_answer_document")).height
    print(f"{new_rows.height} sources checked, {flagged} flagged")  # noqa: T201


if __name__ == "__main__":
    app()
