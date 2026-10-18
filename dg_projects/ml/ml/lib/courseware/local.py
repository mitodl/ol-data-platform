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
from ml.lib.courseware.describe import describe_concepts
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
from ml.lib.embed import BedrockEmbeddingClient, build_embedding_client
from ml.resources.llm import LLMClientFactory

app = cyclopts.App(help=__doc__)
# Descriptions are short, batched and many; the small model is the default.
BEDROCK_DESCRIPTION_MODEL = "us.anthropic.claude-haiku-5-5"
OUTPUT_TABLES = (
    "courseware_claim_extraction",
    "courseware_claim",
    "courseware_claim_evidence",
)


def _existing(output_dir: Path, table: str) -> pl.DataFrame | None:
    path = output_dir / f"{table}.parquet"
    return pl.read_parquet(path) if path.exists() else None


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
    stored = _existing(output_dir, "courseware_claim_extraction")
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
    for table, new_rows in zip(
        OUTPUT_TABLES, (tables.extractions, tables.claims, tables.evidence), strict=True
    ):
        previous = _existing(output_dir, table)
        combined = new_rows if previous is None else pl.concat([previous, new_rows])
        combined.write_parquet(output_dir / f"{table}.parquet")
    print(  # noqa: T201
        f"{tables.extractions.height} extractions, {tables.claims.height} claims, "
        f"{tables.evidence.height} evidence rows; "
        f"{tables.extractions['input_tokens'].sum():,} in / "
        f"{tables.extractions['output_tokens'].sum():,} out tokens"
    )


@app.command
def _latest_run(
    output_dir: Path, table: str, run_id: str | None
) -> pl.DataFrame | None:
    rows = _existing(output_dir, table)
    if rows is None or run_id is None:
        return None
    return rows.filter(pl.col("resolution_run_id") == run_id)


def _append(output_dir: Path, table: str, new_rows: pl.DataFrame) -> None:
    previous = _existing(output_dir, table)
    combined = new_rows if previous is None else pl.concat([previous, new_rows])
    combined.write_parquet(output_dir / f"{table}.parquet")


@app.command
def resolve(
    concept_labels: Path,
    output_dir: Path,
    *,
    model_id: str = BEDROCK_DESCRIPTION_MODEL,
    aws_region: str = "us-east-1",
    rebuild: bool = False,
) -> None:
    """Assign concept labels to stable concept ids, resolving only new labels.

    :param concept_labels: int__courseware__concept_labels exported to parquet
    :param output_dir: where the resolution tables are read and appended
    :param model_id: Bedrock model or inference profile id for adjudication
    :param aws_region: Bedrock region
    :param rebuild: re-resolve every label, carrying ids forward by overlap
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    client = LLMClientFactory(
        client_class="bedrock", aws_region=aws_region
    ).get_client()
    if not isinstance(client, AnthropicBedrock):
        msg = f"bedrock client_class built a {type(client).__name__}"
        raise TypeError(msg)
    embedding_client = build_embedding_client(
        LLMClientFactory(client_class="bedrock_embeddings", aws_region=aws_region),
        dim=RESOLUTION_EMBEDDING_DIM,
    )
    if not isinstance(embedding_client, BedrockEmbeddingClient):
        msg = f"bedrock_embeddings built a {type(embedding_client).__name__}"
        raise TypeError(msg)

    runs = _existing(output_dir, "courseware_resolution_run")
    last_run = (
        runs.sort("started_at")["resolution_run_id"][-1] if runs is not None else None
    )
    state = ResolutionState()
    for attribute, table in (
        ("registry", "courseware_concept_registry"),
        ("assignments", "courseware_concept_assignment"),
        ("relations", "courseware_concept_relation"),
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
        model_id=model_id,
        rebuild=rebuild,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, new_rows in (
        ("courseware_concept_registry", result.registry),
        ("courseware_concept_assignment", result.assignments),
        ("courseware_concept_relation", result.relations),
        ("courseware_resolution_decision", result.decisions),
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
    combined = new_rows if previous is None else pl.concat([previous, new_rows])
    combined.write_parquet(output_dir / f"{table}.parquet")
    print(f"{new_rows.height} concepts described")  # noqa: T201


if __name__ == "__main__":
    app()
