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
from ml.lib.courseware.extract import (
    BEDROCK_DEFAULT_MODEL,
    DEFAULT_EFFORT,
    Effort,
    extract_scopes,
    extraction_key,
)
from ml.lib.courseware.prompts import SYSTEM_PROMPT, scope_message
from ml.lib.courseware.scopes import CHARS_PER_TOKEN, build_scopes
from ml.resources.llm import LLMClientFactory

app = cyclopts.App(help=__doc__)
EXTRACTION_TABLE = "courseware_claim_extraction"


def _existing(output_dir: Path, table: str) -> pl.DataFrame | None:
    path = output_dir / f"{table}.parquet"
    return pl.read_parquet(path) if path.exists() else None


def _write(frame: pl.DataFrame, output_dir: Path, table: str) -> None:
    """Replace a table's file whole, so an interrupted write leaves the old one."""
    path = output_dir / f"{table}.parquet"
    staged = path.with_suffix(".parquet.tmp")
    frame.write_parquet(staged)
    staged.replace(path)


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


if __name__ == "__main__":
    app()
