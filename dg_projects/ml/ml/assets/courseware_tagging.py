"""Dictionary tagging of courseware text with the concepts dim_concept_alias knows.

A run tags only the sources whose text changed since the newest stored run, unless
the dictionary, TAGGER_ID or the tagger's parameters changed, which re-tags every
source (see ml.lib.courseware.tagging). courseware_concept_tagging_source records,
per run and current source, the run whose mention rows hold that source's mentions,
and int__courseware__concept_tags reads mentions through the newest run's records.
"""

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    MetadataValue,
    Output,
    multi_asset,
)
from ml.lib.courseware.tagging import (
    SOURCE_COLUMNS,
    SOURCE_SCHEMA,
    TAGGER_ID,
    ConceptDictionary,
    PreviousRun,
    newest_run,
    tag_sources,
    tagging_input_key,
)
from ml.lib.courseware.warehouse import latest_run_rows
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes

SOURCES_KEY = AssetKey(["intermediate", "int__courseware__extraction_sources"])
ALIASES_KEY = AssetKey(["dimensional", "dim_concept_alias"])
RUN_TABLE = "courseware_concept_tagging_run"
SOURCE_TABLE = "courseware_concept_tagging_source"
# Written in this order; the run row goes last, because dbt and the next run read
# the newest run in courseware_concept_tagging_run and then that run's source
# records, which name the mention rows to read.
TAGGING_TABLES = (
    "courseware_concept_mention",
    SOURCE_TABLE,
    "courseware_concept_tagging_alias",
    RUN_TABLE,
)
_OUTPUT_METADATA = {
    "schema": warehouse_database("intermediate"),
    "write_mode": "append",
    "schema_update_mode": "update",
}


@multi_asset(
    group_name="courseware",
    deps=[SOURCES_KEY, ALIASES_KEY],
    outs={
        table: AssetOut(
            key=AssetKey(["intermediate", table]),
            io_manager_key="io_manager",
            metadata=_OUTPUT_METADATA,
            # A new TAGGER_ID re-tags every source, so its deploy requests a run.
            code_version=TAGGER_ID,
            automation_condition=upstream_or_code_changes(),
            # Nothing is written when the newest run already has these inputs.
            is_required=False,
        )
        for table in TAGGING_TABLES
    },
)
def courseware_concept_tagging(context: AssetExecutionContext):
    """Tag every extraction source with the known concepts its text mentions.

    Skips the run when the newest stored run has the same input key (tagger,
    parameters, dictionary and every source's text). Otherwise calls
    ml.lib.courseware.tagging.tag_sources with the newest run and its source
    records, so only new or changed sources are tagged while the dictionary and
    tagger are unchanged. No model is called.
    """
    sources = (
        require_table("intermediate", SOURCES_KEY.path[-1])
        .select(SOURCE_COLUMNS)
        .collect()
    )
    aliases = (
        require_table("dimensional", ALIASES_KEY.path[-1])
        .select("concept_label_key", "concept_fk", "alias")
        .collect()
    )
    runs = read_table("intermediate", RUN_TABLE)
    last_run = newest_run(runs.collect() if runs is not None else None)
    if last_run is not None and last_run["input_key"] == tagging_input_key(
        ConceptDictionary(aliases), sources
    ):
        context.log.info(
            "Tagging run %s already has these inputs; nothing written",
            last_run["tagging_run_id"],
        )
        return

    previous = None
    if last_run is not None:
        records = latest_run_rows(
            "intermediate", SOURCE_TABLE, "tagging_run_id", last_run["tagging_run_id"]
        )
        previous = PreviousRun(
            last_run,
            records if records is not None else pl.DataFrame(schema=SOURCE_SCHEMA),
        )
    result = tag_sources(sources, aliases, previous)
    run = result.run.row(0, named=True)
    context.log.info(
        "Tagging run %s tagged %d of %d sources (%d mentions), carrying forward %s",
        run["tagging_run_id"],
        run["retagged_source_count"],
        run["source_count"],
        run["mention_count"],
        run["previous_tagging_run_id"],
    )
    for table, frame in zip(
        TAGGING_TABLES,
        (result.mentions, result.sources, result.aliases, result.run),
        strict=True,
    ):
        yield Output(
            frame,
            output_name=table,
            metadata={"row_count": MetadataValue.int(frame.height)},
        )
