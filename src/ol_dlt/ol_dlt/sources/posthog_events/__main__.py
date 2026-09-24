"""Standalone run for posthog_events (reads the prod S3 landing zone; needs AWS).

A bare run reads one hour so the smoke test stays cheap::

    DLT_PROFILE=dev python -m ol_dlt.sources.posthog_events

This CLI runs against the ``posthog_events_backfill`` pipeline state, separate
from the hourly Dagster job, so a bounded chunk here cannot move the scheduled
cursor. Name the window once, then repeat without ``--start-date`` to walk
forward from where the previous chunk stopped::

    DLT_PROFILE=production python -m ol_dlt.sources.posthog_events \
        --start-date 2025-01-01 --max-objects 240
    DLT_PROFILE=production python -m ol_dlt.sources.posthog_events \
        --max-objects 240   # resumes at the backfill cursor

Each run is also capped at ``--budget-bytes`` of compressed export (256 MB by
default, a few hours at recent volumes), because one dlt load holds all of its
data in memory. Raising it only helps on a machine with the memory for it; a
long backfill is a loop over this command.
"""

import logging
from datetime import date

from cyclopts import App

from ol_dlt import config
from ol_dlt.sources.posthog_events import (
    BUDGET_BYTES,
    posthog_events_backfill_pipeline,
    posthog_events_source,
)

app = App(name="posthog-events", help="Load the PostHog hourly event export.")


@app.default
def run(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    max_objects: int = 1,
    budget_bytes: int = BUDGET_BYTES,
) -> None:
    """Run the PostHog event ingestion.

    Args:
        start_date: Ingest from this UTC date, ignoring the backfill cursor.
            Use 2025-01-01 to reach the start of the export. Omit it to resume
            where the previous chunk stopped.
        end_date: Stop at the end of this UTC date.
        max_objects: Hour objects to read in this run. Defaults to 1 so a bare
            invocation is a smoke test rather than a backfill.
        budget_bytes: Compressed export bytes this run may read. See
            ``BUDGET_BYTES`` for how that maps to memory.
    """
    logging.basicConfig(level=logging.INFO)
    load_info = posthog_events_backfill_pipeline.run(
        config.with_nullable_load_id(
            posthog_events_source(
                start_date=start_date,
                end_date=end_date,
                max_objects=max_objects,
                budget_bytes=budget_bytes,
            )
        ),
        loader_file_format="parquet",
    )
    logging.getLogger(__name__).info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    app()
