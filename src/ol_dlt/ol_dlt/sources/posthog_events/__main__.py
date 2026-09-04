"""Standalone run for posthog_events (reads the prod S3 landing zone; needs AWS).

A bare run reads one hour so the smoke test stays cheap::

    DLT_PROFILE=dev python -m ol_dlt.sources.posthog_events

A backfill names its own window and chunk size. Repeat it, advancing
``--start-date``, until the cursor reaches the present::

    DLT_PROFILE=production python -m ol_dlt.sources.posthog_events \
        --start-date 2025-01-01 --max-objects 240
"""

import logging
from datetime import date

from cyclopts import App

from ol_dlt.sources.posthog_events import (
    posthog_events_pipeline,
    posthog_events_source,
)

app = App(name="posthog-events", help="Load the PostHog hourly event export.")


@app.default
def run(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    max_objects: int = 1,
) -> None:
    """Run the PostHog event ingestion.

    Args:
        start_date: Ingest from this UTC date, ignoring the stored cursor. Use
            2025-01-01 to reach the start of the export.
        end_date: Stop at the end of this UTC date.
        max_objects: Hour objects to read in this run. Defaults to 1 so a bare
            invocation is a smoke test rather than a backfill.
    """
    logging.basicConfig(level=logging.INFO)
    load_info = posthog_events_pipeline.run(
        posthog_events_source(
            start_date=start_date,
            end_date=end_date,
            max_objects=max_objects,
        ),
        loader_file_format="parquet",
    )
    logging.getLogger(__name__).info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    app()
