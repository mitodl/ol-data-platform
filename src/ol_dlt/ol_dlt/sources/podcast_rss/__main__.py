"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.podcast_rss``."""

import logging

from ol_dlt.sources.podcast_rss import build_source, podcast_rss_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", podcast_rss_pipeline.run(build_source())
)
