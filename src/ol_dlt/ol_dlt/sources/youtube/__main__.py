"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.youtube``."""

import logging

from ol_dlt.sources.youtube import build_source, youtube_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", youtube_pipeline.run(build_source())
)
