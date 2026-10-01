"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.see``."""

import logging

from ol_dlt.sources.see import build_source, see_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", see_pipeline.run(build_source())
)
