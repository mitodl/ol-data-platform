"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.openlearning``."""

import logging

from ol_dlt.sources.openlearning import build_source, openlearning_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", openlearning_pipeline.run(build_source())
)
