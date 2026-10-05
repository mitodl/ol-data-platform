"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.medium``."""

import logging

from ol_dlt.sources.medium import build_source, medium_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", medium_pipeline.run(build_source())
)
