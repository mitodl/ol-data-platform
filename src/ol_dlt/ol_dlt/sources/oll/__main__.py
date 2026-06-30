"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.oll``."""

import logging

from ol_dlt.sources.oll import build_source, oll_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", oll_pipeline.run(build_source())
)
