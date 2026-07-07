"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.mitpe``."""

import logging

from ol_dlt.sources.mitpe import build_source, mitpe_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", mitpe_pipeline.run(build_source())
)
