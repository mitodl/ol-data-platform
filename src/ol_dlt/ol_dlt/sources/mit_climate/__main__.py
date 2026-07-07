"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.mit_climate``."""

import logging

from ol_dlt.sources.mit_climate import build_source, mit_climate_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", mit_climate_pipeline.run(build_source())
)
