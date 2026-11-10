"""Standalone smoke run: ``DLT_PROFILE=ci python -m ol_dlt.sources.google_ads``."""

import logging

from ol_dlt.sources.google_ads import build_source, google_ads_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", google_ads_pipeline.run(build_source())
)
