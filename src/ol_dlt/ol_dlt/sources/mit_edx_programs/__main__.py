"""Standalone smoke run for the MIT edX programs source (needs EDX_API_* creds).

Run with ``DLT_PROFILE=dev python -m ol_dlt.sources.mit_edx_programs``.
"""

import logging

from ol_dlt.sources.mit_edx_programs import build_source, mit_edx_programs_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", mit_edx_programs_pipeline.run(build_source())
)
