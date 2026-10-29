"""Standalone run of one budgeted batch.

DLT_PROFILE=dev python -m ol_dlt.sources.edxorg_program_reports
"""

import logging

from ol_dlt.sources.edxorg_program_reports import (
    edxorg_program_reports_pipeline,
    edxorg_program_reports_source,
)

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s",
    edxorg_program_reports_pipeline().run(edxorg_program_reports_source()),
)
