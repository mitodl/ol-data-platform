"""Standalone run of one budgeted batch of the table named on the command line.

DLT_PROFILE=dev python -m ol_dlt.sources.tracking_logs <raw_table>
"""

import logging
import sys

from ol_dlt.sources.tracking_logs import (
    tracking_logs_pipeline_for,
    tracking_logs_source,
)

logging.basicConfig(level=logging.INFO)
raw_table = sys.argv[1]
logging.getLogger(__name__).info(
    "Pipeline completed: %s",
    tracking_logs_pipeline_for(raw_table).run(
        tracking_logs_source(raw_table=raw_table)
    ),
)
