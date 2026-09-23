"""Standalone smoke run of both tables.

DLT_PROFILE=dev python -m ol_dlt.sources.course_xml_blocks
"""

import logging

from ol_dlt.sources.course_xml_blocks import (
    TABLES,
    course_xml_blocks_pipeline_for,
    course_xml_blocks_source,
)

logging.basicConfig(level=logging.INFO)
for raw_table in TABLES:
    logging.getLogger(__name__).info(
        "Pipeline completed: %s",
        course_xml_blocks_pipeline_for(raw_table).run(
            course_xml_blocks_source(raw_table=raw_table)
        ),
    )
