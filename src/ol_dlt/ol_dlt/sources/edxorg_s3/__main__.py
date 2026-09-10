"""Standalone smoke run for edxorg_s3 (reads the prod S3 landing zone; needs AWS).

Run with ``DLT_PROFILE=dev python -m ol_dlt.sources.edxorg_s3``.
"""

import logging

from ol_dlt.sources.edxorg_s3 import (
    edxorg_s3_pipeline,
    edxorg_s3_source,
    pop_unreadable_files,
)

# Load a small subset for a standalone smoke run.
_SMOKE_TABLES = ["auth_user", "student_courseenrollment"]

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s",
    edxorg_s3_pipeline.run(
        edxorg_s3_source(tables=_SMOKE_TABLES), loader_file_format="parquet"
    ),
)
if unreadable := {
    t: files for t in _SMOKE_TABLES if (files := pop_unreadable_files(t))
}:
    msg = f"Skipped unreadable edxorg TSVs: {unreadable}"
    raise SystemExit(msg)
