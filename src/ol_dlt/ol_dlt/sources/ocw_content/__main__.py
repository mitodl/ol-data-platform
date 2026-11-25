"""Run one load of the OCW content source: a sweep, or the courses named."""

import sys

from ol_dlt.sources.ocw_content import (
    build_source,
    ocw_content_pipeline,
    ocw_course_pipeline,
)

if __name__ == "__main__":
    named = sys.argv[1:] or None
    pipeline = ocw_course_pipeline if named else ocw_content_pipeline
    print(pipeline.run(build_source(named)))  # noqa: T201
