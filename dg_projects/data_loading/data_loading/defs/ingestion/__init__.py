"""Dagster wrappers around the ol_dlt pipelines.

Maps ``DAGSTER_ENVIRONMENT`` -> ``DLT_PROFILE`` on package import, before any
``ol_dlt`` source module is imported. ol_dlt pipeline factories read
``DLT_PROFILE`` at import time (in ``ol_dlt.config.pipeline_for``), so the
profile must be set first. The mapping is identity today (dev/ci/qa/production),
but centralising it here keeps the env->profile contract in one place.
"""

import os

os.environ.setdefault("DLT_PROFILE", os.getenv("DAGSTER_ENVIRONMENT", "dev"))
