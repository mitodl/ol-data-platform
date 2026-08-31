"""Standalone smoke run: ``DLT_PROFILE=dev python -m ol_dlt.sources.mitxonline_app``.

Requires the local-dev Postgres cluster to be reachable, e.g.
``kubectl port-forward -n local-infra svc/local-pg-rw 5432:5432``.
"""

import logging

from ol_dlt.sources.mitxonline_app import build_source, mitxonline_app_pipeline

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info(
    "Pipeline completed: %s", mitxonline_app_pipeline.run(build_source())
)
