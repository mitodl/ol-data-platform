import os
from pathlib import Path

import pydantic_ai.models
import pytest
from ol_llm import Settings, TracingConfig, configure, configure_tracing
from pyiceberg.catalog.sql import SqlCatalog

# The courseware assets resolve their dev schema from this at import time, and
# fail without it.
os.environ.setdefault("DBT_SCHEMA_SUFFIX", "test")

# Without these ol-llm reads OL_LLM_ENVIRONMENT and the Opik SDK's own settings,
# which would turn tracing on and send get_prompt to an Opik that isn't there.
configure(Settings(environment="ci"))
configure_tracing(TracingConfig(enabled=False))

# A test that reaches a real provider model (e.g. an ol-llm override left behind in
# the caller's context while process_batch runs the call on a worker thread) fails
# instead of sending a request.
pydantic_ai.models.ALLOW_MODEL_REQUESTS = False


@pytest.fixture
def iceberg_catalog(tmp_path: Path) -> SqlCatalog:
    """Return a real Iceberg catalog on SQLite with a some_db namespace.

    For tests of what a write does to a table: fakes of pyiceberg's table API
    miss how its reads and writes depend on snapshots and schemas.
    """
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("some_db")
    return catalog
