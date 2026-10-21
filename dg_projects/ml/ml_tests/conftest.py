import os

from ol_llm import Settings, TracingConfig, configure, configure_tracing

# The courseware assets resolve their dev schema from this at import time, and
# fail without it.
os.environ.setdefault("DBT_SCHEMA_SUFFIX", "test")

# Without these ol-llm reads OL_LLM_ENVIRONMENT and the Opik SDK's own settings,
# which would turn tracing on and send get_prompt to an Opik that isn't there.
configure(Settings(environment="ci"))
configure_tracing(TracingConfig(enabled=False))
