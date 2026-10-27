import os

# The courseware assets resolve their dev schema from this at import time, and
# fail without it.
os.environ.setdefault("DBT_SCHEMA_SUFFIX", "test")
