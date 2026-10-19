import os

# litellm fetches its model cost map from GitHub at import unless this is set.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
