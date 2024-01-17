import os
from typing import Literal

DAGSTER_ENV: Literal["ci", "qa", "production"] = os.environ.get(  # type: ignore[assignment]
    "DAGSTER_ENVIRONMENT", "ci"
)
