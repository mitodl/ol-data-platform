import os
from typing import Literal

DAGSTER_ENV: Literal["dev", "qa", "production"] = os.environ.get(  # type: ignore[assignment]
    "DAGSTER_ENVIRONMENT", "dev"
)

if DAGSTER_ENV == "dev":
    VAULT_ADDRESS = os.getenv("VAULT_ADDR", "https://vault-qa.odl.mit.edu")
else:
    VAULT_ADDRESS = os.getenv("VAULT_ADDR", f"https://vault-{DAGSTER_ENV}.odl.mit.edu")

OPENEDX_DEPLOYMENTS = ["mitx", "mitxonline", "xpro"]
