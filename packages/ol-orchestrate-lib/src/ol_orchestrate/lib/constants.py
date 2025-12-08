import os
from typing import Literal

DAGSTER_ENV: Literal["dev", "ci", "qa", "production"] = os.environ.get(  # type: ignore[assignment]
    "DAGSTER_ENVIRONMENT", "dev"
)

if DAGSTER_ENV == "dev":
    VAULT_ADDRESS = os.getenv("VAULT_ADDR", "https://vault-qa.odl.mit.edu")
else:
    VAULT_ADDRESS = os.getenv("VAULT_ADDR", f"https://vault-{DAGSTER_ENV}.odl.mit.edu")

OPENEDX_DEPLOYMENTS = ["mitx", "mitxonline", "xpro"]

EXPORT_TYPE_COMMON_CARTRIDGE = "common_cartridge"
EXPORT_TYPE_ZIP = "zip"
EXPORT_TYPE_QTI = "qti"

EXPORT_TYPE_EXTENSIONS = {
    EXPORT_TYPE_COMMON_CARTRIDGE: "imscc",
    EXPORT_TYPE_ZIP: EXPORT_TYPE_ZIP,  # Extension matches the type
    EXPORT_TYPE_QTI: EXPORT_TYPE_QTI,  # Extension matches the type
}
