from typing import Any, Literal

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
)
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.edxorg_api import edxorg_program_metadata
from ol_orchestrate.io_managers.filepath import S3FileObjectIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import default_io_manager
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from ol_orchestrate.resources.secrets.vault import Vault

if DAGSTER_ENV == "dev":
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault._auth_github()  # noqa: SLF001
else:
    vault = Vault(
        vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
    )
    vault._auth_aws_iam()  # noqa: SLF001


def s3_uploads_bucket(
    dagster_env: Literal["dev", "qa", "production"],
) -> dict[str, Any]:
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
        "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": ""},
        "production": {
            "bucket": "ol-data-lake-landing-zone-production",
            "prefix": "",
        },
    }
    return bucket_map[dagster_env]


edxorg_api_daily_schedule = ScheduleDefinition(
    name="edxorg_api_schedule",
    target=AssetSelection.assets(edxorg_program_metadata),
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)

edxorg_program_metadata_extract = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "edxorg_api": OpenEdxApiClientFactory(deployment="edxorg", vault=vault),
    },
    assets=[edxorg_program_metadata],
    schedules=[edxorg_api_daily_schedule],
)
