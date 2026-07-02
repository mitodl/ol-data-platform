from b2b_organization.assets.data_export import export_b2b_organization_data
from b2b_organization.assets.starrocks_mv_refresh import (
    refresh_starrocks_analytics_mvs,
)
from b2b_organization.resources.starrocks import StarRocksResource
from b2b_organization.sensors.b2b_organization import b2b_organization_list_sensor
from dagster import (
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault

b2b_bucket_map = {
    "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
    "ci": {"bucket": "ol-b2b-partners-storage-ci", "prefix": ""},
    "qa": {"bucket": "ol-b2b-partners-storage-qa", "prefix": ""},
    "production": {"bucket": "ol-b2b-partners-storage-production", "prefix": ""},
}

# Hosts match the starrocks_qa / starrocks_production target defaults in
# src/ol_dbt/profiles.yml. dev/ci fall back to the QA FE for schema parity;
# the MV-refresh asset isn't expected to run for real outside qa/production.
starrocks_host_map = {
    "dev": "lakehouse.qa.starrocks.ol.mit.edu",
    "ci": "lakehouse.qa.starrocks.ol.mit.edu",
    "qa": "lakehouse.qa.starrocks.ol.mit.edu",
    "production": "lakehouse-starrocks-fe-service.starrocks.svc.cluster.local",
}

# Matches the database-starrocks-{env} mount convention used by
# bin/starrocks-auth and ol_dbt_cli/commands/starrocks.py.
starrocks_vault_mount_map = {
    "dev": "database-starrocks-qa",
    "ci": "database-starrocks-qa",
    "qa": "database-starrocks-qa",
    "production": "database-starrocks-production",
}

# Initialize vault with resilient loading
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    from ol_orchestrate.resources.secrets.vault import Vault

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False


b2b_organization_data_export_job = define_asset_job(
    name="b2b_organization_data_export_job",
    selection=[export_b2b_organization_data],
)

# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=b2b_bucket_map[DAGSTER_ENV]["bucket"],
            path_prefix=b2b_bucket_map[DAGSTER_ENV]["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "starrocks": StarRocksResource(
            vault=vault,
            vault_mount_point=starrocks_vault_mount_map[DAGSTER_ENV],
            host=starrocks_host_map[DAGSTER_ENV],
            database="b2b_analytics",
        ),
    },
    assets=[export_b2b_organization_data, refresh_starrocks_analytics_mvs],
    jobs=[b2b_organization_data_export_job],
    sensors=[b2b_organization_list_sensor],
)
