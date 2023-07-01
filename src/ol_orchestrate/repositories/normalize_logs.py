import os

import duckdb
from dagster import Definitions
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.normalize_logs import normalize_tracking_logs
from ol_orchestrate.lib.yaml_config_helper import load_yaml_config


resources = {
    # TODO is the .configured() necessary?
    "s3": s3_resource.configured(
        load_yaml_config("/etc/dagster/normalize_logs.yaml")["resources"]["s3"][
            "config"
        ]
    ),
    # TODO IMPORT AND CONFIGURE DUCKDB RESOURCE
    "duckDB": duckdb,
}

mitx_tracking_log_bucket = {
    "qa": "mitx-qa-edxapp-tracking",
    "production": "mitx-production-edxapp-tracking",
}

mitxonline_tracking_log_bucket = {
    "qa": "mitxonline-qa-edxapp-tracking",
    "production": "mitxonline-production-edxapp-tracking",
}

xpro_tracking_log_bucket = {
    "qa": "xpro-qa-edxapp-tracking",
    "production": "xpro-production-edxapp-tracking",
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")

mitx_normalization_job = normalize_tracking_logs.to_job(
    name="mitx_s3_log_normalization",
    resource_defs=resources,
    config={
        "ops": {
            "get_bucket_prefixes_from_s3": {
                "config": {
                    "tracking_log_bucket": mitx_tracking_log_bucket[dagster_deployment],
                    # TODO "start_date":
                }
            },
            "get_file_names_from_s3": {
                "config": {
                    "tracking_log_bucket": mitx_tracking_log_bucket[dagster_deployment],
                }
            },
            "load_s3_files_to_duckdb": {
                "config": {
                    "tracking_log_bucket": mitx_tracking_log_bucket[dagster_deployment],
                }
            },
            "transform_data_in_duckdb": {},
            "export_processed_data_to_s3": {
                "config": {
                    "tracking_log_bucket": mitx_tracking_log_bucket[dagster_deployment],
                }
            },
        }
    },
)


normalize_logs = Definitions(
    jobs=[
        mitx_normalization_job,
        # mitxonline_normalization_job,
        # xpro_normalization_job
    ]
)
