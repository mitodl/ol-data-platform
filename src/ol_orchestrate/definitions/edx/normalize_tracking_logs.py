import os  # noqa: INP001
from datetime import UTC, datetime  # type: ignore  # noqa: PGH003
from functools import partial
from typing import Literal

from boto3 import Session
from dagster import (
    Definitions,
    daily_partitioned_config,
)
from dagster_aws.s3.resources import s3_resource
from dagster_duckdb import DuckDBResource
from ol_orchestrate.jobs.normalize_logs import (
    jsonify_tracking_logs,
    normalize_tracking_logs,
)

dagster_env: Literal["dev", "qa", "production"] = os.environ.get(  # type: ignore  # noqa: E501, PGH003
    "DAGSTER_ENVIRONMENT", "dev"
)
# deployment: Literal["mitx", "mitxonline", "xpro"] = os.environ.get(  # type: ignore  # noqa: E501, PGH003
#     "OPEN_EDX_DEPLOYMENT_NAME", "xpro")


def earliest_log_date(
    dagster_env: Literal["dev", "qa", "production"],
    deployment_name: Literal["mitx", "mitxonline", "xpro"],
) -> datetime:
    if dagster_env == "dev":
        dagster_env = "qa"
    date_map = {
        "qa": {
            "mitx": datetime(2021, 3, 10, tzinfo=UTC),
            "mitxonline": datetime(2021, 7, 26, tzinfo=UTC),
            "xpro": datetime(2021, 3, 31, tzinfo=UTC),
        },
        "production": {
            "mitx": datetime(2017, 6, 13, tzinfo=UTC),
            "mitxonline": datetime(2021, 8, 18, tzinfo=UTC),
            "xpro": datetime(2019, 8, 28, tzinfo=UTC),
        },
    }
    return date_map[dagster_env][deployment_name]


def daily_tracking_log_config(
    deployment, destination, log_date: datetime, _end: datetime
):
    global dagster_env  # noqa: PLW0603
    if dagster_env == "dev":
        dagster_env = "qa"
    log_bucket = f"{deployment}-{dagster_env}-edxapp-tracking"
    session = Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    s3_creds = {
        "s3_key": current_credentials.access_key,
        "s3_secret": current_credentials.secret_key,
    }
    if current_credentials.token:
        s3_creds["s3_token"] = current_credentials.token
    return {
        "resources": {
            "duckdb": {
                "config": {
                    "database": f"{deployment}_tracking_logs_{log_date.strftime('%Y_%m_%d')}.duckdb",  # noqa: E501
                }
            }
        },
        "ops": {
            "load_s3_files_to_duckdb": {
                "config": {
                    "tracking_log_bucket": log_bucket,
                    **s3_creds,
                    "path_prefix": "logs" if destination == "valid" else "valid",
                },
                "inputs": {
                    "log_date": f"{log_date.strftime('%Y-%m-%d')}/",
                },
            },
            "export_processed_data_to_s3": {
                "config": {
                    "tracking_log_bucket": log_bucket,
                    "source_path_prefix": "logs" if destination == "valid" else "valid",
                    "destination_path_prefix": destination,
                },
                "inputs": {
                    "log_date": f"{log_date.strftime('%Y-%m-%d')}/",
                },
            },
        },
    }


normalize_logs = Definitions(
    resources={
        "duckdb": DuckDBResource.configure_at_launch(),
        "s3": s3_resource,
    },
    jobs=[
        normalize_tracking_logs.to_job(
            config=daily_partitioned_config(
                start_date=earliest_log_date(dagster_env, deployment)  # type: ignore  # noqa: E501, PGH003
            )(partial(daily_tracking_log_config, deployment, "valid")),
            name=f"normalize_{deployment}_{dagster_env}_tracking_logs",
        )
        for deployment in ["xpro", "mitx", "mitxonline"]
    ]
    + [
        jsonify_tracking_logs.to_job(
            config=daily_partitioned_config(
                start_date=earliest_log_date(dagster_env, deployment)  # type: ignore  # noqa: E501, PGH003
            )(partial(daily_tracking_log_config, deployment, "logs")),
            name=f"jsonify_{deployment}_{dagster_env}_tracking_logs",
        )
        for deployment in ["xpro", "mitx", "mitxonline"]
    ],
)
