from typing import Literal
from dagster_duckdb import DuckDBResource, build_duckdb_io_manager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_aws.s3.resources import s3_resource
import os
from dagster import Definitions, daily_partitioned_config
from datetime import datetime

from ol_orchestrate.jobs.normalize_logs import normalize_tracking_logs


dagster_env = os.environ.get("DAGSTER_ENVIRONMENT", "dev")
deployment = os.environ["OPEN_EDX_DEPLOYMENT_NAME"]


def earliest_log_date(
    dagster_env: Literal["qa", "production"],
    deployment_name: Literal["mitx", "mitxonline", "xpro"],
) -> datetime:
    date_map = {
        "qa": {
            "mitx": datetime(2021, 3, 10),  # noqa: DTZ001
            "mitxonline": datetime(2021, 7, 26),  # noqa: DTZ001
            "xpro": datetime(2021, 3, 31),  # noqa: DTZ001
        },
        "production": {
            "mitx": datetime(2017, 6, 13),  # noqa: DTZ001
            "mitxonline": datetime(2021, 8, 18),  # noqa: DTZ001
            "xpro": datetime(2019, 8, 28),  # noqa: DTZ001
        },
    }
    return date_map[dagster_env][deployment_name]


@daily_partitioned_config(start_date=earliest_log_date(dagster_env, deployment))
def daily_tracking_log_config(log_date: datetime, _end: datetime):
    return {
        "ops": {
            "load_s3_files_to_duckdb": {
                "config": {
                    "tracking_log_bucket": f"{deployment}-{dagster_env}-tracking-logs"
                },
                "inputs": {
                    "log_date": f"{log_date.strftime('%Y-%m-%d')}/",
                },
            },
            "export_processed_data_to_s3": {
                "config": {
                    "tracking_log_bucket": f"{deployment}-{dagster_env}-tracking-logs"
                }
            },
        }
    }


duckdb_io_manager = build_duckdb_io_manager([DuckDBPandasTypeHandler])

normalize_logs = Definitions(
    resources={
        "duckdb": DuckDBResource(database="tracking_logs.duckdb"),
        "duckdb_io": duckdb_io_manager.configured(
            {"database": "tracking_logs_io.duckdb"}
        ),
        "s3": s3_resource,
    },
    jobs=[
        normalize_tracking_logs.to_job(
            config=daily_tracking_log_config,
            name=f"normalize_{deployment}_{dagster_env}_tracking_logs",
        )
    ],
)
