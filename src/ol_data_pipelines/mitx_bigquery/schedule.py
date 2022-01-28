from datetime import datetime, time

from dagster import daily_schedule

from ol_data_pipelines.lib.yaml_config_helper import load_yaml_config


@daily_schedule(
    pipeline_name="mitx_bigquery_pipeline",
    start_date=datetime(2021, 2, 1),
    execution_time=time(4, 0, 0),
    execution_timezone="Etc/UTC",
)
def mitx_bigquery_daily_schedule(execution_date):
    return load_yaml_config("/etc/dagster/mitx_bigquery.yaml")
