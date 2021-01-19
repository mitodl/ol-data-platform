from datetime import datetime, time

from dagster import daily_schedule

from ol_data_pipelines.mitx_bigquery.solids import mitx_bigquery_pipeline


@daily_schedule(
    pipeline_name="mitx_bigquery_pipeline",
    start_date=datetime(2020, 9, 23),
    execution_time=time(0, 0, 0),
    mode="production",
)
def good_morning_schedule(date):
    return {
        "solids": {
            "hello_cereal": {"inputs": {"date": {"value": date.strftime("%Y-%m-%d")}}}
        }
    }


def residential_edx_daily_schedule(execution_date):
    return mitx_bigquery_pipeline.run_config
