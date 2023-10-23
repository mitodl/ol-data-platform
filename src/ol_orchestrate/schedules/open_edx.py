from dagster import RunRequest, schedule

from ol_orchestrate.lib.yaml_config_helper import load_yaml_config


@schedule(
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def residential_edx_daily_schedule(execution_date):  # noqa: ARG001
    return RunRequest(
        run_key="residential_edx_course_pipeline",
        run_config=load_yaml_config("/etc/dagster/residential_edx.yaml"),
        tags={"business_unit": "residential"},
    )


@schedule(
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def xpro_edx_daily_schedule(execution_date):  # noqa: ARG001
    return RunRequest(
        run_key="xpro_edx_course_pipeline",
        run_config=load_yaml_config("/etc/dagster/xpro_edx.yaml"),
        tags={"business_unit": "mitxpro"},
    )


@schedule(
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def mitxonline_edx_daily_schedule(execution_date):  # noqa: ARG001
    return RunRequest(
        run_key="mitxonline_edx_course_pipeline",
        run_config=load_yaml_config("/etc/dagster/mitxonline_edx.yaml"),
        tags={"business_unit": "mitxonline"},
    )
