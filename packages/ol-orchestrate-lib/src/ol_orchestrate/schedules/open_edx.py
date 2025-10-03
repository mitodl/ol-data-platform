from dagster import RunRequest, schedule


@schedule(
    job_name="dummy_job",
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def residential_edx_daily_schedule(execution_date):  # noqa: ARG001
    return RunRequest(
        run_key="residential_edx_course_pipeline",
        tags={"business_unit": "residential"},
    )


@schedule(
    job_name="dummy_job",
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def xpro_edx_daily_schedule(execution_date):  # noqa: ARG001
    return RunRequest(
        run_key="xpro_edx_course_pipeline",
        tags={"business_unit": "mitxpro"},
    )


@schedule(
    job_name="dummy_job",
    cron_schedule="@daily",
    execution_timezone="Etc/UTC",
)
def mitxonline_edx_daily_schedule(execution_date):  # noqa: ARG001
    return RunRequest(
        run_key="mitxonline_edx_course_pipeline",
        tags={"business_unit": "mitxonline"},
    )
