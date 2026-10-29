"""Re-export every Canvas course partition every six hours."""

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV

from data_loading.defs.canvas.assets import canvas_course_ids, export_course_content

# Asset job that will be executed per partition (course_id)
canvas_course_export_job = define_asset_job(
    name="canvas_course_export_job",
    selection=[export_course_content],
    partitions_def=canvas_course_ids,
)


@schedule(
    cron_schedule="0 */6 * * *",
    job=canvas_course_export_job,
    execution_timezone="Etc/UTC",
    required_resource_keys={"google_sheet_config"},
    # RUNNING in production in the canvas location at the move (99 runs a tick
    # in production Loki, none in QA's). Instigator state is keyed on location
    # name, so here it would otherwise start STOPPED.
    default_status=(
        DefaultScheduleStatus.RUNNING
        if DAGSTER_ENV == "production"
        else DefaultScheduleStatus.STOPPED
    ),
)
def canvas_course_export_schedule(context: ScheduleEvaluationContext):
    """Return a RunRequest for each canvas course ID found in the Google Sheet"""
    partition_keys = context.instance.get_dynamic_partitions("canvas_course_ids")

    return [
        RunRequest(
            run_key=partition_key,
            partition_key=partition_key,
        )
        for partition_key in partition_keys
    ]


defs = Definitions(
    jobs=[canvas_course_export_job],
    schedules=[canvas_course_export_schedule],
)
