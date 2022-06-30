from dagster import RunRequest, sensor
from dagster.core.storage.pipeline_run import PipelineRunsFilter, PipelineRunStatus

from ol_orchestrate.lib.yaml_config_helper import load_yaml_config


@sensor(
    pipeline_name="update_enrollments_pipeline",
    minimum_interval_seconds=600,
)
def mitx_bigquery_pipeline_completion_sensor(context):
    """
    Run update_enrollments_pipeline after mitx_bigquery_pipeline completes.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :yield: A RunRequest to begin update_enrollments_pipeline execution
    """
    runs = context.instance.get_runs(
        filters=PipelineRunsFilter(
            pipeline_name="mitx_bigquery_pipeline",
            statuses=[PipelineRunStatus.SUCCESS],
        ),
    )

    if runs:
        run = runs[0]

        yield RunRequest(
            run_key=str(run.run_id),
            run_config=load_yaml_config(
                "/etc/dagster/open-discussions-enrollment-update.yaml"
            ),
        )
