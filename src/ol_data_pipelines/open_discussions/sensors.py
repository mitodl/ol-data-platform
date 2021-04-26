from dagster import RunRequest, sensor
from dagster.core.storage.pipeline_run import PipelineRunsFilter, PipelineRunStatus

from ol_data_pipelines.open_discussions.solids import update_enrollments_pipeline

production_preset = update_enrollments_pipeline.get_preset("production")


@sensor(
    pipeline_name="update_enrollments_pipeline",
    mode="production",
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
            run_key=str(run.run_id), run_config=production_preset.run_config
        )
