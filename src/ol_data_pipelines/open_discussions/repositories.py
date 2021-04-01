"""Reposiotries for open-discussions pipelines."""
from dagster import repository

from ol_data_pipelines.open_discussions.sensors import (
    mitx_bigquery_pipeline_completion_sensor,
)
from ol_data_pipelines.open_discussions.solids import (
    pull_open_data_pipeline,
    update_enrollments_pipeline,
)


@repository
def open_data_repository():
    """Repositories for open enrollments pipeline.

    :returns: open data pipelines and schedules
    """
    return [
        pull_open_data_pipeline,
        update_enrollments_pipeline,
        mitx_bigquery_pipeline_completion_sensor,
    ]
