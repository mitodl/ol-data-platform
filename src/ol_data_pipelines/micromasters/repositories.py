"""Reposiotries for micromasters pipelines."""
from dagster import repository

from ol_data_pipelines.micromasters.solids import pull_micromasters_data_pipeline


@repository
def micromasters_repository():
    """Repositories for micromasters pipeline.

    :returns: micrinasters pipelines
    """
    return [pull_micromasters_data_pipeline]
