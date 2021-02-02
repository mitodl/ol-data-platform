"""Reposiotries for open-discussions pipelines"""
from dagster import repository

from ol_data_pipelines.open_discussions.solids import pull_open_data_pipeline


@repository
def open_data_repository():
    """repository for pulling open and run data to s3"""
    return [pull_open_data_pipeline]
