from pathlib import PosixPath

from dagster import PythonObjectDagsterType, usable_as_dagster_type
from google.cloud.bigquery.dataset import DatasetListItem


@usable_as_dagster_type
class DagsterPath(PosixPath):
    pass  # noqa: WPS420, WPS604


DatasetDagsterType = PythonObjectDagsterType(DatasetListItem, name="DatasetDagsterType")
