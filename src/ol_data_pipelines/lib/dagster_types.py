# -*- coding: utf-8 -*-
from pathlib import PosixPath
from google.cloud.bigquery.dataset import DatasetListItem
from dagster import usable_as_dagster_type, PythonObjectDagsterType


@usable_as_dagster_type
class DagsterPath(PosixPath):
    pass


DatasetDagsterType = PythonObjectDagsterType(DatasetListItem, name="DatasetDagsterType")
