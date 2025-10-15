from dagster import PythonObjectDagsterType
from google.cloud.bigquery.dataset import DatasetListItem

DatasetDagsterType = PythonObjectDagsterType(DatasetListItem, name="DatasetDagsterType")
