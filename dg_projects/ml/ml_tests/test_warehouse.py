import pyarrow as pa
import pytest
from dagster import Failure
from ml.lib import warehouse
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import NoSuchTableError

ROWS = pa.table(
    {
        "etl_source": ["mitxonline", "mitxonline", "xpro"],
        "run_readable_id": ["run-a", "run-b", "run-a"],
        "content": ["A of mitxonline.", "B of mitxonline.", "A of xpro."],
        "key": ["a", "b", "c"],
    }
)


@pytest.fixture
def catalog(tmp_path, monkeypatch) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("ol_warehouse_production_test_integrations")
    catalog.create_table(
        "ol_warehouse_production_test_integrations.content_files", ROWS.schema
    ).append(ROWS)

    def load(database_name: str, table_name: str):
        return catalog.load_table(f"{database_name}.{table_name}")

    monkeypatch.setattr(warehouse, "load_dbt_model_table", load)
    return catalog


@pytest.mark.usefixtures("catalog")
def test_require_rows_reads_only_the_matching_rows_and_columns():
    rows = warehouse.require_rows(
        "integrations",
        "content_files",
        ["key", "content"],
        etl_source="mitxonline",
        run_readable_id="run-a",
    )

    assert rows.columns == ["key", "content"]
    assert rows.to_dicts() == [{"key": "a", "content": "A of mitxonline."}]


def test_require_rows_of_a_missing_table_fails(monkeypatch):
    def missing(_database_name: str, table_name: str):
        raise NoSuchTableError(table_name)

    monkeypatch.setattr(warehouse, "load_dbt_model_table", missing)

    with pytest.raises(Failure, match="has not been built yet"):
        warehouse.require_rows("integrations", "content_files", ["key"], key="a")
