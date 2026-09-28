"""Small shared helpers for checking Iceberg table existence via a Glue catalog."""

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError


def table_exists(catalog: Catalog, table_identifier: str) -> bool:
    """Whether table_identifier (e.g. '<database>.<table>') exists in the catalog."""
    try:
        catalog.load_table(table_identifier)
    except NoSuchTableError:
        return False
    return True
