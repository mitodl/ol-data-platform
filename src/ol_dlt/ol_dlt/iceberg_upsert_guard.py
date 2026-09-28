"""Keep pyiceberg's upsert from reading matched rows at a stale schema.

pyiceberg's ``Transaction.upsert`` reads the rows its batch matches through a
scan pinned to the branch head (``use_ref``), and a pinned scan projects the
head *snapshot's* schema rather than the table's current one. dlt evolves the
table (``union_by_name``) before upserting, which commits a new schema but no
new snapshot. Until something writes a snapshot under that schema, any upsert
whose batch matches existing rows reads them without the new column and fails:

    ValueError: Target schema's field names are not matching the table's field
    names

That is how adding ``_dlt_load_id`` stuck three production merge tables
(DAGSTER-5Y). An empty append commits a snapshot under the current schema
without changing any rows, after which the upsert reads matched rows correctly.

Remove this module once a pyiceberg release fixes apache/iceberg-python#3105;
``tests/test_iceberg_upsert_guard.py`` fails as soon as the unguarded path
stops raising.
"""

import pyarrow as pa
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.common.schema.typing import TTableSchema
from pyiceberg.table import Table as IcebergTable

_dlt_merge_iceberg_table = dlt_pyiceberg.merge_iceberg_table


def merge_iceberg_table(
    table: IcebergTable,
    data: pa.Table,
    schema: TTableSchema,
    load_table_name: str,
) -> None:
    """Run dlt's merge after bringing the head snapshot up to the current schema."""
    with table.update_schema() as update:
        update.union_by_name(
            dlt_pyiceberg.ensure_iceberg_compatible_arrow_schema(data.schema)
        )
    snapshot = table.current_snapshot()
    if snapshot is not None and snapshot.schema_id != table.schema().schema_id:
        table.append(table.schema().as_arrow().empty_table())
    _dlt_merge_iceberg_table(
        table=table, data=data, schema=schema, load_table_name=load_table_name
    )


# dlt imports merge_iceberg_table inside IcebergLoadFilesystemJob.run at call
# time, so replacing the module attribute reaches every Iceberg merge load.
dlt_pyiceberg.merge_iceberg_table = merge_iceberg_table
