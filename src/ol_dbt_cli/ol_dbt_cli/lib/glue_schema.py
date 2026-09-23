"""Read landed table schemas out of AWS Glue.

Split from ``lib.cursor_audit`` so the classification stays pure and
offline-testable: this is the only part that needs credentials, and it is the
only part that cannot be unit-tested without them.

The Glue catalog is used rather than the source database on purpose. What a
loader can key on is constrained by what actually LANDED, not by what the
application's ORM declares -- a column that exists upstream but was never
selected into the connection is not a cursor, and reading Glue needs no source
replica credential.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

DEFAULT_GLUE_DATABASE = "ol_warehouse_production_raw"


def columns_by_table(
    database: str = DEFAULT_GLUE_DATABASE,
    *,
    prefixes: Iterable[str] | None = None,
    region: str = "us-east-1",
) -> dict[str, list[str]]:
    """Map raw table name -> its landed column names.

    ``prefixes`` narrows the scan with a Glue expression per prefix, which is
    much cheaper than listing a 2,000-table database when only a few units are
    of interest. Omitted, the whole database is read.

    boto3 is imported here rather than at module scope so that importing this
    module -- which ``ol-dbt`` does at startup to register the command -- does
    not pull boto3 in for every other subcommand.
    """
    import boto3  # noqa: PLC0415

    client = boto3.client("glue", region_name=region)
    paginator = client.get_paginator("get_tables")
    expressions = [f"{p}.*" for p in prefixes] if prefixes else [None]

    out: dict[str, list[str]] = {}
    for expression in expressions:
        kwargs = {"DatabaseName": database}
        if expression:
            kwargs["Expression"] = expression
        for page in paginator.paginate(**kwargs):
            for table in page["TableList"]:
                storage = table.get("StorageDescriptor") or {}
                out[table["Name"]] = [column["Name"] for column in storage.get("Columns", [])]
    return out
