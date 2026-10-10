"""`ol-dbt fixtures` — raw tables for the local lake, from committed files.

The local lake (ol-infrastructure local-dev, `data-platform` in enabled_apps)
starts with an empty raw schema, and a unit whose inventory entry says
``strategies.local: fixture`` has no loader that could fill it. ``load`` creates
those tables from ``ingestion/inventory/fixtures/``, which is all a contributor
needs: no Vault, no VPN, no AWS credentials. ``capture`` is the maintainer's
half, the one command here that reads Glue. The format and the reasons for it
are in ``ol_dbt_cli.lib.raw_fixtures``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Any, cast

import yaml
from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.cursor_audit import select_units
from ol_dbt_cli.lib.glue_schema import DEFAULT_GLUE_DATABASE, column_types_by_table
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, Unit, load_units
from ol_dbt_cli.lib.raw_fixtures import (
    FIXTURE_STRATEGY,
    create_table_sql,
    fixture_path,
    fixture_type_from_glue,
    insert_sql,
    load_fixtures,
    merge_captured,
    qualified_name,
    row_parameters,
)

console = Console()
err_console = Console(stderr=True)

fixtures_app = App(
    name="fixtures",
    help="Create the local lake's raw tables from committed fixture files.",
)

# The starrocks_local target in src/ol_dbt/profiles.yml: the catalog it names,
# the raw schema its sources resolve to (`<target.schema>_raw`), and the
# passwordless root the local StarRocks takes. `load` drops and recreates
# tables, so the catalog is not an option: no other environment has this name.
LOCAL_CATALOG = "ol_data_lake_local"
LOCAL_RAW_SCHEMA = "ol_warehouse_local_raw"
LOCAL_USER = "root"

InventoryDir = Annotated[
    Path,
    Parameter(name=["--inventory-dir", "-i"], help="Directory holding units/ and fixtures/."),
]
UnitKeys = Annotated[
    list[str] | None,
    Parameter(help="Limit to these units, as deployment/layer. Repeatable.", show_default=False),
]


def _select(inventory_dir: Path, unit: list[str] | None) -> list[Unit]:
    units = load_units(inventory_dir)
    if not unit:
        return units
    selected, missing = select_units(units, unit)
    if missing:
        err_console.print(f"[bold red]No unit matched: {', '.join(missing)}")
        sys.exit(1)
    return cast("list[Unit]", selected)


@fixtures_app.command
def load(
    *,
    inventory_dir: InventoryDir = DEFAULT_INVENTORY_DIR,
    unit: UnitKeys = None,
    host: Annotated[
        str | None, Parameter(help="StarRocks FE host. Default: DBT_STARROCKS_HOST, else 127.0.0.1.")
    ] = None,
    port: Annotated[int | None, Parameter(help="StarRocks query port. Default: DBT_STARROCKS_PORT, else 9030.")] = None,
    schema: Annotated[str, Parameter(help="Raw schema to create the tables in.")] = LOCAL_RAW_SCHEMA,
) -> None:
    """Create and fill the raw tables of every committed fixture.

    Each table is dropped and created again, so the lake holds exactly what the
    file says and a changed column list takes effect. Run it after the local-dev
    stack is up, then build with `ol-dbt starrocks build --env dev`.
    """
    fixtures = load_fixtures(inventory_dir, _select(inventory_dir, unit), selected=bool(unit))
    if not fixtures:
        err_console.print(f"[bold red]No fixtures under {escape(str(inventory_dir))}.")
        sys.exit(1)

    # Imported here for the reason boto3 is in lib.glue_schema: only this
    # command connects to StarRocks.
    import mysql.connector  # noqa: PLC0415

    connection = mysql.connector.connect(
        host=host or os.environ.get("DBT_STARROCKS_HOST", "127.0.0.1"),
        port=port or int(os.environ.get("DBT_STARROCKS_PORT", "9030")),
        user=LOCAL_USER,
        password="",
        autocommit=True,
    )
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {qualified_name(LOCAL_CATALOG, schema)}")
    for fixture in fixtures:
        for table in fixture.tables:
            cursor.execute(f"DROP TABLE IF EXISTS {qualified_name(LOCAL_CATALOG, schema, table.raw_table)} FORCE")
            cursor.execute(create_table_sql(table, LOCAL_CATALOG, schema))
            if table.rows:
                cursor.executemany(insert_sql(table, LOCAL_CATALOG, schema), row_parameters(table))
            console.print(f"{escape(schema)}.{escape(table.raw_table)}: {len(table.rows)} row(s)")
    connection.close()


@fixtures_app.command
def capture(
    *,
    unit: Annotated[
        list[str],
        Parameter(help="Units to capture, as deployment/layer. Repeatable."),
    ],
    inventory_dir: InventoryDir = DEFAULT_INVENTORY_DIR,
    table: Annotated[
        list[str] | None,
        Parameter(
            help="Raw tables to capture. Default: the unit's tables with `modeled: true`. Repeatable.",
            show_default=False,
        ),
    ] = None,
    glue_database: Annotated[
        str,
        Parameter(help="Glue database holding the landed raw tables."),
    ] = DEFAULT_GLUE_DATABASE,
    region: str = "us-east-1",
) -> None:
    """Write the columns and types of a unit's raw tables into its fixture file.

    Reads the landed schema from Glue, so it needs AWS credentials, and it reads
    no rows: the rows in a fixture are written by hand (see
    `ol_dbt_cli.lib.raw_fixtures`). A table already in the file keeps its rows
    and takes the captured columns. A column whose type the format cannot hold
    (struct, array, map, binary) is left out and named, since a staging model
    that reads it will not build from the fixture.
    """
    wanted = set(table or [])
    found: set[str] = set()
    for selected in _select(inventory_dir, unit):
        if selected.data["strategies"]["local"] != FIXTURE_STRATEGY:
            err_console.print(
                f"[bold red]{escape(selected.key)} has strategies.local: "
                f"{escape(str(selected.data['strategies']['local']))}; `load` would reject its fixture."
            )
            sys.exit(1)
        tables = [
            str(entry["raw_table"])
            for entry in selected.tables
            if (entry["raw_table"] in wanted if wanted else entry["modeled"])
        ]
        found.update(tables)
        if not tables:
            continue
        landed = column_types_by_table(glue_database, prefixes=[str(selected.data["table_prefix"])], region=region)
        captured: dict[str, dict[str, str]] = {}
        for raw_table in tables:
            if raw_table not in landed:
                err_console.print(f"[yellow]{escape(raw_table)} is not in {escape(glue_database)}; skipped.")
                continue
            columns: dict[str, str] = {}
            for column, glue_type in landed[raw_table].items():
                fixture_type = fixture_type_from_glue(glue_type)
                if fixture_type is None:
                    err_console.print(
                        f"[yellow]{escape(raw_table)}.{escape(column)}: no fixture type for "
                        f"{escape(glue_type)}; left out."
                    )
                    continue
                columns[column] = fixture_type
            captured[raw_table] = columns

        path = fixture_path(inventory_dir, selected)
        existing: dict[str, Any] | None = yaml.safe_load(path.read_text()) if path.exists() else None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("---\n" + yaml.safe_dump(merge_captured(existing, captured), sort_keys=False))
        console.print(f"Wrote {escape(str(path))}: {len(captured)} table(s)")

    if unknown := sorted(wanted - found):
        err_console.print(f"[bold red]Not a table of the selected units: {', '.join(unknown)}")
        sys.exit(1)
