"""`ol-dbt fixtures` — raw tables for the local lake, from committed files.

The local lake (ol-infrastructure local-dev, `data-platform` in enabled_apps)
starts with an empty raw schema, and a unit whose inventory entry says
``strategies.local: fixture`` has no loader that could fill it. ``load`` creates
those tables from ``ingestion/inventory/fixtures/``, which is all a contributor
needs: no Vault, no VPN, no AWS credentials. A unit with a factory package
under ``ol_dbt_cli.fixture_factories`` also gets that package's generated
rows, after the hand-written ones. ``generate`` writes what ``load`` would
insert without connecting to anything. ``capture`` is the maintainer's half,
the one command here that reads Glue. The format and the reasons for it are in
``ol_dbt_cli.lib.raw_fixtures``.
"""

from __future__ import annotations

import dataclasses
import json
import math
import sys
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, cast

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape
from ruamel.yaml import YAML

from ol_dbt_cli import fixture_factories
from ol_dbt_cli.lib.cursor_audit import select_units
from ol_dbt_cli.lib.dimensional_fixtures import (
    DEFAULT_DIMENSIONAL_FIXTURES_DIR,
    LOCAL_DIMENSIONAL_SCHEMA,
    dimensional_fixture_path,
    parse_dimensional_fixture,
    resolve_parameters,
)
from ol_dbt_cli.lib.glue_schema import DEFAULT_GLUE_DATABASE, column_types_by_table
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, Unit, load_units
from ol_dbt_cli.lib.raw_fixtures import (
    FIXTURE_STRATEGIES,
    TableFixture,
    create_table_sql,
    fixture_path,
    fixture_type_from_glue,
    insert_sql,
    load_fixtures,
    merge_captured,
    qualified_name,
    row_parameters,
    unit_factories,
)

if TYPE_CHECKING:
    from mysql.connector.abstracts import MySQLConnectionAbstract

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
# Where Tilt forwards the local StarRocks, and what `ol-dbt starrocks --env dev`
# sets DBT_STARROCKS_HOST to. Not read from that variable: a shell that exports
# it for the QA profile would send this command's root login to QA.
LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 9030
# Rows per INSERT. mysql-connector sends an executemany as one multi-row INSERT,
# and one of ~10k rows of a 28-column table OOM-killed the local StarRocks. Each
# batch commits one Iceberg snapshot.
INSERT_BATCH_ROWS = 1000
YAML_WIDTH = 4096
# Where `scaffold` writes a unit's factory package.
FACTORIES_DIR = Path(fixture_factories.__file__).parent
# The dbt project whose staging tests `scaffold` drafts from for a unit with no models.json.
DEFAULT_DBT_DIR = Path("src/ol_dbt")

InventoryDir = Annotated[
    Path,
    Parameter(name=["--inventory-dir", "-i"], help="Directory holding units/ and fixtures/."),
]
UnitKeys = Annotated[
    list[str] | None,
    Parameter(help="Limit to these units, as deployment/layer. Repeatable.", show_default=False),
]
Scale = Annotated[
    float,
    Parameter(help="Multiply every factory's default row count. Hand-written rows are not scaled."),
]
Seed = Annotated[
    int | None,
    Parameter(help="Seed generation with this in place of each factory package's SEED.", show_default=False),
]


@dataclasses.dataclass(frozen=True)
class _TableRows:
    """One table's rows in the order they load. ``generated`` is ``None`` for a unit with no factory package."""

    table: TableFixture
    hand_written: list[dict[str, Any]]
    generated: list[dict[str, Any]] | None

    @property
    def rows(self) -> list[dict[str, Any]]:
        return self.hand_written + (self.generated or [])

    def parameters(self) -> list[tuple[Any, ...]]:
        """Return the INSERT parameters, a mapping or list already written as JSON text."""
        return row_parameters(dataclasses.replace(self.table, rows=self.rows))


def _json_value(value: Any) -> str:
    # Decimal's str() turns a zero in a decimal(38,9) into 0E-9.
    return format(value, "f") if isinstance(value, Decimal) else str(value)


def connect_local_lake(host: str = LOCAL_HOST, port: int = LOCAL_PORT) -> MySQLConnectionAbstract:
    """Open the local StarRocks as its passwordless root.

    The one place that login is made, so the host stays the constant above
    unless a caller passes another.
    """
    # Imported here for the reason boto3 is in lib.glue_schema: only the
    # commands on the local lake connect to StarRocks.
    import mysql.connector  # noqa: PLC0415

    return cast(
        "MySQLConnectionAbstract",
        mysql.connector.connect(host=host, port=port, user=LOCAL_USER, password="", autocommit=True),
    )


def _replace_table(cursor: Any, schema: str, table: TableFixture, parameters: list[tuple[Any, ...]]) -> None:
    """Drop ``table`` in the local catalog's ``schema``, create it again and insert ``parameters``."""
    cursor.execute(f"DROP TABLE IF EXISTS {qualified_name(LOCAL_CATALOG, schema, table.raw_table)} FORCE")
    cursor.execute(create_table_sql(table, LOCAL_CATALOG, schema))
    for start in range(0, len(parameters), INSERT_BATCH_ROWS):
        cursor.executemany(insert_sql(table, LOCAL_CATALOG, schema), parameters[start : start + INSERT_BATCH_ROWS])


def _select(inventory_dir: Path, unit: list[str] | None) -> list[Unit]:
    units = load_units(inventory_dir)
    if not unit:
        return units
    selected, missing = select_units(units, unit)
    if missing:
        err_console.print(f"[bold red]No unit matched: {', '.join(missing)}")
        sys.exit(1)
    return cast("list[Unit]", selected)


def _table_rows(
    inventory_dir: Path, unit: list[str] | None, *, generate: bool, scale: float, seed: int | None
) -> list[_TableRows]:
    """Read the selected units' fixtures and run their factory packages.

    Every package runs before anything is written, so one that fails leaves
    the lake as it was.
    """
    if not (math.isfinite(scale) and scale > 0):
        err_console.print(f"[bold red]--scale is {scale}, expected a positive number.")
        sys.exit(1)
    units = _select(inventory_dir, unit)
    fixtures = load_fixtures(inventory_dir, units, selected=bool(unit))
    if not fixtures:
        err_console.print(f"[bold red]No fixtures under {escape(str(inventory_dir))}.")
        sys.exit(1)
    units_by_path = {fixture_path(inventory_dir, selected): selected for selected in units}
    table_rows: list[_TableRows] = []
    for fixture in fixtures:
        fixture_unit = units_by_path[fixture.path]
        factories = unit_factories(fixture_unit) if generate else None
        if factories is None:
            table_rows += [_TableRows(table, table.rows, None) for table in fixture.tables]
            continue
        # factory_boy and mimesis take seconds to import, and only generating needs them.
        from ol_dbt_cli.lib.fixture_generation import generate_rows  # noqa: PLC0415

        rows = generate_rows(fixture, fixture_unit, factories, scale, seed)
        table_rows += [
            _TableRows(table, rows.hand_written[table.raw_table], rows.generated.get(table.raw_table, []))
            for table in fixture.tables
        ]
    return table_rows


@fixtures_app.command
def load(
    *,
    inventory_dir: InventoryDir = DEFAULT_INVENTORY_DIR,
    unit: UnitKeys = None,
    host: Annotated[str, Parameter(help="StarRocks FE host.")] = LOCAL_HOST,
    port: Annotated[int, Parameter(help="StarRocks query port.")] = LOCAL_PORT,
    schema: Annotated[str, Parameter(help="Raw schema to create the tables in.")] = LOCAL_RAW_SCHEMA,
    generate: Annotated[
        bool, Parameter(help="Add each factory package's generated rows. --no-generate loads the files' rows only.")
    ] = True,
    scale: Scale = 1.0,
    seed: Seed = None,
) -> None:
    """Create and fill the raw tables of every committed fixture.

    Each table is dropped and created again, so the lake holds exactly what the
    file and the unit's factory package make and a changed column list takes
    effect. Run it after the local-dev stack is up, then build with
    `ol-dbt starrocks build --env dev`.
    """
    # Every row is made into parameters first, so a value that cannot be
    # written as JSON fails before a table is dropped.
    prepared = [
        (loaded, loaded.parameters())
        for loaded in _table_rows(inventory_dir, unit, generate=generate, scale=scale, seed=seed)
    ]

    connection = connect_local_lake(host, port)
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {qualified_name(LOCAL_CATALOG, schema)}")
    for loaded, parameters in prepared:
        table = loaded.table
        _replace_table(cursor, schema, table, parameters)
        counts = (
            f"{len(loaded.hand_written)} row(s)"
            if loaded.generated is None
            else f"{len(loaded.hand_written)} hand-written + {len(loaded.generated)} generated row(s)"
        )
        console.print(f"{escape(schema)}.{escape(table.raw_table)}: {counts}", soft_wrap=True)
    connection.close()


@fixtures_app.command(name="load-dimensional")
def load_dimensional(
    name: Annotated[str, Parameter(help="The fixture, as its file name under --fixtures-dir without .yml.")],
    *,
    set_: Annotated[
        list[str] | None,
        Parameter(name="--set", help="A value for a parameter the file declares, as NAME=VALUE. Repeatable."),
    ] = None,
    fixtures_dir: Annotated[
        Path, Parameter(help="Directory holding the dimensional fixture files.")
    ] = DEFAULT_DIMENSIONAL_FIXTURES_DIR,
    host: Annotated[str, Parameter(help="StarRocks FE host.")] = LOCAL_HOST,
    port: Annotated[int, Parameter(help="StarRocks query port.")] = LOCAL_PORT,
) -> None:
    """Create and fill dimensional tables from a committed fixture, for consumers that read only that layer.

    Writes into ol_warehouse_local_dimensional, where the `dimensional` dbt
    source resolves under `--env dev`. Each table is dropped and created
    again, which replaces any dimensional model of the same name you built
    locally. A file's parameters are ids the local stack assigns (e.g. a
    Keycloak organization id); the command lists any it is not given.
    """
    path = dimensional_fixture_path(fixtures_dir, name)
    if not path.exists():
        available = sorted(candidate.stem for candidate in fixtures_dir.glob("*.yml"))
        err_console.print(f"[bold red]No {escape(str(path))}. Fixtures: {', '.join(available) or 'none'}.")
        sys.exit(1)
    fixture = parse_dimensional_fixture(path)
    values: dict[str, str] = {}
    for assignment in set_ or []:
        key, separator, value = assignment.partition("=")
        # An empty value is most often an unset shell variable, and it would
        # load views keyed to nothing, which reads as a broken dashboard.
        if not separator or not key or not value:
            err_console.print(f"[bold red]--set {escape(assignment)}: expected NAME=VALUE with a non-empty VALUE.")
            sys.exit(1)
        if key in values:
            err_console.print(f"[bold red]--set {escape(key)} is given more than once.")
            sys.exit(1)
        values[key] = value
    try:
        tables = resolve_parameters(fixture, values)
    except ValueError as error:
        err_console.print(f"[bold red]{escape(str(error))}")
        for parameter in fixture.parameters.values():
            err_console.print(
                f"  --set {parameter.name}=<{parameter.fixture_type}>  {escape(parameter.description)}", soft_wrap=True
            )
        sys.exit(1)
    prepared = [(table, row_parameters(table)) for table in tables]

    connection = connect_local_lake(host, port)
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {qualified_name(LOCAL_CATALOG, LOCAL_DIMENSIONAL_SCHEMA)}")
    for table, parameters in prepared:
        _replace_table(cursor, LOCAL_DIMENSIONAL_SCHEMA, table, parameters)
        console.print(f"{LOCAL_DIMENSIONAL_SCHEMA}.{escape(table.raw_table)}: {len(parameters)} row(s)", soft_wrap=True)
    connection.close()


@fixtures_app.command(name="generate")
def generate_command(
    *,
    unit: Annotated[list[str], Parameter(help="Units to generate, as deployment/layer. Repeatable.")],
    inventory_dir: InventoryDir = DEFAULT_INVENTORY_DIR,
    table: Annotated[
        list[str] | None,
        Parameter(help="Only these raw tables. Default: every table of the units. Repeatable.", show_default=False),
    ] = None,
    scale: Scale = 1.0,
    seed: Seed = None,
    out: Annotated[
        Path | None,
        Parameter(help="Write OUT/RAW_TABLE.jsonl, one row per line, instead of stdout.", show_default=False),
    ] = None,
) -> None:
    """Write the rows `load` would insert, without connecting to StarRocks.

    Hand-written rows come first, as the factory package left them, then the
    generated ones. Each row has every column, NULL where the row leaves one
    out, and a mapping or list in a string column is the JSON text `load`
    inserts. On stdout each line is {"raw_table": ..., "row": {...}}. A date,
    timestamp or decimal is written as its string, and a timestamp keeps an
    offset the connector drops.
    """
    table_rows = _table_rows(inventory_dir, unit, generate=True, scale=scale, seed=seed)
    wanted = set(table or [])
    if unknown := sorted(wanted - {loaded.table.raw_table for loaded in table_rows}):
        err_console.print(f"[bold red]Not a fixture table of the selected units: {', '.join(unknown)}")
        sys.exit(1)
    if wanted:
        table_rows = [loaded for loaded in table_rows if loaded.table.raw_table in wanted]
    if out is not None:
        out.mkdir(parents=True, exist_ok=True)
    for loaded in table_rows:
        raw_table = loaded.table.raw_table
        rows = [dict(zip(loaded.table.columns, parameters, strict=True)) for parameters in loaded.parameters()]
        if out is None:
            for row in rows:
                sys.stdout.write(json.dumps({"raw_table": raw_table, "row": row}, default=_json_value) + "\n")
            continue
        path = out / f"{raw_table}.jsonl"
        path.write_text("".join(json.dumps(row, default=_json_value) + "\n" for row in rows))
        err_console.print(f"Wrote {escape(str(path))}: {len(rows)} row(s)")


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
    units = _select(inventory_dir, unit)
    wanted = set(table or [])
    declared = {str(entry["raw_table"]) for selected in units for entry in selected.tables}
    if unknown := sorted(wanted - declared):
        err_console.print(f"[bold red]Not a table of the selected units: {', '.join(unknown)}")
        sys.exit(1)
    for selected in units:
        if selected.data["strategies"]["local"] not in FIXTURE_STRATEGIES:
            err_console.print(
                f"[bold red]{escape(selected.key)} has strategies.local: "
                f"{escape(str(selected.data['strategies']['local']))}; `load` would reject its fixture."
            )
            sys.exit(1)

    for selected in units:
        tables = [
            str(entry["raw_table"])
            for entry in selected.tables
            if (entry["raw_table"] in wanted if wanted else entry["modeled"])
        ]
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
            if not columns:
                err_console.print(f"[yellow]{escape(raw_table)} has no column the format can hold; skipped.")
                continue
            captured[raw_table] = columns

        path = fixture_path(inventory_dir, selected)
        if not captured:
            err_console.print(f"[yellow]Nothing captured for {escape(selected.key)}; {escape(str(path))} not written.")
            continue
        # Round-trip, so the comments explaining hand-written rows survive.
        round_trip = YAML()
        round_trip.explicit_start = True
        round_trip.width = YAML_WIDTH
        existing = round_trip.load(path.read_text()) if path.exists() else None
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as handle:
            round_trip.dump(merge_captured(existing, captured), handle)
        console.print(f"Wrote {escape(str(path))}: {len(captured)} table(s)")


@fixtures_app.command
def scaffold(
    *,
    unit: Annotated[str, Parameter(help="The unit, as deployment/layer.")],
    models: Annotated[
        Path | None,
        Parameter(
            help="What bin/django-model-dump.py wrote. Default: the models.json already in the unit's package.",
            show_default=False,
        ),
    ] = None,
    inventory_dir: InventoryDir = DEFAULT_INVENTORY_DIR,
    factories_dir: Annotated[
        Path, Parameter(help="The directory holding one factory package per unit.")
    ] = FACTORIES_DIR,
    dbt_dir: Annotated[
        Path, Parameter(help="The dbt project, whose staging tests draft a unit that has no models.json.")
    ] = DEFAULT_DBT_DIR,
) -> None:
    """Write a unit's base factories from its source app's Django models.

    Writes `_generated.py` and `models.json` (the dump, cut down to the unit's
    fixture tables) in the unit's factory package, and an `__init__.py` that
    adds no rows if the package has none. A hand-written `__init__.py` is never
    touched. Columns the fixture and the models disagree on are printed.

    A unit with no Django app has no dump. Without --models and with no
    models.json in its package, `_generated.py` is drafted from the fixture's
    column types and the tests of the staging models that read each table,
    and no models.json is written. Tests that do not map are printed.
    """
    # It reads fixture_trees, which imports the generation runtime: seconds
    # that every other command would pay.
    from ol_dbt_cli.lib.fixture_scaffold import (  # noqa: PLC0415
        GENERATED_FILE,
        MODELS_FILE,
        scaffold_from_tests,
        starter_package,
    )
    from ol_dbt_cli.lib.fixture_scaffold import scaffold as fixture_scaffold  # noqa: PLC0415

    (selected,) = _select(inventory_dir, [unit])
    (fixture,) = load_fixtures(inventory_dir, [selected], selected=True)
    package = factories_dir / selected.path.stem
    models_path = models or package / MODELS_FILE
    if models is not None and not models.exists():
        err_console.print(f"[bold red]No {escape(str(models))}.")
        sys.exit(1)
    written: dict[Path, str] = {}
    if models_path.exists():
        result = fixture_scaffold(fixture, selected, json.loads(models_path.read_text()))
        written[package / MODELS_FILE] = json.dumps(result.models, indent=2) + "\n"
    else:
        try:
            result = scaffold_from_tests(fixture, selected, dbt_dir)
        except ValueError as error:
            err_console.print(
                f"[bold red]No {escape(str(models_path))}, and {escape(str(error))}. Dump the app's models with "
                "bin/django-model-dump.py and pass it as --models."
            )
            sys.exit(1)
    written[package / GENERATED_FILE] = result.source
    if not (package / "__init__.py").exists():
        written[package / "__init__.py"] = starter_package(selected)
    package.mkdir(parents=True, exist_ok=True)
    for path, text in written.items():
        path.write_text(text)
    for note in result.notes:
        err_console.print(f"[yellow]{escape(note)}", soft_wrap=True)
    for path in written:
        console.print(f"Wrote {escape(str(path))}", soft_wrap=True)
