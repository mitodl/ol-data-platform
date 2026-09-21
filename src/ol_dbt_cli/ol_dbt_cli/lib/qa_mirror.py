"""The QA mirror of singleton units — RFC 12711 step 6.

A singleton has no QA deployment to ingest from, so QA holds a masked copy of
production instead: one ``CREATE TABLE ... AS SELECT`` per table, from the
production raw layer into the QA raw layer, keeping only the columns the
inventory's ``mirror.columns`` allowlist names (QA_DATA_TOPOLOGY_SPEC.md §8).

Rendering lives here, beside the inventory, so it is testable without a
cluster. The Dagster asset that runs it is
``dg_projects/lakehouse/lakehouse/assets/qa_mirror.py``. Like ``lib.inventory``
this module imports neither dbt nor sqlglot, because the lakehouse code
location imports it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ol_dbt_cli.lib.inventory import (
    MIRROR_STRATEGY,
    MIRROR_WHERE_VIOLATION,
    mirror_where_is_a_statement,
)
from ol_dbt_cli.lib.qa_observation import QA_GLUE_DATABASE, qa_strategy

if TYPE_CHECKING:
    from ol_dbt_cli.lib.inventory import Unit

PRODUCTION_CATALOG = "ol_data_lake_production"
PRODUCTION_DATABASE = "ol_warehouse_production_raw"
QA_CATALOG = "ol_data_lake_qa"

SOURCE_PLACEHOLDER = "{source}"
"""Stands for the production relation inside ``mirror.where``."""

STATEMENT_TIMEOUT_SECONDS = 4 * 60 * 60
"""StarRocks' 300-second default kills a filtered scan of the 760 GB program learner report."""

_STRING_TYPE_PREFIXES = ("varchar", "char", "string")


class MirrorDeclarationError(ValueError):
    """The inventory's mirror declaration does not fit the production table."""


@dataclass(frozen=True)
class MirrorTable:
    unit: str
    raw_table: str
    columns: dict[str, str]
    where: str | None

    @property
    def name(self) -> str:
        """The Glue table name, which Glue stores lowercased (salesforce's inventory names are not)."""
        return self.raw_table.lower()

    @property
    def source(self) -> str:
        return f"{PRODUCTION_CATALOG}.{PRODUCTION_DATABASE}.{quote(self.name)}"

    @property
    def target(self) -> str:
        return f"{QA_CATALOG}.{QA_GLUE_DATABASE}.{quote(self.name)}"


@dataclass(frozen=True)
class MirrorStatement:
    sql: str
    dropped: list[str]
    """Production columns the allowlist leaves out, reported so a new upstream column is noticed."""


def quote(identifier: str) -> str:
    """StarRocks identifier quoting. Raw column names include spaces (``user id``)."""
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def mirror_tables(units: list[Unit]) -> dict[str, list[MirrorTable]]:
    """Every table the mirror copies, grouped by unit key.

    Only units whose ``strategies.qa`` is ``mirror``, and only tables declaring
    a ``mirror`` block: a table without one has no allowlist, so it is not
    copied. ``inventory validate`` rejects the block anywhere else.
    """
    grouped: dict[str, list[MirrorTable]] = {}
    for unit in units:
        if qa_strategy(unit) != MIRROR_STRATEGY:
            continue
        tables = [
            MirrorTable(
                unit=unit.key,
                raw_table=table["raw_table"],
                columns=dict(table["mirror"]["columns"]),
                where=table["mirror"].get("where"),
            )
            for table in unit.tables
            if "mirror" in table
        ]
        if tables:
            grouped[unit.key] = tables
    return grouped


STRING_ONLY_MODES = frozenset({"hash", "redact"})
REDACTED = "redacted"


def _expression(column: str, mode: str) -> str:
    quoted = quote(column)
    if mode == "hash":
        # Blank stays blank and NULL stays NULL. Hashing a blank would give every
        # blank username one shared digest, and turning it into NULL would break
        # not_null tests on anything concatenated from it (emeritus full names).
        return f"if({quoted} = '', '', sha2({quoted}, 256)) AS {quoted}"
    if mode == "redact":
        # NULL stays NULL, so a not_null test sees what it would see in production.
        return f"CASE WHEN {quoted} IS NULL THEN NULL ELSE '{REDACTED}' END AS {quoted}"
    if mode == "nullify":
        # A bare NULL would be typed NULL_TYPE and change the column's type in
        # QA. The dead branch carries the production column's type instead.
        return f"CASE WHEN FALSE THEN {quoted} END AS {quoted}"
    if mode == "copy":
        return quoted
    # The asset reads the inventory without the JSON Schema, so a typo'd mode
    # must fail here rather than copy a PII column in the clear.
    msg = f"{column}: unknown mirror mode {mode!r}"
    raise MirrorDeclarationError(msg)


def render_mirror(table: MirrorTable, production_types: dict[str, str]) -> MirrorStatement:
    """Render the CTAS for *table* against the production column types.

    *production_types* maps column name to StarRocks type, as ``DESCRIBE``
    reports it. The declaration is checked against it first: an allowlisted
    column production does not have means the declaration is stale, and
    ``hash`` or ``redact`` on a non-string column would change the column's
    type under the staging models that cast it. ``mirror.where`` is checked
    for being a predicate rather than a statement, because a ``UNION`` there
    would read production columns the allowlist leaves out.
    """
    # `inventory validate` rejects this too, but the asset renders straight from
    # the inventory without it, so the boundary has to close here as well.
    if mirror_where_is_a_statement(table.where):
        msg = f"{table.raw_table}: mirror.where {MIRROR_WHERE_VIOLATION}"
        raise MirrorDeclarationError(msg)

    types = {name.lower(): kind.lower() for name, kind in production_types.items()}
    missing = sorted(column for column in table.columns if column.lower() not in types)
    if missing:
        msg = f"{table.raw_table}: mirror.columns names {missing}, which production does not have"
        raise MirrorDeclarationError(msg)
    not_strings = sorted(
        column
        for column, mode in table.columns.items()
        if mode in STRING_ONLY_MODES and not types[column.lower()].startswith(_STRING_TYPE_PREFIXES)
    )
    if not_strings:
        msg = f"{table.raw_table}: `hash` and `redact` need string columns, and {not_strings} are not"
        raise MirrorDeclarationError(msg)

    select_list = ",\n    ".join(_expression(column, mode) for column, mode in table.columns.items())
    # Parenthesized so an OR in the predicate keeps its meaning if anything is
    # ever combined with it. Set operators are rejected above.
    where = f"\nWHERE ({table.where.replace(SOURCE_PLACEHOLDER, table.source)})" if table.where else ""
    hint = f"/*+ SET_VAR(query_timeout = {STATEMENT_TIMEOUT_SECONDS}, insert_timeout = {STATEMENT_TIMEOUT_SECONDS}) */"
    sql = f"CREATE TABLE {table.target}\nAS SELECT {hint}\n    {select_list}\nFROM {table.source}{where}"
    kept = {column.lower() for column in table.columns}
    return MirrorStatement(sql=sql, dropped=sorted(set(types) - kept))
