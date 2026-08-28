"""Which column an incremental load could key on, per table, and whether it still holds.

Airbyte's Postgres sources mostly replicate by ``xmin``, which dlt has no
practical equivalent for (INGESTION_INVENTORY_SPEC.md §3.4). Every such table
needs either a replacement cursor column or an explicit decision to re-read it
whole. This module answers that from the LANDED warehouse schema, which is the
only thing that actually constrains what a loader can key on -- a column the ORM
declares but that never reached the warehouse is not a cursor.

Two questions, deliberately separated:

*Candidate* -- does a column of the right shape exist? Pure name matching, done
here, cheap, and re-runnable as schemas drift.

*Usable* -- is that column stamped on EVERY mutation path? Not answerable from a
schema, and it is the one that bites. A write-once column yields a load that
captures new rows and silently never reflects an edit; see the Keycloak
``LAST_MODIFIED_TIMESTAMP`` note in ``ol_dlt/database.py``, and note that
Django's ``auto_now=True`` covers ``Model.save()`` but not ``queryset.update()``.
So a ``CURSOR_AVAILABLE`` finding is a shortlist entry, never an approval.

The reason this is a standing check and not a one-off audit: apps are added and
app schemas change. A declared ``cursor_field`` whose column has since been
dropped or renamed does not fail loudly -- the load just stops advancing, or
falls back to a full read, and nobody is told. ``CURSOR_MISSING`` is that case,
and it is the finding this module exists for.

No boto3 here, matching ``lib.inventory``: the Glue read happens at the command
edge and this stays pure, offline-testable, and importable from the dlt code
location.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

# Ordered by preference. Django's own convention first, then the variants the
# other apps in this estate actually use -- measured across 608 landed tables:
# updated_on 253, updated 15, updated_at 13, modified 5, last_modified 2.
MODIFIED_COLUMNS: tuple[str, ...] = (
    "updated_on",
    "updated_at",
    "modified",
    "modified_on",
    "last_modified",
    "date_modified",
    "updated",
)

# Wagtail pages carry no modification timestamp of their own. A revision
# timestamp is the nearest thing and is offered as a SECONDARY candidate --
# weaker, because it moves only when a revision is created, so a field changed
# outside the revision flow does not advance it.
SECONDARY_COLUMNS: tuple[str, ...] = (
    "latest_revision_created_at",
    "last_published_at",
)

# A creation timestamp is a perfectly good cursor for an append-only ledger and
# a trap for anything mutable, and the schema cannot tell the two apart. Hence
# its own bucket rather than being folded into either neighbour.
CREATED_COLUMNS: tuple[str, ...] = (
    "created_on",
    "created_at",
    "created",
    "date_created",
    "added_on",
)

# Loader bookkeeping, not source columns. Airbyte's `_airbyte_*` and dlt's
# `_dlt_*` both appear in landed schemas depending on which loaded the table.
LOADER_COLUMN = re.compile(r"^_(airbyte|dlt)_")

# Join tables are id plus a couple of foreign keys. Narrow tables with no
# timestamp are the normal, uninteresting case for `replace`; wide ones are
# where somebody has to think.
JOIN_TABLE_MAX_COLUMNS = 4


class Verdict(StrEnum):
    """What the landed schema supports for this table."""

    CURSOR_OK = "cursor_ok"
    """A cursor_field is declared and the column is present. Nothing to do."""

    CURSOR_MISSING = "cursor_missing"
    """A cursor_field is declared but the column is GONE from the landed schema.

    The failure this check exists to catch: silent, and it does not surface as
    an error anywhere else.
    """

    CURSOR_AVAILABLE = "cursor_available"
    """No cursor_field declared, but a modification timestamp exists."""

    SECONDARY_AVAILABLE = "secondary_available"
    """Only a revision/publish timestamp exists. Weaker; see SECONDARY_COLUMNS."""

    INSERT_ONLY = "insert_only"
    """Only a creation timestamp. Valid for an append-only ledger, a trap otherwise."""

    REPLACE = "replace"
    """No time column at all. Re-read whole, which also propagates deletes."""

    NOT_LANDED = "not_landed"
    """Declared in the inventory but absent from the warehouse schema."""


@dataclass(frozen=True)
class TableFinding:
    """One table's cursor situation."""

    unit_key: str
    raw_table: str
    stream: str
    verdict: Verdict
    declared_cursor: str | None = None
    candidate: str | None = None
    source_columns: int = 0
    time_like_columns: tuple[str, ...] = ()

    @property
    def is_join_table_shaped(self) -> bool:
        """Narrow enough that a full re-read is obviously the right call."""
        return self.source_columns <= JOIN_TABLE_MAX_COLUMNS

    @property
    def needs_attention(self) -> bool:
        """Worth a human looking, as opposed to a settled or obvious case."""
        if self.verdict is Verdict.CURSOR_MISSING:
            return True
        if self.verdict is Verdict.INSERT_ONLY:
            return True
        if self.verdict is Verdict.REPLACE:
            return not self.is_join_table_shaped
        return False


@dataclass
class AuditResult:
    findings: list[TableFinding] = field(default_factory=list)

    def by_verdict(self, verdict: Verdict) -> list[TableFinding]:
        return [f for f in self.findings if f.verdict is verdict]

    @property
    def counts(self) -> dict[Verdict, int]:
        return {v: len(self.by_verdict(v)) for v in Verdict}

    @property
    def broken(self) -> list[TableFinding]:
        """Declared cursors whose column has vanished. The gate-worthy set."""
        return self.by_verdict(Verdict.CURSOR_MISSING)


def _source_columns(columns: Iterable[str]) -> list[str]:
    return sorted({c.lower() for c in columns if not LOADER_COLUMN.match(c.lower())})


def _first_present(candidates: Sequence[str], columns: set[str]) -> str | None:
    return next((c for c in candidates if c in columns), None)


_TIME_LIKE = re.compile(r"(_at|_on|_date|_time|stamp)$|^(date|time)_")


def classify_table(
    *,
    unit_key: str,
    raw_table: str,
    stream: str,
    columns: Iterable[str] | None,
    declared_cursor: str | None = None,
) -> TableFinding:
    """Classify one table against its landed column list.

    ``columns`` is None when the table is declared but has not landed.
    ``declared_cursor`` is the inventory's ``cursor_field`` for the table, if any
    -- checking it against reality is the point of the standing check.
    """
    if columns is None:
        return TableFinding(
            unit_key=unit_key,
            raw_table=raw_table,
            stream=stream,
            verdict=Verdict.NOT_LANDED,
            declared_cursor=declared_cursor,
        )

    source = _source_columns(columns)
    present = set(source)
    time_like = tuple(c for c in source if _TIME_LIKE.search(c))

    def finding(verdict: Verdict, candidate: str | None = None) -> TableFinding:
        return TableFinding(
            unit_key=unit_key,
            raw_table=raw_table,
            stream=stream,
            verdict=verdict,
            declared_cursor=declared_cursor,
            candidate=candidate,
            source_columns=len(source),
            time_like_columns=time_like,
        )

    if declared_cursor:
        return finding(
            Verdict.CURSOR_OK if declared_cursor.lower() in present else Verdict.CURSOR_MISSING,
            declared_cursor,
        )

    if modified := _first_present(MODIFIED_COLUMNS, present):
        return finding(Verdict.CURSOR_AVAILABLE, modified)
    if secondary := _first_present(SECONDARY_COLUMNS, present):
        return finding(Verdict.SECONDARY_AVAILABLE, secondary)
    if created := _first_present(CREATED_COLUMNS, present):
        return finding(Verdict.INSERT_ONLY, created)
    return finding(Verdict.REPLACE)


def audit(
    units: Iterable[object],
    columns_by_table: Mapping[str, list[str]],
) -> AuditResult:
    """Classify every table of every Airbyte-loaded unit.

    ``units`` are ``lib.inventory.Unit`` objects, taken structurally rather than
    imported so this module keeps no dependency on the inventory loader.
    ``columns_by_table`` maps raw table name -> landed column names.
    """
    result = AuditResult()
    for unit in units:
        data = getattr(unit, "data", {}) or {}
        key = f"{data.get('deployment', '?')}/{data.get('layer', '?')}"
        for table in getattr(unit, "tables", []) or []:
            raw_table = str(table.get("raw_table", ""))
            cursor = table.get("cursor_field")
            # The inventory stores cursor_field as a list of path segments; a
            # composite cursor is not something this check can reason about, so
            # it takes the first and lets the finding carry it verbatim.
            declared = str(cursor[0]) if isinstance(cursor, list) and cursor else None
            result.findings.append(
                classify_table(
                    unit_key=key,
                    raw_table=raw_table,
                    stream=str(table.get("name", "")),
                    columns=columns_by_table.get(raw_table),
                    declared_cursor=declared,
                )
            )
    result.findings.sort(key=lambda f: (f.unit_key, f.raw_table))
    return result
