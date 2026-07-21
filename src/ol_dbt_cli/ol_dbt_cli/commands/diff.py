"""Diff command — same-engine row/column comparison of two model relations.

Wraps the ``dbt_audit_helper`` package to compare a baseline model against a
candidate model (typically an old vs a migrated mart/reporting model). Because
both relations are built on the *same* engine (default target ``dev_local``, a
local DuckDB reading Iceberg directly), dialect differences cancel and the diff
reflects real data divergence rather than SQL-translation noise.

Design notes (the non-obvious bits):

* Column sets are reconciled BEFORE any comparison runs, so a schema mismatch
  produces a clear structured message instead of a raw SQL error from
  audit_helper. The diff proceeds on the intersection minus ``--exclude-columns``.
* Known non-determinism (e.g. ``dim_user.user_pk`` is an email-keyed surrogate
  that collapses NULL emails) is handled by passing ``--primary-key`` and/or
  ``--exclude-columns``.
* The comparison itself is run via ``dbt show --inline`` invoking audit_helper
  macros with ``--output json``; results are parsed tolerantly from stdout.
* ``--old``/``--new`` normally resolve through ``ref()``, which requires a real
  dbt model in the manifest. ``--old-raw``/``--new-raw`` bypass that, building a
  literal ``api.Relation.create(...)`` instead — for relations dbt doesn't know
  about: an already-materialized table sitting in a different schema/database
  than the current --target.
* ``--old-glue``/``--new-glue`` are a variant of ``--old-raw``/``--new-raw``
  specifically for the ``glue__{glue_database}__{table}`` DuckDB views created by
  ``ol-dbt local register`` — live views onto the real production Iceberg table,
  not a copy. Pass just the Glue database name; ``--old``/``--new`` stay the bare
  table name on both sides instead of the full ``glue__...`` identifier.

Driveable both by hand and by CI (Phase 2 auto-diff vs a base ref). See
docs/specs/DBT_WAREHOUSE_CI_QA_SPEC.md §3.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.manifest import (
    ManifestRegistry,
    find_manifest,
    load_manifest,
)
from ol_dbt_cli.lib.sql_parser import (
    ParsedModel,
    find_compiled_dir,
    parse_model_file,
)
from ol_dbt_cli.lib.yaml_registry import YamlRegistry, build_yaml_registry

console = Console()
err_console = Console(stderr=True)

# Cap on how many columns we run a per-column value comparison for, to bound the
# number of dbt invocations. When exceeded we log it (never silently truncate).
_MAX_PER_COLUMN_COMPARISONS = 25

# Model and column names are interpolated into inline Jinja SQL passed to
# `dbt show --inline`, so they must be plain dbt identifiers — anything else
# (quotes, braces, whitespace) could inject Jinja/SQL and is rejected up front.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class InvalidIdentifierError(ValueError):
    """Raised when a user-supplied name is not a safe dbt identifier."""


def _validate_identifiers(kind: str, names: list[str]) -> None:
    """Reject any *names* that are not plain dbt identifiers.

    Guards against Jinja/SQL injection through the inline template built for
    ``dbt show``. Raises :class:`InvalidIdentifierError` naming the offenders.
    """
    bad = [n for n in names if not _IDENTIFIER_RE.match(n)]
    if bad:
        msg = f"Invalid {kind}: {', '.join(repr(b) for b in bad)}. Expected plain identifiers ([A-Za-z_][A-Za-z0-9_]*)."
        raise InvalidIdentifierError(msg)


class Verdict(StrEnum):
    MATCH = "match"
    MISMATCH = "mismatch"
    SCHEMA_DIVERGENCE = "schema_divergence"


@dataclass
class ColumnReconciliation:
    only_in_old: list[str] = field(default_factory=list)
    only_in_new: list[str] = field(default_factory=list)
    compared: list[str] = field(default_factory=list)

    @property
    def diverged(self) -> bool:
        return bool(self.only_in_old or self.only_in_new)


@dataclass
class ColumnMismatch:
    column: str
    mismatch_rate: float
    mismatched_rows: int


@dataclass
class DiffResult:
    old: str
    new: str
    target: str
    primary_key: list[str]
    excluded_columns: list[str]
    column_reconciliation: ColumnReconciliation
    row_counts: dict[str, int]  # {"old": .., "new": .., "delta": ..}
    column_mismatches: list[ColumnMismatch]
    sample_mismatches: list[dict[str, Any]]
    verdict: Verdict
    # Text-output-only labels annotating each side's source (e.g. "name (glue:
    # db)") when --old/new-raw or --old/new-glue make `old`/`new` ambiguous —
    # identical to `old`/`new` for a plain ref()-vs-ref() diff. Kept separate
    # from `old`/`new` so JSON output stays a stable, bare identifier. Uses
    # parens, not brackets — rich.console.print() parses `[...]` as markup and
    # silently swallows anything that isn't a recognized style tag.
    old_label: str
    new_label: str
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Column reconciliation
# ---------------------------------------------------------------------------


def _resolve_columns(
    name: str,
    sql_models_by_name: dict[str, ParsedModel],
    manifest: ManifestRegistry | None,
    yaml_registry: YamlRegistry,
) -> set[str]:
    """Best-effort resolution of a model's output columns.

    Prefers parsed (compiled) SQL output columns, then manifest column metadata,
    then the YAML schema. Column names are lower-cased for a case-insensitive
    comparison (dbt/DuckDB fold unquoted identifiers to lower case).
    """
    parsed = sql_models_by_name.get(name)
    if parsed is not None and parsed.output_columns:
        return {c.lower() for c in parsed.output_columns}
    if manifest is not None:
        model = manifest.get_model(name)
        if model is not None and model.column_names:
            return {c.lower() for c in model.column_names}
    yaml_model = yaml_registry.get_model(name)
    if yaml_model is not None and yaml_model.column_names:
        return {c.lower() for c in yaml_model.column_names}
    return set()


def _resolve_raw_columns(
    name: str,
    dbt_dir: Path,
    target: str,
    *,
    database: str | None = None,
    schema: str | None = None,
    glue_database: str | None = None,
) -> tuple[set[str], str | None]:
    """Resolve a raw (non-``ref()``) relation's columns by sampling one row.

    Raw relations (Glue-registered views, an already-materialized table in
    another schema) have no manifest/YAML metadata to resolve columns from
    statically, so this samples a live row and reads its keys instead.

    Returns ``(columns, error)``. *error* carries the dbt failure message when
    the sampling query itself fails (e.g. the relation doesn't exist) — the
    caller must surface it rather than let an empty column set masquerade as
    "unparsed / not in manifest", which is misleading for a raw relation.
    """
    expr = _relation_jinja(name, raw=True, database=database, schema=schema, glue_database=glue_database)
    # Rendered into a Jinja template compiled by dbt (not executed as a raw query
    # here), so the S608 heuristic is a false positive — same as _compare_column_sql.
    # No literal LIMIT here: `dbt show` already wraps the compiled query in its own
    # limiting logic via --limit below; a second, literal `limit 1` in the SQL text
    # collides with that wrapper and breaks the DuckDB parser.
    inline_sql = f"select * from {{{{ {expr} }}}}"  # noqa: S608
    try:
        rows = _run_dbt_show(inline_sql, dbt_dir, target, limit=1)
    except RuntimeError as exc:
        return set(), str(exc)
    return ({c.lower() for c in rows[0]} if rows else set()), None


def _materialize_glue_relation(name: str, glue_database: str, dbt_dir: Path, target: str) -> str:
    """Copy a Glue-registered ``iceberg_scan()`` view into a real local DuckDB table.

    DuckDB's Iceberg extension can run a plain ``SELECT``/``COUNT(*)`` against a
    live ``glue__...`` view (that's why column resolution works) but not the
    ``UNION ALL``/``EXCEPT`` set operations audit_helper's compare macros
    generate — that fails with ``Not implemented Error: IcebergScan
    serialization not implemented``. Copying the view into a real table first
    sidesteps the limitation entirely.

    Uses ``create or replace table`` with a name deterministic in
    (*glue_database*, *name*), so repeated diffs reuse/refresh the same scratch
    table instead of accumulating new ones. Returns the scratch table's bare
    identifier (materialized in the target's own database/schema).
    """
    scratch_name = f"_ol_dbt_diff_scratch__{glue_database}__{name}"
    src_expr = _relation_jinja(name, glue_database=glue_database)
    dst_expr = _relation_jinja(scratch_name, raw=True)
    ctas_sql = f"create or replace table {{{{ {dst_expr} }}}} as select * from {{{{ {src_expr} }}}}"  # noqa: S608
    # limit=-1: dbt show's own --limit wrapping would otherwise truncate the
    # CTAS to N rows -- the whole point here is a full, unwrapped copy.
    _run_dbt_show(ctas_sql, dbt_dir, target, limit=-1)
    return scratch_name


def reconcile_columns(
    old_cols: set[str],
    new_cols: set[str],
    exclude_columns: set[str],
) -> ColumnReconciliation:
    """Reconcile two column sets, returning the divergence and the compared set.

    The compared set is the intersection minus excluded columns. Excluded
    columns are dropped from the only_in_* lists too — an intentional exclusion
    is not a divergence.
    """
    exclude = {c.lower() for c in exclude_columns}
    old = {c.lower() for c in old_cols if c.lower() not in exclude}
    new = {c.lower() for c in new_cols if c.lower() not in exclude}
    return ColumnReconciliation(
        only_in_old=sorted(old - new),
        only_in_new=sorted(new - old),
        compared=sorted(old & new),
    )


# ---------------------------------------------------------------------------
# dbt show / audit_helper invocation
# ---------------------------------------------------------------------------


def _extract_show_rows(stdout: str) -> list[dict[str, Any]] | None:
    """Extract the row list from ``dbt show --output json`` stdout.

    For our invocation (``dbt show --inline ... --output json``, default text
    logging) dbt-core emits ``{"show": [ ...rows... ]}`` on stdout — see
    ``dbt.events.types.ShowNode.message``. The non-inline form is
    ``{"node": ..., "show": [...]}`` and a bare list is also accepted. dbt
    interleaves log lines with that document, so we try the whole payload first,
    then scan for an embedded JSON value.

    Returns the row list (possibly empty) when a JSON document is found, or
    ``None`` when no JSON document could be located at all. ``None`` is a *parse
    anomaly* the caller must not treat as "no rows" — otherwise a parsing failure
    would masquerade as a clean, difference-free comparison.
    """

    def _rows_from(obj: Any) -> list[dict[str, Any]] | None:
        if isinstance(obj, dict) and isinstance(obj.get("show"), list):
            return obj["show"]
        if isinstance(obj, list):
            return obj
        return None

    stripped = stdout.strip()
    if stripped:
        try:
            rows = _rows_from(json.loads(stripped))
            if rows is not None:
                return rows
        except json.JSONDecodeError:
            pass

    # Fall back: scan for a JSON document (object OR array) embedded among log
    # lines. raw_decode parses one value from the offset and ignores trailing text.
    decoder = json.JSONDecoder()
    for idx, char in enumerate(stdout):
        if char not in "{[":
            continue
        try:
            obj, _ = decoder.raw_decode(stdout[idx:])
        except json.JSONDecodeError:
            continue
        rows = _rows_from(obj)
        if rows is not None:
            return rows
    return None


def _run_dbt_show(
    inline_sql: str,
    dbt_dir: Path,
    target: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Run ``dbt show --inline`` and return the parsed rows.

    Raises RuntimeError on a dbt failure or a missing dbt binary so the caller
    can surface a clean error and exit non-zero.
    """
    cmd = [
        "dbt",
        "show",
        "--inline",
        inline_sql,
        "--target",
        target,
        "--output",
        "json",
        "--limit",
        str(limit),
    ]
    try:
        result = subprocess.run(  # noqa: S603, S607
            cmd,
            cwd=str(dbt_dir),
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        msg = f"dbt show failed: {detail[-500:]}"
        raise RuntimeError(msg) from exc
    except FileNotFoundError as exc:
        msg = "'dbt' command not found; install dbt and ensure it is on PATH."
        raise RuntimeError(msg) from exc

    rows = _extract_show_rows(result.stdout)
    if rows is None:
        # dbt exited 0 but we found no JSON document — a parse anomaly (e.g. an
        # unexpected output format from a future dbt version). Fail loudly rather
        # than return [] and let it masquerade as a difference-free comparison.
        head = result.stdout.strip()[:300]
        msg = f"could not parse rows from `dbt show` output (unexpected format). stdout starts: {head!r}"
        raise RuntimeError(msg)
    return rows


def _jinja_list(values: list[str]) -> str:
    """Render a Python list of identifiers as a Jinja list literal."""
    inner = ", ".join(f"'{v}'" for v in values)
    return f"[{inner}]"


def _relation_jinja(
    name: str,
    *,
    raw: bool = False,
    database: str | None = None,
    schema: str | None = None,
    glue_database: str | None = None,
) -> str:
    """Render a relation reference for inline Jinja SQL.

    Normally a plain ``ref('name')`` — requires *name* to be a real dbt model in
    the manifest. When ``raw=True``, builds a literal ``api.Relation.create(...)``
    instead, addressed by *database*/*schema* (defaulting to the current
    ``--target``'s own database/schema) — for relations dbt doesn't know about,
    e.g. an already-materialized table elsewhere.

    *glue_database*, when given, forces raw mode and rewrites the identifier to
    the ``glue__{glue_database}__{name}`` naming convention that ``ol-dbt local
    register`` uses for its DuckDB views (see ``local_dev.py``) — so callers can
    pass the same bare table *name* for both a Glue-registered view and a real
    dbt model instead of spelling out the full ``glue__...`` identifier.
    """
    if glue_database:
        raw = True
        name = f"glue__{glue_database}__{name}"
    if not raw:
        return f"ref('{name}')"
    db = f"'{database}'" if database else "target.database"
    sch = f"'{schema}'" if schema else "target.schema"
    return f"api.Relation.create(database={db}, schema={sch}, identifier='{name}')"


def _compare_relations_sql(
    old: str,
    new: str,
    primary_key: list[str],
    exclude_columns: list[str],
    *,
    summarize: bool,
    old_raw: bool = False,
    new_raw: bool = False,
    old_schema: str | None = None,
    new_schema: str | None = None,
    old_database: str | None = None,
    new_database: str | None = None,
    old_glue: str | None = None,
    new_glue: str | None = None,
) -> str:
    """Build inline SQL calling ``audit_helper.compare_relations``.

    With ``summarize=True`` audit_helper returns the ``in_a``/``in_b``/``count``/
    ``percent_of_total`` grouping; with ``summarize=False`` it returns the actual
    differing rows (used to sample mismatches).
    """
    pk_arg = ""
    if primary_key:
        pk_repr = f"'{primary_key[0]}'" if len(primary_key) == 1 else _jinja_list(primary_key)
        pk_arg = f", primary_key={pk_repr}"
    exclude_arg = f", exclude_columns={_jinja_list(exclude_columns)}" if exclude_columns else ""
    a_expr = _relation_jinja(old, raw=old_raw, database=old_database, schema=old_schema, glue_database=old_glue)
    b_expr = _relation_jinja(new, raw=new_raw, database=new_database, schema=new_schema, glue_database=new_glue)
    # `}}` in an f-string is an escape for a single literal `}` (same as `{{` for
    # `{`) — closing the Jinja `{{ ... }}` block from an f-string needs `}}}}`.
    return (
        "{{ audit_helper.compare_relations("
        f"a_relation={a_expr}, b_relation={b_expr}"
        f"{exclude_arg}{pk_arg}, summarize={'true' if summarize else 'false'}) }}}}"
    )


def _compare_column_sql(
    old: str,
    new: str,
    primary_key: list[str],
    column: str,
    *,
    old_raw: bool = False,
    new_raw: bool = False,
    old_schema: str | None = None,
    new_schema: str | None = None,
    old_database: str | None = None,
    new_database: str | None = None,
    old_glue: str | None = None,
    new_glue: str | None = None,
) -> str:
    """Build inline SQL calling ``audit_helper.compare_column_values`` for one column."""
    pk = f"'{primary_key[0]}'" if len(primary_key) == 1 else _jinja_list(primary_key)
    a_expr = _relation_jinja(old, raw=old_raw, database=old_database, schema=old_schema, glue_database=old_glue)
    b_expr = _relation_jinja(new, raw=new_raw, database=new_database, schema=new_schema, glue_database=new_glue)
    # Only the primary key(s) + the one column being compared are needed here --
    # `compare_column_values` does a full outer join on a_query/b_query, and
    # selecting every column (as this used to) forces that join to carry the
    # whole row width for a single-column comparison, which is needless I/O on
    # wide tables.
    select_cols = ", ".join(dict.fromkeys([*primary_key, column]))
    # a_query/b_query are built with {% set %}/{% endset %} so `a_expr`/`b_expr`
    # (a ref()/api.Relation.create() call) are rendered by Jinja BEFORE
    # compare_column_values ever sees the string. Embedding `{{ ... }}` directly
    # inside a quoted string argument (the previous approach) is inert: Jinja
    # treats string literals atomically and never re-parses braces inside them,
    # so the literal text `{{ ref(...) }}` reached the database unrendered --
    # a real, pre-existing bug (every diff with a primary key hit this).
    return (
        "{% set a_query %}"  # noqa: S608
        f"select {select_cols} from {{{{ {a_expr} }}}}"
        "{% endset %}"
        "{% set b_query %}"
        f"select {select_cols} from {{{{ {b_expr} }}}}"
        "{% endset %}"
        "{{ audit_helper.compare_column_values("
        f"a_query=a_query, b_query=b_query, primary_key={pk}, column_to_compare='{column}') }}}}"
    )


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _summarize_relations(rows: list[dict[str, Any]]) -> tuple[dict[str, int], int]:
    """Turn ``compare_relations(summarize=true)`` rows into row counts + mismatch count.

    Each row is grouped by (in_a, in_b) with a ``count``. Rows in A = sum of
    counts where in_a is true; likewise B. Mismatched rows = rows present in only
    one relation.
    """
    old_rows = new_rows = mismatched = 0
    for row in rows:
        in_a = bool(row.get("in_a"))
        in_b = bool(row.get("in_b"))
        count = _int(row.get("count"))
        if in_a:
            old_rows += count
        if in_b:
            new_rows += count
        if in_a != in_b:
            mismatched += count
    counts = {"old": old_rows, "new": new_rows, "delta": new_rows - old_rows}
    return counts, mismatched


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------


def _result_to_dict(result: DiffResult) -> dict[str, Any]:
    return {
        "old": result.old,
        "new": result.new,
        "target": result.target,
        "primary_key": result.primary_key,
        "excluded_columns": result.excluded_columns,
        "column_reconciliation": {
            "only_in_old": result.column_reconciliation.only_in_old,
            "only_in_new": result.column_reconciliation.only_in_new,
            "compared": result.column_reconciliation.compared,
        },
        "row_counts": result.row_counts,
        "column_mismatches": [
            {"column": c.column, "mismatch_rate": c.mismatch_rate, "mismatched_rows": c.mismatched_rows}
            for c in result.column_mismatches
        ],
        "sample_mismatches": result.sample_mismatches,
        "verdict": result.verdict.value,
        "notes": result.notes,
        "old_label": result.old_label,
        "new_label": result.new_label,
    }


def _format_sample_mismatches(
    rows: list[dict[str, Any]], primary_key: list[str], old_label: str, new_label: str
) -> list[str]:
    """Turn raw ``compare_relations(summarize=false)`` rows into readable per-key diffs.

    audit_helper tags each mismatching row with the full row content from
    whichever side(s) it appears on (``in_a``/``in_b``) -- printed as raw JSON,
    the 1-3 fields that actually differ are buried under the ~40 that don't.
    This groups by primary key and reports only the differing fields, or
    "only in <side>" when a row exists on just one side.
    """
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(k) for k in primary_key)
        groups.setdefault(key, []).append(row)

    pk_desc = ", ".join(primary_key)
    lines: list[str] = []
    for key, group_rows in groups.items():
        key_str = f"{pk_desc}={key[0]}" if len(key) == 1 else f"({pk_desc})={key}"
        a_row = next((r for r in group_rows if r.get("in_a") and not r.get("in_b")), None)
        b_row = next((r for r in group_rows if r.get("in_b") and not r.get("in_a")), None)
        # A non-unique primary key can put more than 2 rows in one group -- flag
        # it rather than silently comparing only the first pair and dropping the
        # rest, which would understate how many rows actually differ.
        extra = f" (+{len(group_rows) - 2} more rows in this key group, not shown)" if len(group_rows) > 2 else ""
        if a_row and not b_row:
            lines.append(f"{key_str}: only in {old_label}{extra}")
        elif b_row and not a_row:
            lines.append(f"{key_str}: only in {new_label}{extra}")
        elif a_row and b_row:
            diffs = [
                f"{field}: {a_row.get(field)!r} → {b_row.get(field)!r}"
                for field in a_row
                if field not in ("in_a", "in_b") and a_row.get(field) != b_row.get(field)
            ]
            lines.append(
                f"{key_str}: " + ("; ".join(diffs) if diffs else "(no field differences in this sample)") + extra
            )
    return lines


def _print_json(result: DiffResult) -> None:
    print(json.dumps(_result_to_dict(result), indent=2))


def _print_text(result: DiffResult) -> None:
    color = {
        Verdict.MATCH: "bold green",
        Verdict.MISMATCH: "bold red",
        Verdict.SCHEMA_DIVERGENCE: "bold yellow",
    }[result.verdict]
    console.print(
        f"\n[{color}]{result.verdict.value.upper()}[/]  "
        f"[bold]{result.old_label}[/] → [bold]{result.new_label}[/]  ([dim]{result.target}[/])"
    )

    recon = result.column_reconciliation
    if recon.diverged:
        if recon.only_in_old:
            console.print(f"   [yellow]Only in {result.old_label}:[/] {', '.join(recon.only_in_old)}")
        if recon.only_in_new:
            console.print(f"   [yellow]Only in {result.new_label}:[/] {', '.join(recon.only_in_new)}")
    console.print(f"   [bold]Compared columns:[/] {len(recon.compared)}")

    rc = result.row_counts
    delta = rc.get("delta", 0)
    delta_str = f"[green]{delta}[/]" if delta == 0 else f"[red]{delta:+d}[/]"
    console.print(
        f"   [bold]Row counts:[/] {result.old_label}={rc.get('old', 0)}  "
        f"{result.new_label}={rc.get('new', 0)}  Δ={delta_str}"
    )

    if result.column_mismatches:
        console.print("   [bold]Column mismatches:[/]")
        for c in result.column_mismatches:
            console.print(f"     • {c.column}: {c.mismatch_rate:.2%} ({c.mismatched_rows} rows)")

    if result.sample_mismatches:
        if result.primary_key:
            formatted = _format_sample_mismatches(
                result.sample_mismatches, result.primary_key, result.old_label, result.new_label
            )
            console.print(f"   [bold]Sample mismatches[/] (first {len(formatted)} keys):")
            for line in formatted:
                console.print(f"     • {line}")
        else:
            # No --primary-key means rows can't be reliably paired across sides,
            # so fall back to the raw full-row dump rather than risk grouping
            # unrelated rows together.
            console.print(f"   [bold]Sample mismatched rows[/] (first {len(result.sample_mismatches)}):")
            for row in result.sample_mismatches:
                console.print(f"     {json.dumps(row, default=str)}")

    for note in result.notes:
        console.print(f"   [dim]{note}[/]")

    console.print(
        f"\n[bold]Summary:[/] {result.verdict.value} — "
        f"row Δ {delta}, {len(result.column_mismatches)} column mismatch(es)."
    )


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def diff(
    old: Annotated[
        str,
        Parameter(name=["--old"], help="Baseline model name (the 'before' relation)."),
    ],
    new: Annotated[
        str | None,
        Parameter(
            name=["--new"],
            help="Candidate model name (the 'after' relation). Defaults to --old's value if omitted — "
            "the common case is comparing the same table name via --old-raw/--old-glue against its "
            "ref()-resolved build.",
        ),
    ] = None,
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to dbt project root (contains dbt_project.yml). Defaults to src/ol_dbt relative to repo root.",
        ),
    ] = None,
    target: Annotated[
        str,
        Parameter(
            name=["--target", "-t"],
            help=(
                "dbt target to build/compare on (default: dev_local). dev_local uses a local "
                "DuckDB instance reading Iceberg directly and requires no network access; both "
                "sides build on the same engine so dialect differences cancel."
            ),
        ),
    ] = "dev_local",
    primary_key: Annotated[
        tuple[str, ...],
        Parameter(
            name=["--primary-key", "-k"],
            help=(
                "Column(s) uniquely identifying a row, used as the comparison join key. "
                "Repeatable. Required for per-column mismatch rates. Use this for models with "
                "known surrogate-key non-determinism (e.g. dim_user.user_pk email-keyed collapse)."
            ),
        ),
    ] = (),
    exclude_columns: Annotated[
        tuple[str, ...],
        Parameter(
            name=["--exclude-columns"],
            help="Column(s) to exclude from the comparison (e.g. non-deterministic load timestamps). Repeatable.",
        ),
    ] = (),
    output_format: Annotated[
        Literal["text", "json"],
        Parameter(name=["--format", "-f"], help="Output format: text (default) or json."),
    ] = "text",
    limit: Annotated[
        int,
        Parameter(name=["--limit"], help="Cap on sample mismatched rows shown (default: 20)."),
    ] = 20,
    auto_build: Annotated[
        bool,
        Parameter(
            name=["--auto-build"],
            help="Run 'dbt build' on both models (on --target) before comparing, so both relations exist. "
            "Skipped for any side marked --old-raw/--new-raw, since raw relations aren't dbt models.",
        ),
    ] = False,
    old_raw: Annotated[
        bool,
        Parameter(
            name=["--old-raw"],
            help="Treat --old as a literal existing relation, not a dbt model — bypasses ref()/manifest "
            "lookup. Use for a Glue-registered view (`ol-dbt local register`, a live view onto the real "
            "production Iceberg table) or an already-materialized table dbt doesn't know about. Defaults "
            "to the current --target's own database/schema; override with --old-database/--old-schema.",
        ),
    ] = False,
    new_raw: Annotated[
        bool,
        Parameter(name=["--new-raw"], help="Same as --old-raw, but for --new."),
    ] = False,
    old_schema: Annotated[
        str | None,
        Parameter(
            name=["--old-schema"],
            help="Schema override for --old. Requires --old-raw (default: the current --target's schema).",
        ),
    ] = None,
    new_schema: Annotated[
        str | None,
        Parameter(
            name=["--new-schema"],
            help="Schema override for --new. Requires --new-raw (default: the current --target's schema).",
        ),
    ] = None,
    old_database: Annotated[
        str | None,
        Parameter(
            name=["--old-database"],
            help="Database/catalog override for --old. Requires --old-raw (default: the current --target's database).",
        ),
    ] = None,
    new_database: Annotated[
        str | None,
        Parameter(
            name=["--new-database"],
            help="Database/catalog override for --new. Requires --new-raw (default: the current --target's database).",
        ),
    ] = None,
    old_glue: Annotated[
        str | None,
        Parameter(
            name=["--old-glue"],
            help="Treat --old as a Glue-registered DuckDB view (`ol-dbt local register`) named "
            "glue__{OLD_GLUE}__{old} — pass just the Glue database name and keep --old as the bare "
            "table name. A live view onto the real production Iceberg table, not a copy. Implies "
            "--old-raw; mutually exclusive with it.",
        ),
    ] = None,
    new_glue: Annotated[
        str | None,
        Parameter(name=["--new-glue"], help="Same as --old-glue, but for --new."),
    ] = None,
    refresh_glue: Annotated[
        bool,
        Parameter(
            name=["--refresh-glue"],
            help="Run 'ol-dbt local register --all-layers' before comparing. Glue-registered views go "
            "stale (HTTP 404 on their pinned S3 metadata) once production rebuilds the underlying table "
            "-- this refreshes them first. Covers not just --old-glue/--new-glue but any upstream "
            "Glue-registered dependency --auto-build's dbt build might hit via override_ref.sql.",
        ),
    ] = False,
) -> None:
    r"""Diff two dbt model relations (same-engine row/column comparison).

    Reconciles column sets first (so a schema mismatch is reported clearly rather
    than as a raw SQL error), then runs a dbt_audit_helper comparison on the
    intersection minus --exclude-columns. Exits non-zero on any divergence, so it
    is gate-able in CI.

    Examples:
        Compare an old mart against its migrated replacement on local DuckDB:
            ol-dbt diff --old dim_user_old --new dim_user --primary-key user_pk

        Exclude a non-deterministic column and emit JSON for CI:
            ol-dbt diff --old m_old --new m_new -k id --exclude-columns _loaded_at --format json

        Build both sides first, then compare:
            ol-dbt diff --old m_old --new m_new --auto-build

        Compare a migrated model's local build against the real, already-materialized
        production table via its Glue-registered view. --new defaults to --old's value,
        so only one bare table name is needed. --refresh-glue re-registers Glue views
        first (`ol-dbt local register --all-layers`) instead of requiring it to have
        been run separately -- worthwhile whenever the production table might have
        rebuilt (and its old __dbt_tmp snapshot been cleaned up) since you last
        registered it, which surfaces as an HTTP 404 on stale S3 metadata:
            ol-dbt diff \\
                --old enrollment_detail_report --old-glue ol_warehouse_production_reporting \\
                --primary-key courserunenrollment_id --refresh-glue

        Compare two already-materialized copies of the same model across schemas
        (e.g. your personal dev schema vs. real production) on one Trino target:
            ol-dbt diff --target dev_production \\
                --old enrollment_detail_report --old-raw --old-schema ol_warehouse_production_reporting \\
                --new enrollment_detail_report --new-raw --new-schema ol_warehouse_production_rlougee_reporting \\
                --primary-key courserunenrollment_id

    """
    new = new or old
    primary_key_list = list(primary_key)
    exclude_list = list(exclude_columns)
    notes: list[str] = []

    if old_raw and old_glue:
        err_console.print("[red]Error:[/] --old-raw and --old-glue are mutually exclusive.")
        sys.exit(1)
    if new_raw and new_glue:
        err_console.print("[red]Error:[/] --new-raw and --new-glue are mutually exclusive.")
        sys.exit(1)

    # --old-glue/--new-glue are a --*-raw variant (see _relation_jinja), so schema/
    # database overrides are valid alongside either.
    old_is_raw = old_raw or bool(old_glue)
    new_is_raw = new_raw or bool(new_glue)

    if (old_schema or old_database) and not old_is_raw:
        err_console.print("[red]Error:[/] --old-schema/--old-database require --old-raw or --old-glue.")
        sys.exit(1)
    if (new_schema or new_database) and not new_is_raw:
        err_console.print("[red]Error:[/] --new-schema/--new-database require --new-raw or --new-glue.")
        sys.exit(1)

    # Validate every value interpolated into inline Jinja SQL before use.
    try:
        _validate_identifiers("model name", [old, new])
        _validate_identifiers("primary key", primary_key_list)
        _validate_identifiers("excluded column", exclude_list)
        _validate_identifiers(
            "schema/database override",
            [v for v in (old_schema, new_schema, old_database, new_database) if v],
        )
        _validate_identifiers("glue database", [v for v in (old_glue, new_glue) if v])
    except InvalidIdentifierError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    if old_glue:
        notes.append(
            f"--old-glue: '{old}' resolved as Glue-registered view "
            f"'glue__{old_glue}__{old}' (database={old_database or 'target.database'}, "
            f"schema={old_schema or 'target.schema'})."
        )
    elif old_raw:
        notes.append(
            f"--old-raw: '{old}' resolved as a literal relation "
            f"(database={old_database or 'target.database'}, schema={old_schema or 'target.schema'})."
        )
    if new_glue:
        notes.append(
            f"--new-glue: '{new}' resolved as Glue-registered view "
            f"'glue__{new_glue}__{new}' (database={new_database or 'target.database'}, "
            f"schema={new_schema or 'target.schema'})."
        )
    elif new_raw:
        notes.append(
            f"--new-raw: '{new}' resolved as a literal relation "
            f"(database={new_database or 'target.database'}, schema={new_schema or 'target.schema'})."
        )

    # Text-output labels disambiguating which side is which when --old/new-raw or
    # --old/new-glue make the bare `old`/`new` names identical or otherwise
    # ambiguous (e.g. both "enrollment_detail_report"). Unannotated for a plain
    # ref()-vs-ref() diff, where `old`/`new` are already unambiguous.
    old_label = f"{old} (glue:{old_glue})" if old_glue else (f"{old} (raw)" if old_raw else old)
    new_label = f"{new} (glue:{new_glue})" if new_glue else (f"{new} (raw)" if new_raw else new)

    # Resolve dbt project directory (shared preamble with impact/validate).
    if dbt_dir_path:
        dbt_dir = Path(dbt_dir_path).resolve()
    else:
        try:
            dbt_dir = get_repo_root() / "src" / "ol_dbt"
        except RuntimeError:
            dbt_dir = Path("src/ol_dbt").resolve()

    if not (dbt_dir / "dbt_project.yml").exists():
        err_console.print(f"[red]Error:[/] dbt project not found at {dbt_dir}")
        err_console.print("  Use --dbt-dir to specify the path.")
        sys.exit(1)

    # Fail fast with an actionable message rather than letting dbt's raw
    # "N package(s) specified... only M installed" Compilation Error (surfaced
    # through 500 chars of dbt log noise) reach the user.
    if not (dbt_dir / "dbt_packages" / "audit_helper").exists():
        err_console.print(f"[red]Error:[/] the 'audit_helper' dbt package is not installed in {dbt_dir}.")
        err_console.print(f"  Run: cd {dbt_dir} && dbt deps")
        sys.exit(1)

    # Refresh Glue-registered views BEFORE anything below reads one -- both the
    # materialization just below and --auto-build's `dbt build` (whose compiled
    # SQL can hit other Glue-registered views transitively, via override_ref.sql)
    # can fail with a stale-metadata 404 once production rebuilds the underlying
    # table out from under a previously-registered view.
    if refresh_glue:
        if output_format == "text":
            console.print("[dim]Running: ol-dbt local register --all-layers ...[/]")
        try:
            subprocess.run(  # noqa: S603, S607
                ["ol-dbt", "local", "register", "--all-layers"],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or str(exc)).strip()
            err_console.print(f"[red]Error:[/] ol-dbt local register --all-layers failed: {detail[-500:]}")
            sys.exit(1)
        except FileNotFoundError:
            err_console.print("[red]Error:[/] 'ol-dbt' command not found; ensure it is on PATH.")
            sys.exit(1)

    # --old-glue/--new-glue relations are backed by iceberg_scan() views, which
    # DuckDB's Iceberg extension can't serialize into audit_helper's UNION
    # ALL/EXCEPT comparison SQL (see _materialize_glue_relation). Swap them for a
    # materialized copy before any column resolution or comparison runs. These
    # `*_query` variables are the ones actually used to build SQL below;
    # `old`/`new`/`old_label`/`new_label` are left untouched so notes/output keep
    # showing the original Glue identifier the user asked to compare.
    old_query, old_query_glue, old_query_database, old_query_schema = old, old_glue, old_database, old_schema
    new_query, new_query_glue, new_query_database, new_query_schema = new, new_glue, new_database, new_schema
    if old_glue:
        try:
            old_query = _materialize_glue_relation(old, old_glue, dbt_dir, target)
        except RuntimeError as exc:
            err_console.print(f"[red]Error:[/] failed to materialize --old-glue relation: {exc}")
            sys.exit(1)
        old_query_glue, old_query_database, old_query_schema = None, None, None
    if new_glue:
        try:
            new_query = _materialize_glue_relation(new, new_glue, dbt_dir, target)
        except RuntimeError as exc:
            err_console.print(f"[red]Error:[/] failed to materialize --new-glue relation: {exc}")
            sys.exit(1)
        new_query_glue, new_query_database, new_query_schema = None, None, None

    models_dir = dbt_dir / "models"

    # Build the metadata sources used for column reconciliation.
    compiled_dir = find_compiled_dir(dbt_dir)
    manifest: ManifestRegistry | None = None
    manifest_path = find_manifest(dbt_dir)
    if manifest_path:
        try:
            manifest = load_manifest(manifest_path)
        except Exception as exc:  # noqa: BLE001
            notes.append(f"Could not load manifest ({exc}); reconciling columns from SQL/YAML only.")

    yaml_registry = build_yaml_registry(models_dir)
    sql_models_by_name: dict[str, ParsedModel] = {}
    for sql_file in models_dir.rglob("*.sql"):
        try:
            sql_models_by_name[sql_file.stem] = parse_model_file(sql_file, compiled_dir=compiled_dir)
        except Exception:  # noqa: BLE001, S112
            continue

    # --- Column reconciliation (before any comparison) ---
    old_raw_error: str | None = None
    new_raw_error: str | None = None
    if old_is_raw:
        old_cols, old_raw_error = _resolve_raw_columns(
            old_query,
            dbt_dir,
            target,
            database=old_query_database,
            schema=old_query_schema,
            glue_database=old_query_glue,
        )
    else:
        old_cols = _resolve_columns(old, sql_models_by_name, manifest, yaml_registry)
    if new_is_raw:
        new_cols, new_raw_error = _resolve_raw_columns(
            new_query,
            dbt_dir,
            target,
            database=new_query_database,
            schema=new_query_schema,
            glue_database=new_query_glue,
        )
    else:
        new_cols = _resolve_columns(new, sql_models_by_name, manifest, yaml_registry)

    unresolved_note = "Column set for '{}' could not be resolved (unparsed / not in manifest); reconciliation skipped."
    if not old_cols:
        notes.append(
            f"Column set for '{old}' could not be resolved: {old_raw_error}"
            if old_raw_error
            else unresolved_note.format(old)
        )
    if not new_cols:
        notes.append(
            f"Column set for '{new}' could not be resolved: {new_raw_error}"
            if new_raw_error
            else unresolved_note.format(new)
        )
    recon = reconcile_columns(old_cols, new_cols, set(exclude_list))

    # When a column set can't be resolved statically, the schema-divergence gate
    # cannot run — an empty reconciliation is "unverified", NOT "verified equal".
    # Surface this loudly (both output modes) so it is never mistaken for a clean
    # schema check: audit_helper.compare_relations derives its column list from
    # the OLD relation, so a column *added* in the new model would be ignored by
    # the value comparison and is normally caught only by this static gate.
    schema_gate_skipped = not (old_cols and new_cols)
    if schema_gate_skipped:
        notes.append(
            "Schema-divergence gate SKIPPED (columns unresolved) — comparison alone is authoritative "
            "and will not detect columns added in the new model. Run `dbt parse` for full schema validation."
        )
        err_console.print(
            "[yellow]Warning:[/] could not resolve column sets for both models; the schema-divergence "
            "gate was skipped. Columns added in the new model may go undetected — run `dbt parse` "
            "(or point --dbt-dir at a compiled project) for full schema validation."
        )

    # Optionally build both relations first. Raw relations aren't dbt models —
    # they're assumed to already exist (a Glue view, an already-materialized
    # table) — so they're excluded from --select rather than passed to dbt build.
    if auto_build:
        build_targets = [name for name, raw in ((old, old_is_raw), (new, new_is_raw)) if not raw]
        if not build_targets:
            notes.append("--auto-build skipped: both --old and --new are raw relations (nothing to build).")
        else:
            # Single space-joined --select value, consistent with ol-dbt run/impact.
            build_cmd = ["dbt", "build", "--target", target, "--select", " ".join(build_targets)]
            if output_format == "text":
                console.print(f"[dim]Running: {' '.join(build_cmd)} ...[/]")
            try:
                subprocess.run(build_cmd, cwd=str(dbt_dir), capture_output=True, text=True, check=True)  # noqa: S603, S607
            except subprocess.CalledProcessError as exc:
                # dbt often logs useful detail to stdout, not stderr — prefer either.
                detail = (exc.stderr or exc.stdout or str(exc)).strip()
                err_console.print(f"[red]Error:[/] dbt build failed: {detail[-500:]}")
                sys.exit(1)
            except FileNotFoundError:
                err_console.print("[red]Error:[/] 'dbt' command not found; install dbt and ensure it is on PATH.")
                sys.exit(1)

    # --- Comparison ---
    row_counts: dict[str, int] = {"old": 0, "new": 0, "delta": 0}
    column_mismatches: list[ColumnMismatch] = []
    sample_mismatches: list[dict[str, Any]] = []
    mismatched_rows = 0

    # If the resolved column sets diverge, a value comparison would fail on
    # mismatched columns — report the divergence clearly and skip it.
    if recon.diverged:
        notes.append("Column schemas diverge; skipping value comparison. Reconcile columns or use --exclude-columns.")
        _emit_and_exit(
            DiffResult(
                old=old,
                new=new,
                target=target,
                primary_key=primary_key_list,
                excluded_columns=exclude_list,
                column_reconciliation=recon,
                row_counts=row_counts,
                column_mismatches=column_mismatches,
                sample_mismatches=sample_mismatches,
                verdict=Verdict.SCHEMA_DIVERGENCE,
                old_label=old_label,
                new_label=new_label,
                notes=notes,
            ),
            output_format,
        )

    try:
        summary_rows = _run_dbt_show(
            _compare_relations_sql(
                old_query,
                new_query,
                primary_key_list,
                exclude_list,
                summarize=True,
                old_raw=old_is_raw,
                new_raw=new_is_raw,
                old_schema=old_query_schema,
                new_schema=new_query_schema,
                old_database=old_query_database,
                new_database=new_query_database,
                old_glue=old_query_glue,
                new_glue=new_query_glue,
            ),
            dbt_dir,
            target,
            limit=1000,
        )
        row_counts, mismatched_rows = _summarize_relations(summary_rows)

        if mismatched_rows:
            sample_mismatches = _run_dbt_show(
                _compare_relations_sql(
                    old_query,
                    new_query,
                    primary_key_list,
                    exclude_list,
                    summarize=False,
                    old_raw=old_is_raw,
                    new_raw=new_is_raw,
                    old_schema=old_query_schema,
                    new_schema=new_query_schema,
                    old_database=old_query_database,
                    new_database=new_query_database,
                    old_glue=old_query_glue,
                    new_glue=new_query_glue,
                ),
                dbt_dir,
                target,
                limit=limit,
            )

        # Per-column mismatch rates require a primary key.
        if primary_key_list and recon.compared:
            cols = recon.compared
            if len(cols) > _MAX_PER_COLUMN_COMPARISONS:
                notes.append(f"Per-column comparison capped at {_MAX_PER_COLUMN_COMPARISONS} of {len(cols)} columns.")
                cols = cols[:_MAX_PER_COLUMN_COMPARISONS]
            for col in cols:
                mismatch = _compare_single_column(
                    old_query,
                    new_query,
                    primary_key_list,
                    col,
                    dbt_dir,
                    target,
                    old_raw=old_is_raw,
                    new_raw=new_is_raw,
                    old_schema=old_query_schema,
                    new_schema=new_query_schema,
                    old_database=old_query_database,
                    new_database=new_query_database,
                    old_glue=old_query_glue,
                    new_glue=new_query_glue,
                )
                if mismatch is not None and mismatch.mismatched_rows > 0:
                    column_mismatches.append(mismatch)
        elif not primary_key_list:
            notes.append("Per-column mismatch rates require --primary-key; skipped.")
    except RuntimeError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    # --- Verdict ---
    if row_counts["delta"] != 0 or mismatched_rows > 0 or column_mismatches:
        verdict = Verdict.MISMATCH
    else:
        verdict = Verdict.MATCH

    _emit_and_exit(
        DiffResult(
            old=old,
            new=new,
            target=target,
            primary_key=primary_key_list,
            excluded_columns=exclude_list,
            column_reconciliation=recon,
            row_counts=row_counts,
            column_mismatches=column_mismatches,
            sample_mismatches=sample_mismatches,
            verdict=verdict,
            old_label=old_label,
            new_label=new_label,
            notes=notes,
        ),
        output_format,
    )


def _emit_and_exit(result: DiffResult, output_format: str) -> None:
    """Render the result in the requested format and exit non-zero on divergence."""
    if output_format == "json":
        _print_json(result)
    else:
        _print_text(result)
    if result.verdict != Verdict.MATCH:
        sys.exit(1)


def _compare_single_column(
    old: str,
    new: str,
    primary_key: list[str],
    column: str,
    dbt_dir: Path,
    target: str,
    *,
    old_raw: bool = False,
    new_raw: bool = False,
    old_schema: str | None = None,
    new_schema: str | None = None,
    old_database: str | None = None,
    new_database: str | None = None,
    old_glue: str | None = None,
    new_glue: str | None = None,
) -> ColumnMismatch | None:
    """Run a per-column value comparison, returning its mismatch rate.

    audit_helper.compare_column_values returns rows tagged by ``match_status``;
    the mismatch rate is the share of rows not in a "✅" perfect-match bucket.
    """
    rows = _run_dbt_show(
        _compare_column_sql(
            old,
            new,
            primary_key,
            column,
            old_raw=old_raw,
            new_raw=new_raw,
            old_schema=old_schema,
            new_schema=new_schema,
            old_database=old_database,
            new_database=new_database,
            old_glue=old_glue,
            new_glue=new_glue,
        ),
        dbt_dir,
        target,
        limit=1000,
    )
    total = 0
    mismatched = 0
    for row in rows:
        count = _int(row.get("count_records") or row.get("count"))
        total += count
        # audit_helper tags the perfect-match bucket with a leading "✅"; every
        # other bucket (value mismatch, missing from a/b) counts as a mismatch.
        if not str(row.get("match_status", "")).startswith("✅"):
            mismatched += count
    if total == 0:
        return None
    return ColumnMismatch(column=column, mismatch_rate=mismatched / total, mismatched_rows=mismatched)
