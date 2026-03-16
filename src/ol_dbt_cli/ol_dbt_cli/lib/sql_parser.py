"""sqlglot-based SQL column extractor for dbt models.

Handles Jinja templating by regex-stripping macros before parsing, allowing
sqlglot to analyse the SQL structure. This is an offline fallback; when
``target/manifest.json`` is available use :mod:`.manifest` instead.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp

# ---------------------------------------------------------------------------
# Jinja stripping
# ---------------------------------------------------------------------------

# Replace {{ ref('model_name') }} with ref_model_name (a valid SQL identifier)
_REF_RE = re.compile(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
# Replace {{ source('schema', 'table') }} with source_schema_table
_SOURCE_RE = re.compile(r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
# Replace {{ config(...) }} blocks — may be multiline, strip entirely
_CONFIG_RE = re.compile(r"\{\{\s*config\s*\(.*?\)\s*\}\}", re.DOTALL)
# Replace {{ duckdb_init() }} and similar zero-arg macro calls
_MACRO_CALL_RE = re.compile(r"\{\{\s*\w+\s*\(\s*\)\s*\}\}")
# Replace {{ var('x') }} and {{ var('x', 'default') }}
_VAR_RE = re.compile(r"\{\{\s*var\s*\(.*?\)\s*\}\}", re.DOTALL)
# Replace remaining {{ ... }} blocks with a placeholder string literal
_JINJA_BLOCK_RE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
# Strip {%- ... -%} / {% ... %} control blocks
_JINJA_CONTROL_RE = re.compile(r"\{%-?.*?-?%\}", re.DOTALL)

# Block-level macro injection: {{ macro_call(...) }} that appears alone on its own line
# (e.g. ``{{ deduplicate_raw_table(...) }}`` injecting CTE SQL fragments between CTEs).
# These must be replaced with an SQL comment, NOT a string literal, because they occupy
# statement-level positions where a string literal is invalid SQL.
# This regex is applied before _JINJA_BLOCK_RE so these are handled first.
_BLOCK_MACRO_RE = re.compile(r"(?m)^[^\S\r\n]*\{\{[^}]+\}\}[^\S\r\n]*$", re.DOTALL)

# Post-replacement cleanup: some macros embed partial SQL in their Jinja arguments,
# causing the {{ }} boundary to split a column expression.  After stripping, the
# __jinja__ placeholder is followed by orphaned SQL fragments (closing parens, CASE
# branches, etc.) before the actual column alias.
#
# Pattern detected: ", __jinja__" (no direct alias) + continuation SQL + ) as alias
# The alias MUST be the last token on its line (optionally followed by a SQL comment)
# to prevent treating type-casts like `) as array(varchar)` as a column alias.
#
# Example (stg__ocw__studio__postgres__websites_websitecontent.sql):
#   , __jinja__) as array (varchar)), ', '   <- orphaned tail (array(varchar) is a cast)
#   ) as course_level                        <- real alias, must be last token on its line
#
# The regex uses a tempered greedy token `(?:(?!<next_jinja_col>)[^\n]*\n)*?` to
# avoid consuming past the next broken-column placeholder.
_BROKEN_COL_RE = re.compile(
    r"([ \t]*,[ \t]*)__jinja__(?![ \t]+as\b)"  # column-sep + placeholder, no direct alias
    r"[^\n]*\n"  # rest of the first (broken) line
    r"(?:(?![ \t]*,[ \t]*__jinja__)[^\n]*\n)*?"  # optional continuation lines (stop before next __jinja__ col)
    r"[ \t]*\)[ \t]+as[ \t]+(\w+)[ \t]*(?:--[^\n]*)?(?:\n|$)",  # ) as alias — must be last token on line
)


@dataclass
class JinjaStripResult:
    """Result of stripping Jinja from a SQL string."""

    clean_sql: str
    ref_names: list[str] = field(default_factory=list)
    """Model names from ``{{ ref(...) }}`` calls, in order of appearance."""
    source_names: list[str] = field(default_factory=list)
    """Source names from ``{{ source(...) }}`` calls, as ``"source_name.table_name"``."""
    ref_placeholder_map: dict[str, str] = field(default_factory=dict)
    """Maps the SQL placeholder identifier back to the original model name.

    e.g. ``{"ref_stg_users": "stg_users"}``
    """
    source_placeholder_map: dict[str, str] = field(default_factory=dict)
    """Maps the SQL placeholder identifier back to ``"source_name.table_name"``.

    e.g. ``{"source_ol_warehouse_raw_data_raw__users_user": "ol_warehouse_raw_data.raw__users_user"}``
    """


def _collapse_broken_column_expressions(sql: str) -> str:
    """Collapse broken column expressions left by macros that split SQL boundaries.

    Some macros embed partial SQL in their Jinja arguments so that the ``{{ }}``
    boundary falls *inside* a column expression.  After stripping the Jinja block,
    the ``__jinja__`` placeholder is followed by orphaned SQL (closing parens, CASE
    branches, cast keywords) before the real ``…) as alias``.

    This function iteratively replaces such patterns with ``__jinja__ as alias``,
    producing a clean column definition that sqlglot can parse.

    Example (before)::

        , __jinja__) as array (varchar)), ', '   <- orphaned tail on same line
        ) as course_level                        <- real alias

    Example (after)::

        , __jinja__ as course_level
    """

    def _replace(m: re.Match[str]) -> str:
        return f"{m.group(1)}__jinja__ as {m.group(2)}\n"

    # Apply iteratively to handle multiple broken columns (each pass fixes one).
    prev = None
    while prev != sql:
        prev = sql
        sql = _BROKEN_COL_RE.sub(_replace, sql)
    return sql


def strip_jinja(sql: str) -> JinjaStripResult:
    """Strip Jinja from *sql* and return a :class:`JinjaStripResult`.

    The returned ``clean_sql`` is valid SQL (modulo Trino dialect quirks) and
    can be handed to sqlglot. The placeholder maps allow callers to reverse-look-up
    which original ``ref()`` or ``source()`` a parsed identifier came from.
    """
    ref_names: list[str] = []
    source_names: list[str] = []
    ref_placeholder_map: dict[str, str] = {}
    source_placeholder_map: dict[str, str] = {}

    def _replace_ref(m: re.Match[str]) -> str:
        name = m.group(1)
        ref_names.append(name)
        placeholder = f"ref_{_to_identifier(name)}"
        ref_placeholder_map[placeholder] = name
        return placeholder

    def _replace_source(m: re.Match[str]) -> str:
        schema, table = m.group(1), m.group(2)
        source_key = f"{schema}.{table}"
        source_names.append(source_key)
        placeholder = f"source_{_to_identifier(schema)}_{_to_identifier(table)}"
        source_placeholder_map[placeholder] = source_key
        return placeholder

    cleaned = _CONFIG_RE.sub("", sql)
    cleaned = _JINJA_CONTROL_RE.sub("", cleaned)
    cleaned = _REF_RE.sub(_replace_ref, cleaned)
    cleaned = _SOURCE_RE.sub(_replace_source, cleaned)
    # All Jinja replacements use UNQUOTED identifiers so that templates that already have
    # surrounding SQL quotes (e.g. `like '{{ var("x") }}'`) produce valid SQL (the outer
    # quotes are preserved as string delimiters and the placeholder becomes the content).
    # Using quoted replacements would turn `'{{ x }}'` into `''__jinja__''` (invalid SQL).
    cleaned = _VAR_RE.sub("__var__", cleaned)
    cleaned = _MACRO_CALL_RE.sub("__macro__", cleaned)
    # Block-level macros (CTE-injecting) must become comments, not string literals.
    cleaned = _BLOCK_MACRO_RE.sub("/* __jinja_macro__ */", cleaned)
    cleaned = _JINJA_BLOCK_RE.sub("__jinja__", cleaned)
    # Collapse broken column expressions where a macro splits a SQL column boundary.
    cleaned = _collapse_broken_column_expressions(cleaned)
    return JinjaStripResult(
        clean_sql=cleaned,
        ref_names=ref_names,
        source_names=source_names,
        ref_placeholder_map=ref_placeholder_map,
        source_placeholder_map=source_placeholder_map,
    )


def _to_identifier(name: str) -> str:
    """Convert an arbitrary string to a safe SQL identifier fragment."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


# ---------------------------------------------------------------------------
# Column extraction
# ---------------------------------------------------------------------------


@dataclass
class ParsedModel:
    """Result of parsing a single dbt SQL model."""

    name: str
    output_columns: set[str] = field(default_factory=set)
    """Column names produced by this model's final SELECT."""
    refs: list[str] = field(default_factory=list)
    """Model names referenced via {{ ref(...) }}."""
    source_refs: list[str] = field(default_factory=list)
    """Source references via {{ source(...) }}."""
    has_star: bool = False
    """True if the outermost SELECT contains an unresolved SELECT *."""
    star_resolved: bool = False
    """True if a SELECT * was present but successfully expanded via CTE or registry analysis."""
    star_source: str = ""
    """Name of the relation the unresolved * draws from (CTE, ref placeholder, or table)."""
    ref_placeholder_map: dict[str, str] = field(default_factory=dict)
    """Maps SQL placeholder identifier → original model name (e.g. ``"ref_stg_users" → "stg_users"``)."""
    source_placeholder_map: dict[str, str] = field(default_factory=dict)
    """Maps SQL placeholder identifier → ``"source_name.table_name"``."""
    parse_error: str | None = None
    """Non-None if sqlglot could not parse the SQL."""
    compiled_path: Path | None = None
    """Path to the compiled SQL file used for parsing, if any."""


def _extract_select_columns(select: exp.Select) -> tuple[set[str], bool]:
    """Return ``(column_aliases, has_star)`` from a SELECT expression."""
    cols: set[str] = set()
    has_star = False
    for expr in select.expressions:
        if isinstance(expr, exp.Star):
            has_star = True
        elif isinstance(expr, exp.Alias):
            cols.add(expr.alias.lower())
        elif isinstance(expr, exp.Column):
            # bare column reference with no alias — use the column name
            cols.add(expr.name.lower())
        elif hasattr(expr, "alias") and expr.alias:
            cols.add(expr.alias.lower())
    return cols, has_star


def _build_cte_column_map(parsed: exp.Expression) -> dict[str, tuple[set[str], bool]]:
    """Build a map of CTE alias -> ``(columns, has_star)`` for all CTEs in *parsed*.

    Iterates CTEs in definition order so later CTEs can reference earlier ones.
    Works for both ``exp.Select`` (sqlglot's representation of ``WITH … SELECT``)
    and ``exp.With`` nodes.
    """
    cte_cols: dict[str, tuple[set[str], bool]] = {}

    for cte in parsed.find_all(exp.CTE):
        cte_name = cte.alias.lower()
        cte_query = cte.args.get("this")
        if not isinstance(cte_query, exp.Select):
            continue
        cols, has_star = _extract_select_columns(cte_query)
        if has_star:
            # Try to resolve the star from an already-processed CTE
            resolved, still_star = _try_resolve_star(cte_query, cte_cols)
            if not still_star:
                cte_cols[cte_name] = (cols | resolved, False)
                continue
        cte_cols[cte_name] = (cols, has_star)

    return cte_cols


def _try_resolve_star(
    select: exp.Select,
    cte_cols: dict[str, tuple[set[str], bool]],
) -> tuple[set[str], bool]:
    """Attempt to expand SELECT * by resolving the source from *cte_cols*.

    Returns ``(columns, has_unresolved_star)``.  When the star's source is a
    known CTE with fully resolved columns, returns those columns and False.
    Otherwise returns an empty set and True.
    """
    from_clause = select.args.get("from_")
    if from_clause is None:
        return set(), True

    # Gather FROM source + any JOINs
    joins = select.args.get("joins") or []
    source_tables: list[str] = []
    from_table = from_clause.find(exp.Table)
    if from_table:
        source_tables.append(from_table.alias_or_name.lower())
    for join in joins:
        jt = join.find(exp.Table)
        if jt:
            source_tables.append(jt.alias_or_name.lower())

    # Single-source SELECT *: attempt expansion from a known CTE
    if len(source_tables) == 1:
        source_name = source_tables[0]
        if source_name in cte_cols:
            resolved_cols, cte_has_star = cte_cols[source_name]
            if not cte_has_star:
                return resolved_cols, False

    # Multi-source or unknown source — cannot safely expand
    return set(), True


def _outermost_select(parsed: exp.Expression) -> exp.Select | None:
    """Return the outermost SELECT from a parsed expression.

    Handles four cases:
    - Plain ``exp.Select`` (most models): returned directly.
    - ``exp.Select`` with embedded CTEs (``WITH … SELECT`` parsed as Select): returned directly.
    - ``exp.With`` whose body is a Select or Union (some dialect variations).
    - ``exp.Union`` chains (UNION ALL models): traverse the chain to the leftmost Select.

    Fallback: walk the entire tree looking for the first Select that is NOT directly
    inside a CTE (i.e., it is the main query body, not a CTE body).
    """
    if isinstance(parsed, exp.Select):
        return parsed

    if isinstance(parsed, exp.With):
        body = parsed.this  # The SELECT/UNION body attached to the WITH clause
        if isinstance(body, exp.Select):
            return body
        if isinstance(body, exp.Union):
            # Take the leftmost branch of the UNION as representative
            sel = _leftmost_select_from_union(body)
            if sel:
                return sel
        # Fallback: With.this is None (sqlglot parse quirk) — find the first
        # Select whose parent is not a CTE
        return _first_non_cte_select(parsed)

    if isinstance(parsed, exp.Union):
        sel = _leftmost_select_from_union(parsed)
        if sel:
            return sel
        return _first_non_cte_select(parsed)

    return None


def _leftmost_select_from_union(union: exp.Union) -> exp.Select | None:
    """Traverse a Union chain and return the leftmost leaf Select."""
    node: exp.Expression = union
    while isinstance(node, exp.Union):
        node = node.this  # Walk left
    return node if isinstance(node, exp.Select) else None


def _first_non_cte_select(root: exp.Expression) -> exp.Select | None:
    """Return the first Select node whose immediate parent is NOT a CTE."""
    for node in root.walk():
        if isinstance(node, exp.Select) and not isinstance(node.parent, exp.CTE):
            return node
    return None


def _get_star_source(select: exp.Select) -> str:
    """Return the alias-or-name of the relation the outermost SELECT * draws from."""
    from_clause = select.args.get("from_")
    if from_clause is None:
        return ""
    table = from_clause.find(exp.Table)
    return table.alias_or_name.lower() if table else ""


def parse_model_sql(name: str, sql: str) -> ParsedModel:
    """Parse *sql* content for model *name* and extract column metadata."""
    stripped = strip_jinja(sql)
    result = ParsedModel(
        name=name,
        refs=stripped.ref_names,
        source_refs=stripped.source_names,
        ref_placeholder_map=stripped.ref_placeholder_map,
        source_placeholder_map=stripped.source_placeholder_map,
    )

    try:
        statements = sqlglot.parse(stripped.clean_sql, dialect="trino", error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception as exc:  # noqa: BLE001
        result.parse_error = str(exc)
        return result

    # Take the last non-None statement (handles multiple statements)
    parsed = next((s for s in reversed(statements) if s is not None), None)
    if parsed is None:
        result.parse_error = "No parseable SQL statements found"
        return result

    # Build CTE column map first so we can expand SELECT * where possible
    cte_cols = _build_cte_column_map(parsed)

    select = _outermost_select(parsed)
    if select is None:
        result.parse_error = f"Could not find outermost SELECT in {name}"
        return result

    cols, has_star = _extract_select_columns(select)

    if has_star:
        # Merge any explicit aliases alongside the star with the resolved star cols
        resolved_star, still_unresolved = _try_resolve_star(select, cte_cols)
        if not still_unresolved:
            result.output_columns = cols | resolved_star
            result.has_star = False
            result.star_resolved = True
        else:
            # Record that star is present and from which source
            result.output_columns = cols  # explicit aliases alongside * (rare but valid)
            result.has_star = True
            result.star_source = _get_star_source(select)
    else:
        result.output_columns = cols
        result.has_star = False

    return result


def parse_model_file(path: Path, compiled_dir: Path | None = None) -> ParsedModel:
    """Parse a dbt model `.sql` file.

    When *compiled_dir* is provided and a compiled counterpart exists, uses the
    fully-rendered (Jinja-free) SQL from ``target/compiled/`` for accurate column
    extraction.  Refs and sources are always parsed from the raw SQL via
    :func:`strip_jinja` so that lineage information is available even when using
    compiled SQL (compiled SQL uses physical relation names, not ``ref_*`` prefixes).

    Falls back to raw SQL parsing with Jinja stripping otherwise.
    """
    raw_sql = path.read_text()

    if compiled_dir is not None:
        compiled_sql, compiled_path = _find_compiled_sql(path.stem, compiled_dir)
        if compiled_sql is not None:
            # Column extraction from compiled SQL (accurate, Jinja-free).
            result = _parse_clean_sql(path.stem, compiled_sql)
            result.compiled_path = compiled_path
            # Lineage (refs/sources) must come from the raw SQL because compiled
            # SQL replaces {{ ref('x') }} with physical relation names that have no
            # reliable 'ref_' prefix convention.
            jinja_result = strip_jinja(raw_sql)
            result.refs = jinja_result.ref_names
            result.source_refs = jinja_result.source_names
            result.ref_placeholder_map = jinja_result.ref_placeholder_map
            result.source_placeholder_map = jinja_result.source_placeholder_map
            return result

    return parse_model_sql(path.stem, raw_sql)


def _find_compiled_sql(model_name: str, compiled_dir: Path) -> tuple[str | None, Path | None]:
    """Search *compiled_dir* recursively for ``model_name.sql`` and return its content."""
    matches = list(compiled_dir.rglob(f"{model_name}.sql"))
    if matches:
        p = matches[0]
        return p.read_text(), p
    return None, None


def _parse_clean_sql(name: str, sql: str) -> ParsedModel:
    """Parse already-rendered (Jinja-free) SQL and return a :class:`ParsedModel`.

    Unlike :func:`parse_model_sql`, this path skips Jinja stripping entirely.
    Used when compiled SQL from ``target/compiled/`` is available.
    """
    result = ParsedModel(name=name)
    try:
        statements = sqlglot.parse(sql, dialect="trino", error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception as exc:  # noqa: BLE001
        result.parse_error = str(exc)
        return result

    parsed = next((s for s in reversed(statements) if s is not None), None)
    if parsed is None:
        result.parse_error = "No parseable SQL statements found"
        return result

    # Note: refs/sources are NOT populated here.  Compiled SQL uses physical
    # relation names (no `ref_`/`source_` prefix), so lineage information must
    # come from strip_jinja() on the raw SQL — see parse_model_file().

    cte_cols = _build_cte_column_map(parsed)
    select = _outermost_select(parsed)
    if select is None:
        result.parse_error = f"Could not find outermost SELECT in {name}"
        return result

    cols, has_star = _extract_select_columns(select)
    if has_star:
        resolved_star, still_unresolved = _try_resolve_star(select, cte_cols)
        if not still_unresolved:
            result.output_columns = cols | resolved_star
            result.star_resolved = True
        else:
            result.output_columns = cols
            result.has_star = True
            result.star_source = _get_star_source(select)
    else:
        result.output_columns = cols

    return result


def find_compiled_dir(dbt_dir: Path) -> Path | None:
    """Return the ``target/compiled/<project_name>/models`` directory if it exists."""
    target = dbt_dir / "target" / "compiled"
    if not target.is_dir():
        return None
    # Find the first project sub-directory (e.g. open_learning)
    for candidate in target.iterdir():
        models_dir = candidate / "models"
        if candidate.is_dir() and models_dir.is_dir():
            return models_dir
    return None


def parse_model_sql_at_content(name: str, sql_content: str) -> ParsedModel:
    """Parse SQL content (e.g., from git show) for the named model."""
    return parse_model_sql(name, sql_content)
