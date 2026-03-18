"""sqlglot-based SQL column extractor for dbt models.

Handles Jinja templating using Jinja2 rendering with safe fallbacks, allowing
sqlglot to analyse the SQL structure. This is an offline fallback; when
``target/manifest.json`` is available use :mod:`.manifest` instead.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import jinja2
import markupsafe
import sqlglot
import sqlglot.expressions as exp

# ---------------------------------------------------------------------------
# Jinja2 rendering (primary path)
# ---------------------------------------------------------------------------


class _SqlSafeUndefined(jinja2.Undefined):
    """Jinja2 Undefined that renders as safe SQL placeholders instead of raising.

    This allows dbt SQL templates that reference undefined variables, macros,
    and adapter-specific functions to render without errors.  The rendered SQL
    will contain placeholder tokens (``__undefined__``, ``__macro__``) that
    sqlglot ignores or treats as opaque identifiers.
    """

    def __str__(self) -> str:
        return "__undefined__"

    def __call__(self, *args: object, **kwargs: object) -> markupsafe.Markup:
        return markupsafe.Markup("__macro__")

    def __getattr__(self, name: str) -> _SqlSafeUndefined:
        if name.startswith("_"):
            raise AttributeError(name)
        return _SqlSafeUndefined(name=name)

    def __iter__(self):  # noqa: ANN201
        return iter([])

    def __len__(self) -> int:
        return 0

    def __bool__(self) -> bool:
        return False


def _make_jinja_env(
    ref_names: list[str],
    source_names: list[str],
    ref_placeholder_map: dict[str, str],
    source_placeholder_map: dict[str, str],
) -> jinja2.Environment:
    """Build a Jinja2 environment with dbt-compatible globals.

    The ``ref()`` and ``source()`` globals collect lineage side-effects into
    the provided lists/maps while returning SQL-safe placeholder identifiers
    that downstream sqlglot parsing can handle.
    """

    def _ref(model_name: str) -> markupsafe.Markup:
        ref_names.append(model_name)
        placeholder = f"ref_{_to_identifier(model_name)}"
        ref_placeholder_map[placeholder] = model_name
        return markupsafe.Markup(placeholder)  # noqa: S704

    def _source(source_name: str, table_name: str) -> markupsafe.Markup:
        source_key = f"{source_name}.{table_name}"
        source_names.append(source_key)
        placeholder = f"source_{_to_identifier(source_name)}_{_to_identifier(table_name)}"
        source_placeholder_map[placeholder] = source_key
        return markupsafe.Markup(placeholder)  # noqa: S704

    def _config(**_kwargs: object) -> markupsafe.Markup:
        return markupsafe.Markup("")  # noqa: S704

    def _var(name: str, default: object = "") -> str:
        return str(default) if default else "__var__"

    def _env_var(name: str, default: object = "") -> str:
        return str(default) if default else "__env_var__"

    def _is_incremental() -> bool:
        return False

    def _run_query(sql: str) -> list[object]:  # noqa: ARG001
        return []

    env = jinja2.Environment(
        undefined=_SqlSafeUndefined,
        autoescape=False,  # noqa: S701 — generating SQL, not HTML; autoescaping not applicable
        # Allow Jinja2 to handle dbt's {%- -%} whitespace-trimming syntax
        trim_blocks=False,
        lstrip_blocks=False,
    )
    env.globals.update(
        {
            "ref": _ref,
            "source": _source,
            "config": _config,
            "var": _var,
            "env_var": _env_var,
            "is_incremental": _is_incremental,
            "run_query": _run_query,
            # Common dbt utils / adapter namespaces — render as safe undefined
            "dbt_utils": _SqlSafeUndefined(name="dbt_utils"),
            "dbt": _SqlSafeUndefined(name="dbt"),
            "adapter": _SqlSafeUndefined(name="adapter"),
            "modules": _SqlSafeUndefined(name="modules"),
            "this": _SqlSafeUndefined(name="this"),
            "model": _SqlSafeUndefined(name="model"),
            "execute": False,
            "flags": _SqlSafeUndefined(name="flags"),
            "graph": _SqlSafeUndefined(name="graph"),
            "invocation_id": "__invocation_id__",
            "thread_id": "__thread_id__",
            "target": _SqlSafeUndefined(name="target"),
            "schemas": _SqlSafeUndefined(name="schemas"),
        }
    )
    return env


def _render_jinja(sql: str) -> tuple[str, list[str], list[str], dict[str, str], dict[str, str]]:
    """Render a dbt SQL template using Jinja2.

    Returns ``(rendered_sql, ref_names, source_names, ref_placeholder_map, source_placeholder_map)``.
    Raises ``jinja2.TemplateError`` if rendering fails.

    Unlike the regex-based :func:`strip_jinja`, this function properly evaluates
    ``{% if %}`` / ``{% for %}`` control blocks rather than simply stripping them,
    producing SQL that represents *one* valid execution path.
    """
    ref_names: list[str] = []
    source_names: list[str] = []
    ref_placeholder_map: dict[str, str] = {}
    source_placeholder_map: dict[str, str] = {}

    env = _make_jinja_env(ref_names, source_names, ref_placeholder_map, source_placeholder_map)
    template = env.from_string(sql)
    rendered = template.render()

    # Block-level macro injections (macros that inject CTE SQL fragments) render as
    # "__macro__" which occupies a statement-level position where a string literal
    # is invalid SQL.  Replace standalone __macro__ / __undefined__ lines with SQL comments.
    rendered = re.sub(r"(?m)^[^\S\r\n]*(?:__macro__|__undefined__)[^\S\r\n]*$", "/* __jinja_macro__ */", rendered)
    # Collapse any broken column expressions (same post-processing as regex path).
    rendered = _collapse_broken_column_expressions(rendered)

    return rendered, ref_names, source_names, ref_placeholder_map, source_placeholder_map


# ---------------------------------------------------------------------------
# Jinja stripping (regex fallback)
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
# Matches both ``__jinja__`` (regex path) and ``__macro__`` (Jinja2 path) placeholders.
_BROKEN_COL_PLACEHOLDER = r"(?:__jinja__|__macro__|__undefined__)"
_BROKEN_COL_RE = re.compile(
    rf"([ \t]*,[ \t]*){_BROKEN_COL_PLACEHOLDER}(?![ \t]+as\b)"  # column-sep + placeholder, no direct alias
    r"[^\n]*\n"  # rest of the first (broken) line
    rf"(?:(?![ \t]*,[ \t]*{_BROKEN_COL_PLACEHOLDER})[^\n]*\n)*?"  # optional continuation lines
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
    """Process Jinja in *sql* and return a :class:`JinjaStripResult`.

    **Primary path**: Jinja2 rendering with :class:`_SqlSafeUndefined`.  This
    properly evaluates ``{% if %}`` / ``{% for %}`` control blocks rather than
    simply stripping them, producing SQL that represents one valid execution path.

    **Fallback**: Regex-based stripping is used when Jinja2 rendering raises a
    ``TemplateError`` (e.g. syntax errors from unusual Jinja constructs in the
    wild).  The fallback produces a ``clean_sql`` where all ``{% ... %}`` blocks
    are stripped and ``{{ ... }}`` blocks are replaced with placeholder tokens.

    In both cases the returned ``clean_sql`` is valid SQL (modulo Trino dialect
    quirks) and can be handed to sqlglot.  The placeholder maps allow callers to
    reverse-look-up which original ``ref()`` or ``source()`` a parsed identifier
    came from.
    """
    try:
        rendered, ref_names, source_names, ref_placeholder_map, source_placeholder_map = _render_jinja(sql)
        return JinjaStripResult(
            clean_sql=rendered,
            ref_names=ref_names,
            source_names=source_names,
            ref_placeholder_map=ref_placeholder_map,
            source_placeholder_map=source_placeholder_map,
        )
    except jinja2.TemplateError:
        pass

    return _strip_jinja_regex(sql)


def _strip_jinja_regex(sql: str) -> JinjaStripResult:
    """Regex-based Jinja stripping — fallback when Jinja2 rendering fails."""
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
    source_path: Path | None = None
    """Path to the raw (Jinja) SQL source file."""


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


def _build_cte_column_map(parsed: exp.Expression) -> dict[str, tuple[set[str], bool, str]]:
    """Build a map of CTE alias -> ``(columns, has_star, star_source)`` for all CTEs.

    Iterates CTEs in definition order so later CTEs can reference earlier ones.
    Works for both ``exp.Select`` (sqlglot's representation of ``WITH … SELECT``)
    and ``exp.With`` nodes.

    Handles three CTE body types:

    * ``exp.Select`` — standard SELECT; columns extracted directly.
    * ``exp.Union`` — UNION ALL / UNION model; the *leftmost* SELECT branch is used
      to derive column names (all branches must share the same schema).
    * Anything else — skipped (no column info available).

    A CTE whose own SELECT * draws from another already-resolved CTE is expanded
    inline so that downstream CTEs (and the outermost SELECT) can in turn resolve
    their stars through it.

    ``star_source`` is the name of the relation that an unresolved CTE star draws
    from, enabling callers to trace passthrough CTE chains back to the original
    ref/source placeholder.
    """
    cte_cols: dict[str, tuple[set[str], bool, str]] = {}

    for cte in parsed.find_all(exp.CTE):
        cte_name = cte.alias.lower()
        cte_query = cte.args.get("this")

        # Resolve the representative SELECT for this CTE body.
        if isinstance(cte_query, exp.Select):
            cte_select: exp.Select | None = cte_query
        elif isinstance(cte_query, exp.Union):
            # UNION ALL / UNION: take the leftmost branch as the authoritative schema.
            cte_select = _leftmost_select_from_union(cte_query)
        else:
            continue

        if cte_select is None:
            continue

        cols, has_star = _extract_select_columns(cte_select)
        if has_star:
            # Try to resolve the star from an already-processed CTE
            resolved, still_star = _try_resolve_star(cte_select, cte_cols)
            if not still_star:
                cte_cols[cte_name] = (cols | resolved, False, "")
                continue
        star_src = _get_star_source(cte_select) if has_star else ""
        cte_cols[cte_name] = (cols, has_star, star_src)

    return cte_cols


def _try_resolve_star(
    select: exp.Select,
    cte_cols: dict[str, tuple[set[str], bool, str]],
) -> tuple[set[str], bool]:
    """Attempt to expand SELECT * by resolving the source from *cte_cols*.

    Returns ``(columns, has_unresolved_star)``.  When the star's source is a
    known CTE with fully resolved columns, returns those columns and False.
    Otherwise returns an empty set and True.

    Also handles ``SELECT * FROM (VALUES …) AS alias (col1, col2, …)`` by
    extracting the column names from the subquery alias list.
    """
    from_clause = select.args.get("from_")
    if from_clause is None:
        return set(), True

    # VALUES inline table: SELECT * FROM (VALUES …) AS alias (col1, col2)
    # sqlglot represents this as a Values node directly under From (not wrapped in Subquery).
    values_node = from_clause.find(exp.Values)
    if values_node is not None:
        alias_node = from_clause.find(exp.TableAlias)
        if alias_node is not None:
            alias_cols = alias_node.args.get("columns") or []
            if alias_cols:
                return {c.name.lower() for c in alias_cols if hasattr(c, "name")}, False

    # VALUES subquery: SELECT * FROM (VALUES …) AS alias (col1, col2)
    # sqlglot represents this as a Subquery whose alias has a columns list.
    subquery = from_clause.find(exp.Subquery)
    if subquery is not None:
        alias_node = subquery.args.get("alias")
        if alias_node is not None:
            alias_cols = alias_node.args.get("columns") or []
            if alias_cols:
                return {c.name.lower() for c in alias_cols if hasattr(c, "name")}, False

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
            resolved_cols, cte_has_star, _src = cte_cols[source_name]
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


def _resolve_star_source_through_ctes(
    star_source: str,
    cte_cols: dict[str, tuple[set[str], bool, str]],
) -> str:
    """Walk through thin passthrough CTEs to find the ultimate star source.

    Some models have a CTE that is just ``SELECT * FROM ref_<upstream>`` and
    then the final SELECT does ``SELECT * FROM that_cte``.  The outermost
    ``star_source`` is the CTE name, but for registry-based resolution we
    need the actual ref/source placeholder.

    Follows the chain up to 20 hops; returns the first source that is NOT
    itself a passthrough CTE (i.e., a ref/source placeholder or a CTE with
    explicit columns).
    """
    seen: set[str] = set()
    current = star_source
    for _ in range(20):
        if current in seen:
            break
        seen.add(current)
        entry = cte_cols.get(current)
        if entry is None:
            # Not a known CTE — it's an external table/ref/source placeholder
            break
        _cols, still_star, inner_src = entry
        if not still_star:
            # This CTE resolved; the chain stops here (use current CTE as source)
            break
        if not inner_src or inner_src == current:
            break
        # Passthrough: this CTE's own star draws from inner_src → follow it
        current = inner_src
    return current


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
            # Record that star is present and from which source.
            # If the direct source is a passthrough CTE, trace it back to the
            # actual ref/source placeholder so registry-based resolution works.
            raw_src = _get_star_source(select)
            result.output_columns = cols  # explicit aliases alongside * (rare but valid)
            result.has_star = True
            result.star_source = _resolve_star_source_through_ctes(raw_src, cte_cols)
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
            result.source_path = path
            # Lineage (refs/sources) must come from the raw SQL because compiled
            # SQL replaces {{ ref('x') }} with physical relation names that have no
            # reliable 'ref_' prefix convention.
            jinja_result = strip_jinja(raw_sql)
            result.refs = jinja_result.ref_names
            result.source_refs = jinja_result.source_names
            result.ref_placeholder_map = jinja_result.ref_placeholder_map
            result.source_placeholder_map = jinja_result.source_placeholder_map
            return result

    result = parse_model_sql(path.stem, raw_sql)
    result.source_path = path
    return result


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
            raw_src = _get_star_source(select)
            result.output_columns = cols
            result.has_star = True
            result.star_source = _resolve_star_source_through_ctes(raw_src, cte_cols)
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
