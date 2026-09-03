"""Surrogate-key hash-input extraction and downstream drift detection.

``dbt_utils.generate_surrogate_key([...])`` mints a primary key by hashing an
ordered list of expressions. A model materialized ``table`` or ``view`` re-runs
that hash over every row on every build, so editing the input list silently
re-keys the whole dimension. Its incremental descendants stored the *old* key
as a foreign key at insert time and only revisit rows inside their watermark,
so those FKs orphan — and because no column was added or removed,
``on_schema_change='append_new_columns'`` sees nothing to react to. Two
production incidents came from exactly this: the ``dim_discount.discount_pk``
hash-input change (#2411) and the ``dim_user`` re-key (#2618).

Nothing in this module touches disk, git, or a database. Callers supply SQL
text and a parsed manifest, which is what lets both consumers share one
implementation: ``ol-dbt impact`` diffs a branch against its merge base, while
the Dagster build diffs the deployed manifest against the key state recorded
after the previous build.
"""

from __future__ import annotations

import re
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry

# Materializations that rebuild the whole relation on every run, and therefore
# re-derive every surrogate key they mint. A change to the hash inputs of a
# model materialized any other way is not this failure mode: an incremental
# ancestor keeps its already-minted keys, and an ephemeral one is inlined into
# its consumers rather than stored.
FULL_REFRESH_MATERIALIZATIONS = frozenset({"table", "view"})

INCREMENTAL_MATERIALIZATION = "incremental"

# `dbt_utils.` is optional because the macro is also reachable unqualified when
# dbt_utils is in the project's dispatch search path.
_SURROGATE_KEY_CALL = re.compile(r"(?:dbt_utils\s*\.\s*)?generate_surrogate_key\s*\(", re.IGNORECASE)

# The alias the minted key is bound to, matched from just past the call's
# closing paren (no anchor: `Pattern.match(s, pos)` already anchors there, and
# `\A` would instead demand the start of the file). `as` is required rather
# than optional: without the keyword the next identifier after the Jinja block
# is just as likely to be `from` or the start of the following select item, and
# a wrong alias here silently attributes the change to a column nobody stores.
_KEY_ALIAS = re.compile(r"\s*(?:\}\})?\s*as\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?", re.IGNORECASE)

_STRING_LITERAL = re.compile(r"'([^']*)'|\"([^\"]*)\"")

_WHITESPACE_RUN = re.compile(r"\s+")


@dataclass(frozen=True)
class SurrogateKeyDef:
    """One ``generate_surrogate_key`` call site and the column it is bound to."""

    column: str
    """The ``as <alias>`` the minted key is bound to; empty when unaliased."""
    inputs: tuple[str, ...]
    """The hashed expressions, normalized, in the order they are hashed."""


@dataclass(frozen=True)
class SurrogateKeyChange:
    """A key column whose hash inputs differ between two versions of a model."""

    column: str
    base_inputs: tuple[tuple[str, ...], ...]
    current_inputs: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class AffectedIncrementalModel:
    """An incremental model holding a value derived from a re-keyed column."""

    model_name: str
    unique_id: str
    fk_column: str
    evidence: str
    """How the link was established — see :data:`_EVIDENCE_RANK`."""
    depth: int
    """Hops from the re-keyed ancestor; 1 is a direct consumer."""


@dataclass
class KeyRegenFinding:
    """A hash-input change plus the incremental models it orphans."""

    ancestor: str
    ancestor_unique_id: str
    changed_key_column: str
    base_inputs: list[list[str]]
    current_inputs: list[list[str]]
    affected_models: list[AffectedIncrementalModel] = field(default_factory=list)

    @property
    def affected_model_names(self) -> list[str]:
        return sorted({m.model_name for m in self.affected_models})


# Strongest first. A `relationships` test is a declared FK edge naming both
# sides and both columns, so it beats anything inferred from the SQL; a column
# read seen by the parser beats the manifest's documented column list, which
# only says the name exists on both sides.
_EVIDENCE_RANK = {"relationships_test": 0, "column_read": 1, "column_metadata": 2}

ColumnReadLookup = Callable[[str, str], set[str] | None]
"""``(child_model, parent_model) -> columns the child reads from that parent``.

Returns ``None`` when the SQL could not be analysed, which is the signal to
fall back to the manifest's documented columns rather than to conclude the
child reads nothing.
"""


def _no_column_reads(_child: str, _parent: str) -> set[str] | None:
    return None


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def _normalize_input(expression: str) -> str:
    """Fold indentation and keyword case so a reformat is not read as a re-key.

    Reindenting a multi-line argument list, or recasing ``CAST``/``LOWER``,
    changes no hash in the warehouse, so it must not full-refresh a fact table
    here either.

    Quoted regions are copied through untouched. A SQL string literal and a
    quoted identifier are both case- and space-sensitive: ``concat(kind, 'A')``
    and ``concat(kind, 'a')`` hash differently, and folding them together would
    make the detector miss the exact re-key it exists to catch. ``''`` inside a
    single-quoted literal is SQL's own escape for a quote and stays inside the
    literal.

    Outside a quoted region, each run of whitespace folds to a single space —
    enough to absorb a reindent, and safe there because whitespace between SQL
    tokens is insignificant. ``concat(a, ', ', b)`` and ``concat(a, ',', b)``
    still compare different, since what separates them lives inside a literal.
    """
    out: list[str] = []
    for text, quote in _quoted_regions(expression):
        # Each run of whitespace collapses to a single space rather than being
        # removed: dropping it would join a literal to its neighbour, turning
        # `'a' 'b'` into `'a''b'` -- a different expression, since `''` is an
        # escaped quote inside one literal rather than two adjacent ones.
        out.append(text if quote else _WHITESPACE_RUN.sub(" ", text).lower())
    return "".join(out).strip()


def _quoted_regions(text: str) -> list[tuple[str, str | None]]:
    """Split *text* into ``(chunk, quote_char_or_None)`` runs.

    A chunk tagged with a quote character is a complete SQL string literal or
    quoted identifier including its delimiters, with ``''``/``""`` doubling
    treated as an escaped quote inside it rather than as a close followed by a
    reopen. An unterminated quote runs to the end of *text*, which keeps the
    split total: a malformed expression must still normalize to something
    stable rather than raise.
    """
    regions: list[tuple[str, str | None]] = []
    start = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char not in "'\"":
            index += 1
            continue
        if start != index:
            regions.append((text[start:index], None))
        end = _end_of_quoted(text, index)
        regions.append((text[index:end], char))
        start = index = end
    if start < len(text):
        regions.append((text[start:], None))
    return regions


def _end_of_quoted(text: str, start: int) -> int:
    """Index just past the literal opening at *start*, or the end of *text*."""
    quote = text[start]
    index = start + 1
    while index < len(text):
        if text[index] != quote:
            index += 1
        elif index + 1 < len(text) and text[index + 1] == quote:
            index += 2  # doubled quote: SQL's escape, still inside the literal
        else:
            return index + 1
    return len(text)


def _balanced_call_args(sql: str, open_paren: int) -> tuple[str, int] | None:
    """Return the argument text of the call whose ``(`` is at *open_paren*.

    Also returns the index just past the matching ``)``. Quote-aware so a
    parenthesis inside a hashed SQL expression (``cast(x as varchar)`` is
    itself parenthesised, and string literals may contain either) does not end
    the scan early. Returns None if the parentheses never balance.
    """
    depth = 0
    quote: str | None = None
    index = open_paren
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == quote:
                quote = None
        elif char in "'\"":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return sql[open_paren + 1 : index], index + 1
        index += 1
    return None


def _argument_literals(args: str) -> tuple[str, ...]:
    r"""Return the hashed expressions in a ``generate_surrogate_key`` argument list.

    Two string literals separated by nothing but whitespace are one argument,
    not two: Jinja concatenates adjacent literals, so ``['O' 'Brien']`` reaches
    dbt as the single value ``OBrien``. Reading them as two would make
    ``['O' 'Brien']`` and ``['O', 'Brien']`` — which hash differently, one
    input versus two — compare identical, and a re-key between them would go
    unreported.

    Jinja's lexer has no backslash escape (``['it\\'s']`` is a syntax error),
    so a quote inside a literal can only be written by delimiting with the
    other quote character. That is why the literal pattern needs no escape
    handling at this level; the ``''`` doubling that SQL uses is one level
    down, inside the expression, and is handled by :func:`_normalize_input`.
    """
    inputs: list[str] = []
    previous_end: int | None = None
    for match in _STRING_LITERAL.finditer(args):
        single, double = match.group(1), match.group(2)
        value = single if single is not None else double
        if previous_end is not None and not args[previous_end : match.start()].strip():
            inputs[-1] += value
        else:
            inputs.append(value)
        previous_end = match.end()
    return tuple(_normalize_input(value) for value in inputs)


def extract_surrogate_keys(sql: str) -> list[SurrogateKeyDef]:
    """Every ``generate_surrogate_key`` call in *sql*, in source order.

    Reads the raw (Jinja) SQL rather than compiled output on purpose: the
    compiled form is an opaque nested ``md5(...)`` expression whose text also
    changes whenever the adapter or dbt_utils version does, while the argument
    list is exactly the thing whose change re-keys the dimension.

    Only string-literal arguments are recovered. ``generate_surrogate_key`` is
    documented to take a list of quoted column names or SQL expressions, so a
    call built from a Jinja variable instead yields an empty input tuple —
    which compares equal across versions and is therefore reported as
    unchanged rather than as a spurious re-key.
    """
    keys: list[SurrogateKeyDef] = []
    for match in _SURROGATE_KEY_CALL.finditer(sql):
        call = _balanced_call_args(sql, match.end() - 1)
        if call is None:
            continue
        args, after = call
        inputs = _argument_literals(args)
        alias_match = _KEY_ALIAS.match(sql, after)
        column = alias_match.group(1).lower() if alias_match else ""
        keys.append(SurrogateKeyDef(column=column, inputs=inputs))
    return keys


def surrogate_key_inputs(sql: str) -> dict[str, tuple[tuple[str, ...], ...]]:
    """Map each key column in *sql* to the input tuple of every call binding it.

    A column is mapped to a tuple *of* input tuples rather than to one, so a
    model that mints the same alias in several union branches compares all of
    them — changing any one branch re-keys some of the rows, which is the same
    incident with a smaller blast radius.

    Unaliased calls are dropped: with no column name there is nothing
    downstream can have stored, and nothing to match the two versions of the
    model on.
    """
    grouped: dict[str, list[tuple[str, ...]]] = {}
    for key in extract_surrogate_keys(sql):
        if not key.column:
            continue
        grouped.setdefault(key.column, []).append(key.inputs)
    return {column: tuple(inputs) for column, inputs in grouped.items()}


def changed_surrogate_keys(base_sql: str, current_sql: str) -> list[SurrogateKeyChange]:
    """Key columns whose hash inputs differ between *base_sql* and *current_sql*.

    Only columns present on both sides are compared. A key column that appeared
    or disappeared is an ordinary column addition or removal — nothing
    downstream can hold a stale value of a column that did not exist, and a
    column that is gone breaks its consumers outright rather than silently, so
    both are already covered by ``ol-dbt impact``'s column diff.
    """
    base = surrogate_key_inputs(base_sql)
    current = surrogate_key_inputs(current_sql)
    return [
        SurrogateKeyChange(
            column=column,
            base_inputs=base[column],
            current_inputs=current[column],
        )
        for column in sorted(base.keys() & current.keys())
        if base[column] != current[column]
    ]


# ---------------------------------------------------------------------------
# Downstream tracing
# ---------------------------------------------------------------------------


def _carried_into(
    registry: ManifestRegistry,
    parent: ManifestModel,
    child: ManifestModel,
    carried: frozenset[str],
    column_reads: ColumnReadLookup,
) -> dict[str, str]:
    """Columns of *child* that hold a value derived from *carried* on *parent*.

    Returns ``{child_column: evidence}``. The keys are named in *child*'s own
    schema, so they are what propagates to the next hop — a fact table's
    ``discount_fk`` is the same stale value as ``dim_discount.discount_pk``
    under a different name, and only the child's spelling survives further
    downstream.
    """
    found: dict[str, str] = {}

    explained: set[str] = set()
    for fk in registry.foreign_keys_between(child.unique_id, parent.name):
        if fk.parent_column in carried:
            found[fk.child_column] = "relationships_test"
            explained.add(fk.parent_column)

    # A column the test already accounted for is not looked for again: the read
    # of `dim_discount.discount_pk` is how `tfact_order.discount_fk` gets its
    # value, so counting both would report one stale FK as two.
    remaining = carried - explained
    if not remaining:
        return found

    read = column_reads(child.name, parent.name)
    if read is not None:
        for column in remaining & read:
            found.setdefault(column, "column_read")
    else:
        # The SQL could not be analysed. The manifest's documented columns are
        # the weaker fallback: a same-named column on both sides usually is the
        # upstream one carried through, and over-selecting here costs a
        # redundant rebuild while under-selecting reproduces the incident.
        for column in remaining & child.column_names:
            found.setdefault(column, "column_metadata")

    return found


def affected_incremental_descendants(
    registry: ManifestRegistry,
    ancestor: ManifestModel,
    key_column: str,
    column_reads: ColumnReadLookup = _no_column_reads,
) -> list[AffectedIncrementalModel]:
    """Incremental models holding a value derived from *ancestor*.*key_column*.

    Walks the manifest's child map breadth-first, carrying the re-keyed value
    forward under whatever name each hop binds it to. Tracing does not stop at
    the first incremental model: full-refreshing that table corrects its own
    rows, but an incremental model downstream of *it* copied the stale FK too
    and needs the same treatment.

    A node is re-walked whenever a path reaches it carrying a column it has not
    been seen holding before, rather than being marked visited on first sight.
    Marking on sight makes the result depend on traversal order: where two
    paths converge, the first to arrive might carry nothing the child reads,
    and the child -- along with every incremental model below it -- would be
    struck off before the path that does carry the key is ever tried. Carried
    sets only grow and are bounded by the column names in the graph, so the
    worklist still terminates.
    """
    results: dict[tuple[str, str], AffectedIncrementalModel] = {}
    queue: deque[tuple[ManifestModel, frozenset[str], int]] = deque([(ancestor, frozenset({key_column}), 0)])
    carried_by: dict[str, frozenset[str]] = {ancestor.unique_id: frozenset({key_column})}

    while queue:
        parent, carried, depth = queue.popleft()
        for child in registry.get_model_children(parent.unique_id):
            child_carried = _carried_into(registry, parent, child, carried, column_reads)
            if not child_carried:
                continue

            known = carried_by.get(child.unique_id, frozenset())
            if child_carried.keys() <= known:
                continue  # nothing this path carries is new to the child

            if child.materialized == INCREMENTAL_MATERIALIZATION:
                for column, evidence in child_carried.items():
                    found = results.get((child.name, column))
                    if found is not None and _EVIDENCE_RANK.get(found.evidence, 9) <= _EVIDENCE_RANK.get(evidence, 9):
                        continue
                    results[(child.name, column)] = AffectedIncrementalModel(
                        model_name=child.name,
                        unique_id=child.unique_id,
                        fk_column=column,
                        evidence=evidence,
                        depth=min(depth + 1, found.depth if found else depth + 1),
                    )

            carried_by[child.unique_id] = known | child_carried.keys()
            queue.append((child, carried_by[child.unique_id], depth + 1))

    return sorted(
        results.values(),
        key=lambda m: (m.model_name, _EVIDENCE_RANK.get(m.evidence, 9), m.fk_column),
    )


def detect_key_regen(
    changed_keys: Mapping[str, Sequence[SurrogateKeyChange]],
    registry: ManifestRegistry,
    column_reads: ColumnReadLookup = _no_column_reads,
) -> list[KeyRegenFinding]:
    """Report every re-keyed full-refresh model that has incremental consumers.

    *changed_keys* maps a model name to the key columns whose hash inputs
    changed, as produced by :func:`changed_surrogate_keys`.

    A change is dropped when the model is not materialized ``table``/``view``
    (nothing re-derives the key on the next build) or when nothing incremental
    stores a value derived from it (nothing is left holding a stale copy).
    """
    findings: list[KeyRegenFinding] = []
    for model_name in sorted(changed_keys):
        model = registry.get_model(model_name)
        if model is None or model.materialized not in FULL_REFRESH_MATERIALIZATIONS:
            continue
        for change in changed_keys[model_name]:
            affected = affected_incremental_descendants(registry, model, change.column, column_reads)
            if not affected:
                continue
            findings.append(
                KeyRegenFinding(
                    ancestor=model.name,
                    ancestor_unique_id=model.unique_id,
                    changed_key_column=change.column,
                    base_inputs=[list(i) for i in change.base_inputs],
                    current_inputs=[list(i) for i in change.current_inputs],
                    affected_models=affected,
                )
            )
    return findings


# ---------------------------------------------------------------------------
# Manifest-level key state
# ---------------------------------------------------------------------------

SurrogateKeyState = dict[str, dict[str, list[list[str]]]]
"""``{model_name: {key_column: [[hashed_expression, ...], ...]}}``.

Plain JSON types on purpose: this is what gets written to S3 after a build and
read back before the next one, so it has to survive a round trip through
``json.dumps`` unchanged.
"""


def surrogate_key_state(manifest: Mapping[str, Any]) -> SurrogateKeyState:
    """Hash inputs of every key minted by a full-refresh model in *manifest*.

    Reads ``raw_code`` straight from the decoded manifest rather than from the
    checkout, so a deployment that only ships the parsed manifest can still
    compare its keys against the previous build's. Models with no surrogate key
    are omitted, which keeps the artifact small enough to read on every run.
    """
    state: SurrogateKeyState = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if (node.get("config") or {}).get("materialized") not in FULL_REFRESH_MATERIALIZATIONS:
            continue
        keys = surrogate_key_inputs(node.get("raw_code") or "")
        if keys:
            state[node["name"]] = {column: [list(inputs) for inputs in calls] for column, calls in keys.items()}
    return state


def changed_keys_from_state(
    previous: Mapping[str, Any], current: Mapping[str, Any]
) -> dict[str, list[SurrogateKeyChange]]:
    """Diff two :data:`SurrogateKeyState` snapshots into per-model key changes.

    Same comparison rule as :func:`changed_surrogate_keys`: only key columns
    present in both snapshots, since a column that appeared or disappeared is
    an ordinary schema change with its own, louder failure mode.

    A model absent from *previous* is skipped rather than treated as new — a
    snapshot predating this check, or one written before the model existed,
    should not full-refresh the whole downstream on the next build.
    """
    changed: dict[str, list[SurrogateKeyChange]] = {}
    for model_name, current_keys in current.items():
        previous_keys = previous.get(model_name)
        if not previous_keys:
            continue
        model_changes = [
            SurrogateKeyChange(
                column=column,
                base_inputs=tuple(tuple(call) for call in previous_keys[column]),
                current_inputs=tuple(tuple(call) for call in current_keys[column]),
            )
            for column in sorted(previous_keys.keys() & current_keys.keys())
            if previous_keys[column] != current_keys[column]
        ]
        if model_changes:
            changed[model_name] = model_changes
    return changed
