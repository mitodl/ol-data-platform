"""Write a unit's base factories from what its source app's Django models say.

``bin/django-model-dump.py`` runs in the app's environment and writes its
models' tables as JSON. :func:`scaffold` turns that, the unit's fixture file
and the inventory unit into the source of ``fixture_factories/<unit>/_generated.py``:
one base factory per fixture table, parents first. What a model says is used:

- the primary key counts on the table's sequence, and a key that is also a
  foreign key (wagtail's ``page_ptr_id``) copies its parent's;
- a foreign key to another fixture table reads its parent from ``class Params``
  (:func:`~ol_dbt_cli.lib.fixture_generation.parent`). A nullable one is NULL
  when ``generate`` passes no parent, and one to a table the fixture leaves out
  stays NULL;
- ``unique`` text comes from the sequence, ``choices`` cycle through the stored
  values, and any other nullable column is NULL in a share of rows;
- the field class and the column name pick a mimesis provider.

What a model cannot say is left unset with a comment, for the unit's
hand-written ``__init__.py``: JSON shapes (JSONField, StreamField), a
treebeard tree's ``path``, ``depth`` and ``numchild`` (placed by
:class:`~ol_dbt_cli.lib.fixture_trees.Tree`), a ``parent_id`` to the same table
(:func:`~ol_dbt_cli.lib.fixture_trees.attach`), a generic foreign key's object
id, and the rules between columns. Every run writes the same file from the
same inputs, formatted as ruff formats it.

Column names are matched without case: Airbyte lowercases them, so a model's
``CEUs`` is the fixture's ``ceus``.

A unit with no Django app (an S3 or API unit) has no dump.
:func:`scaffold_from_tests` drafts one from the fixture's column types and the
dbt tests of the staging models that read each table, and writes it the same
way. A test on a staging column maps back to the raw column the model passes
through it, renamed or cast and nothing more
(:func:`~ol_dbt_cli.lib.sql_parser.passthrough_columns_via_scope`):
``accepted_values`` gives choices, ``unique`` a sequence, ``relationships`` to
another table of the fixture a foreign key, and a compound unique test a
unique-together comment. A column no ``not_null`` test covers is NULL in a
share of rows. The tests of a derived column, and a relationship to a table the
fixture leaves out, are not mapped and are noted.
"""

from __future__ import annotations

import json
import keyword
import re
import textwrap
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ol_dbt_cli.lib.fixture_trees import TREEBEARD_STEPLEN
from ol_dbt_cli.lib.sql_parser import parse_model_file, passthrough_columns_via_scope, read_macro_sources
from ol_dbt_cli.lib.yaml_registry import build_yaml_registry

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.raw_fixtures import TableFixture, UnitFixture
    from ol_dbt_cli.lib.yaml_registry import DataTest

MODELS_FILE = "models.json"
GENERATED_FILE = "_generated.py"
FORMAT_VERSION = 1
# Rows per table at scale 1. Tens of rows give the dbt tests something to fail
# on; a hand-written subclass re-declares `@table` to change one.
SCAFFOLD_DEFAULT_COUNT = 10
# The line length ruff holds src/ol_dbt_cli to, so the written module needs no
# reformatting to pass it.
LINE_LIMIT = 120
_INDENT = "    "
_LOADER_PREFIXES = ("_airbyte_", "_dlt_")
_TREE_COLUMNS = frozenset({"path", "depth", "numchild", "url_path"})
_INTEGER_TYPES = frozenset(
    {
        "AutoField",
        "BigAutoField",
        "SmallAutoField",
        "IntegerField",
        "BigIntegerField",
        "SmallIntegerField",
        "PositiveIntegerField",
        "PositiveBigIntegerField",
        "PositiveSmallIntegerField",
    }
)
_TEXT_TYPES = frozenset({"CharField", "TextField", "SlugField", "EmailField", "URLField", "FilePathField"})
# Column-name rules, tried in order on a text column: a regex on the name and
# the provider it gets. `None` means url(). Whole words of the name, so
# `capacity` is not a city. A phone number or an IP address has no rule:
# mimesis's can be real, so value() refuses them.
_NAME_PROVIDERS: tuple[tuple[str, str | None], ...] = (
    (r"(^|_)email($|_)", "person.email"),
    (r"^first_name$", "person.first_name"),
    (r"^last_name$", "person.last_name"),
    (r"^(full|display|legal)_name$|(^|_)(person|instructor|author|contact|owner|user)_name$", "person.full_name"),
    (r"(^|_)name$", "text.title"),
    (r"(^|_)username$", "person.username"),
    (r"(^|_)(url|link)$", None),
    (r"(^|_)title$", "text.title"),
    (r"(^|_)slug$", "internet.slug"),
    (r"(^|_)city$", "address.city"),
    (r"(^|_)country(_code)?$", "address.country_code"),
    (r"(^|_)(state|region)$", "address.state"),
    (r"(^|_)(postal_code|zip_code|zip)$", "address.postal_code"),
    (r"(^|_)address(_line_?\d)?$", "address.address"),
    (r"(^|_)currency(_code)?$", "finance.currency_iso_code"),
    (r"(^|_)(uuid|guid)$", "cryptographic.uuid"),
    (r"(^|_)(description|summary|body|content|text|note|comment|message)$", "text.sentence"),
)
# Text field classes that say what they hold, whatever the column is named.
# `None` means url().
_FIELD_PROVIDERS: dict[str, str | None] = {
    "URLField": None,
    "EmailField": "person.email",
    "SlugField": "internet.slug",
}
# A CharField shorter than this gets a number: mimesis words run to 12 letters.
_SHORT_TEXT = 12
# Digits a unique text value has room for: a million rows, past 100x of tens.
_UNIQUE_DIGITS = 6
# Names the scaffolded module uses inside a class body. A column of one of
# these names would shadow it, so it is left for __init__.py.
_MODULE_NAMES = frozenset({"fg", "factory"})
# Dates fall in a fixed range: a provider's default range ends at the current
# year, so its rows would change with the date.
_DATE_RANGE = "start=2020, end=2025"
_DECIMAL = re.compile(r"^decimal\((\d+),(\d+)\)$")
# By fixture type: a column the model does not have, or a field class the
# scaffold has no provider for.
_FIXTURE_TYPE_DECLARATIONS = {
    "boolean": 'fg.value("development.boolean")',
    "int": 'fg.value("numeric.integer_number", start=0, end=1000)',
    "long": 'fg.value("numeric.integer_number", start=0, end=1000)',
    "float": 'fg.value("numeric.float_number", start=0, end=1000, precision=2)',
    "double": 'fg.value("numeric.float_number", start=0, end=1000, precision=2)',
    "date": f'fg.value("datetime.date", {_DATE_RANGE})',
    "timestamp": f'fg.value("datetime.datetime", {_DATE_RANGE})',
    "string": 'fg.value("text.word")',
}
# Under the dbt project: the models whose tests draft a unit with no dump.
STAGING_DIR = Path("models") / "staging"
# By fixture type: the field a dump would report for such a column, so a
# drafted column is drawn as a model's is.
_FIXTURE_TYPE_FIELDS = {
    "boolean": "BooleanField",
    "int": "IntegerField",
    "long": "BigIntegerField",
    "float": "FloatField",
    "double": "FloatField",
    "date": "DateField",
    "timestamp": "DateTimeField",
    "string": "CharField",
}
# The fields a drafted column can be whose unique values the writer draws.
_UNIQUE_TYPES = _INTEGER_TYPES | {"CharField"}
# A compound unique test, and the argument that lists its columns.
_UNIQUE_TOGETHER_TESTS = {
    "dbt_utils.unique_combination_of_columns": "combination_of_columns",
    "dbt_expectations.expect_compound_columns_to_be_unique": "column_list",
}
# A relationships test's `to:`.
_REF = re.compile(r"""^ref\(\s*['"]([^'"]+)['"]\s*\)$""")
_SOURCE = re.compile(r"""^source\(\s*['"][^'"]+['"]\s*,\s*['"]([^'"]+)['"]\s*\)$""")


@dataclass(frozen=True)
class Scaffold:
    """What :func:`scaffold` makes."""

    source: str
    """The text of ``_generated.py``."""
    models: dict[str, Any]
    """The dump, cut down to the unit's fixture tables, to commit next to it.

    From :func:`scaffold_from_tests`, the document drafted from the tests,
    which is not committed: the next run drafts it again.
    """
    notes: list[str] = field(default_factory=list)
    """What the fixture and the models disagree on, for the caller to print."""


@dataclass
class _Class:
    """One factory class as it is being written."""

    name: str
    raw_table: str
    docstring: list[str]
    comments: list[str] = field(default_factory=list)
    params: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)
    constants: list[tuple[str, list[str]]] = field(default_factory=list)


def trimmed_models(dump: Mapping[str, Any], fixture: UnitFixture, unit: Unit) -> dict[str, Any]:
    """Return ``dump`` holding only the tables the unit's fixture has, so the committed file stays small.

    A table added to the fixture later needs a new dump.

    :param dump: What ``bin/django-model-dump.py`` wrote, or an earlier trimmed file.
    :param fixture: The unit's parsed fixture file.
    :param unit: The inventory unit, for its table prefix.
    :returns: The same document with fewer tables.
    :rtype: dict[str, Any]
    """
    wanted = {_db_table(table.raw_table, unit) for table in fixture.tables}
    return {**dump, "tables": {name: body for name, body in dump["tables"].items() if name in wanted}}


def starter_package(unit: Unit) -> str:
    """Return an ``__init__.py`` for a unit with no package yet: its seed, and a ``generate`` that adds no rows.

    Without one the directory imports as an empty package, and ``ol-dbt
    fixtures load`` refuses it.
    """
    return f'''"""The factory package of the `{unit.key}` unit.

`_generated.py` holds the base factories `ol-dbt fixtures scaffold` wrote from
the app's models, or for a unit with no app from its staging models' tests. This
module subclasses them for what those cannot say and wires the tables together
in `generate` (see ol_dbt_cli.fixture_factories).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

SEED = {zlib.crc32(unit.key.encode())}


def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, list[dict[str, Any]]]:
    """Return no generated rows until the tables are wired together."""
    return {{}}
'''


def scaffold(fixture: UnitFixture, unit: Unit, models: Mapping[str, Any]) -> Scaffold:
    """Write the base factories of every table in ``fixture``.

    :param fixture: The unit's parsed fixture file.
    :param unit: The inventory unit, for its table prefix and primary keys.
    :param models: What ``bin/django-model-dump.py`` wrote.
    :returns: The module's source, the trimmed dump and the disagreements.
    :rtype: Scaffold
    :raises ValueError: If ``models`` is not a dump this module reads.
    """
    if models.get("format_version") != FORMAT_VERSION:
        msg = f"models has format_version {models.get('format_version')!r}; this scaffold reads {FORMAT_VERSION}"
        raise ValueError(msg)
    provenance = (
        f"From {models['app']} at {models['commit']}. Written by `ol-dbt fixtures scaffold` from models.json "
        "and rewritten wholesale on every run: change a factory by subclassing it in __init__.py."
    )
    return _scaffold(
        fixture, unit, models, f"Base factories of the `{unit.key}` unit, scaffolded from its models.", provenance, []
    )


def scaffold_from_tests(fixture: UnitFixture, unit: Unit, dbt_dir: Path) -> Scaffold:
    """Draft the base factories of a unit with no dump from its fixture and its staging models' tests.

    :param fixture: The unit's parsed fixture file.
    :param unit: The inventory unit, for its table prefix and primary keys.
    :param dbt_dir: The dbt project, whose staging models read the unit's tables.
    :returns: The module's source, the drafted document and what was not mapped.
    :rtype: Scaffold
    :raises ValueError: If no staging model reads a table of ``fixture``.
    """
    models, notes = _models_from_tests(fixture, unit, dbt_dir)
    provenance = (
        "From the column types of its fixture file and the dbt tests of the staging models that read each table, "
        "mapped back through renames and casts. Written by `ol-dbt fixtures scaffold`, which drafts from tests "
        "when the unit has no models.json, and rewritten wholesale on every run: change a factory by subclassing "
        "it in __init__.py."
    )
    return _scaffold(
        fixture,
        unit,
        models,
        f"Base factories of the `{unit.key}` unit, drafted from its staging models' tests.",
        provenance,
        notes,
    )


def _models_from_tests(fixture: UnitFixture, unit: Unit, dbt_dir: Path) -> tuple[dict[str, Any], list[str]]:
    """Return a document shaped as ``bin/django-model-dump.py`` writes, from the fixture and the staging tests.

    Every column is the field its fixture type would be, NULL in a share of
    rows unless the inventory makes it part of the primary key or a staging
    test says it is not null. A string column that a hand-written row holds a
    mapping or list in is JSON. No table has a model.

    :param fixture: The unit's parsed fixture file.
    :param unit: The inventory unit, for its table prefix and primary keys.
    :param dbt_dir: The dbt project, whose staging models read the unit's tables.
    :returns: The document, and a note for each test that was not mapped.
    :rtype: tuple[dict[str, Any], list[str]]
    :raises ValueError: If no staging model reads a table of ``fixture``.
    """
    tables = {table.raw_table: table for table in fixture.tables}
    staged = _staged_models(tables, dbt_dir)
    if not staged:
        msg = f"no staging model under {dbt_dir / STAGING_DIR} reads a table of {fixture.path}"
        raise ValueError(msg)
    keys = {entry["raw_table"]: {path[0].lower() for path in entry.get("primary_key") or []} for entry in unit.tables}
    columns = {
        raw_table: {
            column.lower(): _column_facts(table, column, key=column.lower() in keys.get(raw_table, set()))
            for column in table.columns
        }
        for raw_table, table in tables.items()
    }
    # A staging model that keeps one row of many (the newest load of a history
    # table) can test unique what the raw table repeats.
    repeated = {
        (raw_table, column.lower())
        for raw_table, table in tables.items()
        for column in table.columns
        if _repeats([row.get(column) for row in table.rows])
    }
    read = {raw_table for model in staged.values() for raw_table in model.reads}
    notes = [
        f"{raw_table}: no staging model reads it; every column is drawn by its fixture type"
        for raw_table in tables
        if raw_table not in read
    ]
    unique_together: dict[str, list[list[str]]] = {raw_table: [] for raw_table in tables}
    registry = build_yaml_registry(dbt_dir / STAGING_DIR)
    for model_name, model in staged.items():
        if not model.passed:
            notes.append(f"{model_name}: passes no column of {', '.join(sorted(model.reads))} through; no test maps")
            continue
        declared = registry.get_model(model_name)
        if declared is None:
            notes.append(f"{model_name}: in no schema YAML, so it has no tests to draw from")
            continue
        for column_name, column in declared.columns.items():
            raw = model.passed.get(column_name, [])
            for test in column.data_tests:
                notes += _map_test(test, f"{model_name}.{column_name}", raw, columns, repeated, staged, unit)
        for test in declared.data_tests:
            if test.name not in _UNIQUE_TOGETHER_TESTS:
                notes.append(f"{model_name}: {test.name} not mapped; the writer has no equivalent")
                continue
            listed = [name.lower() for name in test.arguments[_UNIQUE_TOGETHER_TESTS[test.name]]]
            raw_columns = [model.passed.get(name, []) for name in listed]
            if all(len(raw) == 1 for raw in raw_columns) and len({raw[0][0] for raw in raw_columns}) == 1:
                unique_together[raw_columns[0][0][0]].append([raw[0][1] for raw in raw_columns])
            else:
                notes.append(
                    f"{model_name}: {test.name} on {', '.join(listed)} not mapped; "
                    "its columns are not all passed through from one table"
                )
    document: dict[str, Any] = {"format_version": FORMAT_VERSION, "tables": {}}
    for raw_table in tables:
        body: dict[str, Any] = {"model": None, "columns": columns[raw_table]}
        if unique_together[raw_table]:
            body["unique_together"] = sorted(unique_together[raw_table])
        document["tables"][_db_table(raw_table, unit)] = body
    return document, notes


@dataclass(frozen=True)
class _Staged:
    """A staging model that reads fixture tables: which, and the columns it passes through."""

    reads: frozenset[str]
    passed: dict[str, list[tuple[str, str]]]
    """By the model's column, each ``(raw table, raw column)`` it passes through."""


def _staged_models(tables: Mapping[str, TableFixture], dbt_dir: Path) -> dict[str, _Staged]:
    """Return the staging models that ``source()`` a fixture table, by name, in path order."""
    macro_sources = read_macro_sources(dbt_dir)
    staged: dict[str, _Staged] = {}
    for path in sorted((dbt_dir / STAGING_DIR).rglob("*.sql")):
        # Parsing every staging model takes seconds; a model that reads a table names it.
        if not any(raw_table in path.read_text() for raw_table in tables):
            continue
        parsed = parse_model_file(path, macro_sources=macro_sources)
        upstream = {
            key: {column.lower() for column in tables[raw_table].columns}
            for key in parsed.source_refs
            if (raw_table := key.partition(".")[2]) in tables
        }
        if not upstream:
            continue
        staged[parsed.name] = _Staged(
            reads=frozenset(key.partition(".")[2] for key in upstream),
            # A column the fixture does not declare can still qualify to its table.
            passed={
                column: [
                    (key.partition(".")[2], raw_column) for key, raw_column in passed if raw_column in upstream[key]
                ]
                for column, passed in passthrough_columns_via_scope(parsed, upstream).items()
            },
        )
    return staged


def _column_facts(table: TableFixture, column: str, *, key: bool) -> dict[str, Any]:
    """Return what a dump would say of a column of ``table`` from its fixture type alone."""
    fixture_type = table.columns[column]
    if match := _DECIMAL.match(fixture_type):
        facts: dict[str, Any] = {"field": "DecimalField", "internal_type": "DecimalField"}
        facts["max_digits"], facts["decimal_places"] = int(match.group(1)), int(match.group(2))
    else:
        facts = {"field": _FIXTURE_TYPE_FIELDS[fixture_type], "internal_type": _FIXTURE_TYPE_FIELDS[fixture_type]}
    facts |= {"null": not key, "primary_key": key, "unique": key}
    if any(isinstance(row.get(column), dict | list) for row in table.rows):
        facts["json"] = True
    return facts


def _map_test(
    test: DataTest,
    where: str,
    raw: list[tuple[str, str]],
    columns: Mapping[str, Mapping[str, dict[str, Any]]],
    repeated: set[tuple[str, str]],
    staged: Mapping[str, _Staged],
    unit: Unit,
) -> list[str]:
    """Set what ``test`` on a staging column says on each raw column it passes, and return what was not mapped."""
    if test.name not in {"not_null", "unique", "accepted_values", "relationships"}:
        return [f"{where}: {test.name} not mapped; the writer has no equivalent"]
    if not raw:
        return [f"{where}: {test.name} not mapped; the column is derived, not a raw column renamed or cast"]
    targets = [columns[raw_table][raw_column] for raw_table, raw_column in raw]
    if test.name == "not_null":
        for facts in targets:
            facts["null"] = False
    elif test.name == "unique":
        # Each table counts on its own sequence, so the branches of a union
        # would repeat each other's values.
        if len(targets) > 1:
            return [f"{where}: unique not mapped; it unions {len(targets)} raw columns"]
        if raw[0] in repeated:
            return [f"{where}: unique not mapped; the fixture's rows repeat a value of {raw[0][0]}.{raw[0][1]}"]
        if targets[0]["internal_type"] not in _UNIQUE_TYPES:
            return [f"{where}: unique not mapped; the writer draws no distinct {targets[0]['internal_type']}"]
        targets[0]["unique"] = True
    elif test.name == "accepted_values":
        for facts in targets:
            # YAML reads an unquoted 2025 as a number; a string column holds its text.
            values = [str(value) if facts["field"] == "CharField" else value for value in test.arguments["values"]]
            facts["choices"] = [value for value in facts.get("choices", values) if value in values]
            if not facts["choices"]:
                del facts["choices"]
                return [f"{where}: accepted_values not mapped; another staging model accepts none of its values"]
    else:
        parent = _parent(str(test.arguments["to"]), str(test.arguments["field"]).lower(), columns, staged)
        if parent is None:
            return [
                f"{where}: relationships to {test.arguments['to']} not mapped; "
                f"its {test.arguments['field']} is not a column of a fixture table renamed or cast"
            ]
        for facts in targets:
            facts["foreign_key"] = {"table": _db_table(parent[0], unit), "column": parent[1]}
    return []


def _repeats(values: list[Any]) -> bool:
    present = [json.dumps(value, sort_keys=True, default=str) for value in values if value is not None]
    return len(set(present)) < len(present)


def _parent(
    to: str, field_name: str, columns: Mapping[str, Mapping[str, Any]], staged: Mapping[str, _Staged]
) -> tuple[str, str] | None:
    """Return the fixture table and column a relationships test's ``to`` and ``field`` name, if it is one."""
    if match := _REF.match(to.strip()):
        model = staged.get(match.group(1))
        passed = model.passed.get(field_name, []) if model else []
        return passed[0] if len(passed) == 1 else None
    if (match := _SOURCE.match(to.strip())) and field_name in columns.get(match.group(1), {}):
        return match.group(1), field_name
    return None


def _scaffold(
    fixture: UnitFixture,
    unit: Unit,
    models: Mapping[str, Any],
    summary: str,
    provenance: str,
    notes: list[str],
) -> Scaffold:
    tables = {name: _lowered(body) for name, body in models["tables"].items()}
    primary_keys = {entry["raw_table"]: entry.get("primary_key") or [] for entry in unit.tables}
    fixture_tables = {table.raw_table for table in fixture.tables}
    names = _class_names(fixture.tables, unit, tables)
    classes: list[_Class] = []
    alphabets: dict[str, str] = {}
    for table in _parents_first(fixture.tables, unit, tables):
        db_table = _db_table(table.raw_table, unit)
        model = tables.get(db_table)
        if model is None:
            notes.append(f"{table.raw_table}: no model has table {db_table}; every column is drawn by its fixture type")
            model = {"model": None, "columns": {}}
        elif tree := model.get("treebeard"):
            alphabets[table.raw_table] = tree["alphabet"]
            if tree["steplen"] != TREEBEARD_STEPLEN:
                notes.append(f"{table.raw_table}: steplen {tree['steplen']}, and Tree places {TREEBEARD_STEPLEN}")
        inventory_key = [path[0].lower() for path in primary_keys.get(table.raw_table, [])]
        model_key = [column for column, facts in model["columns"].items() if facts["primary_key"]]
        if model["model"] is not None and inventory_key != model_key:
            notes.append(
                f"{table.raw_table}: the inventory's primary key is {inventory_key}, the model's is {model_key}"
            )
        classes.append(_factory(table, db_table, model, names, unit, fixture_tables, inventory_key, notes))
    return Scaffold(
        source=_module(summary, provenance, classes, alphabets),
        models=trimmed_models(models, fixture, unit),
        notes=notes,
    )


def _lowered(model: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``model`` with every column name lowercased, as Airbyte lands them."""
    columns = {
        column.lower(): {
            **facts,
            **(
                {"foreign_key": {**facts["foreign_key"], "column": facts["foreign_key"]["column"].lower()}}
                if "foreign_key" in facts
                else {}
            ),
        }
        for column, facts in model["columns"].items()
    }
    lowered: dict[str, Any] = {**model, "columns": columns}
    if "unique_together" in model:
        lowered["unique_together"] = [[column.lower() for column in columns_] for columns_ in model["unique_together"]]
    if "generic_foreign_keys" in model:
        lowered["generic_foreign_keys"] = [
            {**entry, "content_type": entry["content_type"].lower(), "object_id": entry["object_id"].lower()}
            for entry in model["generic_foreign_keys"]
        ]
    return lowered


def _db_table(raw_table: str, unit: Unit) -> str:
    return raw_table.removeprefix(str(unit.data["table_prefix"]))


def _camel(name: str) -> str:
    return "".join(part[:1].upper() + part[1:] for part in re.split(r"[^0-9A-Za-z]+", name) if part)


def _class_names(fixture_tables: Iterable[TableFixture], unit: Unit, tables: Mapping[str, Any]) -> dict[str, str]:
    """Name each table's factory after its model, with the app label where two models share a name."""
    by_table: dict[str, tuple[str, str]] = {}
    for table in fixture_tables:
        db_table = _db_table(table.raw_table, unit)
        model = (tables.get(db_table) or {}).get("model")
        app, _, name = model.partition(".") if model else ("", "", db_table)
        by_table[db_table] = (app, name)
    counts: dict[str, int] = {}
    for _, name in by_table.values():
        counts[_camel(name)] = counts.get(_camel(name), 0) + 1
    return {
        db_table: f"{_camel(app) if counts[_camel(name)] > 1 else ''}{_camel(name)}Base"
        for db_table, (app, name) in by_table.items()
    }


def _parents_first(fixture_tables: list[TableFixture], unit: Unit, tables: Mapping[str, Any]) -> list[TableFixture]:
    """Order the tables so each comes after the fixture tables it references, the file's order otherwise."""
    by_db_table = {_db_table(table.raw_table, unit): table for table in fixture_tables}
    references = {
        db_table: {
            facts["foreign_key"]["table"]
            for facts in (tables.get(db_table) or {"columns": {}})["columns"].values()
            if "foreign_key" in facts and facts["foreign_key"]["table"] in by_db_table
        }
        - {db_table}
        for db_table in by_db_table
    }
    ordered: list[str] = []
    while len(ordered) < len(by_db_table):
        ready = [name for name in by_db_table if name not in ordered and references[name] <= set(ordered)]
        # A cycle: take the next table in file order and carry on.
        ordered.append(ready[0] if ready else next(name for name in by_db_table if name not in ordered))
    return [by_db_table[name] for name in ordered]


def _factory(
    table: TableFixture,
    db_table: str,
    model: Mapping[str, Any],
    names: Mapping[str, str],
    unit: Unit,
    fixture_tables: set[str],
    key_columns: list[str],
    notes: list[str],
) -> _Class:
    prefix = str(unit.data["table_prefix"])
    summary = f"Table {db_table}, from model {model['model']}." if model["model"] else f"Table {db_table}, no model."
    docstring = (
        [summary]
        if len(f'{_INDENT}"""{summary}"""') <= LINE_LIMIT
        else [f"Table {db_table}.", f"From model {model['model']}."]
    )
    written = _Class(names[db_table], table.raw_table, docstring)
    tree = "treebeard" in model
    generic = {entry["object_id"]: entry for entry in model.get("generic_foreign_keys", [])}
    for columns in model.get("unique_together", []):
        written.comments.append(f"Unique together: {', '.join(columns)}. generate() must keep them distinct.")
    if tree:
        written.comments.append(
            f"A treebeard tree: place rows with Tree(rows, alphabet=TREE_ALPHABETS[{names[db_table]}.raw_table])."
        )
    for column, fixture_type in table.columns.items():
        if column.startswith(_LOADER_PREFIXES):
            continue
        if keyword.iskeyword(column) or column in _MODULE_NAMES:
            written.comments.append(
                f"{column}: a Python keyword or a name this module uses, so it cannot be a class attribute; "
                "set it in generate()."
            )
            continue
        facts = model["columns"].get(column.lower())
        if facts is None:
            if model["model"] is not None:
                notes.append(f"{table.raw_table}.{column}: not a column of {model['model']}; drawn by its fixture type")
            if column.lower() in key_columns:
                key = "factory.Sequence(lambda n: n)" if fixture_type in {"int", "long"} else _unique_text(column, None)
                written.lines.append(f"{column} = {key}")
            else:
                written.lines.append(f"{column} = {_by_fixture_type(fixture_type)}")
            continue
        if tree and column.lower() in _TREE_COLUMNS:
            written.comments.append(f"{column}: set by Tree.add.")
            continue
        if column.lower() in generic:
            entry = generic[column.lower()]
            written.comments.append(
                f"{column}: the object id of generic foreign key {entry['name']}, "
                f"set in generate() with {entry['content_type']}."
            )
            continue
        declaration = _declaration(column, facts, fixture_type, db_table, prefix, fixture_tables, written)
        if declaration is not None:
            written.lines.append(f"{column} = {declaration}")
    return written


def _declaration(  # noqa: PLR0911
    column: str,
    facts: Mapping[str, Any],
    fixture_type: str,
    db_table: str,
    prefix: str,
    fixture_tables: set[str],
    written: _Class,
) -> str | None:
    """Return the factory_boy declaration of one column, or ``None`` to leave it unset with a comment."""
    if foreign_key := facts.get("foreign_key"):
        return _foreign_key(column, facts, foreign_key, db_table, prefix, fixture_tables, written)
    if facts.get("json"):
        written.comments.append(f"{column}: JSON whose shape the model does not say; set it in __init__.py.")
        return None
    internal_type = facts["internal_type"]
    if facts["primary_key"] and internal_type in _INTEGER_TYPES:
        return "factory.Sequence(lambda n: n)"
    # A unique column comes from the sequence, choices or not: cycling
    # through choices would repeat one.
    if facts["unique"] and internal_type in _INTEGER_TYPES:
        return _nullable("factory.Sequence(lambda n: n)", facts)
    if (facts["primary_key"] or facts["unique"]) and internal_type == "UUIDField":
        return _nullable('fg.value("cryptographic.uuid")', facts)
    if facts["primary_key"] or (facts["unique"] and internal_type in _TEXT_TYPES):
        if (unique := _unique_text(column, facts)) is None:
            written.comments.append(
                f"{column}: unique, and {facts['max_length']} characters hold too few distinct values to generate; "
                "set it in __init__.py."
            )
            return None
        return unique if facts["primary_key"] else _nullable(unique, facts)
    # A blank choice of None is the column's NULL, which or_null already makes.
    if choices := [choice for choice in facts.get("choices", []) if choice is not None]:
        return _choices(column, choices, facts, written)
    return _nullable(_provider(column, facts, fixture_type), facts)


def _foreign_key(
    column: str,
    facts: Mapping[str, Any],
    foreign_key: Mapping[str, Any],
    db_table: str,
    prefix: str,
    fixture_tables: set[str],
    written: _Class,
) -> str | None:
    target = foreign_key["table"]
    if target == db_table:
        unless = "NULL unless generate() sets it" if facts["null"] else "generate() sets it"
        written.comments.append(f"{column}: references this table; {unless} with attach().")
        return None
    if prefix + target not in fixture_tables and facts["null"]:
        written.comments.append(f"{column}: references {target}, which the fixture has no table for; NULL.")
        return None
    # A column without the _id suffix (a db_column) would be the param's own
    # name, and factory_boy leaves a param out of the row.
    param = column.lower().removesuffix("_id") if column.lower().endswith("_id") else f"{column.lower()}_row"
    if keyword.iskeyword(param):
        param = f"{param}_row"
    written.params.append(param)
    if facts.get("one_to_one"):
        written.comments.append(f"{column}: one-to-one with {target}; draw its parents with pair().")
    if prefix + target not in fixture_tables:
        written.comments.append(f"{column}: references {target}, which the fixture has no table for.")
    if facts["null"]:
        return f'fg.parent("{param}", "{foreign_key["column"]}", nullable=True)'
    return f'fg.parent("{param}", "{foreign_key["column"]}")'


def _nullable(declaration: str, facts: Mapping[str, Any]) -> str:
    return f"fg.or_null({declaration})" if facts["null"] else declaration


def _unique_text(column: str, facts: Mapping[str, Any] | None) -> str | None:
    """Return a sequence of distinct values that fit the column, or ``None`` when none would."""
    max_length = (facts or {}).get("max_length")
    if "email" in column.lower() or (facts or {}).get("field") == "EmailField":
        return 'factory.Sequence(lambda n: f"user{n}@{fg.RESERVED_DOMAIN}")'
    stem = column.lower().removesuffix("_id").replace("_", "-")
    if max_length is None or len(stem) + 1 + _UNIQUE_DIGITS <= max_length:
        return f'factory.Sequence(lambda n: f"{stem}-{{n}}")'
    if max_length >= _UNIQUE_DIGITS:
        return "factory.Sequence(str)"
    return None


def _by_fixture_type(fixture_type: str) -> str:
    if match := _DECIMAL.match(fixture_type):
        precision, scale = int(match.group(1)), int(match.group(2))
        return _decimal(precision, scale)
    return _FIXTURE_TYPE_DECLARATIONS[fixture_type]


def _decimal(max_digits: int, decimal_places: int) -> str:
    # Below the column's largest value, so rounding to its places cannot carry
    # into a digit it does not have.
    top = 10 ** min(max_digits - decimal_places, 6) - 1
    return f'fg.value("numeric.decimal_number", start=0, end={top})'


def _provider(column: str, facts: Mapping[str, Any], fixture_type: str) -> str:  # noqa: PLR0911
    internal_type = facts["internal_type"]
    if internal_type in _TEXT_TYPES:
        # By class first: URLField and EmailField report CharField as their
        # internal type, and a field's class says more than its name.
        if facts["field"] in _FIELD_PROVIDERS:
            provider = _FIELD_PROVIDERS[facts["field"]]
            return "fg.url()" if provider is None else f'fg.value("{provider}")'
        for pattern, provider in _NAME_PROVIDERS:
            if re.search(pattern, column.lower()):
                return "fg.url()" if provider is None else f'fg.value("{provider}")'
        if internal_type == "TextField":
            return 'fg.value("text.sentence")'
        # Too short for a word (min_weekly_hours is a CharField(5)): a number
        # that fits, which the string column holds as its text.
        if (facts.get("max_length") or _SHORT_TEXT) < _SHORT_TEXT:
            return f'fg.value("numeric.integer_number", start=0, end={10 ** min(facts["max_length"], 4) - 1})'
        return 'fg.value("text.word")'
    if internal_type in _INTEGER_TYPES:
        return 'fg.value("numeric.integer_number", start=0, end=1000)'
    if internal_type == "BooleanField":
        return 'fg.value("development.boolean")'
    if internal_type == "DecimalField":
        return _decimal(facts["max_digits"], facts["decimal_places"])
    if internal_type == "FloatField":
        return 'fg.value("numeric.float_number", start=0, end=1000, precision=2)'
    if internal_type == "DateTimeField":
        return f'fg.value("datetime.datetime", {_DATE_RANGE})'
    if internal_type == "DateField":
        return f'fg.value("datetime.date", {_DATE_RANGE})'
    if internal_type == "TimeField":
        return 'fg.value("datetime.time")'
    if internal_type == "UUIDField":
        return 'fg.value("cryptographic.uuid")'
    if internal_type in {"FileField", "ImageField"}:
        return 'fg.value("file.file_name")'
    # A field class with no rule here (a DurationField): what its column holds.
    return _by_fixture_type(fixture_type)


def _choices(column: str, choices: list[Any], facts: Mapping[str, Any], written: _Class) -> str:
    """Cycle through ``choices``, from a module constant when the list does not fit on the column's line."""
    # Double-quoted strings, as ruff formats them.
    items = [json.dumps(choice) if isinstance(choice, str) else repr(choice) for choice in choices]
    declaration = _nullable(f"factory.Iterator([{', '.join(items)}])", facts)
    if len(f"{_INDENT}{column} = {declaration}") <= LINE_LIMIT:
        return declaration
    model = re.sub(r"(?<!^)(?=[A-Z])", "_", written.name.removesuffix("Base"))
    constant = f"{model}_{column}_choices".upper()
    written.constants.append((constant, items))
    return _nullable(f"factory.Iterator({constant})", facts)


def _module(summary: str, provenance: str, classes: list[_Class], alphabets: dict[str, str]) -> str:
    body: list[str] = []
    for written in classes:
        body += ["", "", *_fit("", f'@fg.table("{written.raw_table}", default_count=SCAFFOLD_DEFAULT_COUNT)')]
        body.append(f"class {written.name}(fg.RowFactory):")
        if len(written.docstring) == 1:
            body.append(f'{_INDENT}"""{written.docstring[0]}"""')
        else:
            first, *rest = written.docstring
            body += [f'{_INDENT}"""{first}', "", *[f"{_INDENT}{line}" for line in rest], f'{_INDENT}"""']
        if written.comments:
            body.append("")
            body += [f"{_INDENT}# {line}" for comment in written.comments for line in _wrapped(comment, 6)]
        if written.params:
            body += ["", f"{_INDENT}class Params:"] + [f"{_INDENT * 2}{param} = None" for param in written.params]
        if written.lines:
            body.append("")
            body += [fitted for line in written.lines for fitted in _fit(_INDENT, line)]
    text = "\n".join(body)
    header = [
        f'"""{summary}',
        "",
        *_wrapped(provenance, 0),
        '"""',
        "",
        "from __future__ import annotations",
        "",
    ]
    if "factory." in text:
        header += ["import factory", ""]
    # Through the module, because a class body resolves a name to its own
    # attributes first: a column named `value` would shadow value() for the
    # columns after it.
    header.append("from ol_dbt_cli.lib import fixture_generation as fg")
    header += ["", f"SCAFFOLD_DEFAULT_COUNT = {SCAFFOLD_DEFAULT_COUNT}"]
    for written in classes:
        for constant, items in written.constants:
            header += ["", f"{constant} = [", *[f"{_INDENT}{item}," for item in items], "]"]
    if alphabets:
        class_names = {written.raw_table: written.name for written in classes}
        body += ["", "", "# Each tree's MP_Node.alphabet, for fixture_trees.Tree.", "TREE_ALPHABETS = {"]
        body += [
            f'{_INDENT}{class_names[raw_table]}.raw_table: "{alphabet}",'
            for raw_table, alphabet in sorted(alphabets.items())
        ]
        body.append("}")
    return "\n".join([*header, *body]) + "\n"


def _wrapped(comment: str, indent: int) -> list[str]:
    return textwrap.wrap(comment, LINE_LIMIT - indent, break_long_words=False, break_on_hyphens=False)


def _fit(indent: str, line: str) -> list[str]:
    """Return ``line`` at ``indent``, its outermost call's arguments one a line when it is too long, as ruff does.

    The trailing comma keeps ruff from joining them again.
    """
    if len(indent + line) <= LINE_LIMIT or not line.endswith(")") or "(" not in line:
        return [indent + line]
    head, _, rest = line.partition("(")
    arguments = _split_arguments(rest.removesuffix(")"))
    inner = [fitted for argument in arguments for fitted in _fit(indent + _INDENT, f"{argument},")]
    return [f"{indent}{head}(", *inner, f"{indent})"]


def _split_arguments(arguments: str) -> list[str]:
    """Split a call's arguments at the commas outside brackets and strings."""
    parts: list[str] = []
    depth, quote, start = 0, "", 0
    for position, character in enumerate(arguments):
        if quote:
            quote = "" if character == quote and arguments[position - 1] != "\\" else quote
        elif character in "\"'":
            quote = character
        elif character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        elif character == "," and depth == 0:
            parts.append(arguments[start:position].strip())
            start = position + 1
    parts.append(arguments[start:].strip())
    return [part for part in parts if part]
