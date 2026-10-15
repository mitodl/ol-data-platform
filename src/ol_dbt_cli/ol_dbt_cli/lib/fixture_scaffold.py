"""Write a unit's base factories from what its source app's Django models say.

``bin/django-model-dump.py`` runs in the app's environment and writes its
models' tables as JSON. :func:`scaffold` turns that, the unit's fixture file
and the inventory unit into the source of ``fixture_factories/<unit>/_generated.py``:
one base factory per fixture table, parents first. What a model says is used:

- the primary key counts on the table's sequence, and a key that is also a
  foreign key (wagtail's ``page_ptr_id``) copies its parent's;
- a foreign key to another fixture table reads its parent from ``class Params``
  (:func:`~ol_dbt_cli.lib.fixture_generation.parent`), and a nullable one to a
  table the fixture leaves out stays NULL;
- ``unique`` text comes from the sequence, ``choices`` cycle through the stored
  values, and a nullable column is NULL in a share of rows;
- the field class and the column name pick a mimesis provider.

What a model cannot say is left unset with a comment, for the unit's
hand-written ``__init__.py``: JSON shapes (JSONField, StreamField), a
treebeard tree's ``path``, ``depth`` and ``numchild`` (placed by
:class:`~ol_dbt_cli.lib.fixture_trees.Tree`), a ``parent_id`` to the same table
(:func:`~ol_dbt_cli.lib.fixture_trees.attach`), a generic foreign key's object
id, and the rules between columns. Every run writes the same file from the
same inputs.
"""

from __future__ import annotations

import json
import re
import textwrap
import zlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.raw_fixtures import TableFixture, UnitFixture

MODELS_FILE = "models.json"
GENERATED_FILE = "_generated.py"
# Rows per table at scale 1. Tens of rows give the dbt tests something to fail
# on; a hand-written subclass re-declares `@table` to change one.
SCAFFOLD_DEFAULT_COUNT = 10
# The line length ruff holds src/ol_dbt_cli to, so the written module needs no
# reformatting to pass it.
LINE_LIMIT = 120
_LOADER_PREFIXES = ("_airbyte_", "_dlt_")
_TREE_COLUMNS = frozenset({"path", "depth", "numchild"})
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
# the provider it gets. `None` means url().
_NAME_PROVIDERS: tuple[tuple[str, str | None], ...] = (
    (r"email", "person.email"),
    (r"^first_name$", "person.first_name"),
    (r"^last_name$", "person.last_name"),
    (r"^(full|display|legal)_name$", "person.full_name"),
    (r"(^|_)name$", "text.title"),
    (r"username", "person.username"),
    (r"phone", "person.telephone"),
    (r"url$|_url_|^url|link$", None),
    (r"title", "text.title"),
    (r"slug", "internet.slug"),
    (r"city", "address.city"),
    (r"country", "address.country_code"),
    (r"state|region", "address.state"),
    (r"postal|zip", "address.postal_code"),
    (r"address", "address.address"),
    (r"currency", "finance.currency_iso_code"),
    (r"uuid|guid", "cryptographic.uuid"),
    (r"description|summary|body|content|text|note|comment|message", "text.sentence"),
)
# A text column by field class when no name rule matches.
_TEXT_PROVIDERS = {"TextField": "text.sentence", "EmailField": "person.email", "SlugField": "internet.slug"}
# Dates fall in a fixed range: a provider's default range ends at the current
# year, so its rows would change with the date.
_DATE_RANGE = "start=2020, end=2025"
# A column the model does not have, by its fixture type.
_FIXTURE_TYPE_DECLARATIONS = {
    "boolean": 'value("development.boolean")',
    "int": 'value("numeric.integer_number", start=0, end=1000)',
    "long": 'value("numeric.integer_number", start=0, end=1000)',
    "float": 'value("numeric.float_number", start=0, end=1000, precision=2)',
    "double": 'value("numeric.float_number", start=0, end=1000, precision=2)',
    "date": f'value("datetime.date", {_DATE_RANGE})',
    "timestamp": f'value("datetime.datetime", {_DATE_RANGE})',
    "string": 'value("text.word")',
}


@dataclass(frozen=True)
class Scaffold:
    """What :func:`scaffold` makes."""

    source: str
    """The text of ``_generated.py``."""
    models: dict[str, Any]
    """The dump, cut down to the unit's fixture tables, to commit next to it."""
    notes: list[str] = field(default_factory=list)
    """What the fixture and the models disagree on, for the caller to print."""


@dataclass
class _Class:
    """One factory class as it is being written."""

    name: str
    raw_table: str
    docstring: str
    comments: list[str] = field(default_factory=list)
    params: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)
    constants: list[tuple[str, list[str]]] = field(default_factory=list)


def trimmed_models(dump: Mapping[str, Any], fixture: UnitFixture, unit: Unit) -> dict[str, Any]:
    """Return ``dump`` holding only the tables the unit's fixture has, so the committed file stays small.

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
the app's models. This module subclasses them for what the models cannot say
and wires the tables together in `generate` (see ol_dbt_cli.fixture_factories).
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
    if models.get("format_version") != 1:
        msg = f"models has format_version {models.get('format_version')!r}; this scaffold reads 1"
        raise ValueError(msg)
    tables = models["tables"]
    notes: list[str] = []
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
        inventory_key = [path[0] for path in primary_keys.get(table.raw_table, [])]
        model_key = [column for column, facts in model["columns"].items() if facts["primary_key"]]
        if model["model"] is not None and inventory_key != model_key:
            notes.append(
                f"{table.raw_table}: the inventory's primary key is {inventory_key}, the model's is {model_key}"
            )
        classes.append(_factory(table, db_table, model, names, unit, fixture_tables, inventory_key, notes))
    return Scaffold(
        source=_module(models, unit, classes, alphabets),
        models=trimmed_models(models, fixture, unit),
        notes=notes,
    )


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
    source = f"model {model['model']}" if model["model"] else "no model"
    written = _Class(names[db_table], table.raw_table, f"Table {db_table}, from {source}.")
    tree = "treebeard" in model
    generic = {entry["object_id"]: entry for entry in model.get("generic_foreign_keys", [])}
    for columns in model.get("unique_together", []):
        written.comments.append(f"Unique together: {', '.join(columns)}. generate() keeps them distinct.")
    if tree:
        written.comments.append(
            f"A treebeard tree: place rows with Tree(rows, alphabet=TREE_ALPHABETS[{names[db_table]}.raw_table])."
        )
    for column, fixture_type in table.columns.items():
        if column.startswith(_LOADER_PREFIXES):
            continue
        facts = model["columns"].get(column)
        if facts is None:
            if model["model"] is not None:
                notes.append(f"{table.raw_table}.{column}: not a column of {model['model']}; drawn by its fixture type")
            if column in key_columns:
                key = "factory.Sequence(lambda n: n)" if fixture_type in {"int", "long"} else _unique_text(column)
                written.lines.append(f"{column} = {key}")
            else:
                written.lines.append(f"{column} = {_FIXTURE_TYPE_DECLARATIONS[fixture_type]}")
            continue
        if tree and (column in _TREE_COLUMNS or column == "url_path"):
            written.comments.append(f"{column}: set by Tree.add.")
            continue
        if column in generic:
            written.comments.append(
                f"{column}: the object id of generic foreign key {generic[column]['name']}, "
                f"set in generate() with {generic[column]['content_type']}."
            )
            continue
        declaration = _declaration(column, facts, db_table, prefix, fixture_tables, written)
        if declaration is not None:
            written.lines.append(f"{column} = {declaration}")
    return written


def _declaration(  # noqa: PLR0911
    column: str,
    facts: Mapping[str, Any],
    db_table: str,
    prefix: str,
    fixture_tables: set[str],
    written: _Class,
) -> str | None:
    """Return the factory_boy declaration of one column, or ``None`` to leave it unset with a comment."""
    if foreign_key := facts.get("foreign_key"):
        target = foreign_key["table"]
        if target == db_table:
            unless = "NULL unless generate() sets it" if facts["null"] else "generate() sets it"
            written.comments.append(f"{column}: references this table; {unless} with attach().")
            return None
        if prefix + target not in fixture_tables and facts["null"]:
            written.comments.append(f"{column}: references {target}, which the fixture has no table for; NULL.")
            return None
        param = column.removesuffix("_id")
        written.params.append(param)
        if facts.get("one_to_one"):
            written.comments.append(f"{column}: one-to-one with {target}; draw its parents with pair().")
        if prefix + target not in fixture_tables:
            written.comments.append(f"{column}: references {target}, which the fixture has no table for.")
        return f'parent("{param}", "{foreign_key["column"]}")'
    if facts.get("json"):
        written.comments.append(f"{column}: JSON whose shape the model does not say; set it in __init__.py.")
        return None
    internal_type = facts["internal_type"]
    if facts["primary_key"] or (facts["unique"] and internal_type in _INTEGER_TYPES):
        return "factory.Sequence(lambda n: n)" if internal_type in _INTEGER_TYPES else _unique_text(column)
    # A blank choice of None is the column's NULL, which or_null already makes.
    if choices := [choice for choice in facts.get("choices", []) if choice is not None]:
        return _choices(column, choices, facts, written)
    if facts["unique"] and internal_type == "UUIDField":
        return 'value("cryptographic.uuid")'
    if facts["unique"] and internal_type in _TEXT_TYPES:
        return _unique_text(column)
    return _nullable(_provider(column, facts), facts)


def _nullable(declaration: str, facts: Mapping[str, Any]) -> str:
    return f"or_null({declaration})" if facts["null"] else declaration


def _unique_text(column: str) -> str:
    if "email" in column:
        return 'factory.Sequence(lambda n: f"user{n}@{RESERVED_DOMAIN}")'
    stem = column.removesuffix("_id").replace("_", "-")
    return f'factory.Sequence(lambda n: f"{stem}-{{n}}")'


def _provider(column: str, facts: Mapping[str, Any]) -> str:  # noqa: PLR0911
    internal_type = facts["internal_type"]
    if internal_type in _TEXT_TYPES:
        for pattern, provider in _NAME_PROVIDERS:
            if re.search(pattern, column):
                return "url()" if provider is None else f'value("{provider}")'
        if internal_type == "URLField":
            return "url()"
        return f'value("{_TEXT_PROVIDERS.get(internal_type, "text.word")}")'
    if internal_type in _INTEGER_TYPES:
        return 'value("numeric.integer_number", start=0, end=1000)'
    if internal_type == "BooleanField":
        return 'value("development.boolean")'
    if internal_type == "DecimalField":
        top = 10 ** min(facts["max_digits"] - facts["decimal_places"], 6) - 1
        return f'value("numeric.decimal_number", start=0, end={top})'
    if internal_type == "FloatField":
        return 'value("numeric.float_number", start=0, end=1000, precision=2)'
    if internal_type == "DateTimeField":
        return f'value("datetime.datetime", {_DATE_RANGE})'
    if internal_type == "DateField":
        return f'value("datetime.date", {_DATE_RANGE})'
    if internal_type == "TimeField":
        return 'value("datetime.time")'
    if internal_type == "UUIDField":
        return 'value("cryptographic.uuid")'
    if internal_type == "GenericIPAddressField":
        return 'value("internet.ip_v4")'
    if internal_type in {"FileField", "ImageField"}:
        return 'value("file.file_name")'
    return 'value("text.word")'


def _choices(column: str, choices: list[Any], facts: Mapping[str, Any], written: _Class) -> str:
    """Cycle through ``choices``, from a module constant when the list does not fit on the column's line."""
    # Double-quoted strings, as ruff formats them.
    items = [json.dumps(choice) if isinstance(choice, str) else repr(choice) for choice in choices]
    declaration = _nullable(f"factory.Iterator([{', '.join(items)}])", facts)
    if len(f"    {column} = {declaration}") <= LINE_LIMIT:
        return declaration
    model = re.sub(r"(?<!^)(?=[A-Z])", "_", written.name.removesuffix("Base"))
    constant = f"{model}_{column}_choices".upper()
    written.constants.append((constant, items))
    return _nullable(f"factory.Iterator({constant})", facts)


def _module(models: Mapping[str, Any], unit: Unit, classes: list[_Class], alphabets: dict[str, str]) -> str:
    body: list[str] = []
    for written in classes:
        body += ["", "", f'@table("{written.raw_table}", default_count=SCAFFOLD_DEFAULT_COUNT)']
        body.append(f"class {written.name}(RowFactory):")
        body.append(f'    """{written.docstring}"""')
        if written.comments:
            body.append("")
            body += [f"    # {line}" for comment in written.comments for line in _wrapped(comment, 6)]
        if written.params:
            body += ["", "    class Params:"] + [f"        {param} = None" for param in written.params]
        if written.lines:
            body.append("")
            body += [f"    {line}" for line in written.lines]
    text = "\n".join(body)
    runtime = sorted(
        name
        for name in ("RESERVED_DOMAIN", "RowFactory", "or_null", "parent", "table", "url", "value")
        if re.search(rf"\b{name}\b", text)
    )
    header = [
        f'"""Base factories of the `{unit.key}` unit, scaffolded from its models.',
        "",
        f"From {models['app']} at {models['commit']}. Written by `ol-dbt fixtures scaffold`",
        "from models.json and rewritten wholesale on every run: change a factory by",
        "subclassing it in __init__.py.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
    ]
    if "factory." in text:
        header += ["import factory", ""]
    header.append(f"from ol_dbt_cli.lib.fixture_generation import {', '.join(runtime)}")
    header += ["", f"SCAFFOLD_DEFAULT_COUNT = {SCAFFOLD_DEFAULT_COUNT}"]
    for written in classes:
        for constant, items in written.constants:
            header += ["", f"{constant} = [", *[f"    {item}," for item in items], "]"]
    if alphabets:
        class_names = {written.raw_table: written.name for written in classes}
        body += ["", "", "# Each tree's MP_Node.alphabet, for fixture_trees.Tree.", "TREE_ALPHABETS = {"]
        body += [
            f'    {class_names[raw_table]}.raw_table: "{alphabet}",'
            for raw_table, alphabet in sorted(alphabets.items())
        ]
        body.append("}")
    return "\n".join([*header, *body]) + "\n"


def _wrapped(comment: str, indent: int) -> list[str]:
    return textwrap.wrap(comment, LINE_LIMIT - indent, break_long_words=False, break_on_hyphens=False)
