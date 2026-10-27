"""What every unit's fixture factory package builds on, and the call that runs one.

A unit package (:mod:`ol_dbt_cli.fixture_factories`) declares one factory per
raw table and wires the tables together in ``generate``::

    @table("raw__xpro__app__postgres__courses_course", default_count=20)
    class Course(RowFactory):
        id = factory.Sequence(lambda n: n)
        readable_id = factory.Sequence(lambda n: f"course-v1:xPRO+C{n}")
        title = value("text.title")


    @table("raw__xpro__app__postgres__courses_courserun", default_count=40)
    class Run(RowFactory):
        class Params:
            course = None

        id = factory.Sequence(lambda n: n)
        course_id = parent("course", "id")


    def generate(hand_written, scale):
        courses = Course.build_batch(scaled_count(Course, scale))
        parents = pick(hand_written[Course.raw_table] + courses, scaled_count(Run, scale))
        return {
            Course.raw_table: courses,
            Run.raw_table: Run.build_batch(len(parents), course=factory.Iterator(parents)),
        }

factory_boy gives the structure and mimesis the values. Three things about the
pair shape this module:

- mimesis's own factory_boy plugin (``mimesis.plugins.factory``) builds an
  unseeded ``Field``, so its values differ between processes. :func:`value`
  reads the one ``Field`` this module seeds. Do not import the plugin.
- ``factory.SelfAttribute("course.id")`` reads attributes, and a row is a
  dict. :func:`parent` reads a column of a row held in ``class Params``.
- Every ``factory.DictFactory`` subclass counts its sequence on one shared
  counter. :func:`table` gives each raw table its own.

:func:`generate_rows` seeds all of it from the package's ``SEED``, so one
commit generates the same rows in every process. Three things would break that
and are refused: a mimesis provider whose default range ends at the current
year, a key taken from a provider instead of the sequence (``email(unique=
True)`` raises under a seed), and a ``factory.Faker`` column, whose values come
from faker, a second source none of :func:`value`'s checks see.
"""

from __future__ import annotations

import copy
import datetime
import math
import random
import re
import string
import struct
import types
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import factory
import factory.random
from factory.declarations import BaseDeclaration
from mimesis import Field, Locale

from ol_dbt_cli.lib.raw_fixtures import coerce_value, stored_key, validate_generated

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.raw_fixtures import TableFixture, UnitFactories, UnitFixture

type Row = dict[str, Any]

# RFC 2606 reserves both. example.com is IANA's and no user's, so mail a local
# app sends to a generated address reaches nobody. It does resolve, so URLs go
# under .test, where nothing does.
RESERVED_DOMAIN = "example.com"
RESERVED_URL_DOMAIN = "example.test"
# What share of rows `or_null` leaves NULL: enough that a model's NULL branch
# has rows at tens of rows per table.
NULL_SHARE = 0.2
# The day the committed fixtures' `_dlt_load_id` values fall on.
LOADED_AT = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
_DLT_ID_ALPHABET = string.ascii_letters + string.digits
_DLT_ID_LENGTH = 14
_INTEGER_TYPES = frozenset({"int", "long"})

# mimesis methods whose values can be a real host's or subscriber's, and why.
# RFC 5737 and RFC 3849 reserve IP ranges for documentation, and the NANP
# reserves 555-0100 to 555-0199 for fiction. mimesis keeps to neither.
_REAL_HOSTNAME = "makes up a hostname under a real top-level domain; use url()"
_REAL_IP = "makes up IP addresses outside the ranges reserved for documentation, so one can be a real host's"
_REAL_PHONE = "makes up phone numbers outside 555-0100 to 555-0199, so one can be a real subscriber's"
_REAL_ADDRESS_METHODS: dict[str, str] = {
    # http_request_headers holds a Host and a Referer, and http_response_headers
    # can set a cookie's Domain.
    **dict.fromkeys(("url", "uri", "hostname", "dsn", "http_request_headers", "http_response_headers"), _REAL_HOSTNAME),
    **dict.fromkeys(
        (
            "ip_v4",
            "ip_v4_object",
            "ip_v4_with_port",
            "ip_v4_cidr",
            "ip_v6",
            "ip_v6_object",
            "ip_v6_cidr",
            "special_ip_v4",
            "special_ip_v4_object",
        ),
        _REAL_IP,
    ),
    "public_dns": "is the address of a real public DNS resolver",
    "user_agent": "includes crawlers' user agents, which name their real pages",
    **dict.fromkeys(("phone_number", "telephone"), _REAL_PHONE),
}
# mimesis methods whose default range ends at the current year, and the
# arguments that pin it.
_CLOCK_BOUNDS: dict[str, tuple[str, str]] = {
    "date": ("start", "end"),
    "datetime": ("start", "end"),
    "timestamp": ("start", "end"),
    "week_date": ("start", "end"),
    "formatted_date": ("start", "end"),
    "formatted_datetime": ("start", "end"),
    "year": ("minimum", "maximum"),
}
# mimesis methods that count from today, with no argument that pins them.
_CLOCK_METHODS = frozenset({"past_date", "past_datetime", "future_date", "future_datetime", "calver"})
# The factory each raw table was first declared with. A unit's tables are
# declared as its package is imported.
_TABLE_FACTORIES: dict[str, type[RowFactory]] = {}


@dataclass
class _Generation:
    field: Field
    random: random.Random


_generation: _Generation | None = None


def _current() -> _Generation:
    if _generation is None:
        msg = "nothing is seeded; call seed_generation, or generate_rows, before building rows"
        raise RuntimeError(msg)
    return _generation


class RowFactory(factory.BaseDictFactory):
    """Base of every raw-table factory. Builds a row as a ``dict``.

    Declare the table with :func:`table`. A subclass of a declared factory (a
    unit's ``__init__.py`` overriding a scaffolded one) fills the same table
    and counts on the same sequence. A factory that is not one cannot fill it.
    """

    raw_table: str
    default_count: int

    class Meta:
        abstract = True


def table[FactoryT: type[RowFactory]](raw_table: str, default_count: int) -> Callable[[FactoryT], FactoryT]:
    """Declare the raw table a factory fills and its row count at scale 1.

    :param raw_table: The table's name in the unit's fixture file.
    :param default_count: Rows to generate at scale 1.
    :returns: A class decorator.
    :rtype: Callable[[FactoryT], FactoryT]
    :raises ValueError: If ``raw_table`` is empty or ``default_count`` is not a
        positive integer; or, from the decorator, if the factory fills another
        table, another factory it does not subclass fills this one, or a
        column is a ``factory.Faker``.
    """
    if not raw_table:
        msg = "a factory's raw_table is the name of the table it fills, not an empty string"
        raise ValueError(msg)
    if isinstance(default_count, bool) or not isinstance(default_count, int) or default_count < 1:
        msg = f"{raw_table}: default_count is {default_count!r}, expected a positive integer"
        raise ValueError(msg)

    def declare(cls: FactoryT) -> FactoryT:
        if getattr(cls, "raw_table", raw_table) != raw_table:
            msg = (
                f"{cls.__name__} subclasses the factory of {cls.raw_table} and would count on its sequence; "
                f"a factory of {raw_table} subclasses RowFactory"
            )
            raise ValueError(msg)
        # Two factories of a table, neither a subclass of the other, would each
        # count its keys from the same start, so the second's repeat the
        # first's. A module imported again declares its factories again.
        first = _TABLE_FACTORIES.get(raw_table)
        redeclared = first is not None and (first.__module__, first.__qualname__) == (cls.__module__, cls.__qualname__)
        if first is not None and not redeclared and not issubclass(cls, first):
            msg = (
                f"{_name(cls)} and {_name(first)} both fill {raw_table} and would count its keys from the same "
                f"start; to override {first.__name__}, subclass it"
            )
            raise ValueError(msg)
        for column, declaration in cls._meta.declarations.items():  # noqa: SLF001
            if any(isinstance(part, factory.Faker) for part in _parts(declaration)):
                msg = (
                    f"{_name(cls)}.{column} is a factory.Faker, whose values come from faker, not the seeded "
                    f"mimesis Field, and pass none of value()'s checks; use value()"
                )
                raise ValueError(msg)
        # factory_boy shares a sequence counter between a factory and its base
        # when both have a model, and reads every public class attribute as a
        # column. So the model is set on a subclass made here, one per table,
        # and the two attributes are set after the class exists.
        declared = types.new_class(
            cls.__name__,
            (cls,),
            exec_body=lambda namespace: namespace.update(
                Meta=type("Meta", (), {"model": dict}),
                __module__=cls.__module__,
                __qualname__=cls.__qualname__,
                __doc__=cls.__doc__,
            ),
        )
        declared.raw_table = raw_table  # type: ignore[attr-defined]
        declared.default_count = default_count  # type: ignore[attr-defined]
        if first is None or redeclared:
            _TABLE_FACTORIES[raw_table] = declared
        return cast("FactoryT", declared)

    return declare


def _name(cls: type[Any]) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def scaled_count(row_factory: type[RowFactory], scale: float) -> int:
    """Return how many rows ``row_factory`` makes at ``scale``, never fewer than one."""
    return max(1, round(row_factory.default_count * scale))


def draw(provider: str, **kwargs: Any) -> Any:
    """Return one value from a mimesis provider, on the seeded ``Field``.

    For use inside ``factory.LazyAttribute`` or ``generate``. A column that is
    only the provider's value is shorter as :func:`value`.

    :param provider: A mimesis field name, e.g. ``"text.title"``.
    :param kwargs: The provider's arguments.
    :raises ValueError: See :func:`value`.
    """
    kwargs = _checked(provider, kwargs)
    return _current().field(provider, **kwargs)


def value(provider: str, **kwargs: Any) -> Any:
    """Declare a column whose value comes from a mimesis provider.

    An email is on :data:`RESERVED_DOMAIN` unless ``domains`` is given. It is
    not unique: a unique address comes from the sequence, e.g.
    ``factory.Sequence(lambda n: f"user{n}@{RESERVED_DOMAIN}")``.

    :param provider: A mimesis field name, e.g. ``"text.title"``.
    :param kwargs: The provider's arguments.
    :returns: A factory_boy declaration.
    :raises ValueError: For a provider that makes up a hostname (use
        :func:`url`), an IP address or a phone number that can be real, or one
        whose default range ends at the current year and is not given its
        bounds.
    """
    kwargs = _checked(provider, kwargs)
    return factory.LazyFunction(lambda: _current().field(provider, **kwargs))


def _checked(provider: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    # mimesis takes other separators than a dot between provider and method.
    method = re.split(r"\W", provider)[-1]
    if method in _REAL_ADDRESS_METHODS:
        msg = f"{provider} {_REAL_ADDRESS_METHODS[method]}"
        raise ValueError(msg)
    if method in _CLOCK_METHODS:
        msg = f"{provider} counts from today, so its rows change with the date"
        raise ValueError(msg)
    if missing := [bound for bound in _CLOCK_BOUNDS.get(method, ()) if bound not in kwargs]:
        msg = f"{provider} defaults {' and '.join(missing)} to the current year, so its rows change with the date"
        raise ValueError(msg)
    if method == "email" and kwargs.get("domains") is None:
        return {**kwargs, "domains": [RESERVED_DOMAIN]}
    return kwargs


def url(domain: str = RESERVED_URL_DOMAIN) -> Any:
    """Declare a column holding an ``https`` URL under ``domain``."""
    return factory.LazyFunction(lambda: f"https://{domain}/{_current().field('internet.slug')}")


def or_null(declaration: Any, share: float = NULL_SHARE) -> Any:
    """Declare a nullable column: NULL in ``share`` of rows, drawn with the seeded RNG, ``declaration`` in the rest."""
    return factory.Maybe(
        factory.LazyFunction(lambda: _current().random.random() < share),
        yes_declaration=None,
        no_declaration=declaration,
    )


def parent(param: str, column: str, *, nullable: bool = False) -> Any:
    """Declare a column that copies ``column`` of the row held in ``class Params`` as ``param``.

    :param param: The ``Params`` name ``generate`` passes the parent row as.
    :param column: The parent's column the foreign key references.
    :param nullable: The foreign key is nullable, so a row built with no
        parent is NULL here. Otherwise building it raises.
    """
    if nullable:
        return factory.LazyAttribute(lambda row: None if getattr(row, param) is None else getattr(row, param)[column])
    return factory.LazyAttribute(lambda row: getattr(row, param)[column])


def pick[T](rows: Sequence[T], count: int) -> list[T]:
    """Draw ``count`` of ``rows``, repeats allowed: the parents of a many-to-one."""
    return _current().random.choices(rows, k=count)


def pair[T](rows: Sequence[T], count: int) -> list[T]:
    """Draw ``count`` of ``rows``, none twice: the parents of a one-to-one.

    :raises ValueError: If ``count`` is more than there are rows.
    """
    if count > len(rows):
        msg = f"cannot pair {count} rows one-to-one with {len(rows)}"
        raise ValueError(msg)
    return _current().random.sample(rows, count)


def link[L, R](left: Sequence[L], right: Sequence[R], count: int) -> list[tuple[L, R]]:
    """Draw ``count`` pairs of one ``left`` and one ``right`` row, none twice: a many-to-many link table.

    :raises ValueError: If ``count`` is more than there are pairs.
    """
    pairs = len(left) * len(right)
    if count > pairs:
        msg = f"cannot make {count} distinct pairs from {len(left)} and {len(right)} rows"
        raise ValueError(msg)
    return [
        (left[number // len(right)], right[number % len(right)])
        for number in _current().random.sample(range(pairs), count)
    ]


def seed_generation(seed: int, sequence_starts: Mapping[str, int] | None = None) -> None:
    """Seed every source of generated values and restart every sequence.

    :param seed: The unit package's ``SEED``.
    :param sequence_starts: The first sequence number of each raw table. A
        table left out starts at 1.
    """
    global _generation  # noqa: PLW0603
    factory.random.reseed_random(seed)
    _generation = _Generation(field=Field(Locale.EN, seed=seed), random=random.Random(seed))  # noqa: S311
    starts = sequence_starts or {}
    # Every factory, not only the tables': a JSON column's factory.DictFactory
    # counts and iterates too.
    for row_factory in _subclasses(factory.Factory):
        for declaration in row_factory._meta.declarations.values():  # noqa: SLF001
            for part in _parts(declaration):
                # An iterator carries its position from one build to the next.
                if isinstance(part, factory.Iterator):
                    part.reset()
        raw_table = getattr(row_factory, "raw_table", None)
        row_factory.reset_sequence(starts.get(raw_table, 1) if raw_table else 0, force=True)


def _subclasses(base: type[Any]) -> Iterator[Any]:
    for subclass in base.__subclasses__():
        yield subclass
        yield from _subclasses(subclass)


def _parts(declaration: Any) -> Iterator[Any]:
    """Yield ``declaration`` and every declaration it holds.

    A ``factory.Dict``, ``List`` or ``Maybe`` holds its parts in attributes of
    its own. A ``factory.SubFactory`` holds its factory, which is not walked.
    """
    yield declaration
    if not isinstance(declaration, BaseDeclaration):
        return
    for held in vars(declaration).values():
        for part in held.values() if isinstance(held, dict) else [held]:
            yield from _parts(part)


@dataclass(frozen=True)
class GeneratedRows:
    """What :func:`generate_rows` returns: each raw table's rows to load, in order."""

    #: The fixture file's rows of every table, as ``generate`` left them. A
    #: tree placement changes a hand-written parent's ``numchild`` here.
    hand_written: dict[str, list[Row]]
    #: The rows the package generated, by raw table.
    generated: dict[str, list[Row]]


def generate_rows(
    fixture: UnitFixture, unit: Unit, factories: UnitFactories, scale: float = 1.0, seed: int | None = None
) -> GeneratedRows:
    """Generate a unit's rows: seed, run its factory package, finish and check the rows.

    Each row comes back as a hand-written row is held, so it loads the same
    way: every value is what its column's fixture type holds (a mapping or a
    list in a ``string`` column is left for the loader to write as JSON), and
    the loader's own columns are filled where the factory left them out.

    ``generate`` is given copies of the hand-written rows, so the parsed
    fixture is never changed. It may change a value in one (a treebeard
    parent's ``numchild``), and not its primary key or how many there are.

    :param fixture: The unit's parsed fixture file.
    :param unit: The inventory unit.
    :param factories: The unit's factory package.
    :param scale: Multiplies every factory's ``default_count``.
    :param seed: Seeds in place of the package's ``SEED``.
    :returns: The hand-written rows as ``generate`` left them, and the generated rows.
    :rtype: GeneratedRows
    :raises ValueError: If ``scale`` is not positive, a value cannot be held by
        its column, ``generate`` added, removed or re-keyed a hand-written row,
        the generated rows fail :func:`validate_generated`, or a treebeard
        table's hand-written rows fail
        :func:`~ol_dbt_cli.lib.fixture_trees.check_tree_rows` or its rows fail
        :func:`~ol_dbt_cli.lib.fixture_trees.check_tree`.
    """
    if not (math.isfinite(scale) and scale > 0):
        msg = f"scale is {scale!r}, expected a positive number"
        raise ValueError(msg)
    key_columns = _key_columns(unit)
    seed_generation(factories.SEED if seed is None else seed, _sequence_starts(fixture, key_columns))
    hand_written = {table.raw_table: copy.deepcopy(table.rows) for table in fixture.tables}
    # fixture_trees imports this module for its draws.
    from ol_dbt_cli.lib.fixture_trees import check_tree, check_tree_rows  # noqa: PLC0415

    # A package with no treebeard table has neither: the scaffold writes
    # TREE_ALPHABETS only for one with a tree, and a tree with no parent left
    # out needs no stand-ins.
    tree_alphabets = getattr(factories, "TREE_ALPHABETS", {})
    stand_ins = getattr(factories, "TREE_STAND_INS", {})
    for raw_table, alphabet in tree_alphabets.items():
        check_tree_rows(raw_table, hand_written[raw_table], key_columns=key_columns[raw_table], alphabet=alphabet)
    generated = factories.generate(types.MappingProxyType(hand_written), scale)
    tables = {table.raw_table: table for table in fixture.tables}
    for table in fixture.tables:
        _check_hand_written(
            table, hand_written[table.raw_table], key_columns.get(table.raw_table, []), f"{unit.key} factories"
        )
    # A table or column the fixture file does not have is left as it is, for
    # the validator to report.
    finished = {
        raw_table: [
            _finished(row, tables[raw_table], f"{unit.key} factories: {raw_table} generated row {number}")
            for number, row in enumerate(rows, start=1)
        ]
        if raw_table in tables
        else rows
        for raw_table, rows in generated.items()
    }
    validate_generated(fixture, unit, finished)
    for raw_table, alphabet in tree_alphabets.items():
        check_tree(
            raw_table,
            hand_written[raw_table],
            finished.get(raw_table, []),
            key_columns=key_columns[raw_table],
            alphabet=alphabet,
            stand_ins=stand_ins.get(raw_table, []),
        )
    return GeneratedRows(hand_written=hand_written, generated=finished)


def _key_columns(unit: Unit) -> dict[str, list[str]]:
    return {
        table["raw_table"]: [path[0] for path in table["primary_key"]]
        for table in unit.tables
        if table.get("primary_key")
    }


def _check_hand_written(table: TableFixture, rows: list[Row], key_columns: list[str], where: str) -> None:
    """Hold the hand-written rows ``generate`` changed to what the validator assumed of them.

    :func:`validate_generated` compares generated keys with the file's, so a
    hand-written row keeps its key and stays a row of the file.
    """
    if len(rows) != len(table.rows):
        msg = (
            f"{where}: generate left {len(rows)} hand-written rows of {table.raw_table}, not the file's "
            f"{len(table.rows)}; return generated rows, do not add them to hand_written"
        )
        raise ValueError(msg)
    # A key column the file does not declare is validate_generated's to report,
    # and generate cannot set it without failing the unknown-column check.
    key_columns = [column for column in key_columns if column in table.columns]
    for number, (before, after) in enumerate(zip(table.rows, rows, strict=True), start=1):
        label = f"{where}: {table.raw_table} hand-written row {number}"
        if unknown := sorted(set(after) - set(table.columns)):
            msg = f"{label} sets {', '.join(unknown)}, which the table does not have"
            raise ValueError(msg)
        # As the table holds them: 1 and 1.0 are equal, and '1' and '1.0' in a string key are not.
        if rekeyed := [
            column
            for column, was, now in zip(
                key_columns,
                stored_key(table, before, key_columns, label),
                stored_key(table, after, key_columns, label),
                strict=True,
            )
            if was != now
        ]:
            msg = f"{label}: generate changed primary key column {', '.join(rekeyed)}"
            raise ValueError(msg)
        for column, changed in after.items():
            original = before.get(column)
            # 0.0 equals 0 and is still not what a long column holds.
            if changed is not None and (changed != original or type(changed) is not type(original)):
                after[column] = _coerced(changed, table.columns[column], label, column)


def _sequence_starts(fixture: UnitFixture, key_columns: Mapping[str, list[str]]) -> dict[str, int]:
    """Return each table's first sequence number: one past the largest hand-written key in an integer column."""
    return {
        table.raw_table: 1
        + max(
            (
                # As the column holds it: a quoted "7" in a long is 7.
                coerce_value(row[column], table.columns[column])
                for row in table.rows
                for column in key_columns.get(table.raw_table, [])
                if table.columns.get(column) in _INTEGER_TYPES and row.get(column) is not None
            ),
            default=0,
        )
        for table in fixture.tables
    }


def _finished(row: Row, table: TableFixture, label: str) -> Row:
    finished = {**_loader_columns(table), **row}
    for column, fixture_type in table.columns.items():
        if finished.get(column) is not None:
            finished[column] = _coerced(finished[column], fixture_type, label, column)
    return finished


def _coerced(value: Any, fixture_type: str, label: str, column: str) -> Any:
    try:
        return coerce_value(value, fixture_type)
    except (ValueError, TypeError, ArithmeticError, struct.error) as error:
        msg = f"{label} has {value!r} in column {column}, which is not a valid {fixture_type}"
        raise ValueError(msg) from error


def _loader_columns(table: TableFixture) -> Row:
    """Return what Airbyte or dlt would have stamped on a row of ``table``."""
    rng = _current().random
    stamps: dict[str, Callable[[], Any]] = {
        "_airbyte_raw_id": lambda: str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "_airbyte_extracted_at": lambda: int(LOADED_AT.timestamp() * 1000),
        "_airbyte_generation_id": lambda: 0,
        "_dlt_load_id": lambda: f"{LOADED_AT.timestamp():.6f}",
        "_dlt_id": lambda: "".join(rng.choices(_DLT_ID_ALPHABET, k=_DLT_ID_LENGTH)),
    }
    return {column: stamp() for column, stamp in stamps.items() if column in table.columns}
