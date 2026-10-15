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
commit generates the same rows in every process. Two things would break that
and are refused: a mimesis provider whose default range ends at the current
year, and a key taken from a provider instead of the sequence (``email(unique=
True)`` raises under a seed).
"""

from __future__ import annotations

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

from ol_dbt_cli.lib.raw_fixtures import coerce_value, validate_generated

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
# The day the committed fixtures' `_dlt_load_id` values fall on.
LOADED_AT = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
_DLT_ID_ALPHABET = string.ascii_letters + string.digits
_DLT_ID_LENGTH = 14
_INTEGER_TYPES = frozenset({"int", "long"})

# mimesis methods that make up a hostname with a real top-level domain.
_REAL_ADDRESS_METHODS = frozenset({"url", "uri", "hostname", "dsn"})
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
    and counts on the same sequence.
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
        positive integer.
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
        return cast("FactoryT", declared)

    return declare


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
    :raises ValueError: For a provider that makes up a real hostname (use
        :func:`url`), or one whose default range ends at the current year and
        is not given its bounds.
    """
    kwargs = _checked(provider, kwargs)
    return factory.LazyFunction(lambda: _current().field(provider, **kwargs))


def _checked(provider: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    # mimesis takes other separators than a dot between provider and method.
    method = re.split(r"\W", provider)[-1]
    if method in _REAL_ADDRESS_METHODS:
        msg = f"{provider} makes up a hostname under a real top-level domain; use url()"
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


def parent(param: str, column: str) -> Any:
    """Declare a column that copies ``column`` of the row held in ``class Params`` as ``param``."""
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
            _reset_iterators(declaration)
        raw_table = getattr(row_factory, "raw_table", None)
        row_factory.reset_sequence(starts.get(raw_table, 1) if raw_table else 0, force=True)


def _subclasses(base: type[Any]) -> Iterator[Any]:
    for subclass in base.__subclasses__():
        yield subclass
        yield from _subclasses(subclass)


def _reset_iterators(declaration: Any) -> None:
    """Restart ``declaration`` and every ``factory.Iterator`` it holds.

    An iterator carries its position from one build to the next. It can sit
    inside a ``factory.Dict``, ``List`` or ``Maybe``, which hold their parts in
    attributes of their own.
    """
    if isinstance(declaration, factory.Iterator):
        declaration.reset()
    if not isinstance(declaration, BaseDeclaration):
        return
    for held in vars(declaration).values():
        for part in held.values() if isinstance(held, dict) else [held]:
            _reset_iterators(part)


def generate_rows(
    fixture: UnitFixture, unit: Unit, factories: UnitFactories, scale: float = 1.0
) -> dict[str, list[Row]]:
    """Generate a unit's rows: seed, run its factory package, finish and check the rows.

    Each row comes back as a hand-written row is held, so it loads the same
    way: every value is what its column's fixture type holds (a mapping or a
    list in a ``string`` column is left for the loader to write as JSON), and
    the loader's own columns are filled where the factory left them out.

    :param fixture: The unit's parsed fixture file.
    :param unit: The inventory unit.
    :param factories: The unit's factory package.
    :param scale: Multiplies every factory's ``default_count``.
    :returns: The generated rows of each raw table, the hand-written ones left out.
    :rtype: dict[str, list[Row]]
    :raises ValueError: If ``scale`` is not positive, a value cannot be held by
        its column, or the rows fail :func:`validate_generated`.
    """
    if not (math.isfinite(scale) and scale > 0):
        msg = f"scale is {scale!r}, expected a positive number"
        raise ValueError(msg)
    seed_generation(factories.SEED, _sequence_starts(fixture, unit))
    generated = factories.generate({table.raw_table: table.rows for table in fixture.tables}, scale)
    tables = {table.raw_table: table for table in fixture.tables}
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
    return finished


def _sequence_starts(fixture: UnitFixture, unit: Unit) -> dict[str, int]:
    """Return each table's first sequence number: one past the largest hand-written key in an integer column."""
    key_columns = {
        table["raw_table"]: [path[0] for path in table["primary_key"]]
        for table in unit.tables
        if table.get("primary_key")
    }
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
        if finished.get(column) is None:
            continue
        try:
            finished[column] = coerce_value(finished[column], fixture_type)
        except (ValueError, TypeError, ArithmeticError, struct.error) as error:
            msg = f"{label} has {finished[column]!r} in column {column}, which is not a valid {fixture_type}"
            raise ValueError(msg) from error
    return finished


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
