"""Tests for the runtime the fixture factory packages build on.

The rows come from `tests/fixtures/fixture_factories/demo__app_postgres/`, a
unit package laid out as a real one is: scaffolded base factories in
`_generated.py`, overrides and `generate` in `__init__.py`. Nothing here needs
StarRocks: `generate_rows` returns rows as a fixture file holds them.
"""

from __future__ import annotations

import datetime
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import factory
import factory.fuzzy
import pytest
import yaml

from ol_dbt_cli.lib import fixture_generation
from ol_dbt_cli.lib.fixture_generation import (
    RESERVED_DOMAIN,
    RESERVED_URL_DOMAIN,
    RowFactory,
    draw,
    generate_rows,
    link,
    or_null,
    pair,
    pick,
    scaled_count,
    seed_generation,
    table,
    value,
)
from ol_dbt_cli.lib.fixture_trees import Tree
from ol_dbt_cli.lib.inventory import Unit, load_units
from ol_dbt_cli.lib.raw_fixtures import UnitFactories, UnitFixture, coerce_value, load_fixtures, unit_factories

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

FACTORY_PACKAGES = Path(__file__).parent / "fixtures"
PREFIX = "raw__demo__app__postgres__"
COURSES, PAGES, RUNS = f"{PREFIX}courses_course", f"{PREFIX}cms_coursepage", f"{PREFIX}courses_courserun"
USERS, ENROLLMENTS = f"{PREFIX}users_user", f"{PREFIX}courses_courserunenrollment"
AIRBYTE = {"_airbyte_raw_id": "string", "_airbyte_extracted_at": "long", "_airbyte_generation_id": "long"}
HAND_WRITTEN_COURSES = [
    {"id": 1, "readable_id": "course-v1:Demo+ByHand", "title": "By hand"},
    {"id": "7", "readable_id": "course-v1:Demo+AlsoByHand", "title": "Also by hand"},
]
TABLES: dict[str, dict[str, Any]] = {
    COURSES: {
        "primary_key": "id",
        "columns": {
            **AIRBYTE,
            "id": "long",
            "readable_id": "string",
            "title": "string",
            "live": "boolean",
            "price": "decimal(5,2)",
            "created_on": "timestamp",
        },
        "rows": HAND_WRITTEN_COURSES,
    },
    PAGES: {
        "primary_key": "page_ptr_id",
        "columns": {"page_ptr_id": "long", "course_id": "long", "description": "string", "thumbnail_url": "string"},
        "rows": [{"page_ptr_id": 3, "course_id": 1}],
    },
    RUNS: {
        "primary_key": "id",
        "columns": {
            "id": "long",
            "course_id": "long",
            "run_tag": "string",
            "courseware_id": "string",
            "start_date": "timestamp",
            "end_date": "timestamp",
            "enrollment_start": "date",
        },
        "rows": [],
    },
    USERS: {
        "primary_key": "id",
        "columns": {
            "_dlt_load_id": "string",
            "_dlt_id": "string",
            "id": "long",
            "username": "string",
            "email": "string",
            "contact_email": "string",
            "name": "string",
            "profile": "string",
        },
        "rows": [],
    },
    ENROLLMENTS: {"primary_key": "id", "columns": {"id": "long", "user_id": "long", "run_id": "long"}, "rows": []},
}
# sha256 of the demo package's rows at scale 1. It changes when mimesis or
# factory_boy change what a seed generates, which is the reason to look at a
# bump of either before merging it.
PINNED_DIGEST = "9fbcd8a1cc51f05e3373312aad768b9c9e3538fd326724c8f0dfc20cc12f7d4a"  # pragma: allowlist secret


def _unit(tmp_path: Path, tables: dict[str, dict[str, Any]] = TABLES) -> tuple[UnitFixture, Unit]:
    unit = {
        "schema_version": 1,
        "deployment": "demo",
        "layer": "app_postgres",
        "scope": "singleton",
        "strategies": {"qa": "omit", "local": "fixture"},
        "loader": "airbyte",
        "table_prefix": PREFIX,
        "tables": [
            {
                "name": raw_table.removeprefix(PREFIX),
                "raw_table": raw_table,
                "sync_mode": "full_refresh_overwrite",
                "modeled": True,
                "primary_key": [[body["primary_key"]]],
            }
            for raw_table, body in tables.items()
        ],
    }
    fixture = {
        "schema_version": 1,
        "tables": {raw_table: {"columns": body["columns"], "rows": body["rows"]} for raw_table, body in tables.items()},
    }
    (tmp_path / "units").mkdir()
    (tmp_path / "units" / "demo__app_postgres.yml").write_text(yaml.safe_dump(unit))
    (tmp_path / "fixtures").mkdir()
    (tmp_path / "fixtures" / "demo__app_postgres.yml").write_text(yaml.safe_dump(fixture))
    (loaded_unit,) = load_units(tmp_path)
    (loaded_fixture,) = load_fixtures(tmp_path, [loaded_unit])
    return loaded_fixture, loaded_unit


@pytest.fixture
def demo(monkeypatch: pytest.MonkeyPatch) -> UnitFactories:
    monkeypatch.syspath_prepend(str(FACTORY_PACKAGES))
    factories = unit_factories(
        Unit(path=Path("demo__app_postgres.yml"), data={}),
        package="fixture_factories",
    )
    assert factories is not None
    return factories


def _stub(generated: dict[str, list[dict[str, Any]]]) -> UnitFactories:
    """Return a factory package that generates exactly ``generated``."""
    return SimpleNamespace(SEED=1, generate=lambda hand_written, scale: generated)


def _digest(rows: dict[str, list[dict[str, Any]]]) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=repr).encode()).hexdigest()


def test_one_seed_generates_the_same_rows_twice(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    first = generate_rows(fixture, unit, demo).generated

    assert generate_rows(fixture, unit, demo).generated == first
    assert [len(first[raw_table]) for raw_table in (COURSES, PAGES, RUNS, USERS, ENROLLMENTS)] == [4, 4, 8, 6, 12]


def test_another_seed_generates_other_rows(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)
    reseeded: Any = SimpleNamespace(SEED=demo.SEED + 1, generate=demo.generate)

    assert generate_rows(fixture, unit, reseeded) != generate_rows(fixture, unit, demo)


def test_a_seed_given_to_generate_rows_stands_in_for_the_package_seed(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)
    reseeded: Any = SimpleNamespace(SEED=demo.SEED + 1, generate=demo.generate)

    assert generate_rows(fixture, unit, demo, seed=demo.SEED + 1) == generate_rows(fixture, unit, reseeded)


def test_a_dependency_bump_that_changes_the_rows_fails_here(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    assert _digest(generate_rows(fixture, unit, demo).generated) == PINNED_DIGEST


def test_each_table_counts_from_past_its_own_hand_written_keys(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, demo).generated

    assert [row["id"] for row in generated[COURSES]] == [8, 9, 10, 11]
    assert [row["page_ptr_id"] for row in generated[PAGES]] == [4, 5, 6, 7]
    assert [row["id"] for row in generated[RUNS]] == [1, 2, 3, 4, 5, 6, 7, 8]


def test_a_subclass_fills_its_base_factorys_table_on_the_same_sequence() -> None:
    @table("raw__demo__app__postgres__things", default_count=5)
    class Thing(RowFactory):
        id = factory.Sequence(lambda n: n)

    class NamedThing(Thing):
        name = factory.Sequence(lambda n: f"thing {n}")

    seed_generation(1, {Thing.raw_table: 10})

    assert [Thing.build(), NamedThing.build(), Thing.build()] == [
        {"id": 10},
        {"id": 11, "name": "thing 11"},
        {"id": 12},
    ]
    assert (NamedThing.raw_table, NamedThing.default_count) == ("raw__demo__app__postgres__things", 5)


def test_a_factory_cannot_take_a_second_table() -> None:
    @table("raw__demo__app__postgres__things", default_count=5)
    class Thing(RowFactory):
        id = factory.Sequence(lambda n: n)

    with pytest.raises(ValueError, match="Other subclasses the factory of raw__demo__app__postgres__things"):

        @table("raw__demo__app__postgres__others", default_count=5)
        class Other(Thing):
            pass

    @table("raw__demo__app__postgres__things", default_count=50)
    class ManyThings(Thing):
        pass

    assert (Thing.default_count, ManyThings.default_count) == (5, 50)


def test_seeding_restarts_what_factory_boy_keeps_between_builds() -> None:
    """Nested iterators, a JSON column's own factory, and factory_boy's random source."""

    class Settings(factory.DictFactory):
        revision = factory.Sequence(lambda n: n)

    @table("raw__demo__app__postgres__things", default_count=5)
    class Thing(RowFactory):
        id = factory.Sequence(lambda n: n)
        size = factory.fuzzy.FuzzyInteger(0, 10**9)
        settings = factory.SubFactory(Settings)
        shape = factory.Dict({"kind": factory.Iterator(["a", "b", "c"])})
        tags = factory.List([factory.Iterator(["x", "y", "z"])])

    seed_generation(7)
    first = Thing.build_batch(4)
    seed_generation(7)

    assert Thing.build_batch(4) == first
    assert [(row["settings"], row["shape"], row["tags"]) for row in first[:2]] == [
        ({"revision": 0}, {"kind": "a"}, ["x"]),
        ({"revision": 1}, {"kind": "b"}, ["y"]),
    ]


@pytest.mark.parametrize("default_count", [0, -1, 2.5, True])
def test_table_rejects_a_default_count_that_is_not_a_positive_integer(default_count: Any) -> None:
    with pytest.raises(ValueError, match="raw__demo__app__postgres__things: default_count is .*positive integer"):
        table("raw__demo__app__postgres__things", default_count=default_count)


def test_rows_reference_the_rows_they_were_built_from(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, demo).generated

    readable_ids = {int(row["id"]): row["readable_id"] for row in HAND_WRITTEN_COURSES + generated[COURSES]}
    assert sorted(page["course_id"] for page in generated[PAGES]) == [8, 9, 10, 11]
    for run in generated[RUNS]:
        assert run["courseware_id"] == f"{readable_ids[run['course_id']]}+{run['run_tag']}"
        assert run["end_date"] > run["start_date"]
    assert {run["course_id"] for run in generated[RUNS]} & {1, 7}, "no run drew a hand-written course"
    enrollments = [(row["user_id"], row["run_id"]) for row in generated[ENROLLMENTS]]
    assert len(set(enrollments)) == len(enrollments)
    assert {user_id for user_id, _ in enrollments} <= {row["id"] for row in generated[USERS]}


def test_pair_refuses_more_children_than_parents() -> None:
    seed_generation(1)

    assert sorted(pair([1, 2, 3], 3)) == [1, 2, 3]
    with pytest.raises(ValueError, match="cannot pair 4 rows one-to-one with 3"):
        pair([1, 2, 3], 4)


def test_link_refuses_more_pairs_than_there_are() -> None:
    seed_generation(1)

    assert sorted(link([1, 2], "ab", 4)) == [(1, "a"), (1, "b"), (2, "a"), (2, "b")]
    with pytest.raises(ValueError, match="cannot make 5 distinct pairs from 2 and 2 rows"):
        link([1, 2], "ab", 5)


def test_pick_draws_with_repeats() -> None:
    seed_generation(1)

    assert set(pick([1, 2], 20)) == {1, 2}


def test_generated_addresses_are_on_the_reserved_domain(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, demo, scale=10).generated

    addresses = [user[column] for user in generated[USERS] for column in ("email", "contact_email")]
    assert all(address.endswith(f"@{RESERVED_DOMAIN}") for address in addresses)
    assert len({user["email"] for user in generated[USERS]}) == len(generated[USERS])
    assert all(page["thumbnail_url"].startswith(f"https://{RESERVED_URL_DOMAIN}/") for page in generated[PAGES])


@pytest.mark.parametrize(
    ("provider", "kwargs", "message"),
    [
        ("internet.url", {}, "internet.url makes up a hostname under a real top-level domain; use url()"),
        ("hostname", {}, "hostname makes up a hostname"),
        ("datetime.datetime", {}, "datetime.datetime defaults start and end to the current year"),
        ("datetime.date", {"start": 2020}, "datetime.date defaults end to the current year"),
        ("datetime.year", {}, "datetime.year defaults minimum and maximum to the current year"),
        ("internet:url", {}, "internet:url makes up a hostname"),
        ("internet.dsn", {}, "internet.dsn makes up a hostname"),
        ("datetime.formatted_date", {}, "datetime.formatted_date defaults start and end to the current year"),
        ("datetime.past_date", {}, "datetime.past_date counts from today"),
        ("development.calver", {}, "development.calver counts from today"),
    ],
)
def test_value_refuses_what_would_not_be_reserved_or_repeatable(
    monkeypatch: pytest.MonkeyPatch, provider: str, kwargs: dict[str, Any], message: str
) -> None:
    # Refused for what it names, so before anything is seeded too.
    monkeypatch.setattr(fixture_generation, "_generation", None)

    with pytest.raises(ValueError, match=message):
        value(provider, **kwargs)
    with pytest.raises(ValueError, match=message):
        draw(provider, **kwargs)


def test_an_email_keeps_the_domain_the_factory_names() -> None:
    seed_generation(1)

    assert draw("person.email", domains=["mail.test"]).endswith("@mail.test")
    assert draw("person.email", domains=None).endswith(f"@{RESERVED_DOMAIN}")


def test_building_before_seeding_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fixture_generation, "_generation", None)

    with pytest.raises(RuntimeError, match="nothing is seeded"):
        draw("text.title")


def test_rows_come_back_as_their_columns_hold_them(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, demo).generated

    course, run, user = generated[COURSES][0], generated[RUNS][0], generated[USERS][0]
    assert isinstance(course["price"], Decimal)
    assert course["price"].as_tuple().exponent == -2  # noqa: PLR2004
    assert isinstance(course["live"], bool)
    assert isinstance(course["created_on"], datetime.datetime)
    assert course["created_on"].tzinfo is None
    assert type(run["enrollment_start"]) is datetime.date
    assert user["profile"] == {"name": user["name"], "tags": ["generated"]}


def test_the_loaders_columns_are_filled_where_the_factory_left_them_out(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, demo).generated

    raw_ids = [course["_airbyte_raw_id"] for course in generated[COURSES]]
    assert len(set(raw_ids)) == len(raw_ids)
    assert {(course["_airbyte_extracted_at"], course["_airbyte_generation_id"]) for course in generated[COURSES]} == {
        (1790812800000, 0)
    }
    assert {user["_dlt_load_id"] for user in generated[USERS]} == {"1790812800.000000"}
    assert len({user["_dlt_id"] for user in generated[USERS]}) == len(generated[USERS])
    assert "_airbyte_raw_id" not in generated[RUNS][0]


def test_a_loader_column_the_factory_set_is_kept(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, _stub({COURSES: [{"id": 8, "_airbyte_generation_id": 3}]})).generated

    assert generated[COURSES][0]["_airbyte_generation_id"] == 3  # noqa: PLR2004


@pytest.mark.parametrize(
    ("fixture_type", "generated", "expected"),
    [
        ("long", 7.0, 7),
        ("int", "7", 7),
        ("double", 1, 1.0),
        ("boolean", 1, True),
        ("decimal(5,2)", 12.345, Decimal("12.34")),
        ("decimal(5,2)", "999.994", Decimal("999.99")),
        ("decimal(38,9)", Decimal(1) / 3, Decimal("0.333333333")),
        ("date", datetime.datetime(2024, 5, 6, 7, 8), datetime.date(2024, 5, 6)),
        ("date", "2024-05-06", datetime.date(2024, 5, 6)),
        ("timestamp", datetime.date(2024, 5, 6), datetime.datetime(2024, 5, 6)),
        (
            "timestamp",
            datetime.datetime(2024, 5, 6, 7, 8, tzinfo=datetime.timezone(datetime.timedelta(hours=-4))),
            datetime.datetime(2024, 5, 6, 11, 8),
        ),
        ("timestamp", "2024-05-06T07:08:00-04:00", datetime.datetime(2024, 5, 6, 11, 8)),
        ("string", 12, "12"),
        ("string", {"a": [1]}, {"a": [1]}),
        ("string", ["a"], ["a"]),
    ],
)
def test_coerce_value(fixture_type: str, generated: Any, expected: Any) -> None:
    coerced = coerce_value(generated, fixture_type)

    assert coerced == expected
    assert type(coerced) is type(expected)


@pytest.mark.parametrize(
    ("column", "generated"),
    [
        ("id", "eight"),
        ("id", 2**63),
        ("id", 8.5),
        ("live", "yes"),
        ("price", 1000),
        ("price", "9.9.9"),
        ("created_on", 1790812800),
        ("title", {"alpha", "beta"}),
        ("title", {"at": datetime.datetime(2024, 5, 6)}),
    ],
)
def test_a_value_its_column_cannot_hold_names_the_column(tmp_path: Path, column: str, generated: Any) -> None:
    fixture, unit = _unit(tmp_path)
    message = f"demo/app_postgres factories: {COURSES} generated row 2 has .* in column {column}, which is not a valid "

    with pytest.raises(ValueError, match=message):
        generate_rows(fixture, unit, _stub({COURSES: [{"id": 8}, {"id": 9, column: generated}]}))


@pytest.mark.parametrize(
    ("generated", "message"),
    [
        ({f"{PREFIX}nope": [{"id": 1}]}, "is not a table of"),
        ({COURSES: [{"id": 8, "nope": 1}]}, "sets nope, which the table does not have"),
        ({COURSES: [{"id": "7"}]}, "repeats primary key id=7"),
    ],
)
def test_generated_rows_are_validated(tmp_path: Path, generated: dict[str, list[dict[str, Any]]], message: str) -> None:
    fixture, unit = _unit(tmp_path)

    with pytest.raises(ValueError, match=message):
        generate_rows(fixture, unit, _stub(generated))


def test_scale_multiplies_every_default_count(tmp_path: Path, demo: UnitFactories) -> None:
    fixture, unit = _unit(tmp_path)

    generated = generate_rows(fixture, unit, demo, scale=2.5).generated

    assert [len(generated[raw_table]) for raw_table in (COURSES, PAGES, RUNS, USERS, ENROLLMENTS)] == [
        10,
        10,
        20,
        15,
        30,
    ]


@pytest.mark.parametrize(("share", "nulls"), [(0.0, range(0, 1)), (0.5, range(10, 31)), (1.0, range(40, 41))])
def test_or_null_leaves_a_seeded_share_of_rows_null(share: float, nulls: range) -> None:
    @table("raw__demo__app__postgres__notes", default_count=1)
    class Note(RowFactory):
        id = factory.Sequence(lambda n: n)
        body = or_null(value("text.word"), share=share)

    def bodies() -> list[Any]:
        seed_generation(3)
        return [row["body"] for row in Note.build_batch(40)]

    first = bodies()

    assert bodies() == first
    assert sum(body is None for body in first) in nulls


def test_scaled_count_never_reaches_zero() -> None:
    @table("raw__demo__app__postgres__things", default_count=4)
    class Thing(RowFactory):
        pass

    assert [scaled_count(Thing, scale) for scale in (0.01, 0.5, 1, 100)] == [1, 2, 4, 400]


@pytest.mark.parametrize("scale", [0, -1, float("nan"), float("inf")])
def test_generate_rows_rejects_a_scale_that_is_not_positive(tmp_path: Path, scale: float) -> None:
    fixture, unit = _unit(tmp_path)

    with pytest.raises(ValueError, match="expected a positive number"):
        generate_rows(fixture, unit, _stub({}), scale=scale)


PAGE_TREE = f"{PREFIX}wagtailcore_page"
TREE_TABLES: dict[str, dict[str, Any]] = {
    PAGE_TREE: {
        "primary_key": "id",
        "columns": {
            "id": "long",
            "path": "string",
            "depth": "long",
            "numchild": "long",
            "slug": "string",
            "url_path": "string",
        },
        "rows": [{"id": 1, "path": "0001", "depth": 1, "numchild": 0, "slug": "home", "url_path": "/home/"}],
    }
}


def _pages_under_the_hand_written_root(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> Any:
    (home,) = hand_written[PAGE_TREE]
    return {PAGE_TREE: Tree(hand_written[PAGE_TREE]).add([{"id": 2, "slug": "a"}, {"id": 3, "slug": "b"}], under=home)}


def test_a_hand_written_parent_loads_as_generate_left_it(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path, TREE_TABLES)
    package: Any = SimpleNamespace(SEED=1, generate=_pages_under_the_hand_written_root)

    rows = generate_rows(fixture, unit, package)

    assert rows.hand_written[PAGE_TREE][0]["numchild"] == 2
    assert [row["url_path"] for row in rows.generated[PAGE_TREE]] == ["/home/a/", "/home/b/"]
    # The parsed fixture is not changed.
    assert fixture.tables[0].rows[0]["numchild"] == 0


def _two_trees(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> Any:
    (home,) = hand_written[PAGE_TREE]
    # The second tree does not know the page the first placed.
    first = Tree(hand_written[PAGE_TREE]).add([{"id": 2, "slug": "a"}], under=home)
    second = Tree(hand_written[PAGE_TREE]).add([{"id": 3, "slug": "b"}], under=home)
    return {PAGE_TREE: first + second}


def _under_a_stand_in(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> Any:
    return {PAGE_TREE: Tree(hand_written[PAGE_TREE]).add([{"id": 2, "slug": "a"}], under={"path": "0002"})}


@pytest.mark.parametrize(
    ("generate", "stand_ins", "message"),
    [
        (_two_trees, {}, "the row with id=2 and the row with id=3 both have path '00010001'"),
        (_under_a_stand_in, {}, "the row with id=2 at path '00020001' is under '0002', which no row or stand-in has"),
        (_under_a_stand_in, {PAGE_TREE: [{"path": "0003"}]}, "under '0002', which no row or stand-in has"),
    ],
)
def test_a_tree_is_checked_whole_against_its_stand_ins(
    tmp_path: Path, generate: Callable[..., Any], stand_ins: dict[str, Any], message: str
) -> None:
    fixture, unit = _unit(tmp_path, TREE_TABLES)
    package: Any = SimpleNamespace(
        SEED=1, generate=generate, TREE_ALPHABETS={PAGE_TREE: "0123456789"}, TREE_STAND_INS=stand_ins
    )

    with pytest.raises(ValueError, match=message):
        generate_rows(fixture, unit, package)


def test_a_declared_stand_in_passes_the_tree_check(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path, TREE_TABLES)
    package: Any = SimpleNamespace(
        SEED=1,
        generate=_under_a_stand_in,
        TREE_ALPHABETS={PAGE_TREE: "0123456789"},
        TREE_STAND_INS={PAGE_TREE: [{"path": "0002"}]},
    )

    assert [row["path"] for row in generate_rows(fixture, unit, package).generated[PAGE_TREE]] == ["00020001"]


def _changing(change: Callable[[list[dict[str, Any]]], Any]) -> Any:
    def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, Any]:
        change(hand_written[PAGE_TREE])
        return {}

    return SimpleNamespace(SEED=1, generate=generate)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda rows: rows.append({"id": 2, "path": "0002"}),
            f"generate left 2 hand-written rows of {PAGE_TREE}, not the file's 1",
        ),
        (lambda rows: rows.clear(), "generate left 0 hand-written rows"),
        (lambda rows: rows[0].update(id=5), "hand-written row 1: generate changed primary key column id"),
        (lambda rows: rows[0].update(owner=5), "hand-written row 1 sets owner, which the table does not have"),
        (
            lambda rows: rows[0].update(numchild="many"),
            "hand-written row 1 has 'many' in column numchild, which is not a valid long",
        ),
    ],
)
def test_a_hand_written_row_keeps_its_key_and_its_place(
    tmp_path: Path, change: Callable[[list[dict[str, Any]]], Any], message: str
) -> None:
    fixture, unit = _unit(tmp_path, TREE_TABLES)

    with pytest.raises(ValueError, match=message):
        generate_rows(fixture, unit, _changing(change))


def _keyed_by(key_type: str, key: Any) -> dict[str, dict[str, Any]]:
    pages = TREE_TABLES[PAGE_TREE]
    return {
        PAGE_TREE: {
            **pages,
            "columns": {**pages["columns"], "id": key_type},
            "rows": [{**pages["rows"][0], "id": key}],
        }
    }


def test_a_key_that_equals_its_old_value_and_is_stored_differently_is_a_new_key(tmp_path: Path) -> None:
    # 1.0 == 1, and a string key stores them as '1.0' and '1'.
    fixture, unit = _unit(tmp_path, _keyed_by("string", 1))

    with pytest.raises(ValueError, match="hand-written row 1: generate changed primary key column id"):
        generate_rows(fixture, unit, _changing(lambda rows: rows[0].update(id=1.0)))


def test_a_key_stored_as_it_was_is_the_same_key(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path, _keyed_by("long", "1"))

    rows = generate_rows(fixture, unit, _changing(lambda rows: rows[0].update(id=1)))

    assert rows.hand_written[PAGE_TREE][0]["id"] == 1


def test_a_table_keyed_by_a_column_the_file_leaves_out_is_not_compared(tmp_path: Path) -> None:
    # The fixture may leave a key column out until the unit generates rows for it.
    tables = {PAGE_TREE: {**TREE_TABLES[PAGE_TREE], "primary_key": "uuid"}}
    fixture, unit = _unit(tmp_path, tables)

    rows = generate_rows(fixture, unit, _changing(lambda rows: rows[0].update(numchild=1)))

    assert rows.hand_written[PAGE_TREE][0]["numchild"] == 1


@pytest.mark.parametrize("changed", ["3", 3.0, 0.0])
def test_a_changed_hand_written_value_is_held_as_its_column_holds_it(tmp_path: Path, changed: Any) -> None:
    fixture, unit = _unit(tmp_path, TREE_TABLES)

    rows = generate_rows(fixture, unit, _changing(lambda rows: rows[0].update(numchild=changed)))

    held = rows.hand_written[PAGE_TREE][0]["numchild"]
    assert (held, type(held)) == (int(changed), int)
