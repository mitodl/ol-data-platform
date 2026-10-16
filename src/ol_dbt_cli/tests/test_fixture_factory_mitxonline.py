"""The mitxonline/app_postgres factory package against what the MITx Online staging models test.

test_fixture_factories holds every package to its Django models. These hold
this one to the staging tests the models do not say (one requirement root per
program, the accepted node types and operators, a topic name unique under its
parent with no parent counted as one, a program page's price), to the names
MIT Learn keeps, and to dates that do not move with the day a model builds.
"""

from __future__ import annotations

import datetime
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from ol_dbt_cli.fixture_factories import mitxonline__app_postgres
from ol_dbt_cli.lib.fixture_generation import LOADED_AT, generate_rows
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, load_units
from ol_dbt_cli.lib.raw_fixtures import load_fixtures, unit_factories

if TYPE_CHECKING:
    from ol_dbt_cli.lib.fixture_generation import GeneratedRows

REPO_ROOT = Path(__file__).resolve().parents[3]
PREFIX = "raw__mitxonline__app__postgres__"


def _mitxonline(scale: float) -> GeneratedRows:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    (unit,) = [unit for unit in load_units(inventory_dir) if unit.key == "mitxonline/app_postgres"]
    (fixture,) = load_fixtures(inventory_dir, [unit], selected=True)
    factories = unit_factories(unit)
    assert factories is not None
    return generate_rows(fixture, unit, factories, scale)


def _all(rows: GeneratedRows, table: str) -> list[dict[str, Any]]:
    return rows.hand_written[PREFIX + table] + rows.generated.get(PREFIX + table, [])


def _generated(rows: GeneratedRows, table: str) -> list[dict[str, Any]]:
    return rows.generated[PREFIX + table]


@pytest.fixture(scope="module", params=[1, 3])
def mitxonline(request: pytest.FixtureRequest) -> GeneratedRows:
    return _mitxonline(request.param)


def test_the_package_generates_every_table_but_the_content_types_and_modes(mitxonline: GeneratedRows) -> None:
    generated = {table.removeprefix(PREFIX) for table, rows in mitxonline.generated.items() if rows}

    assert generated == {table.removeprefix(PREFIX) for table in mitxonline.hand_written} - {
        "django_content_type",
        "courses_enrollmentmode",
    }


def test_each_program_has_one_requirement_root(mitxonline: GeneratedRows) -> None:
    roots = Counter(row["program_id"] for row in _all(mitxonline, "courses_programrequirement") if row["depth"] == 1)

    assert set(roots.values()) == {1}
    assert {row["id"] for row in _generated(mitxonline, "courses_program")} <= set(roots)


def test_requirement_nodes_use_the_accepted_types_and_operators(mitxonline: GeneratedRows) -> None:
    nodes = _generated(mitxonline, "courses_programrequirement")

    assert {row["node_type"] for row in nodes} <= {"program_root", "course", "program", "operator"}
    assert {row["operator"] for row in nodes if row["operator"] is not None} <= {"all_of", "min_number_of"}
    # Staging casts it to an integer.
    assert all(int(row["operator_value"]) >= 1 for row in nodes if row["operator_value"] is not None)
    assert {row["node_type"] for row in nodes} >= {"program_root", "course", "program", "operator"}


def test_a_topic_name_is_unique_under_its_parent_with_no_parent_counted_once(mitxonline: GeneratedRows) -> None:
    """dbt_expectations groups NULL parents together, unlike the database's unique_together."""
    pairs = Counter((row["name"], row.get("parent_id")) for row in _all(mitxonline, "courses_coursestopic"))

    assert [pair for pair, count in pairs.items() if count > 1] == []


def test_generated_topics_and_departments_carry_names_learn_keeps(mitxonline: GeneratedRows) -> None:
    learn_topics = {
        name for parent, children in mitxonline__app_postgres.LEARN_TOPICS.items() for name in (parent, *children)
    }

    assert {row["name"] for row in _generated(mitxonline, "courses_coursestopic")} <= learn_topics
    assert {row["name"] for row in _generated(mitxonline, "courses_department")} <= set(
        mitxonline__app_postgres.LEARN_DEPARTMENTS
    )


def test_a_subtopic_is_under_the_learn_topic_it_belongs_to(mitxonline: GeneratedRows) -> None:
    topics = {row["id"]: row for row in _all(mitxonline, "courses_coursestopic")}
    for row in _generated(mitxonline, "courses_coursestopic"):
        if row["parent_id"] is not None:
            assert row["name"] in mitxonline__app_postgres.LEARN_TOPICS[topics[row["parent_id"]]["name"]]


def test_topics_stop_at_the_names_learn_keeps_however_large_the_scale() -> None:
    names = [row["name"] for row in _mitxonline(10).generated[f"{PREFIX}courses_coursestopic"]]
    learn = sum(1 + len(children) for children in mitxonline__app_postgres.LEARN_TOPICS.values())

    # Less the hand-written Engineering, which the generated subtopics hang under.
    assert len(names) == len(set(names)) == learn - 1


def test_every_page_price_is_a_price_details_block(mitxonline: GeneratedRows) -> None:
    """Staging reads $[0].value.text, and a NULL program price fails its not_null test."""
    for table in ("cms_coursepage", "cms_programpage"):
        for row in _generated(mitxonline, table):
            (block,) = row["price"]
            assert block["type"] == "price_details"
            assert block["value"]["text"]


def test_every_date_is_before_the_extract_or_far_off(mitxonline: GeneratedRows) -> None:
    """A date between today and 2099 would change which runs the models call enrollable as the days pass."""
    extracted = LOADED_AT.replace(tzinfo=None)
    near = [
        (table, column, value)
        for table, rows in mitxonline.generated.items()
        for row in rows
        for column, value in row.items()
        if isinstance(value, datetime.datetime) and extracted < value < datetime.datetime(2090, 1, 1)  # noqa: DTZ001
    ]

    assert near == []


def test_a_run_opens_for_enrollment_before_it_ends(mitxonline: GeneratedRows) -> None:
    for run in _generated(mitxonline, "courses_courserun"):
        assert run["enrollment_start"] < run["enrollment_end"]
        assert run["start_date"] < run["end_date"]


def test_variant_runs_have_a_non_default_variant_set_for_their_language(mitxonline: GeneratedRows) -> None:
    variants = {
        (row["object_id"], row["language"])
        for row in _generated(mitxonline, "variants_supportedvariant")
        if not row["default_variant"]
    }
    variant_runs = {
        (run["course_id"], run["language"])
        for run in _generated(mitxonline, "courses_courserun")
        if run["language"] == mitxonline__app_postgres.VARIANT_LANGUAGE
    }

    assert variant_runs
    assert variant_runs == variants


def test_the_hand_written_rows_load_unchanged() -> None:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    (unit,) = [unit for unit in load_units(inventory_dir) if unit.key == "mitxonline/app_postgres"]
    (fixture,) = load_fixtures(inventory_dir, [unit], selected=True)

    assert {table.raw_table: table.rows for table in fixture.tables} == _mitxonline(1).hand_written
