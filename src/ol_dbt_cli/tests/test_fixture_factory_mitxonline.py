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


def _enrollable(run: dict[str, Any]) -> bool:
    """CourseRun.is_enrollable as int__mitxonline__catalog_course_runs evaluates it, as of the extract."""
    now = LOADED_AT.replace(tzinfo=None)
    return (
        run["live"]
        and run["start_date"] is not None
        and run["enrollment_start"] is not None
        and run["enrollment_start"] <= now
        and (run["enrollment_end"] is None or run["enrollment_end"] > now)
    )


def test_generated_courses_runs_and_programs_reach_learn_published() -> None:
    """The main path: a live in-catalog page over an enrollable run that is not a variant, at the default count."""
    rows = _mitxonline(1)
    wagtail = {row["id"]: row for row in _all(rows, "wagtailcore_page")}

    def listed(table: str, column: str) -> set[int]:
        return {
            row[column]
            for row in _generated(rows, table)
            if wagtail[row["page_ptr_id"]]["live"] and row["include_in_learn_catalog"]
        }

    published_runs = [
        run
        for run in _generated(rows, "courses_courserun")
        if _enrollable(run)
        and not run["b2b_only"]
        and run["language"] == mitxonline__app_postgres.LANGUAGE
        and run["course_id"] in listed("cms_coursepage", "course_id")
    ]
    programs = [
        program
        for program in _generated(rows, "courses_program")
        if program["live"] and not program["b2b_only"] and program["id"] in listed("cms_programpage", "program_id")
    ]

    assert published_runs
    assert programs


def test_every_off_path_branch_has_a_generated_row_at_the_default_count() -> None:
    rows = _mitxonline(1)
    courses = _generated(rows, "courses_course")
    programs = _generated(rows, "courses_program")
    runs = _generated(rows, "courses_courserun")
    wagtail = {row["id"]: row for row in _generated(rows, "wagtailcore_page")}
    course_pages = _generated(rows, "cms_coursepage")
    pages = course_pages + _generated(rows, "cms_programpage")
    requirements = _generated(rows, "courses_programrequirement")
    required = {row["required_program_id"] for row in requirements if row["node_type"] == "program"}
    modes = Counter(row["program_id"] for row in _generated(rows, "courses_program_enrollment_modes"))
    faqs: dict[int, list[dict[str, Any]]] = {}
    for faq in _generated(rows, "cms_productpagefaq"):
        faqs.setdefault(faq["page_id"], []).append(faq)
    branches = {
        "a proctored exam": any("PROCTORED EXAM" in course["title"].upper() for course in courses),
        "a course with no page": {course["id"] for course in courses} - {page["course_id"] for page in course_pages},
        "a draft page": any(not wagtail[page["page_ptr_id"]]["live"] for page in pages),
        "a page out of the Learn catalog": any(not page["include_in_learn_catalog"] for page in pages),
        "a program that is not live": any(not program["live"] for program in programs),
        "a B2B-only program": any(program["b2b_only"] for program in programs),
        "a run that is not live": any(not run["live"] for run in runs),
        "a B2B-only run": any(run["b2b_only"] for run in runs),
        "a source run": any(run["is_source_run"] for run in runs),
        "a required program shown as a course": any(
            program["id"] in required and program["display_mode"] == "course" for program in programs
        ),
        "a required program standing for its courses": any(
            program["id"] in required
            and program["display_mode"] is None
            and any(row["program_id"] == program["id"] and row["node_type"] == "course" for row in requirements)
            for program in programs
        ),
        "an elective": any(row["operator"] == "min_number_of" for row in requirements),
        "a program offering the audit mode": max(modes.values()) == 2,  # noqa: PLR2004
        "a page with no FAQs": {page["page_ptr_id"] for page in pages} - set(faqs),
        "an FAQ with no sort order": any(faq["sort_order"] is None for page in faqs.values() for faq in page),
        "FAQs whose sort order runs against their id": any(
            [faq["id"] for faq in sorted(page, key=lambda faq: faq["sort_order"])] != sorted(faq["id"] for faq in page)
            for page in faqs.values()
            if all(faq["sort_order"] is not None for faq in page)
        ),
    }

    assert [branch for branch, present in branches.items() if not present] == []


def test_the_hand_written_rows_load_unchanged() -> None:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    (unit,) = [unit for unit in load_units(inventory_dir) if unit.key == "mitxonline/app_postgres"]
    (fixture,) = load_fixtures(inventory_dir, [unit], selected=True)

    assert {table.raw_table: table.rows for table in fixture.tables} == _mitxonline(1).hand_written
