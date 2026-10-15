"""The xpro/app_postgres factory package against what the xPRO staging models test.

The staging data tests run only on a built lake. These hold the generated rows to the
unique, compound-unique and accepted-values tests on the 18 tables the xPRO Learn models
read, and to a whole treebeard tree, before anything loads.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from ol_dbt_cli.lib.fixture_generation import generate_rows
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, load_units
from ol_dbt_cli.lib.raw_fixtures import load_fixtures, unit_factories

if TYPE_CHECKING:
    from ol_dbt_cli.lib.fixture_generation import GeneratedRows

REPO_ROOT = Path(__file__).resolve().parents[3]
PREFIX = "raw__xpro__app__postgres__"
# The index pages the fixture leaves out, which generated course and program pages sit under.
LEFT_OUT = {"0001", "00010001", "000100010001", "000100010002"}


def _xpro(scale: float) -> GeneratedRows:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    (unit,) = [unit for unit in load_units(inventory_dir) if unit.key == "xpro/app_postgres"]
    (fixture,) = load_fixtures(inventory_dir, [unit], selected=True)
    factories = unit_factories(unit)
    assert factories is not None
    return generate_rows(fixture, unit, factories, scale)


def _all(rows: GeneratedRows, table: str) -> list[dict[str, Any]]:
    return rows.hand_written[PREFIX + table] + rows.generated.get(PREFIX + table, [])


def _repeats(values: list[Any]) -> list[Any]:
    return [value for value, count in Counter(values).items() if count > 1]


@pytest.fixture(scope="module", params=[1, 3])
def xpro(request: pytest.FixtureRequest) -> GeneratedRows:
    return _xpro(request.param)


def test_the_package_generates_every_table_it_feeds(xpro: GeneratedRows) -> None:
    generated = {table.removeprefix(PREFIX) for table, rows in xpro.generated.items() if rows}

    assert generated == {
        "courses_coursetopic",
        "wagtailimages_image",
        "courses_program",
        "courses_course",
        "courses_courserun",
        "ecommerce_product",
        "ecommerce_productversion",
        "wagtailcore_page",
        "cms_coursepage",
        "cms_programpage",
        "cms_certificatepage",
        "cms_facultymemberspage",
        "cms_coursepage_topics",
    }


@pytest.mark.parametrize(
    ("table", "columns"),
    [
        # The staging models' unique and compound-unique tests, on the raw columns they rename.
        ("courses_program", ("readable_id",)),
        ("courses_course", ("readable_id",)),
        ("courses_courserun", ("courseware_id",)),
        ("courses_courserun", ("course_id", "run_tag")),
        ("courses_coursetopic", ("name",)),
        ("ecommerce_product", ("content_type_id", "object_id")),
        ("wagtailcore_page", ("path",)),
        ("cms_coursepage", ("course_id",)),
        ("cms_programpage", ("program_id",)),
        ("cms_coursepage_topics", ("coursepage_id", "coursetopic_id")),
    ],
)
def test_generated_rows_keep_the_staging_uniques(xpro: GeneratedRows, table: str, columns: tuple[str, ...]) -> None:
    assert _repeats([tuple(row.get(column) for column in columns) for row in _all(xpro, table)]) == []


def test_page_formats_are_ones_the_staging_tests_accept(xpro: GeneratedRows) -> None:
    assert {row["format"] for row in _all(xpro, "cms_coursepage")} <= {"Online", "Other", "Hybrid"}
    assert {row["format"] for row in _all(xpro, "cms_programpage")} <= {"Online", "Other"}


def test_the_pages_are_one_treebeard_tree(xpro: GeneratedRows) -> None:
    pages = _all(xpro, "wagtailcore_page")
    paths = {row["path"] for row in pages}
    for row in pages:
        assert row["path"][:-4] in paths | LEFT_OUT
        assert row["depth"] == len(row["path"]) // 4
        assert row["numchild"] == sum(path[:-4] == row["path"] for path in paths)
    # Every product page row has its wagtail page.
    page_ids = {row["id"] for row in pages}
    for table in ("cms_coursepage", "cms_programpage", "cms_certificatepage", "cms_facultymemberspage"):
        assert {row["page_ptr_id"] for row in _all(xpro, table)} <= page_ids


def test_a_product_points_at_a_run_or_program_that_exists(xpro: GeneratedRows) -> None:
    content_types = {row["id"]: row["model"] for row in _all(xpro, "django_content_type")}
    targets = {
        "courserun": {row["id"] for row in _all(xpro, "courses_courserun")},
        "program": {row["id"] for row in _all(xpro, "courses_program")},
    }
    for product in _all(xpro, "ecommerce_product"):
        assert product["object_id"] in targets[content_types[product["content_type_id"]]]


def test_streamfields_have_the_block_shape_the_models_read(xpro: GeneratedRows) -> None:
    for row in xpro.generated[f"{PREFIX}cms_facultymemberspage"]:
        members = json.loads(row["members"]) if isinstance(row["members"], str) else row["members"]
        assert members
        assert all(block["type"] == "member" and block["value"]["name"] for block in members)
    for row in xpro.generated[f"{PREFIX}cms_certificatepage"]:
        assert all(block["type"] == "signatory" for block in row["signatories"])


def test_a_run_ends_after_it_starts(xpro: GeneratedRows) -> None:
    for run in xpro.generated[f"{PREFIX}courses_courserun"]:
        assert run["enrollment_start"] < run["start_date"] < run["end_date"]


def test_the_hand_written_rows_load_unchanged() -> None:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    (unit,) = [unit for unit in load_units(inventory_dir) if unit.key == "xpro/app_postgres"]
    (fixture,) = load_fixtures(inventory_dir, [unit], selected=True)

    rows = _xpro(1)

    assert {table.raw_table: table.rows for table in fixture.tables} == rows.hand_written


def test_one_seed_generates_the_same_rows() -> None:
    assert _xpro(1).generated == _xpro(1).generated
