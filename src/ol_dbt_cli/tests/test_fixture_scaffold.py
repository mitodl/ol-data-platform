"""Tests for writing a unit's base factories from a dump of its Django models, or from its staging tests."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import factory
import pytest
import yaml

from ol_dbt_cli.commands import fixtures as fixtures_command
from ol_dbt_cli.lib.fixture_generation import generate_rows
from ol_dbt_cli.lib.fixture_scaffold import scaffold, scaffold_from_tests, starter_package, trimmed_models
from ol_dbt_cli.lib.fixture_trees import Tree
from ol_dbt_cli.lib.inventory import load_units
from ol_dbt_cli.lib.raw_fixtures import load_fixtures

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ol_dbt_cli.lib.fixture_scaffold import Scaffold
    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.raw_fixtures import UnitFixture

PREFIX = "raw__demo__app__postgres__"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _column(internal_type: str, **facts: Any) -> dict[str, Any]:
    return {
        "field": internal_type,
        "internal_type": internal_type,
        "null": False,
        "primary_key": False,
        "unique": False,
        **facts,
    }


def _key(internal_type: str = "AutoField") -> dict[str, Any]:
    return _column(internal_type, primary_key=True, unique=True)


def _foreign_key(table: str, column: str = "id", **facts: Any) -> dict[str, Any]:
    return _column("ForeignKey", foreign_key={"table": table, "column": column}, **facts)


# One table per kind of fact a dump holds, as bin/django-model-dump.py writes them.
MODELS: dict[str, Any] = {
    "format_version": 1,
    "app": "demo",
    "commit": "git:0123abc",
    "django": "5.1",
    "tables": {
        "courses_courserun": {
            "model": "courses.CourseRun",
            "managed": True,
            "columns": {
                "id": _key(),
                "course_id": _foreign_key("courses_course"),
                "run_tag": _column("CharField", max_length=100),
            },
            "unique_together": [["course_id", "run_tag"]],
        },
        "courses_course": {
            "model": "courses.Course",
            "managed": True,
            "columns": {
                "id": _key(),
                "readable_id": _column("CharField", unique=True, max_length=255),
                "title": _column("CharField", max_length=255),
                "contact_email": _column("CharField", max_length=254),
                "live": _column("BooleanField", default=False),
                "price": _column("DecimalField", max_digits=7, decimal_places=2),
                "created_on": _column("DateTimeField"),
                "start_date": _column("DateField", null=True),
                "kind": _column("CharField", max_length=10, choices=[None, "online", "hybrid"]),
                "details": _column("JSONField", null=True, json=True),
                "external_uuid": _column("UUIDField", unique=True),
                "owner_id": _foreign_key("users_user", null=True),
                "platform_id": _foreign_key("courses_platform"),
                "program_id": _foreign_key("courses_program", null=True),
                "login_email": _column("CharField", unique=True, max_length=254),
                "homepage_url": _column("CharField", max_length=500),
                "level": _column("CharField", null=True, max_length=20, choices=["intro", "advanced"]),
                "rank": _column("IntegerField", unique=True),
                # Airbyte lands this column as `ceus`.
                "CEUs": _column("DecimalField", null=True, max_digits=5, decimal_places=2),
                "hours": _column("CharField", max_length=5),
            },
        },
        "wagtailcore_page": {
            "model": "wagtailcore.Page",
            "managed": True,
            "columns": {
                "id": _key(),
                "path": _column("CharField", unique=True, max_length=255),
                "depth": _column("PositiveIntegerField"),
                "numchild": _column("PositiveIntegerField", default=0),
                "slug": _column("SlugField", max_length=255),
                "url_path": _column("TextField"),
                "title": _column("CharField", max_length=255),
                "alias_of_id": _foreign_key("wagtailcore_page", null=True),
            },
            "treebeard": {"alphabet": "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", "steplen": 4},
        },
        "cms_coursepage": {
            "model": "cms.CoursePage",
            "managed": True,
            "columns": {
                "page_ptr_id": _foreign_key(
                    "wagtailcore_page", primary_key=True, unique=True, one_to_one=True, parent_link=True
                ),
                "course_id": _foreign_key("courses_course", unique=True, one_to_one=True),
            },
        },
        "ecommerce_product": {
            "model": "ecommerce.Product",
            "managed": True,
            "columns": {
                "id": _key(),
                "content_type_id": _foreign_key("django_content_type"),
                "object_id": _column("PositiveIntegerField"),
            },
            "generic_foreign_keys": [
                {"name": "content_object", "content_type": "content_type_id", "object_id": "object_id"}
            ],
        },
        "users_user": {"model": "users.User", "managed": True, "columns": {"id": _key()}},
        "courses_program": {"model": "courses.Program", "managed": True, "columns": {"id": _key()}},
        "cms_widget": {
            "model": "cms.Widget",
            "managed": True,
            "columns": {
                "id": _key(),
                # Columns named as the helpers later columns call.
                "value": _column("CharField", max_length=50),
                "title": _column("CharField", max_length=100),
                "url": {**_column("CharField", max_length=200), "field": "URLField"},
                "video_url": {**_column("CharField", max_length=200), "field": "URLField"},
                "factory": _column("CharField", max_length=20),
                # Classes Django reports as CharField, with names that hint at nothing.
                "homepage": {**_column("CharField", max_length=200), "field": "URLField"},
                "contact": {**_column("CharField", max_length=254), "field": "EmailField"},
                "code": _column("CharField", unique=True, max_length=4),
                "ref": _column("CharField", unique=True, max_length=8),
            },
        },
        "users_token": {
            "model": "users.Token",
            "managed": True,
            "columns": {
                "key": _key("UUIDField"),
                "class": _column("CharField", max_length=20),
                # A db_column with no _id suffix.
                "owner": _foreign_key("users_user"),
            },
        },
    },
}

FIXTURE_TABLES: dict[str, tuple[str, dict[str, str]]] = {
    "courses_courserun": ("id", {"id": "long", "course_id": "long", "run_tag": "string"}),
    "courses_course": (
        "id",
        {
            "id": "long",
            "readable_id": "string",
            "title": "string",
            "contact_email": "string",
            "live": "boolean",
            "price": "decimal(7,2)",
            "created_on": "timestamp",
            "start_date": "date",
            "kind": "string",
            "details": "string",
            "external_uuid": "string",
            "owner_id": "long",
            "platform_id": "long",
            "program_id": "long",
            "login_email": "string",
            "homepage_url": "string",
            "level": "string",
            "rank": "long",
            "ceus": "decimal(5,2)",
            "hours": "string",
            "_dlt_id": "string",
            "legacy_code": "string",
            "_airbyte_raw_id": "string",
        },
    ),
    "wagtailcore_page": (
        "id",
        {
            "id": "long",
            "path": "string",
            "depth": "long",
            "numchild": "long",
            "slug": "string",
            "url_path": "string",
            "title": "string",
            "alias_of_id": "long",
        },
    ),
    "cms_coursepage": ("page_ptr_id", {"page_ptr_id": "long", "course_id": "long"}),
    "ecommerce_product": ("id", {"id": "long", "content_type_id": "long", "object_id": "long"}),
    "things_unmodeled": ("id", {"id": "long", "note": "string", "amount": "decimal(10,2)"}),
    "courses_program": ("id", {"id": "long"}),
    "cms_widget": (
        "id",
        {
            "id": "long",
            "value": "string",
            "title": "string",
            "url": "string",
            "video_url": "string",
            "factory": "string",
            "homepage": "string",
            "contact": "string",
            "code": "string",
            "ref": "string",
        },
    ),
    "users_token": ("key", {"key": "string", "class": "string", "owner": "long"}),
}


def _unit(
    tmp_path: Path,
    keys: Mapping[str, str] | None = None,
    tables: Mapping[str, tuple[str, dict[str, str]]] = FIXTURE_TABLES,
    rows: Mapping[str, list[dict[str, Any]]] | None = None,
) -> tuple[UnitFixture, Unit]:
    keys = {**{name: key for name, (key, _) in tables.items()}, **(keys or {})}
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
                "name": name,
                "raw_table": PREFIX + name,
                "sync_mode": "full_refresh_overwrite",
                "modeled": True,
                "primary_key": [[keys[name]]],
            }
            for name in tables
        ],
    }
    fixture = {
        "schema_version": 1,
        "tables": {
            PREFIX + name: {"columns": columns, "rows": (rows or {}).get(name, [])}
            for name, (_, columns) in tables.items()
        },
    }
    (tmp_path / "units").mkdir()
    (tmp_path / "units" / "demo__app_postgres.yml").write_text(yaml.safe_dump(unit))
    (tmp_path / "fixtures").mkdir()
    (tmp_path / "fixtures" / "demo__app_postgres.yml").write_text(yaml.safe_dump(fixture))
    (loaded_unit,) = load_units(tmp_path)
    (loaded_fixture,) = load_fixtures(tmp_path, [loaded_unit])
    return loaded_fixture, loaded_unit


def _class_body(source: str, name: str) -> str:
    start = source.index(f"class {name}(fg.RowFactory):")
    end = source.find("\n\n\n", start)
    return source[start : end if end != -1 else None]


@pytest.fixture
def scaffolded(tmp_path: Path) -> str:
    fixture, unit = _unit(tmp_path)
    return scaffold(fixture, unit, MODELS).source


def test_each_column_gets_what_its_model_says(scaffolded: str) -> None:
    course = _class_body(scaffolded, "CourseBase")

    assert "id = factory.Sequence(lambda n: n)" in course
    assert 'readable_id = factory.Sequence(lambda n: f"readable-{n}")' in course
    assert 'title = fg.value("text.title")' in course
    assert 'contact_email = fg.value("person.email")' in course
    assert 'live = fg.value("development.boolean")' in course
    assert 'price = fg.value("numeric.decimal_number", start=0, end=99999)' in course
    assert 'created_on = fg.value("datetime.datetime", start=2020, end=2025)' in course
    assert 'start_date = fg.or_null(fg.value("datetime.date", start=2020, end=2025))' in course
    # The blank None choice is the column's NULL.
    assert 'kind = factory.Iterator(["online", "hybrid"])' in course
    assert 'external_uuid = fg.value("cryptographic.uuid")' in course
    assert 'platform_id = fg.parent("platform", "id")' in course
    # Not a column of the model, so drawn by its fixture type.
    assert 'legacy_code = fg.value("text.word")' in course
    assert "_airbyte_raw_id" not in course


def test_what_the_model_cannot_say_is_left_unset_with_a_comment(scaffolded: str) -> None:
    course = _class_body(scaffolded, "CourseBase")
    page = _class_body(scaffolded, "PageBase")
    product = _class_body(scaffolded, "ProductBase")

    assert "# details: JSON whose shape the model does not say" in course
    assert "details =" not in course
    assert "# owner_id: references users_user, which the fixture has no table for; NULL." in course
    assert "owner_id =" not in course
    assert "# platform_id: references courses_platform, which the fixture has no table for." in course
    for column in ("path", "depth", "numchild", "url_path"):
        assert f"# {column}: set by Tree.add." in page
        assert f"    {column} =" not in page
    assert "# alias_of_id: references this table; NULL unless generate() sets it with attach()." in page
    assert "# object_id: the object id of generic foreign key content_object" in product
    assert "object_id =" not in product
    assert "# Unique together: course_id, run_tag." in _class_body(scaffolded, "CourseRunBase")


def test_nullable_unique_and_case_rules(scaffolded: str) -> None:
    course = _class_body(scaffolded, "CourseBase")
    token = _class_body(scaffolded, "TokenBase")

    # A nullable foreign key into the fixture is NULL when generate() passes no parent.
    assert 'program_id = fg.parent("program", "id", nullable=True)' in course
    assert 'login_email = factory.Sequence(lambda n: f"user{n}@{fg.RESERVED_DOMAIN}")' in course
    assert "homepage_url = fg.url()" in course
    assert 'level = fg.or_null(factory.Iterator(["intro", "advanced"]))' in course
    assert "rank = factory.Sequence(lambda n: n)" in course
    # The model's CEUs, matched without case, and drawn to fit decimal(5,2).
    assert 'ceus = fg.or_null(fg.value("numeric.decimal_number", start=0, end=999))' in course
    # Too short for a word.
    assert 'hours = fg.value("numeric.integer_number", start=0, end=9999)' in course
    assert "_dlt_id" not in course
    assert "from ol_dbt_cli.lib import fixture_generation as fg" in scaffolded
    assert 'key = fg.value("cryptographic.uuid")' in token
    assert "# class: a Python keyword" in token
    assert 'owner = fg.parent("owner_row", "id")' in token
    assert "owner_row = None" in token
    assert 'amount = fg.value("numeric.decimal_number", start=0, end=99999999)' not in scaffolded
    assert 'amount = fg.value("numeric.decimal_number", start=0, end=999999)' in _class_body(
        scaffolded, "ThingsUnmodeledBase"
    )


def test_field_classes_and_short_unique_text(scaffolded: str) -> None:
    widget = _class_body(scaffolded, "WidgetBase")

    assert "homepage = fg.url()" in widget
    assert 'contact = fg.value("person.email")' in widget
    assert "url = fg.url()" in widget
    assert "# factory: a Python keyword or a name this module uses" in widget
    assert "# code: unique, and 4 characters hold too few distinct values to generate" in widget
    assert "    code =" not in widget
    # Too short for "ref-" and six digits, so the digits alone.
    assert "ref = factory.Sequence(str)" in widget


def test_long_names_come_out_as_ruff_formats_and_checks_them(tmp_path: Path) -> None:
    table = "learning_resources_learningresourcerun_resource_prices_and_their_currency_conversions"
    long_parent = "learning_resources_learningresourcerun_with_a_name_too_long_for_one_line"
    tables = {
        **MODELS["tables"],
        table: {
            "model": "learning_resources.LearningResourceRunResourcePricesAndTheirCurrencyConversions",
            "managed": True,
            "columns": {
                "id": _key(),
                f"{long_parent}_id": _foreign_key("courses_course", null=True),
                "status_of_the_conversion": _column(
                    "CharField", null=True, max_length=40, choices=["pending_review", "approved_by_staff"]
                ),
            },
        },
    }
    FIXTURE_TABLES[table] = ("id", {"id": "long", f"{long_parent}_id": "long", "status_of_the_conversion": "string"})
    try:
        fixture, unit = _unit(tmp_path)
    finally:
        del FIXTURE_TABLES[table]
    source = scaffold(
        fixture, unit, {**MODELS, "tables": tables, "app": "mit-learn-application-with-a-long-name"}
    ).source
    # Each kind of line that has to be broken up was.
    assert "@fg.table(\n" in source
    assert f"{long_parent}_id = fg.parent(\n" in source
    assert '    """Table learning_resources' in source
    path = tmp_path / "_generated.py"
    path.write_text(source)
    # Outside the repository ruff would take ol_dbt_cli for a third-party package.
    config = [
        "--config",
        str(REPO_ROOT / "src/ol_dbt_cli/pyproject.toml"),
        "--config",
        "lint.isort.known-first-party = ['ol_dbt_cli']",
    ]

    for command in (["format", "--check", "--diff"], ["check", "--select", "E,F,I,W"]):
        ran = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "ruff", *command, *config, str(path)], capture_output=True, text=True, check=False
        )
        assert ran.returncode == 0, ran.stdout + ran.stderr


def test_a_key_that_is_a_foreign_key_copies_its_parent(scaffolded: str) -> None:
    page = _class_body(scaffolded, "CoursePageBase")

    assert 'page_ptr_id = fg.parent("page_ptr", "id")' in page
    assert "# page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair()." in page
    assert 'course_id = fg.parent("course", "id")' in page


def test_a_table_with_no_model_counts_its_key_on_the_sequence(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path)

    result = scaffold(fixture, unit, MODELS)

    unmodeled = _class_body(result.source, "ThingsUnmodeledBase")
    assert "id = factory.Sequence(lambda n: n)" in unmodeled
    assert 'note = fg.value("text.word")' in unmodeled
    assert any(
        note.startswith(f"{PREFIX}things_unmodeled: no model has table things_unmodeled") for note in result.notes
    )


def test_parents_come_before_the_tables_that_reference_them(scaffolded: str) -> None:
    order = [
        scaffolded.index(f"class {name}(") for name in ("CourseBase", "CourseRunBase", "PageBase", "CoursePageBase")
    ]

    # The file lists runs first; the scaffold puts each parent first.
    assert order[0] < order[1]
    assert order[2] < order[3]
    assert scaffolded.index("class CourseBase(") < scaffolded.index("class CoursePageBase(")


def test_disagreements_are_noted_not_fatal(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path, keys={"courses_course": "readable_id"})

    notes = scaffold(fixture, unit, MODELS).notes

    assert f"{PREFIX}courses_course.legacy_code: not a column of courses.Course; drawn by its fixture type" in notes
    assert f"{PREFIX}courses_course: the inventory's primary key is ['readable_id'], the model's is ['id']" in notes


def test_a_second_run_writes_the_same_module_and_the_trimmed_models_write_it_too(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path)
    first = scaffold(fixture, unit, MODELS)

    assert scaffold(fixture, unit, MODELS).source == first.source
    assert scaffold(fixture, unit, first.models).source == first.source
    # users_user has a model and no fixture table.
    assert sorted(first.models["tables"]) == sorted(set(FIXTURE_TABLES) - {"things_unmodeled"})
    assert trimmed_models(MODELS, fixture, unit) == first.models


def test_choices_too_long_for_the_line_move_to_a_constant(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path)
    languages = [f"language_{number}" for number in range(12)]
    models = json.loads(json.dumps(MODELS))
    models["tables"]["courses_courserun"]["columns"]["run_tag"]["choices"] = languages

    source = scaffold(fixture, unit, models).source

    assert "run_tag = factory.Iterator(COURSE_RUN_RUN_TAG_CHOICES)" in source
    assert (
        "COURSE_RUN_RUN_TAG_CHOICES = [\n" + "".join(f'    "{language}",\n' for language in languages) + "]" in source
    )


def test_a_dump_of_another_format_is_refused(tmp_path: Path) -> None:
    fixture, unit = _unit(tmp_path)

    with pytest.raises(ValueError, match="format_version 2; this scaffold reads 1"):
        scaffold(fixture, unit, {**MODELS, "format_version": 2})


def _import_scaffolded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str) -> Any:
    package = tmp_path / "scaffolded" / "demo__app_postgres"
    package.mkdir(parents=True)
    (package.parent / "__init__.py").write_text("")
    (package / "__init__.py").write_text("")
    (package / "_generated.py").write_text(source)
    monkeypatch.syspath_prepend(str(tmp_path))
    # Another test's module of the same name would be imported again otherwise.
    for name in [name for name in sys.modules if name.split(".")[0] == "scaffolded"]:
        monkeypatch.delitem(sys.modules, name)
    return importlib.import_module("scaffolded.demo__app_postgres._generated")


def test_the_scaffolded_factories_generate_rows_that_pass_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, unit = _unit(tmp_path)
    generated = _import_scaffolded(tmp_path / "package", monkeypatch, scaffold(fixture, unit, MODELS).source)

    def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, list[dict[str, Any]]]:
        courses = generated.CourseBase.build_batch(4, platform={"id": 1})
        # Under wagtail's root, which the fixture leaves out.
        pages = Tree([], alphabet=generated.TREE_ALPHABETS[generated.PageBase.raw_table]).add(
            generated.PageBase.build_batch(4), under={"path": "0001", "url_path": "/"}
        )
        return {
            generated.CourseBase.raw_table: courses,
            generated.CourseRunBase.raw_table: generated.CourseRunBase.build_batch(8, course=factory.Iterator(courses)),
            generated.PageBase.raw_table: pages,
            generated.CoursePageBase.raw_table: generated.CoursePageBase.build_batch(
                4, page_ptr=factory.Iterator(pages), course=factory.Iterator(courses)
            ),
            generated.ProductBase.raw_table: generated.ProductBase.build_batch(3, content_type={"id": 1}),
            generated.ThingsUnmodeledBase.raw_table: generated.ThingsUnmodeledBase.build_batch(3),
            generated.ProgramBase.raw_table: generated.ProgramBase.build_batch(2),
            generated.TokenBase.raw_table: generated.TokenBase.build_batch(2, owner_row={"id": 1}),
            generated.WidgetBase.raw_table: generated.WidgetBase.build_batch(3),
        }

    package: Any = SimpleNamespace(SEED=1, generate=generate)
    rows = generate_rows(fixture, unit, package)

    assert [len(rows.generated[PREFIX + name]) for name in FIXTURE_TABLES] == [8, 4, 4, 4, 3, 3, 2, 3, 2]
    # A column named value or url does not shadow the helpers later columns call.
    widgets = rows.generated[f"{PREFIX}cms_widget"]
    assert all(row["video_url"].startswith("https://") and row["title"] for row in widgets)
    # Built with no program, the nullable program_id is NULL.
    assert {row["program_id"] for row in rows.generated[f"{PREFIX}courses_course"]} == {None}
    assert {row["owner"] for row in rows.generated[f"{PREFIX}users_token"]} == {1}
    assert {row["page_ptr_id"] for row in rows.generated[f"{PREFIX}cms_coursepage"]} == {
        row["id"] for row in rows.generated[f"{PREFIX}wagtailcore_page"]
    }


def test_the_starter_package_adds_no_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture, unit = _unit(tmp_path)
    package = tmp_path / "starter" / "demo__app_postgres"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(starter_package(unit))
    monkeypatch.syspath_prepend(str(tmp_path / "starter"))

    starter = importlib.import_module("demo__app_postgres")

    assert generate_rows(fixture, unit, starter).generated == {}
    assert starter_package(unit) == starter_package(unit)


def _scaffold_command(tmp_path: Path, **options: Any) -> Path:
    factories_dir = tmp_path / "factories"
    fixtures_command.scaffold(
        unit="demo/app_postgres",
        inventory_dir=tmp_path,
        factories_dir=factories_dir,
        **{"dbt_dir": tmp_path / "dbt", **options},
    )
    return factories_dir / "demo__app_postgres"


def test_scaffold_writes_the_package_and_keeps_a_hand_written_init(tmp_path: Path) -> None:
    _unit(tmp_path)
    dump = tmp_path / "dump.json"
    dump.write_text(json.dumps(MODELS))

    package = _scaffold_command(tmp_path, models=dump)

    assert sorted(path.name for path in package.iterdir()) == ["__init__.py", "_generated.py", "models.json"]
    assert "users_user" not in json.loads((package / "models.json").read_text())["tables"]
    (package / "__init__.py").write_text("# written by hand\n")
    first = (package / "_generated.py").read_text()

    # Again, from the models.json the first run left.
    _scaffold_command(tmp_path)

    assert (package / "__init__.py").read_text() == "# written by hand\n"
    assert (package / "_generated.py").read_text() == first


def test_scaffold_without_a_dump_or_a_staging_model_exits(tmp_path: Path) -> None:
    _unit(tmp_path)

    with pytest.raises(SystemExit):
        _scaffold_command(tmp_path)


def test_scaffold_with_a_missing_dump_exits_though_tests_could_draft(tmp_path: Path) -> None:
    _unit(tmp_path, tables=TESTED_TABLES)
    _staging_project(tmp_path / "dbt")

    with pytest.raises(SystemExit):
        _scaffold_command(tmp_path, models=tmp_path / "missing.json")


# A unit with no Django app: its staging models' tests say what the dump would.
TESTED_TABLES: dict[str, tuple[str, dict[str, str]]] = {
    "courses_courserun": (
        "id",
        {"id": "long", "course_id": "long", "run_tag": "string", "year": "string", "status_id": "long"},
    ),
    "courses_course": (
        "id",
        {
            "id": "long",
            "readable_id": "string",
            "rank": "long",
            "code": "string",
            "kind": "string",
            "title": "string",
            "price": "decimal(7,2)",
            "details": "string",
            "_dlt_load_id": "string",
        },
    ),
    "courses_unread": ("id", {"id": "long", "note": "string"}),
}
# A hand-written row holding a mapping makes the column JSON, and two rows
# sharing a code say the raw table repeats what staging tests unique.
TESTED_ROWS = {
    "courses_course": [
        {"id": 1, "readable_id": "course-v1:demo", "code": "D1", "details": {"level": "intro"}},
        {"id": 2, "readable_id": "course-v1:demo2", "code": "D1"},
    ]
}
STAGING_YAML = """
version: 2
models:
- name: stg__demo__courses
  columns:
  - name: course_id
    tests: [not_null, unique]
  - name: course_readable_id
    data_tests:
    - not_null
    - unique:
        config:
          severity: warn
  - name: course_rank
    tests: [unique]
  - name: course_code
    tests: [unique]
  - name: course_kind
    tests:
    - accepted_values:
        arguments:
          values: [online, hybrid]
  - name: course_title
    tests: [not_null]
- name: stg__demo__courseruns
  tests:
  - dbt_utils.recency:
      arguments:
        datepart: day
        field: courserun_year
        interval: 1
  - dbt_utils.unique_combination_of_columns:
      arguments:
        combination_of_columns: [course_id, courserun_tag]
  - dbt_expectations.expect_compound_columns_to_be_unique:
      column_list: [course_id, courserun_tag_upper]
  columns:
  - name: courserun_id
    tests: [not_null, unique, json_array]
  - name: course_id
    tests:
    - not_null
    - relationships:
        arguments:
          to: ref('stg__demo__courses')
          field: course_id
  - name: courserun_year
    tests:
    # The legacy form, and numbers YAML reads from a string column's values.
    - accepted_values:
        values: [2025, 2026]
  - name: status_id
    tests:
    - relationships:
        to: ref('stg__demo__statuses')
        field: status_id
"""


def _staging_project(dbt_dir: Path) -> None:
    """Write a dbt project whose two staging models read the tested tables, one through a macro's CTE."""
    staging = dbt_dir / "models" / "staging" / "demo"
    staging.mkdir(parents=True)
    (dbt_dir / "macros").mkdir()
    # `do`, as deduplicate_raw_table uses it.
    (dbt_dir / "macros" / "keep_newest.sql").write_text(
        "{% macro keep_newest(source_cte) %}\n"
        "    {%- set names = [] -%}\n"
        "    {%- do names.append('newest_' ~ source_cte) -%}\n"
        "    , {{ names[0] }} as (\n"
        "        select * from {{ source_cte }}\n"
        "    )\n"
        "{% endmacro %}\n"
    )
    (staging / "stg__demo__courses.sql").write_text(
        "with source as (\n"
        "    select * from {{ source('ol_warehouse_raw_data', 'raw__demo__app__postgres__courses_course') }}\n"
        ")\n\n"
        "{{ keep_newest('source') }}\n\n"
        "select\n"
        "    id as course_id\n"
        "    , readable_id as course_readable_id\n"
        "    , cast(rank as bigint) as course_rank\n"
        "    , code as course_code\n"
        "    , kind as course_kind\n"
        "    , lower(title) as course_title\n"
        "from newest_source\n"
    )
    (staging / "stg__demo__courseruns.sql").write_text(
        "select\n"
        "    id as courserun_id\n"
        "    , course_id\n"
        "    , run_tag as courserun_tag\n"
        "    , upper(run_tag) as courserun_tag_upper\n"
        "    , year as courserun_year\n"
        "    , status_id\n"
        "from {{ source('ol_warehouse_raw_data', 'raw__demo__app__postgres__courses_courserun') }}\n"
    )
    (staging / "_stg_demo__models.yml").write_text(STAGING_YAML)


@pytest.fixture
def tested_unit(tmp_path: Path) -> tuple[UnitFixture, Unit]:
    _staging_project(tmp_path / "dbt")
    return _unit(tmp_path, tables=TESTED_TABLES, rows=TESTED_ROWS)


@pytest.fixture
def drafted(tmp_path: Path, tested_unit: tuple[UnitFixture, Unit]) -> Scaffold:
    return scaffold_from_tests(*tested_unit, tmp_path / "dbt")


def test_staging_tests_map_back_to_the_raw_columns_they_pass(drafted: Scaffold) -> None:
    course = _class_body(drafted.source, "CoursesCourseBase")

    assert "id = factory.Sequence(lambda n: n)" in course
    # Renamed, with not_null and unique.
    assert 'readable_id = factory.Sequence(lambda n: f"readable-{n}")' in course
    # Cast, unique and not tested not_null.
    assert "rank = fg.or_null(factory.Sequence(lambda n: n))" in course
    assert 'code = fg.or_null(fg.value("text.word"))' in course
    assert 'kind = fg.or_null(factory.Iterator(["online", "hybrid"]))' in course
    # Derived, so its not_null is not mapped.
    assert 'title = fg.or_null(fg.value("text.title"))' in course
    # Read by no staging model: drawn by its fixture type, NULL in a share of rows.
    assert 'price = fg.or_null(fg.value("numeric.decimal_number", start=0, end=99999))' in course
    assert "# details: JSON whose shape the model does not say" in course
    assert "details =" not in course
    assert "_dlt_load_id" not in course


def test_relationships_and_compound_unique_map_where_the_writer_has_an_equivalent(drafted: Scaffold) -> None:
    run = _class_body(drafted.source, "CoursesCourserunBase")

    assert 'course_id = fg.parent("course", "id")' in run
    assert "course = None" in run
    assert "# Unique together: course_id, run_tag." in run
    assert 'year = fg.or_null(factory.Iterator(["2025", "2026"]))' in run
    # Its relationship is to a model that reads no fixture table.
    assert 'status_id = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))' in run
    # The parent comes first, as it does from a dump.
    assert drafted.source.index("class CoursesCourseBase(") < drafted.source.index("class CoursesCourserunBase(")
    assert '"""Base factories of the `demo/app_postgres` unit, drafted from its staging models\' tests.' in (
        drafted.source
    )


def test_what_does_not_map_is_noted(drafted: Scaffold) -> None:
    assert drafted.notes == [
        f"{PREFIX}courses_unread: no staging model reads it; every column is drawn by its fixture type",
        "stg__demo__courseruns.courserun_id: json_array not mapped; the writer has no equivalent",
        "stg__demo__courseruns.status_id: relationships to ref('stg__demo__statuses') not mapped; "
        "its status_id is not a column of a fixture table renamed or cast",
        "stg__demo__courseruns: dbt_utils.recency not mapped; the writer has no equivalent",
        "stg__demo__courseruns: dbt_expectations.expect_compound_columns_to_be_unique on course_id, "
        "courserun_tag_upper not mapped; its columns are not all passed through from one table",
        f"stg__demo__courses.course_code: unique not mapped; the fixture's rows repeat a value of "
        f"{PREFIX}courses_course.code",
        "stg__demo__courses.course_title: not_null not mapped; the column is derived, not a raw column renamed or cast",
    ]


def test_the_drafted_document_is_what_a_dump_would_say(
    drafted: Scaffold, tested_unit: tuple[UnitFixture, Unit], tmp_path: Path
) -> None:
    fixture, unit = tested_unit
    document = drafted.models

    assert document["tables"]["courses_course"]["model"] is None
    assert document["tables"]["courses_courserun"]["columns"]["course_id"] == {
        "field": "BigIntegerField",
        "internal_type": "BigIntegerField",
        "null": False,
        "primary_key": False,
        "unique": False,
        "foreign_key": {"table": "courses_course", "column": "id"},
    }
    assert document["tables"]["courses_courserun"]["unique_together"] == [["course_id", "run_tag"]]
    # The writer reads it as it reads a dump, and a second run drafts the same.
    assert (
        scaffold(fixture, unit, {**document, "app": "demo", "commit": "git:0123abc"}).source.split('"""', 2)[2]
        == (drafted.source.split('"""', 2)[2])
    )
    assert scaffold_from_tests(fixture, unit, tmp_path / "dbt").source == drafted.source


def test_the_drafted_factories_generate_rows_that_pass_validation(
    drafted: Scaffold, tested_unit: tuple[UnitFixture, Unit], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, unit = tested_unit
    generated = _import_scaffolded(tmp_path / "package", monkeypatch, drafted.source)

    def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, list[dict[str, Any]]]:
        courses = generated.CoursesCourseBase.build_batch(4)
        return {
            generated.CoursesCourseBase.raw_table: courses,
            generated.CoursesCourserunBase.raw_table: generated.CoursesCourserunBase.build_batch(
                8, course=factory.Iterator(courses)
            ),
            generated.CoursesUnreadBase.raw_table: generated.CoursesUnreadBase.build_batch(2),
        }

    package: Any = SimpleNamespace(SEED=1, generate=generate)
    rows = generate_rows(fixture, unit, package)

    runs = rows.generated[f"{PREFIX}courses_courserun"]
    assert {row["course_id"] for row in runs} <= {row["id"] for row in rows.generated[f"{PREFIX}courses_course"]}
    assert {row["year"] for row in runs} <= {"2025", "2026", None}


def test_scaffold_drafts_from_tests_when_the_unit_has_no_dump(tmp_path: Path) -> None:
    _unit(tmp_path, tables=TESTED_TABLES, rows=TESTED_ROWS)
    _staging_project(tmp_path / "dbt")

    package = _scaffold_command(tmp_path)

    # No models.json: the next run drafts from the tests again.
    assert sorted(path.name for path in package.iterdir()) == ["__init__.py", "_generated.py"]
    assert "drafted from its staging models' tests" in (package / "_generated.py").read_text()
