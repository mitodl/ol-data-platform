"""Every committed fixture factory package, held to its app's models and to a pinned digest.

Nothing in CI loads a fixture into StarRocks, so a package that generated a
dangling foreign key or a repeated unique value would only be found on a local
lake. These run each package in process, at scale 1 and at a scale where fixed
pools run out, and check its rows against the Django model dump committed next
to it. Primary keys are not checked here: ``generate_rows`` refuses a repeated
one.

Rows are generated at load, so a mimesis or factory_boy bump could change what
every contributor's lake holds with no diff in the repo. The digest covers every
row ``ol-dbt fixtures load`` inserts, hand-written ones too, so it fails on that
and on any edit to the fixture file or the package. The fix is a deliberate
digest update in the same PR.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from ol_dbt_cli.commands.fixtures import FACTORIES_DIR, generate_command
from ol_dbt_cli.lib.fixture_generation import RESERVED_DOMAIN, generate_rows
from ol_dbt_cli.lib.fixture_scaffold import GENERATED_FILE, MODELS_FILE, scaffold
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, Unit, load_units
from ol_dbt_cli.lib.raw_fixtures import load_fixtures, stored_key, unit_factories

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ol_dbt_cli.lib.raw_fixtures import TableFixture, UnitFixture

REPO_ROOT = Path(__file__).resolve().parents[3]
INVENTORY_DIR = REPO_ROOT / DEFAULT_INVENTORY_DIR
DIGESTS_FILE = Path(__file__).with_name("generated_fixture_digests.json")
# Far enough past the default counts that a package drawing from a fixed pool
# runs out of it: xPRO's 24 Learn topic names are used up from scale 5.
LARGER_SCALE = 20
EMAIL = re.compile(r"[\w.+-]+@([\w-]+(?:\.[\w-]+)+)")
UNITS = sorted(
    (unit for unit in load_units(INVENTORY_DIR) if (FACTORIES_DIR / unit.path.stem / "__init__.py").exists()),
    key=lambda unit: unit.key,
)


@dataclass(frozen=True)
class _Generation:
    """One unit's rows at one scale, by the table name its models use."""

    unit: Unit
    fixture: UnitFixture
    tables: dict[str, TableFixture]
    hand_written: dict[str, list[dict[str, Any]]]
    generated: dict[str, list[dict[str, Any]]]
    models: dict[str, Any]

    def rows(self, db_table: str) -> list[dict[str, Any]]:
        return self.hand_written[db_table] + self.generated.get(db_table, [])

    def values(self, db_table: str, column: str, rows: list[dict[str, Any]] | None = None) -> list[Any]:
        """Return the non-NULL values of ``column`` as the table holds them."""
        table = self.tables[db_table]
        rows = self.rows(db_table) if rows is None else rows
        stored = (stored_key(table, row, [column], f"{db_table}.{column}")[0] for row in rows)
        return [value for value in stored if value is not None]


def _generation(unit: Unit, scale: float) -> _Generation:
    (fixture,) = load_fixtures(INVENTORY_DIR, [unit], selected=True)
    factories = unit_factories(unit)
    assert factories is not None
    rows = generate_rows(fixture, unit, factories, scale)
    prefix = str(unit.data["table_prefix"])
    dump = json.loads((FACTORIES_DIR / unit.path.stem / MODELS_FILE).read_text())
    return _Generation(
        unit=unit,
        fixture=fixture,
        tables={table.raw_table.removeprefix(prefix): table for table in fixture.tables},
        hand_written={raw_table.removeprefix(prefix): rows for raw_table, rows in rows.hand_written.items()},
        generated={raw_table.removeprefix(prefix): rows for raw_table, rows in rows.generated.items()},
        models={db_table: _lowered(model) for db_table, model in dump["tables"].items()},
    )


def _lowered(model: dict[str, Any]) -> dict[str, Any]:
    """Return ``model`` with its column names lowercased, as Airbyte lands them."""
    return {
        **model,
        "columns": {column.lower(): facts for column, facts in model["columns"].items()},
        "unique_together": [[column.lower() for column in columns] for columns in model.get("unique_together", [])],
    }


@pytest.fixture(
    scope="module",
    params=[(unit, scale) for unit in UNITS for scale in (1, LARGER_SCALE)],
    ids=lambda param: f"{param[0].key}-{param[1]}x",
)
def generation(request: pytest.FixtureRequest) -> _Generation:
    return _generation(*request.param)


def test_every_committed_package_is_guarded() -> None:
    """A package under fixture_factories with no unit would be skipped by everything below."""
    packages = {path.name for path in FACTORIES_DIR.iterdir() if (path / "__init__.py").exists()}

    assert packages == {unit.path.stem for unit in UNITS}
    assert {"xpro/app_postgres", "mitxonline/app_postgres"} <= {unit.key for unit in UNITS}


def test_every_fixture_table_has_a_model(generation: _Generation) -> None:
    """A table with none would be skipped by every constraint check below."""
    assert set(generation.tables) - set(generation.models) == set()


def test_generated_foreign_keys_reference_rows_that_exist(generation: _Generation) -> None:
    """Or a value the hand-written rows of the column already use.

    The fixture leaves out rows its hand-written ones reference (xPRO's page
    content types), and a generated row reusing one dangles no more than they do.
    """
    dangling = []
    for db_table, model in generation.models.items():
        table = generation.tables.get(db_table)
        if table is None or not generation.generated.get(db_table):
            continue
        for column, facts in model["columns"].items():
            foreign_key = facts.get("foreign_key")
            if foreign_key is None or column not in table.columns:
                continue
            target = generation.tables.get(foreign_key["table"])
            if target is None or foreign_key["column"].lower() not in target.columns:
                continue
            present = set(generation.values(foreign_key["table"], foreign_key["column"].lower()))
            present |= set(generation.values(db_table, column, generation.hand_written[db_table]))
            dangling += [
                f"{db_table}.{column} = {value!r}"
                for value in generation.values(db_table, column, generation.generated[db_table])
                if value not in present
            ]

    assert dangling == []


def test_generated_generic_foreign_keys_reference_rows_that_exist(generation: _Generation) -> None:
    """A content type the fixture does not carry is the plain foreign-key check's to report."""
    content_types = {
        row["id"]: f"{row['app_label']}.{row['model']}"
        for row in (generation.rows("django_content_type") if "django_content_type" in generation.tables else [])
    }
    by_model = {model["model"].lower(): db_table for db_table, model in generation.models.items() if model["model"]}
    dangling = []
    for db_table, model in generation.models.items():
        for entry in model.get("generic_foreign_keys", []):
            for row in generation.generated.get(db_table, []):
                content_type, object_id = row[entry["content_type"].lower()], row[entry["object_id"].lower()]
                target = by_model.get(content_types.get(content_type, ""))
                if object_id is None or target not in generation.tables:
                    continue
                (key,) = [
                    column for column, facts in generation.models[target]["columns"].items() if facts["primary_key"]
                ]
                if object_id not in generation.values(target, key):
                    dangling.append(f"{db_table} {entry['name']} = {target} {object_id!r}")

    assert dangling == []


def test_unique_columns_and_unique_together_hold(generation: _Generation) -> None:
    """Over hand-written and generated rows, NULLs left out as the database leaves them."""
    repeated = []
    for db_table, model in generation.models.items():
        table = generation.tables.get(db_table)
        if table is None:
            continue
        unique = [[column] for column, facts in model["columns"].items() if facts["unique"]]
        for columns in unique + model["unique_together"]:
            if not all(column in table.columns for column in columns):
                continue
            keys = [stored_key(table, row, columns, db_table) for row in generation.rows(db_table)]
            counts = Counter(key for key in keys if None not in key)
            repeated += [f"{db_table} {columns} = {key}" for key, count in counts.items() if count > 1]

    assert repeated == []


def test_trees_have_one_row_per_path_and_count_their_children(generation: _Generation) -> None:
    """A generated row hangs under a row of the table, or under a parent the hand-written rows already assume."""
    for db_table, model in generation.models.items():
        tree = model.get("treebeard")
        if tree is None or db_table not in generation.tables:
            continue
        steplen = tree["steplen"]
        paths = Counter(row["path"] for row in generation.rows(db_table))
        assert [path for path, count in paths.items() if count > 1] == []
        assumed = {row["path"][:-steplen] for row in generation.hand_written[db_table]}
        generated_children = Counter(row["path"][:-steplen] for row in generation.generated.get(db_table, []))
        for row in generation.generated.get(db_table, []):
            is_root = len(row["path"]) == steplen
            assert is_root or row["path"][:-steplen] in paths.keys() | assumed, row["path"]
            assert row["depth"] == len(row["path"]) // steplen, row["path"]
            assert row["numchild"] == generated_children[row["path"]], row["path"]
        # A hand-written parent counts children the fixture leaves out, so only its change is checked.
        for before, after in zip(generation.tables[db_table].rows, generation.hand_written[db_table], strict=True):
            added = int(after["numchild"] or 0) - int(before["numchild"] or 0)
            assert added == generated_children[after["path"]], after["path"]


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for held in value.values():
            yield from _strings(held)
    elif isinstance(value, list):
        for held in value:
            yield from _strings(held)


def test_generated_emails_are_on_the_reserved_domain(generation: _Generation) -> None:
    """A local app mailing a generated address must reach nobody."""
    domains = {
        match.group(1)
        for rows in generation.generated.values()
        for row in rows
        for value in row.values()
        for text in _strings(value)
        for match in EMAIL.finditer(text)
    }

    assert domains <= {RESERVED_DOMAIN}


@pytest.mark.parametrize("unit", UNITS, ids=lambda unit: unit.key)
def test_one_seed_generates_the_same_rows(unit: Unit) -> None:
    first, second = _generation(unit, 1), _generation(unit, 1)

    # The hand-written rows too: generate changes a parent's numchild in them.
    assert (first.hand_written, first.generated) == (second.hand_written, second.generated)


@pytest.mark.parametrize("unit", UNITS, ids=lambda unit: unit.key)
def test_the_scaffold_reproduces_the_committed_base_factories(unit: Unit) -> None:
    """A hand edit to _generated.py belongs in __init__.py, where the next scaffold leaves it."""
    package = FACTORIES_DIR / unit.path.stem
    (fixture,) = load_fixtures(INVENTORY_DIR, [unit], selected=True)

    written = scaffold(fixture, unit, json.loads((package / MODELS_FILE).read_text())).source

    assert written == (package / GENERATED_FILE).read_text(), (
        f"{unit.path.stem}/{GENERATED_FILE} is not what `ol-dbt fixtures scaffold --unit {unit.key}` writes"
    )


def test_every_guarded_unit_has_one_digest() -> None:
    assert set(json.loads(DIGESTS_FILE.read_text())) == {unit.key for unit in UNITS}


@pytest.mark.parametrize("unit", UNITS, ids=lambda unit: unit.key)
def test_generated_rows_match_the_pinned_digest(unit: Unit, capsys: pytest.CaptureFixture[str]) -> None:
    capsys.readouterr()
    generate_command(unit=[unit.key], inventory_dir=INVENTORY_DIR)
    digest = f"sha256:{hashlib.sha256(capsys.readouterr().out.encode()).hexdigest()}"
    pinned = json.loads(DIGESTS_FILE.read_text())[unit.key]

    assert digest == pinned, (
        f"The sha256 of `ol-dbt fixtures generate --unit {unit.key}` is {digest}, not the pinned {pinned}: "
        "the rows every contributor's local lake loads have changed. An edit to the unit's fixture file, its "
        "factory package or the generation runtime does that, and so does a mimesis or factory_boy version "
        f"change (installed: mimesis {importlib.metadata.version('mimesis')}, factory_boy "
        f"{importlib.metadata.version('factory-boy')}). If the change is intended, set the digest in "
        f"{DIGESTS_FILE.name} in the same PR. To see what changed, run "
        f"`ol-dbt fixtures generate --unit {unit.key} --out DIR` on main and on this branch and diff the two."
    )
