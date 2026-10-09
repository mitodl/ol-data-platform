"""Run dbt unit tests on DuckDB with no warehouse access.

dbt reads each unit test input's columns from a real relation, so on its own ``dbt test
--select test_type:unit`` needs every input built first. This parses on the DuckDB
``unit_test`` target, creates an empty stand-in table for every input relation (see
:mod:`ol_dbt_cli.lib.unit_test_inputs`), then runs the unit tests against them.

Usage examples::

    ol-dbt unit-test                                   # every unit test
    ol-dbt unit-test --select tfact_payment            # unit tests on one model
    ol-dbt unit-test --select test_tfact_payment_refreshes_stale_user_fk_after_dim_user_rekey
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Annotated

import duckdb
import yaml
from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.commands.run import _find_dbt_dir
from ol_dbt_cli.lib.dbt_executable import dbt_executable
from ol_dbt_cli.lib.unit_test_inputs import create_stub_relations, stub_relations

console = Console()

# A target whose database nothing else reads: the stand-ins replace whatever is there.
UNIT_TEST_TARGET = "unit_test"
# Kept apart from target/ so the manifest other ol-dbt commands read isn't swapped for one
# parsed against this target.
UNIT_TEST_TARGET_PATH = "target/unit_test"


def _target_database_path(dbt_dir: Path) -> Path:
    profiles = yaml.safe_load((dbt_dir / "profiles.yml").read_text())
    (profile,) = profiles.values()
    output = profile["outputs"][UNIT_TEST_TARGET]
    if output["type"] != "duckdb":
        msg = f"The {UNIT_TEST_TARGET!r} target must be DuckDB, found {output['type']!r}"
        raise ValueError(msg)
    return dbt_dir / output["path"]


def _warn(message: str) -> None:
    console.print(f"[yellow]{message}[/]", soft_wrap=True)


def unit_test(
    select: Annotated[
        str | None,
        Parameter(name=["--select", "-s"], help="dbt selector; only the unit tests it selects run."),
    ] = None,
    project_dir: Annotated[
        str | None,
        Parameter(name="--project-dir", help="dbt project root (default: src/ol_dbt under the repo root)."),
    ] = None,
) -> None:
    """Run dbt unit tests against empty stand-in inputs on a scratch DuckDB database.

    Needs no AWS or warehouse credentials. A fixture value's type is taken from the input
    column's documented ``data_type`` when it has one, else inferred from the fixture values
    (VARCHAR when a column is only ever NULL); document ``data_type`` on any column whose type
    the test depends on.
    """
    dbt_dir = _find_dbt_dir(project_dir)
    base = [dbt_executable()]
    common = ["--profiles-dir", str(dbt_dir), "--target", UNIT_TEST_TARGET, "--target-path", UNIT_TEST_TARGET_PATH]

    parse = subprocess.run([*base, "parse", *common], cwd=dbt_dir)  # noqa: S603
    if parse.returncode:
        sys.exit(parse.returncode)

    manifest = json.loads((dbt_dir / UNIT_TEST_TARGET_PATH / "manifest.json").read_text())
    stubs, unresolved = stub_relations(manifest)
    for problem in unresolved:
        _warn(problem)

    database_path = _target_database_path(dbt_dir)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    database_path.unlink(missing_ok=True)
    with duckdb.connect(str(database_path)) as conn:
        created, failed = create_stub_relations(conn, stubs)
    console.print(f"[dim]Created {len(created)} empty input relations in {database_path}[/]", soft_wrap=True)
    for problem in failed:
        _warn(problem)

    selection = ["--select", select] if select else []
    result = subprocess.run(  # noqa: S603
        [*base, "test", *common, *selection, "--resource-type", "unit_test"], cwd=dbt_dir
    )
    sys.exit(result.returncode)
