"""Run dbt unit tests on DuckDB with no warehouse access.

dbt reads each unit test input's columns from a real relation, so on its own ``dbt test
--select test_type:unit`` needs every input built first. This parses on the plain DuckDB
``dev`` target, creates an empty stand-in table for every input relation (see
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

# The stand-in tables are written into this target's database, so it must be one nothing else
# reads from. The `dev` target is a plain local DuckDB file with no extensions or credentials.
UNIT_TEST_TARGET = "dev"


def _target_database_path(dbt_dir: Path) -> Path:
    profiles = yaml.safe_load((dbt_dir / "profiles.yml").read_text())
    (profile,) = profiles.values()
    output = profile["outputs"][UNIT_TEST_TARGET]
    if output["type"] != "duckdb":
        msg = f"The {UNIT_TEST_TARGET!r} target must be DuckDB, found {output['type']!r}"
        raise ValueError(msg)
    return dbt_dir / output["path"]


def unit_test(
    select: Annotated[
        str,
        Parameter(name=["--select", "-s"], help="dbt selector, intersected with test_type:unit."),
    ] = "test_type:unit",
    project_dir: Annotated[
        str | None,
        Parameter(name="--project-dir", help="dbt project root (default: src/ol_dbt under the repo root)."),
    ] = None,
) -> None:
    """Run dbt unit tests against empty stand-in inputs on the local DuckDB ``dev`` target.

    Needs no AWS or warehouse credentials. A fixture value's type is taken from the input
    column's documented ``data_type`` when it has one, else inferred from the fixture values
    (VARCHAR when a column is only ever NULL); document ``data_type`` on any column whose type
    the test depends on.
    """
    dbt_dir = _find_dbt_dir(project_dir)
    base = [dbt_executable()]
    common = ["--profiles-dir", str(dbt_dir), "--target", UNIT_TEST_TARGET]

    parse = subprocess.run([*base, "parse", *common], cwd=dbt_dir)  # noqa: S603
    if parse.returncode:
        sys.exit(parse.returncode)

    manifest = json.loads((dbt_dir / "target" / "manifest.json").read_text())
    stubs = stub_relations(manifest)
    database_path = _target_database_path(dbt_dir)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as conn:
        created = create_stub_relations(conn, stubs)
    console.print(f"[dim]Created {len(created)} empty input relations in {database_path}[/]", soft_wrap=True)
    for stub in stubs:
        if not stub.columns():
            console.print(
                f"[yellow]No columns known for {stub.identifier}:[/] it documents none and its fixtures set none. "
                "Document its columns or give it a `format: sql` fixture.",
                soft_wrap=True,
            )

    selector = select if select == "test_type:unit" else f"{select},test_type:unit"
    result = subprocess.run([*base, "test", *common, "--select", selector], cwd=dbt_dir)  # noqa: S603
    sys.exit(result.returncode)
