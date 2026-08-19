"""The call site's schedule ids must match the declaration.

Split out from test_scheduled_automation.py because it tests definitions.py
rather than the library: `schedules_for_environment` raises on an unknown id,
but nothing in CI imports the code location -- that import needs /opt/dbt and a
parsed manifest, which only the container has. So a typo'd id at the call site
would first surface as a failed deploy. Reading the call site statically closes
that, which is the same argument the module itself makes about instance state:
if the repo can check it, the repo should.
"""

import ast
from pathlib import Path

import lakehouse
from lakehouse.lib.scheduled_automation import SCHEDULE_ENVIRONMENTS


def _schedule_ids_passed_at_the_call_site() -> set[str]:
    source = Path(lakehouse.__file__).parent.joinpath("definitions.py").read_text()
    calls = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "schedules_for_environment"
    ]
    assert len(calls) == 1, "expected exactly one call site to read ids from"
    return {
        node.elts[0].value
        for node in ast.walk(calls[0])
        if isinstance(node, ast.Tuple)
        and node.elts
        and isinstance(node.elts[0], ast.Constant)
        and isinstance(node.elts[0].value, str)
    }


def test_call_site_and_declaration_agree():
    """Both directions.

    An id passed but not declared fails the deploy; an id declared but never
    passed is worse in its way -- it reads as a live gate on a schedule that no
    longer exists, so someone editing it thinks they changed something.
    """
    assert _schedule_ids_passed_at_the_call_site() == set(SCHEDULE_ENVIRONMENTS)
