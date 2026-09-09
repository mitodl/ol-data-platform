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


def _string_keyed_tuple_ids(node: ast.AST) -> set[str]:
    return {
        child.elts[0].value
        for child in ast.walk(node)
        if isinstance(child, ast.Tuple)
        and child.elts
        and isinstance(child.elts[0], ast.Constant)
        and isinstance(child.elts[0].value, str)
    }


def _schedule_ids_passed_at_the_call_site() -> set[str]:
    source = Path(lakehouse.__file__).parent.joinpath("definitions.py").read_text()
    tree = ast.parse(source)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "schedules_for_environment"
    ]
    assert len(calls) == 1, "expected exactly one call site to read ids from"

    ids = _string_keyed_tuple_ids(calls[0])

    # A schedule can also reach the call as `*some_name`, which is how any
    # conditionally-registered one gets there -- `airbyte_drift_schedules` is
    # built as [] or [(id, schedule)] depending on SKIP_AIRBYTE, because a
    # definition asking for an unregistered resource fails the whole code
    # location. Walking only the call node cannot see those, which is not a
    # hypothetical gap: `airbyte_inventory_drift_daily` arrived that way with no
    # SCHEDULE_ENVIRONMENTS entry, this test stayed green because neither side
    # knew about it, and the code location crash-looped on the KeyError in
    # production. Resolve starred names against their module-level assignment so
    # the guard covers the shape that actually broke.
    starred_names = {
        node.value.id
        for node in ast.walk(calls[0])
        if isinstance(node, ast.Starred) and isinstance(node.value, ast.Name)
    }
    for statement in tree.body:
        if not isinstance(statement, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id in starred_names
            for target in statement.targets
        ):
            ids |= _string_keyed_tuple_ids(statement.value)
    return ids


def test_call_site_and_declaration_agree():
    """Both directions.

    An id passed but not declared fails the deploy; an id declared but never
    passed is worse in its way -- it reads as a live gate on a schedule that no
    longer exists, so someone editing it thinks they changed something.
    """
    assert _schedule_ids_passed_at_the_call_site() == set(SCHEDULE_ENVIRONMENTS)
