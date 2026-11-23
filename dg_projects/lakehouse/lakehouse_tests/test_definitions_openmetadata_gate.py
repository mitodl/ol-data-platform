"""definitions.py maps OpenMetadata tables in production only.

Read statically, like test_definitions_schedule_ids.py, because nothing in CI
imports the code location. QA's manifest is parsed for StarRocks and its nodes
have no database, so an ungated table_fqns_by_asset_key fails the QA load
(the 50a09c38 image crash-looped in QA on exactly that).
"""

import ast
from pathlib import Path

import lakehouse
from lakehouse.lib.scheduled_automation import SCHEDULE_ENVIRONMENTS

OPENMETADATA_SCHEDULE_IDS = (
    "asset_check_openmetadata",
    "freshness_openmetadata",
    "freshness_openmetadata_reconcile_daily",
)


def _openmetadata_table_fqns_value() -> ast.expr:
    source = Path(lakehouse.__file__).parent.joinpath("definitions.py").read_text()
    return next(
        statement.value
        for statement in ast.parse(source).body
        if isinstance(statement, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "openmetadata_table_fqns"
            for target in statement.targets
        )
    )


def test_table_fqns_are_mapped_in_production_only():
    value = _openmetadata_table_fqns_value()

    assert isinstance(value, ast.IfExp)
    assert ast.unparse(value.test) == "DAGSTER_ENV == 'production'"
    assert isinstance(value.body, ast.Call)
    assert ast.unparse(value.body.func) == "table_fqns_by_asset_key"
    assert ast.unparse(value.orelse) == "{}"


def test_the_gate_matches_where_the_sensors_register():
    """Registering them anywhere else would leave them with no tables there."""
    assert {
        schedule_id: SCHEDULE_ENVIRONMENTS[schedule_id]
        for schedule_id in OPENMETADATA_SCHEDULE_IDS
    } == dict.fromkeys(OPENMETADATA_SCHEDULE_IDS, frozenset({"production"}))
