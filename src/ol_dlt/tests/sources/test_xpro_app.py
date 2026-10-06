"""Tests for the xPro application-database source."""

from pathlib import Path
from typing import Any

import yaml

from ol_dlt.sources import xpro_app

# src/ol_dlt/tests/sources/ -> repo root.
INVENTORY_UNIT = (
    Path(__file__).parents[4]
    / "ingestion"
    / "inventory"
    / "units"
    / "xpro__app_postgres.yml"
)

# Unmodeled tables the production Airbyte connection syncs that hold sessions
# and tokens. Named here so that marking one `modeled: true` cannot quietly
# pull it into the load.
CREDENTIAL_TABLE_PREFIXES = ("django_session", "oauth2_provider_", "social_auth_")


def _inventory_tables() -> list[dict[str, Any]]:
    return yaml.safe_load(INVENTORY_UNIT.read_text())["tables"]


def test_spec_matches_the_modeled_tables_of_the_inventory_unit() -> None:
    """The source loads exactly the tables the unit says a dbt model reads.

    The unit lists all 178 streams the production Airbyte connection syncs.
    The dlt source takes the `modeled: true` subset, so a model added against
    an unloaded table fails here and not as an empty QA build.
    """
    modeled = {table["name"] for table in _inventory_tables() if table["modeled"]}
    assert {table.name for table in xpro_app.XPRO_APP_SPEC.tables} == modeled


def test_primary_keys_match_the_inventory_unit() -> None:
    declared = {
        table["name"]: [column for path in table["primary_key"] for column in path]
        for table in _inventory_tables()
    }
    for table in xpro_app.XPRO_APP_SPEC.tables:
        assert [table.primary_key] == declared[table.name], table.name


def test_no_credential_table_is_loaded() -> None:
    assert not [
        table.name
        for table in xpro_app.XPRO_APP_SPEC.tables
        if table.name.startswith(CREDENTIAL_TABLE_PREFIXES)
    ]


def test_password_hash_is_excluded() -> None:
    """``users_user.password`` is a Django PBKDF2 hash, not analytical data."""
    users_user = next(
        table for table in xpro_app.XPRO_APP_SPEC.tables if table.name == "users_user"
    )
    assert "password" in users_user.excluded_columns


def test_no_table_declares_a_cursor() -> None:
    """Every table is re-read whole; see the module docstring for why.

    Adopting a cursor must be a reviewed act per table, so pin the current
    state.
    """
    assert not [
        table.name for table in xpro_app.XPRO_APP_SPEC.tables if table.cursor_column
    ]


def test_resources_follow_the_raw_naming_convention() -> None:
    source = xpro_app.build_source()
    assert "raw__xpro__app__postgres__ecommerce_company" in source.resources
    assert all(
        name.startswith("raw__xpro__app__postgres__") for name in source.resources
    )


def test_pipeline_targets_the_xpro_app_prefix() -> None:
    assert xpro_app.xpro_app_pipeline.pipeline_name == "xpro_app"
