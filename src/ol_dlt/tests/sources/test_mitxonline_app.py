"""Tests for the MITx Online application-database source."""

from pathlib import Path

import yaml

from ol_dlt.sources import mitxonline_app

# src/ol_dlt/tests/sources/ -> repo root.
INVENTORY_UNIT = (
    Path(__file__).parents[4]
    / "ingestion"
    / "inventory"
    / "units"
    / "mitxonline__app_postgres.yml"
)

# RFC 12711 step 8's pilot slice. dim_organization, dim_contract and
# bridge_organization_courserun read exactly these three, so a source that
# drops one silently produces a partial B2B dimensional build.
B2B_PILOT_TABLES = frozenset(
    {"b2b_organizationpage", "b2b_contractpage", "courses_courserun"}
)


def _inventory_streams() -> set[str]:
    unit = yaml.safe_load(INVENTORY_UNIT.read_text())
    return {table["name"] for table in unit["tables"]}


def test_spec_matches_the_inventory_unit() -> None:
    """The source and the inventory unit must declare the same tables.

    This is the whole point of the inventory (RFC 12319 §2): the declaration
    and the loader cannot disagree. A table added to one and not the other is a
    dbt model that quietly goes stale, with no error anywhere.
    """
    assert {
        table.name for table in mitxonline_app.MITXONLINE_APP_SPEC.tables
    } == _inventory_streams()


def test_b2b_pilot_upstreams_are_covered() -> None:
    selected = {table.name for table in mitxonline_app.MITXONLINE_APP_SPEC.tables}
    assert B2B_PILOT_TABLES <= selected


def test_password_hash_is_excluded() -> None:
    """``users_user.password`` is a Django PBKDF2 hash, not analytical data.

    It lands in the production warehouse today and no model reads it. Same
    footing as Keycloak's ``client.secret``.
    """
    users_user = next(
        table
        for table in mitxonline_app.MITXONLINE_APP_SPEC.tables
        if table.name == "users_user"
    )
    assert "password" in users_user.excluded_columns


def test_no_table_declares_a_cursor() -> None:
    """Every table is re-read whole -- see the module docstring for why.

    39 tables carry ``updated_on``, but that is Django ``auto_now=True``, which
    does not fire on ``queryset.update()``, and MITx Online's bulk-update paths
    have not been audited. Adopting a cursor here must be a deliberate,
    reviewed act per table rather than a bulk sweep, so pin the current state.
    """
    assert not [
        table.name
        for table in mitxonline_app.MITXONLINE_APP_SPEC.tables
        if table.cursor_column
    ]


def test_every_table_declares_a_primary_key() -> None:
    assert all(table.primary_key for table in mitxonline_app.MITXONLINE_APP_SPEC.tables)


def test_resources_follow_the_raw_naming_convention() -> None:
    source = mitxonline_app.build_source()
    assert "raw__mitxonline__app__postgres__b2b_organizationpage" in source.resources
    assert all(
        name.startswith("raw__mitxonline__app__postgres__") for name in source.resources
    )


def test_pipeline_targets_the_mitxonline_app_prefix() -> None:
    assert mitxonline_app.mitxonline_app_pipeline.pipeline_name == "mitxonline_app"
