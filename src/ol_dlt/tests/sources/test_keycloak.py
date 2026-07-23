"""Tests for the Keycloak database source."""

from ol_dlt.sources import keycloak

# Tables that hold credential material or high-churn operational state. These
# must never appear in the spec; the assertion exists so adding one is a
# deliberate, reviewed act rather than an accident.
FORBIDDEN_TABLES = frozenset(
    {
        "credential",
        "component_config",
        "user_session",
        "offline_user_session",
        "offline_client_session",
        "admin_event_entity",
        "event_entity",
    }
)


def test_spec_excludes_credential_tables() -> None:
    selected = {table.name for table in keycloak.KEYCLOAK_SPEC.tables}
    assert not selected & FORBIDDEN_TABLES


def test_client_secret_columns_are_excluded() -> None:
    client = next(
        table for table in keycloak.KEYCLOAK_SPEC.tables if table.name == "client"
    )
    assert set(client.excluded_columns) == {"secret", "registration_token"}


def test_organization_membership_is_resolvable() -> None:
    """Orgs need their backing group tables, not a dedicated join table.

    Keycloak models organization membership as membership of the org's backing
    group (``org.group_id`` -> ``keycloak_group.id``), tagged by
    ``user_group_membership.membership_type``. Dropping either group table
    would leave `org` present but its members unresolvable, so pin all four
    together.
    """
    selected = {table.name for table in keycloak.KEYCLOAK_SPEC.tables}
    assert {
        "org",
        "org_domain",
        "keycloak_group",
        "user_group_membership",
    } <= selected


def test_every_table_declares_a_primary_key() -> None:
    assert all(table.primary_key for table in keycloak.KEYCLOAK_SPEC.tables)


def test_resources_follow_the_raw_naming_convention() -> None:
    source = keycloak.build_source()
    assert "raw__keycloak__app__postgres__user_entity" in source.resources
    assert all(
        name.startswith("raw__keycloak__app__postgres__") for name in source.resources
    )


def test_pipeline_targets_the_keycloak_prefix() -> None:
    assert keycloak.keycloak_pipeline.pipeline_name == "keycloak"
