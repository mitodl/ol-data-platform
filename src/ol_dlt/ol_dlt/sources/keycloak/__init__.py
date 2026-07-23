"""Keycloak application-database ingestion via dlt.

First instance of the reusable ``ol_dlt.database`` source. Keycloak is the SSO
system of record for MIT Open Learning identities, so these tables are what let
the warehouse resolve a person across the applications that federate to it.

Data flow:
    Keycloak RDS Postgres  ->  raw__keycloak__app__postgres__<table>

The database read is aligned with the deployment: QA Dagster reads the QA
Keycloak database, production reads production, and local development reads the
``keycloak`` database in the local-dev CloudNativePG cluster (see
``ol_dlt.database``).

Scope is a curated identity subset rather than the whole Keycloak schema:
users, group membership, role assignment, organizations, and the realm/client
context needed to interpret them. The ~80 remaining tables are Keycloak's own
internal machinery (session, event, migration and policy bookkeeping) with no
downstream modelling value, and several of them hold credential material.
Notably absent, deliberately:

    credential            password hashes and OTP secrets
    component_config      identity-provider and LDAP bind secrets
    *_session, *_event    high-churn operational state, not identity facts

Every table is loaded with ``write_disposition="replace"``: Keycloak has no
reliable row-level modification timestamp to drive an incremental cursor
(``user_entity.created_timestamp`` only records creation, while emails, names
and enablement change in place), and the realm is small enough that a full
re-read per run is cheaper than reconciling missed updates.

Run standalone against local-dev (port-forward the CNPG cluster first):
    kubectl port-forward -n local-infra svc/local-pg-rw 5432:5432
    DLT_PROFILE=dev python -m ol_dlt.sources.keycloak
"""

from typing import Any

from ol_dlt.database import (
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

KEYCLOAK_SPEC = DatabaseSourceSpec(
    name="keycloak",
    raw_table_prefix="raw__keycloak__app__postgres__",
    database="keycloak",
    vault_mount="postgres-keycloak",
    tables=(
        # --- identities -----------------------------------------------------
        DatabaseTable(name="user_entity", primary_key="id"),
        DatabaseTable(name="user_attribute", primary_key="id"),
        DatabaseTable(
            name="federated_identity", primary_key=("identity_provider", "user_id")
        ),
        DatabaseTable(
            name="user_required_action", primary_key=("required_action", "user_id")
        ),
        # --- group membership -----------------------------------------------
        DatabaseTable(name="keycloak_group", primary_key="id"),
        DatabaseTable(name="group_attribute", primary_key="id"),
        DatabaseTable(
            name="user_group_membership", primary_key=("group_id", "user_id")
        ),
        # --- role assignment -------------------------------------------------
        DatabaseTable(name="keycloak_role", primary_key="id"),
        DatabaseTable(name="user_role_mapping", primary_key=("role_id", "user_id")),
        # --- organizations ---------------------------------------------------
        # Keycloak models organization membership as membership of the org's
        # backing group (org.group_id -> keycloak_group.id), distinguished by
        # user_group_membership.membership_type (MANAGED/UNMANAGED). Both of
        # those tables are already ingested above, so no join table is needed
        # here; org and org_domain supply the organization itself and the email
        # domains that route users to it.
        DatabaseTable(name="org", primary_key="id"),
        DatabaseTable(name="org_domain", primary_key=("id", "name")),
        # --- realm / client context -----------------------------------------
        DatabaseTable(name="realm", primary_key="id"),
        DatabaseTable(
            name="client",
            primary_key="id",
            # Client secrets and registration tokens are credentials; they have
            # no analytical use and must never land in the warehouse.
            excluded_columns=("secret", "registration_token"),
        ),
        DatabaseTable(name="identity_provider", primary_key="internal_id"),
    ),
)

keycloak_pipeline = pipeline_for(KEYCLOAK_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the Keycloak source (uniform entrypoint for Dagster)."""
    return build_database_source(KEYCLOAK_SPEC, tables=tables)
