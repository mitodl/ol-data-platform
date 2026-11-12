"""MIT Learn application-database ingestion via dlt.

Loads the MIT Learn app database into QA raw (RFC 12711), the way
``mitxonline_app`` loads MITx Online's. QA had no ``raw__mitlearn__app__postgres__``
table at all, and 44 dimensional models declare ``mitlearn/app_postgres`` as a
QA branch. Production stays on the Airbyte connection ``MIT Learn Production →
S3 Data Lake`` until the unit's dlt cutover.

Data flow:
    MIT Learn RDS Postgres  ->  raw__mitlearn__app__postgres__<table>

Scope is the 11 tables the inventory unit ``mitlearn/app_postgres`` marks
``modeled: true``; the production connection syncs 97.

Every table is loaded with ``write_disposition="replace"`` and no cursor, for
the reasons ``mitxonline_app`` gives. The largest, ``users_user`` and
``profiles_profile``, were about 924,000 rows each in production raw on
2026-11-10 (Athena), with 0 duplicate primary keys across all 11 tables.

``users_user.password`` is excluded: a Django password hash that no dbt model
selects.
"""

from typing import Any

from ol_dlt.database import (
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

MITLEARN_APP_SPEC = DatabaseSourceSpec(
    name="mitlearn_app",
    raw_table_prefix="raw__mitlearn__app__postgres__",
    database="mitopen",
    vault_mount="postgres-mitlearn",
    tables=(
        DatabaseTable(name="content_feedback_contentfeedback", primary_key="id"),
        DatabaseTable(
            name="learning_resources_learningresourcetopic", primary_key="id"
        ),
        DatabaseTable(
            name="learning_resources_learningresourcetopicmapping", primary_key="id"
        ),
        DatabaseTable(
            name="learning_resources_search_percolatequery", primary_key="id"
        ),
        DatabaseTable(
            name="learning_resources_search_percolatequery_users", primary_key="id"
        ),
        DatabaseTable(name="learning_resources_userlist", primary_key="id"),
        DatabaseTable(name="learning_resources_userlist_topics", primary_key="id"),
        DatabaseTable(name="learning_resources_userlistrelationship", primary_key="id"),
        DatabaseTable(name="profiles_profile", primary_key="id"),
        DatabaseTable(name="profiles_profile_topic_interests", primary_key="id"),
        DatabaseTable(
            name="users_user", primary_key="id", excluded_columns=("password",)
        ),
    ),
)

mitlearn_app_pipeline = pipeline_for(MITLEARN_APP_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the MIT Learn app source (uniform entrypoint for Dagster)."""
    return build_database_source(MITLEARN_APP_SPEC, tables=tables)
