"""Learn AI application-database ingestion via dlt.

Loads the Learn AI (chatbot) app database into QA raw (RFC 12711), the way
``mitxonline_app`` loads MITx Online's. QA had no ``raw__learn_ai__app__postgres__``
table, and ``tfact_chatbot_events`` and ``afact_learner_courserun_progress``
declare ``learn_ai/app_postgres`` as a QA branch. Production stays on the Airbyte
connection ``Learn AI Production → S3 Data Lake`` until the unit's dlt cutover.

Data flow:
    Learn AI RDS Postgres  ->  raw__learn_ai__app__postgres__<table>

Scope is the 5 tables the inventory unit ``learn_ai/app_postgres`` marks
``modeled: true``; the production connection syncs 28.

Every table is loaded with ``write_disposition="replace"`` and no cursor.
Production loads four of them as Airbyte append streams, so production raw holds
older copies of rows (368,222 extra rows of ``ai_chatbots_djangocheckpoint``'s
3,032,386 on 2026-11-10) that staging deduplicates. A replace load has one row
per key, and the inventory resolves the QA dedup column to ``_dlt_load_id``.

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

LEARN_AI_APP_SPEC = DatabaseSourceSpec(
    name="learn_ai_app",
    raw_table_prefix="raw__learn_ai__app__postgres__",
    database="learnai",
    vault_mount="postgres-learn-ai",
    tables=(
        DatabaseTable(
            name="ai_chatbots_chatresponserating", primary_key="checkpoint_id"
        ),
        # LangGraph checkpoints: `checkpoint` and `metadata` average 32 KB a row
        # (p99 269 KB, max 30 MB) in production raw (Athena, 2026-11-12), and
        # psycopg2 parses the jsonb into Python objects, so a default 50,000-row
        # batch ran the QA run pod out of its 8 GiB.
        DatabaseTable(
            name="ai_chatbots_djangocheckpoint", primary_key="id", chunk_size=1_000
        ),
        # `chat_json` averages 8.4 KB a row (p99 49 KB).
        DatabaseTable(
            name="ai_chatbots_tutorbotoutput", primary_key="id", chunk_size=5_000
        ),
        DatabaseTable(name="ai_chatbots_userchatsession", primary_key="id"),
        DatabaseTable(
            name="users_user", primary_key="id", excluded_columns=("password",)
        ),
    ),
)

learn_ai_app_pipeline = pipeline_for(LEARN_AI_APP_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the Learn AI app source (uniform entrypoint for Dagster)."""
    return build_database_source(LEARN_AI_APP_SPEC, tables=tables)
