"""Tests for ml.lib.redacted_asset."""

import polars as pl
import pyarrow as pa
import pytest
from dagster import AssetKey, materialize, mem_io_manager
from ml.lib import redact, redacted_asset
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


def test_redacted_asset_redacts_only_new_in_scope_rows_by_its_own_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A staging source keyed on one integer column is read from the staging
    database. Rows outside source_filter and rows already in the output are
    skipped, and each text column gets its own <column>_redacted column.
    """
    tables = {
        ("staging", "stg__zendesk__ticket_comment"): pl.LazyFrame(
            {
                "comment_id": [1, 2, 3],
                "ticket_platform": ["mitlearn", "mitlearn", "ocw"],
                "comment_body": ["Hi, I am PII", "already done", "out of scope"],
                "ticket_subject": ["Refund for PII", "Refund", "Login"],
            }
        ),
        ("intermediate", "zendesk_conversation_redacted"): pl.LazyFrame(
            {"comment_id": [2], "redaction_version": [redact.REDACTION_VERSION]}
        ),
    }
    monkeypatch.setattr(
        redacted_asset,
        "get_dbt_model_as_dataframe",
        lambda database_name, table_name: tables[
            (database_name.rsplit("_", 1)[-1], table_name)
        ],
    )
    monkeypatch.setattr(redacted_asset, "get_glue_catalog", lambda: None)
    monkeypatch.setattr(
        redact, "_redact_text", lambda value: value.replace("PII", "<PERSON>")
    )
    written: list[tuple[pl.DataFrame, list[str]]] = []
    monkeypatch.setattr(
        redact,
        "checkpoint_redacted_chunk",
        lambda _catalog, _table, chunk_df, key_cols: written.append(
            (chunk_df, key_cols)
        ),
    )
    asset_def = redacted_asset.build_redacted_asset(
        name="zendesk_conversation_redacted",
        source=AssetKey(["staging", "stg__zendesk__ticket_comment"]),
        key_cols=["comment_id"],
        text_cols=["comment_body", "ticket_subject"],
        code_version="test_v1",
        group_name="test",
        pool="test",
        source_filter=pl.col("ticket_platform") == "mitlearn",
    )

    result = materialize([asset_def], resources={"io_manager": mem_io_manager})

    assert result.success
    ((chunk_df, key_cols),) = written
    assert key_cols == ["comment_id"]
    assert chunk_df.to_dicts() == [
        {
            "comment_id": 1,
            "comment_body_redacted": "Hi, I am <PERSON>",
            "ticket_subject_redacted": "Refund for <PERSON>",
            "redaction_version": redact.REDACTION_VERSION,
        }
    ]


def test_key_filter_on_one_column_matches_only_the_batch_keys() -> None:
    table = pa.table({"comment_id": [1, 2, 3, 4]})
    schema = Schema(NestedField(1, "comment_id", LongType(), required=False))
    chunk_df = pl.DataFrame({"comment_id": [2, 4]})

    key_filter = bind(
        schema, redact._key_filter(chunk_df, ["comment_id"]), case_sensitive=True
    )

    assert table.filter(expression_to_pyarrow(key_filter)).to_pylist() == [
        {"comment_id": 2},
        {"comment_id": 4},
    ]
