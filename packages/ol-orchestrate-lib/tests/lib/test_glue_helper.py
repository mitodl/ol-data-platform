"""Tests for reading a dbt model as a dataframe."""

import json
from pathlib import Path

import pytest
from ol_orchestrate.lib import glue_helper
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe

ROWS = [{"readable_id": "a", "topics": None}, {"readable_id": "b", "topics": "x"}]


def _refuse_glue(database_name: str, table_name: str) -> None:
    msg = f"read {database_name}.{table_name} from Glue"
    raise AssertionError(msg)


def test_a_fixture_directory_replaces_the_glue_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In dev a fixture file named for the table is read and Glue is not."""
    (tmp_path / "some_model.jsonl").write_text(
        "\n".join(json.dumps(row) for row in ROWS)
    )
    monkeypatch.setenv("DBT_MODEL_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setattr(glue_helper, "DAGSTER_ENV", "dev")
    monkeypatch.setattr(glue_helper, "load_dbt_model_table", _refuse_glue)

    df = get_dbt_model_as_dataframe("any_database", "some_model").collect()

    assert df.to_dicts() == ROWS


def test_a_deployed_environment_ignores_the_fixture_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Outside dev the variable cannot put canned rows in front of an asset."""
    monkeypatch.setenv("DBT_MODEL_FIXTURE_DIR", str(tmp_path))
    monkeypatch.setattr(glue_helper, "DAGSTER_ENV", "production")
    monkeypatch.setattr(glue_helper, "load_dbt_model_table", _refuse_glue)

    with pytest.raises(AssertionError, match="from Glue"):
        get_dbt_model_as_dataframe("any_database", "some_model")
