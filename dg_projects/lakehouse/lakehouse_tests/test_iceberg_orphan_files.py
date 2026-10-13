"""Tests for the iceberg_raw_orphan_files asset.

Finding and deleting orphan files is tested in
`packages/ol-orchestrate-lib/tests/lib/test_iceberg_orphan_files.py`. What is
left here is the Dagster side: where deletion may run, and what a run reports.
"""

from typing import Any

import pytest
from dagster import build_asset_context
from lakehouse.assets import iceberg_orphan_files as module
from lakehouse.assets.iceberg_orphan_files import (
    IcebergOrphanFilesConfig,
    iceberg_raw_orphan_files,
)
from ol_orchestrate.lib.iceberg_orphan_files import (
    DatabaseOrphanFiles,
    TableOrphanFiles,
)

DATABASE = module.RAW_GLUE_DATABASE
MIN_AGE_DAYS = 7


def _tables(*, deleted: bool = False, errors: list[str] | None = None):
    return [
        TableOrphanFiles(
            database=DATABASE,
            table="raw__mitxonline__app__postgres__users_user",
            objects_listed=120,
            bytes_listed=9_000,
            orphan_objects=100,
            orphan_bytes=8_000,
            eligible_objects=90,
            eligible_bytes=7_000,
            deleted_objects=(90 - len(errors or [])) if deleted else None,
            deleted_bytes=7_000 if deleted else None,
            delete_errors=errors or [],
        ),
        TableOrphanFiles(
            database=DATABASE,
            table="raw__xpro__app__postgres__users_user",
            objects_listed=10,
            bytes_listed=500,
        ),
        TableOrphanFiles(
            database=DATABASE,
            table="raw__shared",
            refused="directory overlaps with mart.other",
        ),
    ]


def _result(**overrides: Any) -> DatabaseOrphanFiles:
    fields: dict[str, Any] = {
        "database": DATABASE,
        "tables": _tables(),
        "failures": [],
        "unreadable_databases": ["ol_warehouse_production_mart"],
    }
    return DatabaseOrphanFiles(**(fields | overrides))


def _returning(
    monkeypatch: pytest.MonkeyPatch, result: DatabaseOrphanFiles
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(module, "_aws_clients", lambda: ("glue", "s3"))

    def fake_pass(_glue: str, _catalog_factory: Any, _store: Any, **kwargs: Any):
        calls.append(kwargs)
        return result

    monkeypatch.setattr(module, "remove_database_orphan_files", fake_pass)
    return calls


def _metadata(config: IcebergOrphanFilesConfig) -> dict[str, Any]:
    output = iceberg_raw_orphan_files(build_asset_context(), config)
    return {key: value.value for key, value in output.metadata.items()}


def _enable_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module,
        "ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS",
        frozenset({module.DAGSTER_ENV}),
    )


def test_no_environment_deletes_yet():
    assert frozenset() == module.ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS


def test_config_has_no_default_minimum_age():
    with pytest.raises(Exception, match="min_age_days"):
        IcebergOrphanFilesConfig()


def test_delete_is_refused_where_it_is_not_enabled(monkeypatch: pytest.MonkeyPatch):
    calls = _returning(monkeypatch, _result())

    with pytest.raises(ValueError, match="Deletion is not enabled"):
        iceberg_raw_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True),
        )

    assert calls == []


def test_report_run_says_delete_did_not_run(monkeypatch: pytest.MonkeyPatch):
    calls = _returning(monkeypatch, _result())

    metadata = _metadata(IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS))

    assert calls[0]["database"] == DATABASE
    assert calls[0]["delete"] is False
    assert calls[0]["min_age_days"] == MIN_AGE_DAYS
    assert calls[0]["logger"] is not None
    assert metadata["delete_status"] == "not run (report only)"
    assert not [key for key in metadata if key.startswith("deleted_")]
    assert metadata["tables_examined"] == 2
    assert metadata["objects_listed"] == 130
    assert metadata["orphan_bytes"] == 8_000
    assert metadata["eligible_objects"] == 90
    assert metadata["eligible_bytes"] == 7_000
    assert [row["table"] for row in metadata["orphan_details"]] == [
        "raw__mitxonline__app__postgres__users_user"
    ]
    assert metadata["tables_refused"] == 1
    assert metadata["refused_details"] == [
        "raw__shared: directory overlaps with mart.other"
    ]
    assert metadata["unreadable_glue_databases"] == ["ol_warehouse_production_mart"]


def test_delete_run_reports_what_it_deleted(monkeypatch: pytest.MonkeyPatch):
    _enable_delete(monkeypatch)
    calls = _returning(monkeypatch, _result(tables=_tables(deleted=True)))

    metadata = _metadata(
        IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True)
    )

    assert calls[0]["delete"] is True
    assert metadata["delete_status"] == "ran"
    assert metadata["deleted_objects"] == 90
    assert metadata["deleted_bytes"] == 7_000
    assert metadata["delete_error_count"] == 0


def test_delete_errors_fail_the_run(monkeypatch: pytest.MonkeyPatch):
    _enable_delete(monkeypatch)
    tables = _tables(deleted=True, errors=["s3://lake/k: AccessDenied no"])
    _returning(monkeypatch, _result(tables=tables))

    with pytest.raises(RuntimeError, match="could not be deleted"):
        iceberg_raw_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True),
        )


def test_a_few_failed_tables_are_reported_and_do_not_fail_the_run(
    monkeypatch: pytest.MonkeyPatch,
):
    tables = _tables() * 10
    _returning(monkeypatch, _result(tables=tables, failures=["ghost: NoSuchTable"]))

    metadata = _metadata(IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS))

    assert metadata["failure_count"] == 1
    assert metadata["failure_details"] == ["ghost: NoSuchTable"]


def test_failures_over_the_threshold_fail_the_run(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result(failures=["a: boom", "b: boom"]))

    with pytest.raises(RuntimeError, match="failed for 2/5 tables"):
        iceberg_raw_orphan_files(
            build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
        )


def test_a_run_that_examined_no_table_fails(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result(tables=_tables()[2:] * 30))

    with pytest.raises(RuntimeError, match="None of the 30 Iceberg tables"):
        iceberg_raw_orphan_files(
            build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
        )


def test_a_database_with_no_iceberg_tables_fails(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result(tables=[]))

    with pytest.raises(RuntimeError, match="zero work"):
        iceberg_raw_orphan_files(
            build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
        )
