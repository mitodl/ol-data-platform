"""Tests for the iceberg_raw_orphan_files and iceberg_dbt_layer_orphan_files assets.

Finding and deleting orphan files is tested in
`packages/ol-orchestrate-lib/tests/lib/test_iceberg_orphan_files.py`. What is
left here is the Dagster side: where deletion may run, and what a run reports.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from dagster import build_asset_context, instance_for_test
from lakehouse.assets import iceberg_orphan_files as module
from lakehouse.assets.iceberg_orphan_files import (
    IcebergOrphanFilesConfig,
    _run_cap_seconds,
    iceberg_raw_orphan_files,
)
from ol_orchestrate.lib.iceberg_orphan_files import (
    DatabaseOrphanFiles,
    TableOrphanFiles,
)

DATABASE = module.RAW_GLUE_DATABASE
MIN_AGE_DAYS = 7
DAGSTER_BUCKET = "dagster-data-qa"
TWO_DAYS_SECONDS = 2 * 24 * 60 * 60


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
    monkeypatch: pytest.MonkeyPatch,
    result: DatabaseOrphanFiles | Callable[[str], DatabaseOrphanFiles],
) -> list[dict[str, Any]]:
    """Fake the pass with one result, or one per database from a callable."""
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(module, "_aws_clients", lambda: ("glue", "s3"))

    def fake_pass(_glue: str, _catalog_factory: Any, _store: Any, **kwargs: Any):
        calls.append(kwargs)
        return result(kwargs["database"]) if callable(result) else result

    monkeypatch.setattr(module, "remove_database_orphan_files", fake_pass)
    return calls


def _metadata(config: IcebergOrphanFilesConfig) -> dict[str, Any]:
    output = iceberg_raw_orphan_files(build_asset_context(), config)
    return {key: value.value for key, value in output.metadata.items()}


def _enable_delete(
    monkeypatch: pytest.MonkeyPatch, run_cap_seconds: int = TWO_DAYS_SECONDS
) -> None:
    monkeypatch.setattr(
        module,
        "ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS",
        frozenset({module.DAGSTER_ENV}),
    )
    monkeypatch.setattr(module, "_run_cap_seconds", lambda _instance: run_cap_seconds)
    monkeypatch.setenv("DAGSTER_BUCKET_NAME", DAGSTER_BUCKET)


def test_deletes_in_qa_and_production_only():
    assert (
        frozenset({"qa", "production"})
        == module.ICEBERG_ORPHAN_FILES_DELETE_ENVIRONMENTS
    )


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


def test_delete_is_refused_below_the_scheduled_floor(monkeypatch: pytest.MonkeyPatch):
    _enable_delete(monkeypatch)
    calls = _returning(monkeypatch, _result())

    with pytest.raises(ValueError, match="at least 7"):
        iceberg_raw_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS - 1, delete=True),
        )

    assert calls == []


@pytest.mark.parametrize(
    "run_cap_seconds",
    [0, module.ICEBERG_ORPHAN_FILES_MIN_AGE_SECONDS],
    ids=["no cap", "cap at the floor"],
)
def test_delete_is_refused_without_a_run_cap_below_the_floor(
    monkeypatch: pytest.MonkeyPatch, run_cap_seconds: int
):
    _enable_delete(monkeypatch, run_cap_seconds=run_cap_seconds)
    calls = _returning(monkeypatch, _result())

    with pytest.raises(ValueError, match="cap runs below"):
        iceberg_raw_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True),
        )

    assert calls == []


@pytest.mark.parametrize(
    ("run_monitoring", "expected"),
    [
        ({"enabled": True, "max_runtime_seconds": TWO_DAYS_SECONDS}, TWO_DAYS_SECONDS),
        ({"enabled": False, "max_runtime_seconds": TWO_DAYS_SECONDS}, 0),
        ({"enabled": True}, 0),
    ],
    ids=["enabled with a cap", "cap but monitoring off", "monitoring without a cap"],
)
def test_run_cap_reads_the_instance(run_monitoring: dict[str, Any], expected: int):
    with instance_for_test(overrides={"run_monitoring": run_monitoring}) as instance:
        assert _run_cap_seconds(instance) == expected


def test_report_run_needs_no_run_cap(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result())

    metadata = _metadata(IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS))

    assert metadata["delete_status"] == "not run (report only)"


def test_report_run_says_delete_did_not_run(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DAGSTER_BUCKET_NAME", raising=False)
    calls = _returning(monkeypatch, _result())

    metadata = _metadata(IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS))

    assert calls[0]["database"] == DATABASE
    assert calls[0]["delete"] is False
    assert calls[0]["min_age_days"] == MIN_AGE_DAYS
    assert calls[0]["logger"] is not None
    assert calls[0]["record_deletion"] is None
    assert metadata["delete_status"] == "not run (report only)"
    assert "deletion_manifests" not in metadata
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
        f"{DATABASE}.raw__shared: directory overlaps with mart.other"
    ]
    assert metadata["unreadable_glue_databases"] == ["ol_warehouse_production_mart"]


def test_delete_run_reports_what_it_deleted(monkeypatch: pytest.MonkeyPatch):
    _enable_delete(monkeypatch)
    calls = _returning(monkeypatch, _result(tables=_tables(deleted=True)))

    metadata = _metadata(
        IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True)
    )

    assert calls[0]["delete"] is True
    manifests = calls[0]["record_deletion"]
    assert manifests.uri.startswith(
        f"s3://{DAGSTER_BUCKET}/{module.DELETION_MANIFEST_PREFIX}/"
    )
    assert metadata["deletion_manifests"] == manifests.uri
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
    assert metadata["failure_details"] == [f"{DATABASE}.ghost: NoSuchTable"]


def test_failures_over_the_threshold_fail_the_run(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result(failures=["a: boom", "b: boom"]))

    with pytest.raises(RuntimeError, match="failed for 2/5 tables"):
        iceberg_raw_orphan_files(
            build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
        )


def test_a_run_that_examined_no_table_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    _returning(monkeypatch, _result(tables=_tables()[2:] * 30))

    with pytest.raises(RuntimeError, match="None of the 30 Iceberg tables"):
        iceberg_raw_orphan_files(
            build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
        )

    assert f"{DATABASE}: 0 tables examined, 30 refused, 0 failed." in (
        capsys.readouterr().err
    )


def test_a_database_with_no_iceberg_tables_fails(monkeypatch: pytest.MonkeyPatch):
    _returning(monkeypatch, _result(tables=[]))

    with pytest.raises(RuntimeError, match="zero work"):
        iceberg_raw_orphan_files(
            build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
        )


def test_report_run_logs_its_totals(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    _returning(monkeypatch, _result())

    _metadata(IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS))

    assert (
        f"{DATABASE}: 2 tables examined, 1 refused, 0 failed. Orphan 100 objects "
        "(8000 bytes), eligible 90 objects (7000 bytes). Delete: not run (report "
        "only). Unreadable Glue databases: ['ol_warehouse_production_mart']."
    ) in capsys.readouterr().err


def test_a_failed_delete_run_still_logs_its_totals(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    _enable_delete(monkeypatch)
    tables = _tables(deleted=True, errors=["s3://lake/k: AccessDenied no"])
    _returning(monkeypatch, _result(tables=tables))

    with pytest.raises(RuntimeError, match="could not be deleted"):
        iceberg_raw_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True),
        )

    assert (
        "Delete: ran, 89 objects deleted, 7000 bytes sent for deletion, "
        f"error count 1, keys under s3://{DAGSTER_BUCKET}/"
        f"{module.DELETION_MANIFEST_PREFIX}/" in capsys.readouterr().err
    )


def test_a_delete_run_needs_the_dagster_bucket(monkeypatch: pytest.MonkeyPatch):
    _enable_delete(monkeypatch)
    monkeypatch.delenv("DAGSTER_BUCKET_NAME")
    calls = _returning(monkeypatch, _result(tables=_tables(deleted=True)))

    with pytest.raises(KeyError, match="DAGSTER_BUCKET_NAME"):
        iceberg_raw_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True),
        )

    assert calls == []


def _by_database(
    monkeypatch: pytest.MonkeyPatch, results: dict[str, DatabaseOrphanFiles]
) -> list[dict[str, Any]]:
    """Fake the pass per database; a database not in ``results`` has no tables."""
    return _returning(
        monkeypatch,
        lambda database: results.get(
            database,
            DatabaseOrphanFiles(
                database=database, tables=[], failures=[], unreadable_databases=[]
            ),
        ),
    )


def _dbt_table(database: str, table: str, eligible_bytes: int) -> TableOrphanFiles:
    return TableOrphanFiles(
        database=database,
        table=table,
        objects_listed=10,
        bytes_listed=eligible_bytes * 2,
        orphan_objects=4,
        orphan_bytes=eligible_bytes,
        eligible_objects=4,
        eligible_bytes=eligible_bytes,
    )


def test_dbt_layer_pass_deletes_only_in_qa():
    assert (
        frozenset({"qa"}) == module.ICEBERG_DBT_LAYER_ORPHAN_FILES_DELETE_ENVIRONMENTS
    )


def test_dbt_layer_pass_covers_the_dbt_layers_of_its_own_warehouse():
    prefix = f"ol_warehouse_{module.WAREHOUSE_ENV}_"
    assert all(db.startswith(prefix) for db in module.DBT_LAYER_GLUE_DATABASES)
    assert f"{prefix}staging" in module.DBT_LAYER_GLUE_DATABASES
    assert f"{prefix}raw" not in module.DBT_LAYER_GLUE_DATABASES
    assert f"{prefix}history" not in module.DBT_LAYER_GLUE_DATABASES


def _dbt_project_schemas(node: Any) -> set[str]:
    if not isinstance(node, dict):
        return set()
    found = {node["+schema"]} if "+schema" in node else set()
    for child in node.values():
        found |= _dbt_project_schemas(child)
    return found


def test_dbt_layers_match_the_dbt_project_schemas():
    """A new `+schema` folder has to be placed on one side or the other.

    Left out on purpose: history (raw snapshots that cannot be rebuilt) and the
    two b2b schemas, which are StarRocks default_catalog tables, not Iceberg
    tables in Glue.
    """
    project = yaml.safe_load(
        (Path(__file__).resolve().parents[3] / "src/ol_dbt/dbt_project.yml").read_text()
    )
    not_scanned = {"history", "b2b_analytics", "b2b_learner_records"}

    schemas = _dbt_project_schemas(project["models"]) | _dbt_project_schemas(
        project["snapshots"]
    )

    assert set(module.DBT_LAYERS) == schemas - not_scanned
    assert not_scanned <= schemas


def test_dbt_layer_delete_is_refused_where_it_is_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
):
    calls = _by_database(monkeypatch, {})

    with pytest.raises(ValueError, match="Deletion is not enabled"):
        module.iceberg_dbt_layer_orphan_files(
            build_asset_context(),
            IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS, delete=True),
        )

    assert calls == []


def test_dbt_layer_pass_totals_every_database(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """One failure in 22 tables stays under the threshold only counted across
    databases: intermediate alone (1 of 2) would fail the run.
    """
    staging, intermediate = module.DBT_LAYER_GLUE_DATABASES[:2]
    calls = _by_database(
        monkeypatch,
        {
            staging: DatabaseOrphanFiles(
                database=staging,
                tables=[_dbt_table(staging, f"stg__{i}", 100) for i in range(20)],
                failures=[],
                unreadable_databases=[],
            ),
            intermediate: DatabaseOrphanFiles(
                database=intermediate,
                tables=[_dbt_table(intermediate, "int__b", 5_000)],
                failures=["int__c: boom"],
                unreadable_databases=[],
            ),
        },
    )

    output = module.iceberg_dbt_layer_orphan_files(
        build_asset_context(), IcebergOrphanFilesConfig(min_age_days=MIN_AGE_DAYS)
    )
    metadata = {key: value.value for key, value in output.metadata.items()}

    assert [call["database"] for call in calls] == list(module.DBT_LAYER_GLUE_DATABASES)
    assert all(call["delete"] is False for call in calls)
    assert metadata["tables_examined"] == 21
    assert metadata["eligible_bytes"] == 7_000
    assert metadata["failure_count"] == 1
    assert metadata["failure_details"] == [f"{intermediate}.int__c: boom"]
    details = metadata["orphan_details"]
    assert (details[0]["database"], details[0]["table"]) == (intermediate, "int__b")
    assert {row["database"] for row in details[1:]} == {staging}
    assert metadata["delete_status"] == "not run (report only)"
    log = capsys.readouterr().err
    assert f"{staging}: 20 tables examined, 0 refused, 0 failed." in log
    assert f"{intermediate}: 1 tables examined, 0 refused, 1 failed." in log
    assert f"{module.DBT_LAYER_GLUE_DATABASES[-1]}: 0 tables examined" in log
