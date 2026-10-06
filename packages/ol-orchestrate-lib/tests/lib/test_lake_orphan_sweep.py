"""Unit tests for ol_orchestrate.lib.lake_orphan_sweep."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from ol_orchestrate.lib.lake_orphan_sweep import (
    SweepResult,
    delete_prefix,
    is_referenced,
    normalize,
    referenced_paths,
    sweep_warehouse,
    warehouse_scan_targets,
)

NOW = datetime(2026, 10, 6, tzinfo=UTC)
OLD = NOW - timedelta(days=30)
YOUNG = NOW - timedelta(days=2)
UUID_A = "a" * 32
UUID_B = "b" * 32
UUID_C = "c" * 32
MIN_AGE_DAYS = 7
MART_QA_OBJECTS = 5


class _Paginator:
    def __init__(self, pages_for):
        self._pages_for = pages_for

    def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._pages_for(**kwargs)


class FakeGlue:
    """Glue with databases ``{name: location}`` and tables ``{database: [...]}``."""

    def __init__(
        self, databases: dict[str, str], tables: dict[str, list[dict[str, str]]]
    ) -> None:
        self.databases = databases
        self.tables = tables

    def get_paginator(self, operation: str) -> _Paginator:
        if operation == "get_databases":
            return _Paginator(self._databases)
        return _Paginator(self._tables)

    def _databases(self) -> list[dict[str, Any]]:
        return [
            {
                "DatabaseList": [
                    {"Name": name, "LocationUri": uri}
                    for name, uri in self.databases.items()
                ]
            }
        ]

    def _tables(self, **kwargs: str) -> list[dict[str, Any]]:
        return [
            {
                "TableList": [
                    {
                        "Name": table["name"],
                        "StorageDescriptor": {"Location": table.get("location", "")},
                        "Parameters": {
                            "metadata_location": table.get("metadata_location", "")
                        },
                    }
                    for table in self.tables.get(kwargs["DatabaseName"], [])
                ]
            }
        ]


class FakeS3:
    """S3 holding ``{bucket: {key: (size, last_modified)}}``."""

    def __init__(self, objects: dict[str, dict[str, tuple[int, datetime]]]) -> None:
        self.objects = objects
        self.delete_errors: set[str] = set()
        self.listed_prefixes: list[str] = []

    def get_paginator(self, operation: str) -> _Paginator:
        assert operation == "list_objects_v2"
        return _Paginator(self._list)

    def _list(self, **kwargs: str) -> list[dict[str, Any]]:
        bucket, prefix = kwargs["Bucket"], kwargs["Prefix"]
        keys = {k: v for k, v in self.objects[bucket].items() if k.startswith(prefix)}
        if "Delimiter" in kwargs:
            children = {
                prefix + key[len(prefix) :].split("/", 1)[0] + "/"
                for key in keys
                if "/" in key[len(prefix) :]
            }
            return [{"CommonPrefixes": [{"Prefix": p} for p in sorted(children)]}]
        self.listed_prefixes.append(f"{bucket}/{prefix}")
        return [
            {
                "Contents": [
                    {"Key": key, "Size": size, "LastModified": modified}
                    for key, (size, modified) in keys.items()
                ]
            }
        ]

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        errors = []
        for entry in kwargs["Delete"]["Objects"]:
            if entry["Key"] in self.delete_errors:
                errors.append(
                    {"Key": entry["Key"], "Code": "AccessDenied", "Message": "no"}
                )
                continue
            del self.objects[kwargs["Bucket"]][entry["Key"]]
        return {"Errors": errors}


def _lake() -> tuple[FakeGlue, FakeS3]:
    """Return a QA warehouse with one live table and one of each kind of orphan."""
    glue = FakeGlue(
        databases={
            "ol_warehouse_qa_mart": "s3://lake-mart-qa/",
            "ol_warehouse_qa_staging": "s3://lake-staging-qa/",
            # A developer schema: same name prefix, located under processed/.
            "ol_warehouse_qa_dev_staging": (
                "s3://lake-staging-qa/processed/ol_warehouse_qa_dev_staging"
            ),
            "ol_warehouse_production_mart": "s3://lake-mart-production/",
        },
        tables={
            "ol_warehouse_qa_mart": [
                {
                    "name": "live",
                    "location": f"s3://lake-mart-qa/live__dbt_tmp-{UUID_A}",
                },
                # Located only by its metadata pointer, under processed/.
                {
                    "name": "nested",
                    "metadata_location": (
                        "s3://lake-staging-qa/processed/ol_warehouse_qa_mart/"
                        f"nested-{UUID_A}/metadata/1.metadata.json"
                    ),
                },
                # An external table in a bucket the warehouse does not own.
                {"name": "foreign", "location": "s3://other-team/exports/foreign"},
            ],
        },
    )
    mart = "processed/ol_warehouse_qa_mart"
    dev = "processed/ol_warehouse_qa_dev_staging"
    s3 = FakeS3(
        {
            "lake-mart-qa": {
                f"live__dbt_tmp-{UUID_A}/data/1.parquet": (10, OLD),
                f"gone__dbt_tmp-{UUID_B}/data/1.parquet": (100, OLD),
                f"gone__dbt_tmp-{UUID_B}/metadata/1.json": (5, OLD),
                f"building__dbt_tmp-{UUID_C}/data/1.parquet": (7, YOUNG),
                "student_risk_probability/data/1.parquet": (50, OLD),
            },
            "lake-staging-qa": {
                f"{mart}/nested-{UUID_A}/data/1.parquet": (1, OLD),
                f"{mart}/dropped-{UUID_B}/data/1.parquet": (20, OLD),
                f"{dev}/mine-{UUID_B}/data/1.parquet": (30, OLD),
            },
            "lake-mart-production": {f"prod_orphan-{UUID_B}/data/1.parquet": (9, OLD)},
            "other-team": {f"exports/stray-{UUID_B}/1.parquet": (9, OLD)},
        }
    )
    return glue, s3


def _sweep(glue: FakeGlue, s3: FakeS3, *, delete: bool) -> SweepResult:
    return sweep_warehouse(
        glue,
        s3,
        warehouse_env="qa",
        min_age_days=MIN_AGE_DAYS,
        now=NOW,
        delete=delete,
    )


def test_normalize_accepts_only_s3_uris():
    assert normalize("s3a://bucket/a/b/") == "bucket/a/b"
    assert normalize("s3://bucket/") == "bucket"
    assert normalize("hdfs://bucket/a") == ""
    assert normalize("") == ""


def test_is_referenced_matches_ancestors_and_descendants_only():
    referenced = {"bucket/a/b"}
    assert is_referenced("bucket/a", referenced)
    assert is_referenced("bucket/a/b/c", referenced)
    assert not is_referenced("bucket/a/bb", referenced)


def test_a_root_located_database_does_not_claim_its_bucket():
    glue, _ = _lake()
    referenced = referenced_paths(glue, include_databases=True)
    assert not is_referenced(f"lake-mart-qa/gone__dbt_tmp-{UUID_B}", referenced)
    assert is_referenced(
        "lake-staging-qa/processed/ol_warehouse_qa_dev_staging/new_table", referenced
    )


def test_a_root_located_table_claims_its_bucket():
    glue, _ = _lake()
    glue.tables["ol_warehouse_qa_staging"] = [
        {"name": "everything", "location": "s3://lake-mart-qa/"}
    ]
    referenced = referenced_paths(glue, include_databases=True)
    assert is_referenced(f"lake-mart-qa/gone__dbt_tmp-{UUID_B}", referenced)


def test_scan_targets_are_the_deployed_warehouse_only():
    glue, _ = _lake()
    assert warehouse_scan_targets(glue, "qa") == [
        ("lake-mart-qa", ""),
        ("lake-staging-qa", ""),
        ("lake-staging-qa", "processed/ol_warehouse_qa_mart"),
    ]


def test_report_finds_orphans_without_deleting():
    glue, s3 = _lake()
    before = {bucket: dict(keys) for bucket, keys in s3.objects.items()}

    result = _sweep(glue, s3, delete=False)

    assert result.outcomes is None
    assert s3.objects == before
    assert {row["prefix"]: row["eligible"] for row in result.orphans} == {
        f"gone__dbt_tmp-{UUID_B}": True,
        f"building__dbt_tmp-{UUID_C}": False,
        f"processed/ol_warehouse_qa_mart/dropped-{UUID_B}": True,
    }
    gone = next(r for r in result.orphans if r["prefix"].startswith("gone"))
    assert (gone["objects"], gone["bytes"]) == (2, 105)


def test_unsuffixed_orphans_are_listed_and_never_measured():
    glue, s3 = _lake()

    result = _sweep(glue, s3, delete=False)

    assert "lake-mart-qa/student_risk_probability" in result.unsuffixed
    assert not any("student_risk_probability" in p for p in s3.listed_prefixes)


def test_delete_removes_only_old_suffixed_orphans_of_this_warehouse():
    glue, s3 = _lake()

    result = _sweep(glue, s3, delete=True)

    assert result.outcomes is not None
    assert {(o.prefix, o.action, o.objects, o.bytes) for o in result.outcomes} == {
        (f"gone__dbt_tmp-{UUID_B}", "deleted", 2, 105),
        (f"processed/ol_warehouse_qa_mart/dropped-{UUID_B}", "deleted", 1, 20),
    }
    assert sorted(s3.objects["lake-mart-qa"]) == [
        f"building__dbt_tmp-{UUID_C}/data/1.parquet",
        f"live__dbt_tmp-{UUID_A}/data/1.parquet",
        "student_risk_probability/data/1.parquet",
    ]
    assert sorted(s3.objects["lake-staging-qa"]) == [
        f"processed/ol_warehouse_qa_dev_staging/mine-{UUID_B}/data/1.parquet",
        f"processed/ol_warehouse_qa_mart/nested-{UUID_A}/data/1.parquet",
    ]
    assert len(s3.objects["lake-mart-production"]) == 1
    assert len(s3.objects["other-team"]) == 1


def _delete(s3: FakeS3, prefix: str, referenced: set[str], *, execute: bool = True):
    return delete_prefix(
        s3,
        "lake-mart-qa",
        prefix,
        referenced,
        min_age_days=MIN_AGE_DAYS,
        now=NOW,
        execute=execute,
    )


def test_delete_skips_a_prefix_registered_after_the_scan():
    _, s3 = _lake()
    prefix = f"gone__dbt_tmp-{UUID_B}"

    outcome = _delete(s3, prefix, {f"lake-mart-qa/{prefix}"})

    assert (outcome.action, outcome.reason) == ("skipped", "now referenced by Glue")
    assert len(s3.objects["lake-mart-qa"]) == MART_QA_OBJECTS


def test_delete_skips_a_prefix_written_to_since_the_scan():
    _, s3 = _lake()

    outcome = _delete(s3, f"building__dbt_tmp-{UUID_C}", set())

    assert outcome.action == "skipped"
    assert "too recent" in outcome.reason
    assert len(s3.objects["lake-mart-qa"]) == MART_QA_OBJECTS


@pytest.mark.parametrize(
    ("prefix", "reason"),
    [("", "empty prefix"), ("student_risk_probability", "no dbt uuid suffix")],
)
def test_delete_refuses_a_prefix_without_the_uuid_suffix(prefix: str, reason: str):
    _, s3 = _lake()

    outcome = _delete(s3, prefix, set())

    assert (outcome.action, outcome.reason) == ("refused", reason)
    assert len(s3.objects["lake-mart-qa"]) == MART_QA_OBJECTS


def test_delete_without_execute_changes_nothing():
    _, s3 = _lake()

    outcome = _delete(s3, f"gone__dbt_tmp-{UUID_B}", set(), execute=False)

    assert (outcome.action, outcome.objects) == ("would_delete", 2)
    assert len(s3.objects["lake-mart-qa"]) == MART_QA_OBJECTS


def test_delete_reports_per_key_errors():
    _, s3 = _lake()
    key = f"gone__dbt_tmp-{UUID_B}/data/1.parquet"
    s3.delete_errors.add(key)

    outcome = _delete(s3, f"gone__dbt_tmp-{UUID_B}", set())

    assert outcome.errors == [f"{key}: AccessDenied no"]
