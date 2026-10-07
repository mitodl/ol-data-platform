"""Unit tests for ol_orchestrate.lib.iceberg_orphan_files.

The table-level tests run against a real pyiceberg ``SqlCatalog`` on the local
filesystem, so what counts as reachable is decided by pyiceberg's own metadata
and manifests, not by a fake of them.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
from ol_orchestrate.lib import iceberg_orphan_files
from ol_orchestrate.lib.iceberg_orphan_files import (
    S3ObjectStore,
    StoredObject,
    file_id,
    overlapping_tables,
    reachable_files,
    remove_database_orphan_files,
    remove_table_orphan_files,
)
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

NOW = datetime(2026, 10, 7, tzinfo=UTC)
OLD = NOW - timedelta(days=30)
YOUNG = NOW - timedelta(days=2)
MIN_AGE_DAYS = 7
DATABASE = "raw"
TABLE = "raw__mitxonline__app__postgres__users_user"
ROWS = pa.table({"id": pa.array([1], type=pa.int64())})
LATEST = pa.table({"id": pa.array([7, 8], type=pa.int64())})


class LocalStore:
    """Object store over the local filesystem, with ages the test chooses."""

    def __init__(self) -> None:
        self.young: set[str] = set()
        self.deleted: list[str] = []
        self.refuse: set[str] = set()

    def list_objects(self, location: str) -> Iterator[StoredObject]:
        for path in sorted(Path(file_id(location)).rglob("*")):
            if path.is_file():
                uri = f"file://{path}"
                yield StoredObject(
                    uri, path.stat().st_size, YOUNG if uri in self.young else OLD
                )

    def delete_objects(self, uris: list[str]) -> list[str]:
        errors = []
        for uri in uris:
            if uri in self.refuse:
                errors.append(f"{uri}: AccessDenied")
                continue
            Path(file_id(uri)).unlink()
            self.deleted.append(uri)
        return errors


@pytest.fixture
def catalog(tmp_path: Path) -> SqlCatalog:
    catalog = SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace(DATABASE)
    return catalog


def _expired_table(catalog: SqlCatalog, name: str = TABLE) -> Table:
    """Return a table overwritten three times with all but its head expired.

    Each overwrite replaces the data file, so after expiry the earlier data
    files, manifests and manifest lists are on disk and in no metadata.
    """
    table = catalog.create_table(f"{DATABASE}.{name}", ROWS.schema)
    for _ in range(3):
        table.overwrite(ROWS)
    table.overwrite(LATEST)
    table.maintenance.expire_snapshots().older_than(datetime.now(tz=UTC)).commit()
    return catalog.load_table(f"{DATABASE}.{name}")


def _run(catalog: SqlCatalog, store: LocalStore, **overrides: Any):
    kwargs: dict[str, Any] = {
        "min_age_days": MIN_AGE_DAYS,
        "now": NOW,
        "delete": False,
    }
    return remove_table_orphan_files(
        catalog, DATABASE, TABLE, store, **(kwargs | overrides)
    )


def _files(table: Table) -> set[str]:
    return {
        f"file://{path}"
        for path in Path(file_id(table.location())).rglob("*")
        if path.is_file()
    }


class TestFileId:
    def test_s3_schemes_name_the_same_object(self) -> None:
        assert (
            file_id("s3://lake/t/data/a.parquet")
            == file_id("s3a://lake/t/data/a.parquet")
            == "lake/t/data/a.parquet"
        )

    def test_the_key_is_not_normalized(self) -> None:
        assert file_id("s3://lake/t/data/a b#c?.parquet ") == (
            "lake/t/data/a b#c?.parquet "
        )

    def test_a_path_without_a_scheme_is_returned_as_is(self) -> None:
        assert file_id("/tmp/t/a.parquet") == "/tmp/t/a.parquet"  # noqa: S108


class TestReachableFiles:
    def test_every_file_of_an_unexpired_table_is_reachable(
        self, catalog: SqlCatalog
    ) -> None:
        table = catalog.create_table(f"{DATABASE}.{TABLE}", ROWS.schema)
        for _ in range(3):
            table.overwrite(ROWS)

        reachable = reachable_files(table)

        assert {file_id(uri) for uri in _files(table)} == reachable.all
        assert file_id(table.metadata_location) in reachable.required
        assert reachable.history
        assert not reachable.history & reachable.required

    def test_a_cached_manifest_is_not_read_again(self, catalog: SqlCatalog) -> None:
        table = _expired_table(catalog)
        cache: dict[str, set[str]] = {}
        first = reachable_files(table, cache)
        poisoned = {path: {"marker"} for path in cache}

        second = reachable_files(table, poisoned)

        assert "marker" in second.required
        assert "marker" not in first.required


class TestRemoveTableOrphanFiles:
    def test_a_table_with_nothing_expired_has_no_orphans(
        self, catalog: SqlCatalog
    ) -> None:
        table = catalog.create_table(f"{DATABASE}.{TABLE}", ROWS.schema)
        table.overwrite(ROWS)
        table.overwrite(ROWS)

        result = _run(catalog, LocalStore())

        assert result.refused == ""
        assert result.objects_listed == len(_files(table))
        assert result.orphan_objects == 0

    def test_expired_snapshots_leave_orphans_and_a_report_deletes_nothing(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        before = _files(table)
        store = LocalStore()

        result = _run(catalog, store)

        assert result.orphan_objects > 0
        assert result.eligible_objects == result.orphan_objects
        assert result.eligible_bytes == result.orphan_bytes > 0
        assert result.deleted_objects is None
        assert result.deleted_bytes is None
        assert store.deleted == []
        assert _files(table) == before

    def test_a_delete_removes_exactly_the_orphans_and_the_table_still_reads(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        reachable = reachable_files(table).all
        store = LocalStore()

        result = _run(catalog, store, delete=True)

        assert result.deleted_objects == result.eligible_objects > 0
        assert result.delete_errors == []
        assert {file_id(uri) for uri in _files(table)} == reachable
        reloaded = catalog.load_table(f"{DATABASE}.{TABLE}")
        assert reloaded.scan().to_arrow().column("id").to_pylist() == [7, 8]
        assert _run(catalog, store).orphan_objects == 0

    def test_a_young_orphan_is_counted_and_left_alone(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        in_flight = Path(file_id(table.location())) / "data" / "uncommitted.parquet"
        in_flight.write_bytes(b"not yet committed")
        store = LocalStore()
        store.young = {f"file://{in_flight}"}

        result = _run(catalog, store, delete=True)

        assert result.orphan_objects == result.eligible_objects + 1
        assert in_flight.exists()
        assert f"file://{in_flight}" not in store.deleted

    def test_an_old_file_from_a_write_that_never_committed_is_removed(
        self, catalog: SqlCatalog
    ) -> None:
        table = catalog.create_table(f"{DATABASE}.{TABLE}", ROWS.schema)
        table.append(ROWS)
        stray = Path(file_id(table.location())) / "data" / "abandoned.parquet"
        stray.write_bytes(b"abandoned")

        result = _run(catalog, LocalStore(), delete=True)

        assert result.deleted_objects == 1
        assert not stray.exists()

    def test_a_file_that_becomes_referenced_before_the_delete_is_kept(
        self, catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _expired_table(catalog)
        real = reachable_files
        rescued: list[str] = []

        def committed_meanwhile(table: Table, cache: dict[str, set[str]] | None = None):
            found = real(table, cache)
            if rescued:
                return iceberg_orphan_files.ReachableFiles(
                    required=found.required | {file_id(rescued[0])},
                    history=found.history,
                )
            orphans = _files(table) - {f"file://{path}" for path in found.all}
            rescued.append(sorted(orphans)[0])
            return found

        monkeypatch.setattr(
            iceberg_orphan_files, "reachable_files", committed_meanwhile
        )
        store = LocalStore()

        result = _run(catalog, store, delete=True)

        assert rescued[0] not in store.deleted
        assert Path(file_id(rescued[0])).exists()
        assert result.deleted_objects == result.eligible_objects - 1

    def test_a_referenced_file_missing_from_the_listing_refuses_the_table(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        live = next(iter(table.scan().plan_files())).file.file_path
        Path(file_id(live)).unlink()
        store = LocalStore()

        result = _run(catalog, store, delete=True)

        assert "1 referenced files are not in the listing" in result.refused
        assert result.orphan_objects == 0
        assert result.deleted_objects is None
        assert store.deleted == []

    def test_a_location_other_than_the_one_checked_for_overlap_is_refused(
        self, catalog: SqlCatalog
    ) -> None:
        _expired_table(catalog)
        store = LocalStore()

        result = _run(
            catalog, store, delete=True, expected_location="s3://lake-raw/elsewhere"
        )

        assert "differs from the Glue location" in result.refused
        assert store.deleted == []

    def test_a_trailing_slash_on_the_expected_location_still_matches(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)

        result = _run(catalog, LocalStore(), expected_location=f"{table.location()}/")

        assert result.refused == ""

    def test_refused_deletes_are_reported_and_not_counted(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        store = LocalStore()
        orphans = _files(table) - {
            f"file://{path}" for path in reachable_files(table).all
        }
        store.refuse = {sorted(orphans)[0]}

        result = _run(catalog, store, delete=True)

        assert len(result.delete_errors) == 1
        assert result.deleted_objects == len(orphans) - 1

    @pytest.mark.parametrize("min_age_days", [0, -1])
    def test_a_floor_under_one_day_is_rejected(
        self, catalog: SqlCatalog, min_age_days: int
    ) -> None:
        _expired_table(catalog)
        with pytest.raises(ValueError, match="min_age_days"):
            _run(catalog, LocalStore(), min_age_days=min_age_days)


def _glue_row(
    database: str, table: str, location: str, pointer: str = ""
) -> dict[str, str]:
    return {
        "database": database,
        "table": table,
        "location": location,
        "metadata_location": pointer,
        "updated": "",
    }


class TestOverlappingTables:
    def test_sibling_directories_do_not_overlap(self) -> None:
        tables = [
            _glue_row("raw", "users", "s3://lake/users", "s3://lake/users/metadata/1"),
            _glue_row("raw", "users_x", "s3://lake/users_x/"),
        ]
        assert overlapping_tables(tables) == {}

    def test_a_table_inside_another_tables_directory_flags_both(self) -> None:
        tables = [
            _glue_row("raw", "outer", "s3://lake/outer"),
            _glue_row("mart", "inner", "s3a://lake/outer/inner"),
        ]
        assert overlapping_tables(tables) == {
            ("raw", "outer"): "mart.inner",
            ("mart", "inner"): "raw.outer",
        }

    def test_two_tables_at_one_location_flag_each_other(self) -> None:
        tables = [
            _glue_row("raw", "a", "s3://lake/shared"),
            _glue_row("raw", "b", "s3://lake/shared/"),
        ]
        assert set(overlapping_tables(tables)) == {("raw", "a"), ("raw", "b")}

    def test_a_metadata_pointer_inside_another_table_counts(self) -> None:
        tables = [
            _glue_row("raw", "a", "s3://lake/a"),
            _glue_row("raw", "b", "s3://lake/b", "s3://lake/a/metadata/9.json"),
        ]
        assert set(overlapping_tables(tables)) == {("raw", "a"), ("raw", "b")}

    def test_a_location_stored_with_a_stray_newline_still_claims_its_directory(
        self,
    ) -> None:
        tables = [
            _glue_row("raw", "outer", "s3://lake/outer\n"),
            _glue_row("raw", "inner", "s3://lake/outer/inner"),
        ]
        assert set(overlapping_tables(tables)) == {("raw", "outer"), ("raw", "inner")}

    def test_a_table_with_only_a_pointer_claims_the_directory_above_metadata(
        self,
    ) -> None:
        tables = [
            _glue_row("raw", "a", "", "s3://lake/a/metadata/9.json"),
            _glue_row("raw", "b", "s3://lake/a/nested"),
        ]
        assert set(overlapping_tables(tables)) == {("raw", "a"), ("raw", "b")}


class _Paginator:
    def __init__(self, pages_for) -> None:
        self._pages_for = pages_for

    def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self._pages_for(**kwargs)


class FakeS3:
    def __init__(self, objects: dict[str, int], refuse: set[str] | None = None) -> None:
        self.objects = objects
        self.refuse = refuse or set()
        self.list_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    def get_paginator(self, _operation: str) -> _Paginator:
        return _Paginator(self._list)

    def _list(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.list_calls.append(kwargs)
        return [
            {
                "Contents": [
                    {"Key": key, "Size": size, "LastModified": OLD}
                    for key, size in self.objects.items()
                    if key.startswith(kwargs["Prefix"])
                ]
            }
        ]

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        self.delete_calls.append(kwargs)
        return {
            "Errors": [
                {"Key": obj["Key"], "Code": "AccessDenied", "Message": "no"}
                for obj in kwargs["Delete"]["Objects"]
                if obj["Key"] in self.refuse
            ]
        }


class TestS3ObjectStore:
    def test_listing_stops_at_the_directory_boundary(self) -> None:
        s3 = FakeS3({"users/data/a.parquet": 3, "users_x/data/b.parquet": 5})

        found = list(S3ObjectStore(s3).list_objects("s3a://lake/users"))
        list(S3ObjectStore(s3).list_objects("s3://lake/users/"))

        assert s3.list_calls == [{"Bucket": "lake", "Prefix": "users/"}] * 2
        assert found == [StoredObject("s3://lake/users/data/a.parquet", 3, OLD)]

    def test_deletes_are_batched_per_bucket_and_errors_returned(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(iceberg_orphan_files, "S3_DELETE_BATCH_SIZE", 2)
        s3 = FakeS3({}, refuse={"t/b"})

        errors = S3ObjectStore(s3).delete_objects(
            ["s3://lake/t/a", "s3://lake/t/b", "s3://lake/t/c", "s3://other/t/d"]
        )

        assert [
            (call["Bucket"], [o["Key"] for o in call["Delete"]["Objects"]])
            for call in s3.delete_calls
        ] == [("lake", ["t/a", "t/b"]), ("lake", ["t/c"]), ("other", ["t/d"])]
        assert errors == ["s3://lake/t/b: AccessDenied no"]


class FakeGlue:
    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        self.tables = tables
        self.denied: set[str] = set()

    def get_paginator(self, operation: str) -> _Paginator:
        if operation == "get_databases":
            return _Paginator(
                lambda: [{"DatabaseList": [{"Name": name} for name in self.tables]}]
            )
        return _Paginator(self._tables)

    def _tables(self, **kwargs: str) -> list[dict[str, Any]]:
        if kwargs["DatabaseName"] in self.denied:
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                "GetTables",
            )
        return [{"TableList": self.tables[kwargs["DatabaseName"]]}]


def _glue_table(table: Table, name: str) -> dict[str, Any]:
    return {
        "Name": name,
        "StorageDescriptor": {"Location": table.location()},
        "Parameters": {"metadata_location": table.metadata_location},
    }


class TestRemoveDatabaseOrphanFiles:
    def _run(self, glue: FakeGlue, catalog: SqlCatalog, **overrides: Any):
        kwargs: dict[str, Any] = {
            "database": DATABASE,
            "min_age_days": MIN_AGE_DAYS,
            "now": NOW,
            "delete": False,
            "workers": 2,
        }
        return remove_database_orphan_files(
            glue, lambda: catalog, LocalStore(), **(kwargs | overrides)
        )

    def test_only_the_named_databases_iceberg_tables_are_examined(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        glue = FakeGlue(
            {
                DATABASE: [
                    _glue_table(table, TABLE),
                    {
                        "Name": "hive_table",
                        "StorageDescriptor": {"Location": "s3://x/h"},
                    },
                ],
                "mart": [
                    {
                        "Name": "elsewhere",
                        "StorageDescriptor": {"Location": "s3://mart/elsewhere"},
                        "Parameters": {
                            "metadata_location": "s3://mart/elsewhere/metadata/1"
                        },
                    }
                ],
            }
        )

        result = self._run(glue, catalog)

        assert [t.table for t in result.examined] == [TABLE]
        assert result.examined[0].eligible_objects > 0
        assert result.refused == []
        assert result.failures == []

    def test_an_overlapping_table_is_refused_without_being_listed(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        nested = {
            "Name": "nested",
            "StorageDescriptor": {"Location": f"{table.location()}/data/nested"},
            "Parameters": {},
        }
        glue = FakeGlue({DATABASE: [_glue_table(table, TABLE)], "mart": [nested]})

        result = self._run(glue, catalog, delete=True)

        assert result.examined == []
        assert result.refused[0].refused == "directory overlaps with mart.nested"

    def test_a_table_that_cannot_be_loaded_is_a_failure_and_the_rest_carry_on(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        ghost = {
            "Name": "ghost",
            "StorageDescriptor": {"Location": "s3://lake/ghost"},
            "Parameters": {"metadata_location": "s3://lake/ghost/metadata/1.json"},
        }
        glue = FakeGlue({DATABASE: [ghost, _glue_table(table, TABLE)]})

        result = self._run(glue, catalog)

        assert [t.table for t in result.examined] == [TABLE]
        assert len(result.failures) == 1
        assert result.failures[0].startswith("ghost: ")

    def test_a_row_with_only_a_pointer_is_held_to_the_directory_above_metadata(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        pointer_only = {
            "Name": TABLE,
            "Parameters": {"metadata_location": table.metadata_location},
        }
        elsewhere = {
            "Name": TABLE,
            "Parameters": {"metadata_location": "s3://lake/other/metadata/1.json"},
        }

        examined = self._run(FakeGlue({DATABASE: [pointer_only]}), catalog)
        refused = self._run(FakeGlue({DATABASE: [elsewhere]}), catalog, delete=True)

        assert examined.examined[0].eligible_objects > 0
        assert "differs from the Glue location" in refused.refused[0].refused
        assert _files(table) == _files(catalog.load_table(f"{DATABASE}.{TABLE}"))

    def test_a_row_that_names_no_directory_is_refused(
        self, catalog: SqlCatalog
    ) -> None:
        _expired_table(catalog)
        shapeless = {
            "Name": TABLE,
            "Parameters": {"metadata_location": "s3://lake/1.metadata.json"},
        }

        result = self._run(FakeGlue({DATABASE: [shapeless]}), catalog, delete=True)

        assert result.examined == []
        assert result.refused[0].refused == "Glue names no directory for the table"

    def test_a_denied_database_stops_a_delete_and_is_named_by_a_report(
        self, catalog: SqlCatalog
    ) -> None:
        table = _expired_table(catalog)
        glue = FakeGlue({DATABASE: [_glue_table(table, TABLE)], "production": []})
        glue.denied = {"production"}

        report = self._run(glue, catalog)
        assert report.unreadable_databases == ["production"]
        assert report.examined[0].eligible_objects > 0

        with pytest.raises(RuntimeError, match="Refusing to delete"):
            self._run(glue, catalog, delete=True)
        assert _files(table) == _files(catalog.load_table(f"{DATABASE}.{TABLE}"))
