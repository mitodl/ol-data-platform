"""Find and remove files inside a live Iceberg table's directory that it no longer uses.

pyiceberg's ``expire_snapshots`` commits a metadata update and deletes nothing
(0.12.0: ``ExpireSnapshots._commit`` is a single ``RemoveSnapshotsUpdate``). The
data files, manifests and manifest lists that only an expired snapshot
referenced stay in S3 with nothing pointing at them.
``ol_orchestrate.lib.lake_orphan_sweep`` does not reach them either: it deletes
whole directories that no Glue table references, and these sit inside a
directory one does.

This module is the per-table pass. For one table it collects every file the
table's current metadata can reach, across all retained snapshots on every
branch and tag, lists the table's directory, and treats what is listed and not
reachable as an orphan. That covers files left by expired snapshots (including
the ones expired before this existed, which no longer appear in any metadata)
and files left by writes that never committed.

A writer puts its files in the directory before it commits them, so a young
unreferenced file may be about to become live. Only an orphan older than
``min_age_days`` is eligible.

Every function takes its clients as arguments, so the callers decide how they
are built and the tests pass fakes.
"""

import logging
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from pyiceberg.catalog import Catalog
from pyiceberg.manifest import ManifestFile, read_manifest_list
from pyiceberg.table import Table

from ol_orchestrate.lib.lake_orphan_sweep import (
    S3_DELETE_BATCH_SIZE,
    glue_tables,
    refuse_to_delete_blind,
    require_minimum_age,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredObject:
    """One object found under a table's directory."""

    uri: str
    size: int
    last_modified: datetime


class ObjectStore(Protocol):
    """The two storage operations the pass needs."""

    def list_objects(self, location: str) -> Iterator[StoredObject]:
        """Yield every object under the directory ``location``."""
        ...

    def delete_objects(self, uris: list[str]) -> list[str]:
        """Delete ``uris`` and return one message per object that was refused."""
        ...


class S3ObjectStore:
    """:class:`ObjectStore` over a boto3 S3 client."""

    def __init__(self, s3: Any) -> None:
        """Wrap ``s3``, a boto3 S3 client."""
        self._s3 = s3

    def list_objects(self, location: str) -> Iterator[StoredObject]:
        """Yield every current object version under ``location``."""
        bucket, _, key = _directory(location).partition("/")
        pages = self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{key}/"
        )
        for page in pages:
            for obj in page.get("Contents", []):
                yield StoredObject(
                    f"s3://{bucket}/{obj['Key']}", obj["Size"], obj["LastModified"]
                )

    def delete_objects(self, uris: list[str]) -> list[str]:
        """Delete ``uris`` in batches and return what S3 refused."""
        errors: list[str] = []
        by_bucket: dict[str, list[str]] = {}
        for uri in uris:
            bucket, _, key = file_id(uri).partition("/")
            by_bucket.setdefault(bucket, []).append(key)
        for bucket, keys in by_bucket.items():
            for start in range(0, len(keys), S3_DELETE_BATCH_SIZE):
                batch = keys[start : start + S3_DELETE_BATCH_SIZE]
                response = self._s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
                )
                errors += [
                    f"s3://{bucket}/{error['Key']}: {error['Code']} {error['Message']}"
                    for error in response.get("Errors", [])
                ]
        return errors


def file_id(uri: str) -> str:
    """Return ``uri`` without its scheme, and otherwise untouched.

    Iceberg metadata written by different engines names one object as
    ``s3://``, ``s3a://`` or ``s3n://``. Nothing else is normalized: a key is
    compared byte for byte, because a referenced file that fails to match its
    own listing entry would be read as an orphan.
    """
    scheme, found, rest = uri.partition("://")
    return rest if found and "/" not in scheme else uri


def _directory(uri: str) -> str:
    return file_id(uri).rstrip("/")


def glue_directory(row: dict[str, str]) -> str:
    """Return the directory a Glue table row claims, or "" when it names none.

    The Glue location, or the parent of ``metadata/`` in the metadata pointer
    for an entry that carries only the pointer. Surrounding whitespace is
    dropped, as the sweep's ``normalize`` does, so a location stored with a
    stray newline still claims the directory it names.
    """
    if directory := _directory(row["location"].strip()):
        return directory
    return file_id(row["metadata_location"].strip()).rpartition("/metadata/")[0]


@dataclass(frozen=True)
class ReachableFiles:
    """What a table's metadata points at, as :func:`file_id` values."""

    # The table cannot be read without these: the current metadata file,
    # manifest lists, manifests, data and delete files, statistics files.
    required: set[str]
    # Earlier metadata files named in the metadata log. Kept, but a missing one
    # does not make the table unreadable.
    history: set[str]

    @property
    def all(self) -> set[str]:
        """Return everything a listing must not treat as an orphan."""
        return self.required | self.history


def reachable_files(
    table: Table, manifest_cache: dict[str, set[str]] | None = None
) -> ReachableFiles:
    """Return every file ``table``'s current metadata can reach.

    Walks every snapshot left in the metadata, whichever ref holds it. An entry
    a manifest marks DELETED counts as reachable: the file is not needed, but
    keeping it only postpones its removal until that manifest is itself
    unreachable.

    :param manifest_cache: Manifest path to the files it names. Pass the same
        dict to a second call for the same table and only manifests added in
        between are read.
    """
    cache = manifest_cache if manifest_cache is not None else {}
    metadata = table.metadata
    required = {file_id(table.metadata_location)}
    required |= {file_id(stats.statistics_path) for stats in metadata.statistics}
    required |= {
        file_id(stats.statistics_path) for stats in metadata.partition_statistics
    }
    manifests: dict[str, ManifestFile] = {}
    for snapshot in metadata.snapshots:
        required.add(file_id(snapshot.manifest_list))
        # Not Snapshot.manifests(): it caches every ManifestFile process-wide,
        # and this walks thousands of tables in one process.
        for manifest in read_manifest_list(table.io.new_input(snapshot.manifest_list)):
            manifests[manifest.manifest_path] = manifest
    for path, manifest in manifests.items():
        required.add(file_id(path))
        if path not in cache:
            cache[path] = {
                file_id(entry.data_file.file_path)
                for entry in manifest.fetch_manifest_entry(
                    table.io, discard_deleted=False
                )
            }
        required |= cache[path]
    history = {file_id(entry.metadata_file) for entry in metadata.metadata_log}
    return ReachableFiles(required=required, history=history - required)


@dataclass
class TableOrphanFiles:
    """What the pass found, and did, for one table."""

    database: str
    table: str
    location: str = ""
    # Why the table was left alone. "" when it was examined.
    refused: str = ""
    objects_listed: int = 0
    bytes_listed: int = 0
    # Unreferenced at any age.
    orphan_objects: int = 0
    orphan_bytes: int = 0
    # Unreferenced and older than the floor.
    eligible_objects: int = 0
    eligible_bytes: int = 0
    # None when nothing was attempted, which is not the same as zero.
    deleted_objects: int | None = None
    # Bytes in the objects a delete was sent for, refused ones included. In a
    # versioned bucket they stay billed as noncurrent versions until the
    # bucket's lifecycle rule expires them.
    deleted_bytes: int | None = None
    delete_errors: list[str] = field(default_factory=list)


def _location_refusal(location: str, expected_location: str | None) -> str:
    """Return why a table at ``location`` must not be listed, or ""."""
    root = _directory(location)
    if "/" not in root.strip("/"):
        return f"location {location!r} is not a directory in a bucket"
    if expected_location is not None and root != _directory(expected_location):
        return (
            f"Iceberg location {location} differs from the Glue location "
            f"{expected_location}"
        )
    return ""


def remove_table_orphan_files(  # noqa: PLR0913
    catalog: Catalog,
    database: str,
    table_name: str,
    store: ObjectStore,
    *,
    min_age_days: int,
    now: datetime,
    delete: bool,
    expected_location: str | None = None,
    logger: logging.Logger = log,
) -> TableOrphanFiles:
    """Report, and optionally delete, one table's orphan files.

    :param min_age_days: An unreferenced file younger than this is counted and
        left alone. At least 1.
    :param delete: Delete the eligible orphans. The table is loaded again
        first, and anything its metadata reaches by then is dropped from the
        list.
    :param expected_location: The directory the caller checked for overlap with
        other tables. The table is refused when its Iceberg location is a
        different one, because that check then says nothing about what is
        about to be listed.
    """
    require_minimum_age(min_age_days)
    result = TableOrphanFiles(database=database, table=table_name)
    identifier = f"{database}.{table_name}"
    table = catalog.load_table(identifier)
    result.location = table.location()
    root = _directory(result.location)
    if refusal := _location_refusal(result.location, expected_location):
        result.refused = refusal
        return result

    manifest_cache: dict[str, set[str]] = {}
    reachable = reachable_files(table, manifest_cache)

    cutoff = now - timedelta(days=min_age_days)
    listed: set[str] = set()
    eligible: dict[str, StoredObject] = {}
    kept = reachable.all
    for obj in store.list_objects(result.location):
        key = file_id(obj.uri)
        listed.add(key)
        result.objects_listed += 1
        result.bytes_listed += obj.size
        if key in kept:
            continue
        result.orphan_objects += 1
        result.orphan_bytes += obj.size
        if obj.last_modified <= cutoff:
            eligible[key] = obj

    # A required file under the directory that the listing did not return means
    # the paths in the metadata and the keys in the listing are not comparable
    # (or the table is already broken). Either way an "orphan" here proves
    # nothing.
    missing = sorted(
        path
        for path in reachable.required
        if path.startswith(f"{root}/") and path not in listed
    )
    if missing:
        result.refused = (
            f"{len(missing)} referenced files are not in the listing of "
            f"{result.location}, e.g. {missing[0]}"
        )
        result.orphan_objects = result.orphan_bytes = 0
        return result

    result.eligible_objects = len(eligible)
    result.eligible_bytes = sum(obj.size for obj in eligible.values())
    if not delete:
        return result

    for path in reachable_files(catalog.load_table(identifier), manifest_cache).all:
        eligible.pop(path, None)
    doomed = list(eligible.values())
    size = sum(obj.size for obj in doomed)
    # Before the first batch, so a run that dies part way still names the table.
    logger.info(
        "Deleting %d orphan files (%d bytes) under %s",
        len(doomed),
        size,
        result.location,
    )
    result.delete_errors = store.delete_objects([obj.uri for obj in doomed])
    result.deleted_objects = len(doomed) - len(result.delete_errors)
    result.deleted_bytes = size
    return result


def overlapping_tables(tables: Iterable[dict[str, str]]) -> dict[tuple[str, str], str]:
    """Return the Glue tables whose directory shares files with another table's.

    A table's directory is :func:`glue_directory`. Two tables overlap when
    one's directory or metadata pointer equals or lies inside the other's
    directory. Listing either would then return the other's live files, which
    its own metadata does not reference.

    This compares directories, not references. A table located elsewhere whose
    manifests name files inside another table's directory (``add_files``, a
    migrate or snapshot procedure) is not seen. Nothing in this repository
    writes one.

    :returns: ``(database, table)`` to the other table it overlaps with.
    """
    claims: list[tuple[str, tuple[str, str]]] = []
    for table in tables:
        owner = (table["database"], table["table"])
        directory = glue_directory(table)
        pointer = file_id(table["metadata_location"].strip())
        claims.extend((f"{claim}/", owner) for claim in {directory, pointer} - {""})
    claims.sort()
    overlaps: dict[tuple[str, str], str] = {}
    for index, (claim, owner) in enumerate(claims):
        for other_claim, other in claims[index + 1 :]:
            if not other_claim.startswith(claim):
                break
            if other != owner:
                overlaps.setdefault(owner, ".".join(other))
                overlaps.setdefault(other, ".".join(owner))
    return overlaps


@dataclass(frozen=True)
class DatabaseOrphanFiles:
    """One pass over every Iceberg table in a Glue database."""

    database: str
    tables: list[TableOrphanFiles]
    # "<table>: <error>" for each table the pass raised on.
    failures: list[str]
    # Databases Glue denied this caller. Their tables were not checked for
    # overlap with this database's.
    unreadable_databases: list[str]

    @property
    def examined(self) -> list[TableOrphanFiles]:
        """Return the tables whose directory was listed and compared."""
        return [table for table in self.tables if not table.refused]

    @property
    def refused(self) -> list[TableOrphanFiles]:
        """Return the tables left alone, each with its reason."""
        return [table for table in self.tables if table.refused]


def remove_database_orphan_files(  # noqa: PLR0913
    glue: Any,
    catalog_factory: Callable[[], Catalog],
    store: ObjectStore,
    *,
    database: str,
    min_age_days: int,
    now: datetime,
    delete: bool,
    workers: int,
    logger: logging.Logger = log,
) -> DatabaseOrphanFiles:
    """Run :func:`remove_table_orphan_files` over a Glue database's Iceberg tables.

    :param catalog_factory: Called once per worker thread. A pyiceberg catalog
        and its FileIO must not be shared between threads.
    :param delete: Refused when Glue denies this caller any database: a table
        in one of them could be located inside a directory this pass lists.
    """
    require_minimum_age(min_age_days)
    unreadable: list[str] = []
    every_table = glue_tables(glue, unreadable=unreadable)
    if delete:
        refuse_to_delete_blind(unreadable)
    overlaps = overlapping_tables(every_table)
    work = [
        row
        for row in every_table
        if row["database"] == database and row["metadata_location"]
    ]
    local = threading.local()

    def process(row: dict[str, str]) -> TableOrphanFiles:
        name = row["table"]
        if other := overlaps.get((database, name)):
            return TableOrphanFiles(
                database=database,
                table=name,
                location=row["location"],
                refused=f"directory overlaps with {other}",
            )
        # The overlap check covered the directory Glue names. Without one it
        # covered nothing this table could be held to.
        if not (directory := glue_directory(row)):
            return TableOrphanFiles(
                database=database,
                table=name,
                refused="Glue names no directory for the table",
            )
        if not hasattr(local, "catalog"):
            local.catalog = catalog_factory()
        return remove_table_orphan_files(
            local.catalog,
            database,
            name,
            store,
            min_age_days=min_age_days,
            now=now,
            delete=delete,
            expected_location=directory,
            logger=logger,
        )

    tables: list[TableOrphanFiles] = []
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [(row["table"], executor.submit(process, row)) for row in work]
        for name, future in futures:
            try:
                tables.append(future.result())
            except Exception as error:  # noqa: BLE001
                logger.warning(
                    "Orphan-file pass failed for %s.%s: %s", database, name, error
                )
                failures.append(f"{name}: {error}")
    return DatabaseOrphanFiles(
        database=database,
        tables=tables,
        failures=failures,
        unreadable_databases=unreadable,
    )
