"""Find and remove data-lake S3 prefixes that no Glue table references.

Shared by ``bin/lake-orphan-sweep.py`` (the manual, manifest-driven CLI) and the
``lake_orphan_sweep`` asset in the lakehouse code location.

The predicate is always a set difference against live Glue locations computed at
run time, never a name pattern. dbt-trino's ``table`` materialization renames a
temp relation into place, and a rename is catalog-only, so LIVE tables sit in
``__dbt_tmp-<uuid>/`` directories. Deleting by name would destroy them.

Every function takes its boto3 clients as arguments, so the callers decide how
they are built and the tests pass fakes.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# dbt-trino suffixes every table directory it creates with a uuid. A bare name is
# where pyiceberg would recreate a table whose database location is the bucket
# root, so the delete path refuses one.
DBT_DIR = re.compile(r"-[0-9a-f]{32}$")
S3_DELETE_BATCH_SIZE = 1000

# Matched by hand, not with urlparse: an S3 key may contain "#" or "?", and
# urlparse would cut the path there and leave a reference to a shorter prefix
# than the one the table occupies.
_URI_NOISE = str.maketrans("", "", "\t\r\n")
_S3_URI = re.compile(r"^s3[an]?://([^/]+)/?(.*)$", re.IGNORECASE | re.DOTALL)


def glue_tables(
    glue: Any, *, unreadable: list[str] | None = None
) -> list[dict[str, str]]:
    """Return every Glue table with its location and Iceberg metadata pointer.

    :param unreadable: When given, a database this caller is denied access to is
        appended here by name and skipped. Without it the denial raises. An
        environment's role is denied the other environments' databases, so an
        unattended run has to expect some.
    """
    out: list[dict[str, str]] = []
    for dbs in glue.get_paginator("get_databases").paginate():
        for db in dbs["DatabaseList"]:
            try:
                pages = glue.get_paginator("get_tables").paginate(
                    DatabaseName=db["Name"]
                )
                rows = [
                    {
                        "database": db["Name"],
                        "table": table["Name"],
                        "location": table.get("StorageDescriptor", {}).get(
                            "Location", ""
                        ),
                        "metadata_location": table.get("Parameters", {}).get(
                            "metadata_location", ""
                        ),
                        "updated": str(table.get("UpdateTime", "")),
                    }
                    for page in pages
                    for table in page["TableList"]
                ]
            except ClientError as error:
                denied = error.response["Error"]["Code"] == "AccessDeniedException"
                if unreadable is None or not denied:
                    raise
                unreadable.append(db["Name"])
                continue
            out.extend(rows)
    return out


def glue_database_locations(glue: Any) -> dict[str, str]:
    """Return each Glue database's ``LocationUri`` ("" when it has none)."""
    return {
        db["Name"]: db.get("LocationUri", "")
        for dbs in glue.get_paginator("get_databases").paginate()
        for db in dbs["DatabaseList"]
    }


def normalize(uri: str) -> str:
    """Return ``bucket/key`` for an s3 URI, or "" when it is not one."""
    # urlparse, which this replaced, dropped surrounding whitespace and any
    # tab, CR or LF. A location stored with a stray newline still has to match
    # the directory it names.
    match = _S3_URI.match(uri.strip().translate(_URI_NOISE))
    if match is None:
        return ""
    bucket, key = match.groups()
    return f"{bucket}/{key.strip('/')}".rstrip("/")


def references(
    tables: list[dict[str, str]], database_locations: dict[str, str] | None = None
) -> set[str]:
    """Return every ``bucket/key`` path the given Glue entries point at.

    A database location is passed for the delete path because a database whose
    location names a real prefix claims it for tables not yet registered. A
    database located at a bucket ROOT is left out: every deployed warehouse
    database is located at one, and :func:`is_referenced` reads a bare bucket
    as containing every prefix in it, so counting it skips every candidate. A
    TABLE located at a bucket root is kept and does protect the whole bucket.
    """
    out: set[str] = set()
    for table in tables:
        for uri in (table["location"], table["metadata_location"]):
            if path := normalize(uri):
                out.add(path)
    for uri in (database_locations or {}).values():
        if "/" in (path := normalize(uri)):
            out.add(path)
    return out


def referenced_paths(glue: Any, *, include_databases: bool) -> set[str]:
    """Return every ``bucket/key`` path Glue points at. See :func:`references`."""
    return references(
        glue_tables(glue), glue_database_locations(glue) if include_databases else None
    )


def is_referenced(candidate: str, referenced: set[str]) -> bool:
    """Return True when ``candidate`` contains, equals, or sits inside a reference."""
    if candidate in referenced:
        return True
    return any(
        path.startswith(f"{candidate}/") or candidate.startswith(f"{path}/")
        for path in referenced
    )


def prefixes_at_depth(s3: Any, bucket: str, under: str, depth: int) -> list[str]:
    """List prefixes ``depth`` levels below ``under`` ("" means the bucket root)."""
    base = f"{under.strip('/')}/" if under.strip("/") else ""
    current = [base]
    for _ in range(depth):
        found: list[str] = []
        for prefix in current:
            pages = s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix, Delimiter="/"
            )
            for page in pages:
                found += [cp["Prefix"] for cp in page.get("CommonPrefixes", [])]
        current = found
    return [prefix.rstrip("/") for prefix in current]


def measure(s3: Any, bucket: str, prefix: str) -> dict[str, Any]:
    """Count objects, bytes and the newest LastModified under one prefix."""
    count = size = 0
    newest: datetime | None = None
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{prefix}/"
    )
    for page in pages:
        for obj in page.get("Contents", []):
            count += 1
            size += obj["Size"]
            newest = max(newest, obj["LastModified"]) if newest else obj["LastModified"]
    return {
        "bucket": bucket,
        "prefix": prefix,
        "objects": count,
        "bytes": size,
        "newest": newest,
    }


@dataclass(frozen=True)
class PrefixOutcome:
    """What :func:`delete_prefix` decided for one prefix, and what it did."""

    bucket: str
    prefix: str
    action: Literal["refused", "skipped", "would_delete", "deleted"]
    reason: str = ""
    objects: int = 0
    bytes: int = 0
    # Keys S3 refused to delete. A "deleted" outcome with errors is partial.
    errors: list[str] = field(default_factory=list)


def delete_prefix(  # noqa: PLR0913
    s3: Any,
    bucket: str,
    prefix: str,
    referenced: set[str],
    *,
    min_age_days: int,
    now: datetime,
    execute: bool,
    logger: logging.Logger = log,
) -> PrefixOutcome:
    """Delete one prefix if it is still an orphan, re-checking every guard.

    :param referenced: Glue references fetched immediately before the delete
        pass, database locations included. A reference set from an earlier scan
        would miss a table registered in between.
    :param min_age_days: Leave a prefix whose newest object is younger than this.
    :param execute: Without it, decide and report but delete nothing.
    :param logger: Where the line naming a prefix about to be deleted goes. A
        Dagster run only captures its own ``context.log``.
    """
    path = f"{bucket}/{prefix}"
    if not DBT_DIR.search(prefix):
        reason = "no dbt uuid suffix" if prefix else "empty prefix"
        return PrefixOutcome(bucket, prefix, "refused", reason)
    if is_referenced(path, referenced):
        return PrefixOutcome(bucket, prefix, "skipped", "now referenced by Glue")
    keys: list[str] = []
    size = 0
    newest: datetime | None = None
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{prefix}/"
    )
    for page in pages:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            size += obj["Size"]
            newest = max(newest, obj["LastModified"]) if newest else obj["LastModified"]
    if newest is None:
        return PrefixOutcome(bucket, prefix, "skipped", "no objects left")
    if (now - newest).days < min_age_days:
        return PrefixOutcome(
            bucket,
            prefix,
            "skipped",
            f"newest object {newest.isoformat()} too recent",
        )
    if not execute:
        return PrefixOutcome(
            bucket, prefix, "would_delete", objects=len(keys), bytes=size
        )
    # Logged before the first batch, so a run that dies part way through a
    # prefix still names it.
    logger.info("Deleting s3://%s/ (%d objects, %d bytes)", path, len(keys), size)
    errors: list[str] = []
    for start in range(0, len(keys), S3_DELETE_BATCH_SIZE):
        batch = [{"Key": key} for key in keys[start : start + S3_DELETE_BATCH_SIZE]]
        response = s3.delete_objects(
            Bucket=bucket, Delete={"Objects": batch, "Quiet": True}
        )
        errors += [
            f"{error['Key']}: {error['Code']} {error['Message']}"
            for error in response.get("Errors", [])
        ]
    return PrefixOutcome(
        bucket, prefix, "deleted", objects=len(keys), bytes=size, errors=errors
    )


def warehouse_scan_targets(
    tables: list[dict[str, str]], database_locations: dict[str, str], warehouse_env: str
) -> list[tuple[str, str]]:
    """Return the ``(bucket, prefix)`` directories that hold one warehouse's tables.

    Read from Glue instead of a bucket list. The deployed warehouse is the
    ``ol_warehouse_<env>_*`` databases located at a bucket root. A developer
    schema shares the name prefix (``ol_warehouse_production_<person>_staging``)
    but is located under ``processed/<database>`` in the staging bucket, and is
    left out: whether to delete from someone's schema is a retention decision,
    not this sweep's.

    The targets are those bucket roots, plus the parent directory of every table
    in those databases. The second part reaches deployed tables that were
    written under ``processed/<database>/``.

    A table located in a bucket that none of those databases is located in (an
    external table over another team's bucket) contributes nothing, so the sweep
    never lists a bucket the warehouse does not own.
    """
    database_prefix = f"ol_warehouse_{warehouse_env}_"
    databases: set[str] = set()
    buckets: set[str] = set()
    for name, uri in database_locations.items():
        bucket, _, key = normalize(uri).partition("/")
        if name.startswith(database_prefix) and bucket and not key:
            databases.add(name)
            buckets.add(bucket)
    targets = {(bucket, "") for bucket in buckets}
    for table in tables:
        if table["database"] not in databases:
            continue
        table_dir = normalize(table["location"])
        if not table_dir:
            # A Trino-created Iceberg entry can carry its location only in
            # metadata_location, as <table dir>/metadata/<file>. A pointer of
            # any other shape does not say where the table directory is, and a
            # guess that lands inside it would offer the table's own
            # subdirectories as candidates.
            table_dir, found, _ = normalize(table["metadata_location"]).rpartition(
                "/metadata/"
            )
            if not found:
                continue
        bucket, _, key = table_dir.partition("/")
        if bucket in buckets and key:
            targets.add((bucket, key.rpartition("/")[0]))
    return sorted(targets)


@dataclass(frozen=True)
class SweepResult:
    """One sweep of a warehouse environment's table directories."""

    targets: list[tuple[str, str]]
    prefixes_scanned: int
    # Unreferenced and uuid-suffixed, each measured: bucket, prefix, objects,
    # bytes, newest, age_days, eligible.
    orphans: list[dict[str, Any]]
    # Unreferenced without the uuid suffix. Never deleted, so never measured:
    # one of these in the raw bucket can hold millions of objects.
    unsuffixed: list[str]
    # Databases Glue denied this caller, whose tables are therefore not among
    # the references.
    unreadable_databases: list[str]
    # None when the sweep only reported.
    outcomes: list[PrefixOutcome] | None

    @property
    def eligible(self) -> list[dict[str, Any]]:
        """Return the orphans old enough to delete."""
        return [row for row in self.orphans if row["eligible"]]


def _refuse_to_delete_blind(unreadable: list[str]) -> None:
    """Raise when Glue denied any database, before anything is deleted.

    A table's location is independent of its database's, so a database this
    caller cannot read may hold a table located in any bucket. Its references
    are missing, and nothing can then be shown to be an orphan.
    """
    if unreadable:
        msg = (
            f"Glue denied access to {sorted(unreadable)}. Their tables cannot be "
            "counted as references, so nothing can be shown to be an orphan. "
            "Refusing to delete."
        )
        raise RuntimeError(msg)


def sweep_warehouse(  # noqa: PLR0913
    glue: Any,
    s3: Any,
    *,
    warehouse_env: str,
    min_age_days: int,
    now: datetime,
    delete: bool,
    logger: logging.Logger = log,
) -> SweepResult:
    """Find one warehouse's orphaned table directories, and optionally delete them.

    :param min_age_days: An orphan whose newest object is younger than this is
        reported and left alone. A dbt run writes its files before it registers
        the table, so a prefix can be unreferenced and about to become live.
    :param delete: Delete the eligible orphans, each re-checked against Glue
        references fetched after the scan. Refused when Glue denies this caller
        any database. A report run carries on and names them, and its orphan
        list is then an upper bound.
    :param logger: Receives one line per prefix a delete run acts on.
    """
    if min_age_days < 1:
        msg = f"min_age_days must be at least 1, got {min_age_days}"
        raise ValueError(msg)

    unreadable: list[str] = []
    tables = glue_tables(glue, unreadable=unreadable)
    locations = glue_database_locations(glue)
    if delete:
        _refuse_to_delete_blind(unreadable)
    targets = warehouse_scan_targets(tables, locations, warehouse_env)
    referenced = references(tables)
    scanned = 0
    orphans: list[dict[str, Any]] = []
    unsuffixed: list[str] = []
    for bucket, under in targets:
        found = prefixes_at_depth(s3, bucket, under, 1)
        scanned += len(found)
        for prefix in found:
            if is_referenced(f"{bucket}/{prefix}", referenced):
                continue
            if not DBT_DIR.search(prefix):
                unsuffixed.append(f"{bucket}/{prefix}")
                continue
            row = measure(s3, bucket, prefix)
            row["age_days"] = (now - row["newest"]).days if row["newest"] else None
            row["eligible"] = (
                row["age_days"] is not None and row["age_days"] >= min_age_days
            )
            orphans.append(row)

    outcomes: list[PrefixOutcome] | None = None
    if delete:
        fresh_unreadable: list[str] = []
        fresh_tables = glue_tables(glue, unreadable=fresh_unreadable)
        fresh_locations = glue_database_locations(glue)
        _refuse_to_delete_blind(fresh_unreadable)
        fresh = references(fresh_tables, fresh_locations)
        outcomes = []
        for row in orphans:
            if not row["eligible"]:
                continue
            outcome = delete_prefix(
                s3,
                row["bucket"],
                row["prefix"],
                fresh,
                min_age_days=min_age_days,
                now=now,
                execute=True,
                logger=logger,
            )
            logger.info(
                "%s s3://%s/%s/ %s",
                outcome.action,
                outcome.bucket,
                outcome.prefix,
                outcome.reason or f"{len(outcome.errors)} errors",
            )
            outcomes.append(outcome)
    return SweepResult(
        targets=targets,
        prefixes_scanned=scanned,
        orphans=orphans,
        unsuffixed=unsuffixed,
        unreadable_databases=unreadable,
        outcomes=outcomes,
    )
