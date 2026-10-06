#!/usr/bin/env python3
"""Find and remove data-lake S3 prefixes that no Glue table references.

Subcommands:
  report      read-only inventory of unreferenced prefixes
  delete      delete prefixes from a reviewed manifest, re-verifying each one
  verify      post-delete check that live tables kept their data
  drop-stuck  drop __dbt_tmp/__dbt_backup Glue entries, catalog only

The predicate is always a set difference against live Glue locations computed at
run time, never a name pattern. dbt-trino's `table` materialization renames a
temp relation into place, and a rename is catalog-only, so LIVE tables sit in
`__dbt_tmp-<uuid>/` directories. Deleting by name would destroy them.

Neither `remove_orphan_files` nor noncurrent-version expiry reaches these
prefixes: the first only walks a live table's own location, and these bytes are
current versions.

Typical use:

    bin/lake-orphan-sweep.py report ol-data-lake-mart-production -o /tmp/sweep
    # review /tmp/sweep/orphan_prefixes.csv, cut it down to a manifest
    bin/lake-orphan-sweep.py delete manifest.csv            # prints only
    bin/lake-orphan-sweep.py delete manifest.csv --execute
    bin/lake-orphan-sweep.py verify manifest.csv ol-data-lake-mart-production
"""
# ruff: noqa: T201

import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from cyclopts import App
from ol_orchestrate.lib.lake_orphan_sweep import (
    delete_prefix,
    glue_tables,
    is_referenced,
    measure,
    normalize,
    prefixes_at_depth,
    referenced_paths,
)

app = App(name="lake-orphan-sweep")


@cache
def glue() -> Any:
    """Return a cached Glue client."""
    return boto3.client("glue")


@cache
def s3() -> Any:
    """Return a cached S3 client."""
    return boto3.client("s3")


@app.command
def report(
    buckets: list[str],
    *,
    out_dir: Path,
    under: str = "",
    depth: int = 1,
    min_age_days: int = 7,
) -> None:
    """Write an inventory of unreferenced prefixes. Read-only.

    Parameters
    ----------
    buckets: Buckets to scan.
    out_dir: Directory for orphan_prefixes.csv, summary.json, stuck_glue_tables.json.
    under: Base prefix to scan below, e.g. "processed/". Default is the root.
    depth: Levels below `under` to treat as one candidate. Use 2 for
        processed/<schema>/<table>.
    min_age_days: A prefix younger than this is reported but marked ineligible,
        so a dbt run whose temp table is not yet registered is never a candidate.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = glue_tables(glue())
    (out_dir / "glue_tables.json").write_text(json.dumps(tables, indent=1))
    stuck = [
        t for t in tables if "__dbt_tmp" in t["table"] or "__dbt_backup" in t["table"]
    ]
    (out_dir / "stuck_glue_tables.json").write_text(json.dumps(stuck, indent=1))

    referenced = referenced_paths(glue(), include_databases=False)
    candidates: list[tuple[str, str]] = []
    for bucket in buckets:
        found = prefixes_at_depth(s3(), bucket, under, depth)
        orphans = [p for p in found if not is_referenced(f"{bucket}/{p}", referenced)]
        print(f"{bucket}: {len(found)} prefixes scanned, {len(orphans)} unreferenced")
        candidates += [(bucket, p) for p in orphans]

    with ThreadPoolExecutor(32) as pool:
        rows = list(pool.map(lambda bp: measure(s3(), *bp), candidates))

    now = datetime.now(UTC)
    for row in rows:
        age = (now - row["newest"]).days if row["newest"] else None
        row["age_days"] = age
        row["dbt_tmp_named"] = "__dbt_tmp" in row["prefix"]
        row["eligible"] = row["objects"] > 0 and age is not None and age >= min_age_days
        row["newest"] = row["newest"].isoformat() if row["newest"] else ""

    if rows:
        with (out_dir / "orphan_prefixes.csv").open("w", newline="") as handle:
            # \n, not csv's default \r\n: this file gets grepped and awked by
            # hand on the way to becoming a delete manifest, and a trailing \r
            # silently breaks a match on the last column.
            writer = csv.DictWriter(
                handle, fieldnames=list(rows[0]), lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(sorted(rows, key=lambda r: (r["bucket"], -r["bytes"])))

    summary: dict[str, dict[str, float]] = {}
    for row in rows:
        entry = summary.setdefault(
            row["bucket"], {"orphans": 0, "eligible": 0, "eligible_gb": 0.0}
        )
        entry["orphans"] += 1
        entry["eligible"] += int(row["eligible"])
        entry["eligible_gb"] += row["bytes"] / 1e9 if row["eligible"] else 0
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))
    print(f"stuck __dbt_tmp/__dbt_backup Glue tables: {len(stuck)}")


@app.command
def delete(manifest: Path, *, min_age_days: int = 7, execute: bool = False) -> None:
    """Delete reviewed prefixes, re-checking each against Glue fetched now.

    Parameters
    ----------
    manifest: CSV with `bucket,prefix` columns, a reviewed cut of the report.
    min_age_days: Skip a prefix whose newest object is younger than this.
    execute: Without it, print what would be deleted and change nothing.
    """
    # The library logs each prefix before its first delete batch, so a run
    # that dies part way through one still names it.
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rows = list(csv.DictReader(manifest.open()))
    referenced = referenced_paths(glue(), include_databases=True)
    now = datetime.now(UTC)
    for row in rows:
        outcome = delete_prefix(
            s3(),
            row["bucket"],
            row["prefix"],
            referenced,
            min_age_days=min_age_days,
            now=now,
            execute=execute,
        )
        target = f"s3://{outcome.bucket}/{outcome.prefix}/"
        if not outcome.prefix:
            print(f"REFUSE {outcome.bucket}: {outcome.reason}")
        elif outcome.action == "refused":
            print(f"REFUSE {target}: {outcome.reason}")
        elif outcome.action == "skipped":
            print(f"SKIP   {target}: {outcome.reason}")
        else:
            verb = "DELETE" if outcome.action == "deleted" else "WOULD "
            print(f"{verb} {target}: {outcome.objects} objects")
        for error in outcome.errors:
            print(f"  ERROR {error}")


@app.command
def verify(manifest: Path, buckets: list[str]) -> None:
    """Check deleted prefixes are empty and live tables kept data and metadata.

    Parameters
    ----------
    manifest: The manifest that was deleted.
    buckets: Buckets whose live Glue tables should be re-checked.
    """
    rows = list(csv.DictReader(manifest.open()))
    with ThreadPoolExecutor(32) as pool:
        results = pool.map(lambda r: _has_objects(r["bucket"], r["prefix"] + "/"), rows)
        left = [row for row, present in zip(rows, results, strict=True) if present]
    for row in left:
        print(f"STILL HAS OBJECTS: s3://{row['bucket']}/{row['prefix']}/")
    print(f"deleted prefixes with objects remaining: {len(left)} of {len(rows)}")

    locations: list[tuple[str, str, str]] = []
    metadata: list[tuple[str, str, str]] = []
    for table in glue_tables(glue()):
        name = f"{table['database']}.{table['table']}"
        loc = urlparse(table["location"])
        if loc.netloc in buckets:
            locations.append((name, loc.netloc, loc.path.strip("/") + "/"))
        meta = urlparse(table["metadata_location"])
        if meta.netloc in buckets:
            metadata.append((name, meta.netloc, meta.path.lstrip("/")))

    with ThreadPoolExecutor(32) as pool:
        loc_ok = pool.map(lambda x: _has_objects(x[1], x[2]), locations)
        empty = [x for x, ok in zip(locations, loc_ok, strict=True) if not ok]
        meta_ok = pool.map(lambda x: _exists(x[1], x[2]), metadata)
        missing = [x for x, ok in zip(metadata, meta_ok, strict=True) if not ok]
    for name, bucket, prefix in empty:
        print(f"LIVE TABLE LOCATION EMPTY: {name} s3://{bucket}/{prefix}")
    for name, bucket, key in missing:
        print(f"METADATA FILE MISSING: {name} s3://{bucket}/{key}")
    print(f"live table locations checked: {len(locations)}, empty: {len(empty)}")
    print(f"metadata files checked: {len(metadata)}, missing: {len(missing)}")


def _has_objects(bucket: str, prefix: str) -> bool:
    """Return True when at least one object exists under the prefix."""
    return s3().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)["KeyCount"] > 0


def _exists(bucket: str, key: str) -> bool:
    """Return True when the key exists."""
    try:
        s3().head_object(Bucket=bucket, Key=key)
    except s3().exceptions.ClientError:
        return False
    return True


@app.command(name="drop-stuck")
def drop_stuck(
    candidates: Path, *, backup_dir: Path, min_age_days: int = 7, execute: bool = False
) -> None:
    """Drop stuck __dbt_tmp/__dbt_backup Glue entries. Catalog only, no data.

    Parameters
    ----------
    candidates: JSON list of {"database", "table"}, e.g. the report's
        stuck_glue_tables.json.
    backup_dir: Each table definition is written here before it is dropped.
    min_age_days: Skip an entry updated more recently than this.
    execute: Without it, print what would be dropped and change nothing.
    """
    backup_dir.mkdir(parents=True, exist_ok=True)
    owners: dict[str, list[str]] = {}
    for table in glue_tables(glue()):
        if path := normalize(table["location"]):
            owners.setdefault(path, []).append(f"{table['database']}.{table['table']}")
    now = datetime.now(UTC)
    for candidate in json.loads(candidates.read_text()):
        name = f"{candidate['database']}.{candidate['table']}"
        if "__dbt_tmp" not in name and "__dbt_backup" not in name:
            print(f"REFUSE {name}: not a dbt tmp/backup name")
            continue
        try:
            table = glue().get_table(
                DatabaseName=candidate["database"], Name=candidate["table"]
            )["Table"]
        except glue().exceptions.EntityNotFoundException:
            print(f"GONE   {name}")
            continue
        if (now - table["UpdateTime"]).days < min_age_days:
            print(f"SKIP   {name}: updated {table['UpdateTime'].isoformat()}")
            continue
        location = table.get("StorageDescriptor", {}).get("Location", "")
        shared = [o for o in owners.get(normalize(location), []) if o != name]
        if shared:
            print(f"SKIP   {name}: location shared with {shared}")
            continue
        backup = backup_dir / f"{name}.json"
        backup.write_text(json.dumps(table, indent=1, default=str))
        print(
            f"{'DROP  ' if execute else 'WOULD '} {name} (location={location or '-'})"
        )
        if execute:
            glue().delete_table(
                DatabaseName=candidate["database"], Name=candidate["table"]
            )


if __name__ == "__main__":
    app()
