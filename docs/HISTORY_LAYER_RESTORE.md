# Restoring a history-layer table

The history layer (`ol_warehouse_<env>_history`, bucket `ol-data-lake-history-<env>`)
holds the dbt snapshots of raw tables (`snapshots/raw_history/`). Nothing can rebuild
them, so they are protected two ways instead of being backed up:

- Neither orphan sweep deletes under a history bucket. `lake_orphan_sweep` reports a
  history directory with no Glue table and fails its run, and `iceberg_raw_orphan_files`
  refuses a delete run over a history database.
- The history buckets are versioned and keep noncurrent versions for 90 days
  (`expire-noncurrent-versions`), so a deleted file stays recoverable for that long.

A table therefore has to be restored within 90 days of losing its files. A table whose
Glue entry was dropped but whose files are intact can be restored at any time.

## When this applies

- `lake_orphan_sweep` failed with "directories in the history layer have no Glue
  table". Its message names each directory.
- A history table was dropped by mistake, whether the drop left its files (a Glue
  `DeleteTable`) or deleted them.

## Steps

`bin/history-table-restore.py` does the S3 and Glue work. Each command prints what it
would do and changes nothing without `--execute`. It needs AWS credentials that can
list object versions and delete objects in the history bucket and create Glue tables
in the history database.

1. Stop the table's snapshot from running while you restore it. The snapshot is a dbt
   asset in the lakehouse code location: find the job that last materialized it and turn
   off that job's schedule or sensor. A snapshot run against a missing table creates a
   new table, with no history, under the same name.
2. Find the table directory, `s3://ol-data-lake-history-<env>/<table>-<uuid>`, and the
   time the files were deleted. The directory is in the sweep's failure message;
   `aws s3api list-object-versions --prefix <table>-<uuid>/` shows when the delete
   markers were placed.
3. If the files were deleted, make them current again. `--deleted-after` limits the
   undelete to markers placed since the incident:

   ```bash
   uv run python bin/history-table-restore.py undelete \
       s3://ol-data-lake-history-qa/<table>-<uuid> --deleted-after 2026-11-10T17:08:00Z
   # check the count, then add --execute
   ```

4. Register the table from its last committed metadata file. Skip this step if the
   table's Glue entry still exists (only the files were deleted).

   ```bash
   uv run python bin/history-table-restore.py register \
       s3://ol-data-lake-history-qa/<table>-<uuid> --table ol_warehouse_qa_history.<table>
   # check the metadata file, then add --execute
   ```

   It lists the newest metadata files with their write times and picks the newest. A
   commit that failed after writing its file leaves a newer file Glue never pointed at,
   so check that the chosen file's write time matches the last snapshot run that
   succeeded in Dagster. If it doesn't, or two files share the newest version (the
   command refuses), pass the right one with `--metadata-location`. After `--execute` it
   prints the snapshot count, the current snapshot id and the record count. Compare them
   with what the table held before, e.g. from the last snapshot run's row count.
5. Read the table from Trino (`select count(*)` and `select * from "<table>$snapshots"`)
   and resume the snapshot.

If a snapshot run recreated the table before step 1, its name is taken and `register`
fails. The new table holds the versions captured since the incident, which the old
table does not have, so keep both:

1. Record the new table's committed metadata file while Glue still has it
   (`aws glue get-table`, `Parameters.metadata_location`). Drop its Glue entry
   (`aws glue delete-table`, which leaves its files) and register its directory again
   as `<table>__after_<date>` with `register --metadata-location` set to that file.
2. Register the old directory under the original name and resume the snapshot. Its
   next run compares against raw as of that run, so the changes in between are only in
   `<table>__after_<date>`.
3. Merging the two is a separate decision. Until then both stay registered, so the
   sweep sees neither as an orphan.

## Rehearsal

Done in QA on 2026-11-10 (task tk-keep-the-orphan-sweeps-from-deleting-in-the-hist-3abed1):

- Created `ol_warehouse_qa_history.restore_rehearsal_20261110` at
  `s3://ol-data-lake-history-qa/restore_rehearsal_20261110-ce5331ce7f004e92ba97979b0032b302`
  with pyiceberg: two appends, 2 snapshots, 5 rows, 3 metadata files, 9 objects.
- Incident: deleted the Glue entry, then deleted all 9 current objects (delete markers),
  which is what a Glue drop followed by a sweep would have done.
- `undelete --deleted-after 2026-11-10T17:08:00Z --execute` removed 9 delete markers.
  `register --execute` picked metadata `00002-…` and reported 2 snapshots, current
  snapshot 6767714283335990853, 5 records.
- Read back with pyiceberg: the same snapshot ids, current snapshot and rows as before
  the incident. The table was then dropped and its objects deleted.

Not covered by the rehearsal: a Trino-written table (the production history tables are
written by dbt-trino; the rehearsal table was written by pyiceberg) and reading the
restored table from Trino.
