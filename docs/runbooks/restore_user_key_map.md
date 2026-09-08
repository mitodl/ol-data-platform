# Runbook: restore the user_pk key map

Recovering `int__combined__user_key_map` after it has been dropped, truncated, or built
wrong. Read [`../design/adr_durable_user_surrogate_key.md`](../design/adr_durable_user_surrogate_key.md)
first if you have not; this runbook assumes you know what the map is for.

## Why this is an incident and not a rebuild

Every other model in the dbt project is a pure function of its sources: drop it, rebuild
it, get the same thing back. This one is not. It records **when** each identifier was first
assigned a person key, and survivorship depends on that ordering. Assignment order is a
fact about the history of builds, not about the source data.

If you rebuild the map from scratch instead of restoring it, dbt will succeed, the tests
will pass, and the warehouse will be silently re-keyed for every person whose group winner
has changed since their key was first assigned. `dim_user.user_pk` is joined by 27 models,
and the `dimensional` schema grants `select` to `reverse_etl`, so the damage leaves the
warehouse. **A green build is not evidence that you restored correctly.**

## 0. Stop the bleeding

Do this before anything else, or the next scheduled build will mint fresh keys over the
gap and make the situation harder to reason about.

- Pause the dbt automation sensor for the affected environment, or otherwise prevent
  `int__combined__user_key_map` from materializing.
- Do **not** run `dbt build --full-refresh`. `full_refresh=false` on the model makes that
  a no-op by design, but do not rely on that as your only guard while you are working.

## 1. Establish what you lost

```bash
ENV=production   # or qa
BUCKET=ol-data-lake-intermediate-$ENV

aws s3 cp "s3://$BUCKET/_backups/user_key_map/latest.json" - | jq
```

`latest.json` names the newest verified-good backup:

```json
{
  "key": "_backups/user_key_map/dt=2026-08-25/user_key_map-2026-08-25T18-32-40Z.parquet",
  "row_count": 7684472,
  "sha256": "…",
  "source": "ol_warehouse_production_intermediate.int__combined__user_key_map",
  "warehouse_env": "production"
}
```

Confirm `warehouse_env` matches the environment you are restoring. The backup asset scopes
its source database and destination bucket from the same value, so a mismatch means you
are looking at the wrong bucket.

If `latest.json` is missing or looks wrong, list the dated backups directly and pick one:

```bash
aws s3 ls "s3://$BUCKET/_backups/user_key_map/" --recursive | sort | tail -20
```

## 2. Verify the backup before you trust it

```bash
aws s3 cp "s3://$BUCKET/$(jq -r .key latest.json)" /tmp/keymap.parquet
sha256sum /tmp/keymap.parquet          # must equal .sha256 from latest.json
```

```python
import polars as pl
df = pl.read_parquet("/tmp/keymap.parquet")
assert df.height == MANIFEST_ROW_COUNT
assert df["identifier"].n_unique() == df.height     # one row per identifier
assert df["user_pk"].null_count() == 0
assert df["assigned_at"].null_count() == 0
```

The `identifier` uniqueness check is the important one: it is what stops the join in
`dim_user` fanning out.

## 3. Restore

Write the Parquet back to the Iceberg table. Because the model is
`incremental_strategy='append'`, the restored table becomes the base that subsequent runs
append to.

```python
import boto3
import polars as pl
from pyiceberg.catalog.glue import GlueCatalog

df = pl.read_parquet("/tmp/keymap.parquet")
catalog = GlueCatalog("default", client=boto3.client("glue", region_name="us-east-1"))
table = catalog.load_table(
    "ol_warehouse_production_intermediate.int__combined__user_key_map"
)
table.overwrite(df.to_arrow())     # full replace, not append
```

Use `overwrite`, not `append`: if any rows survived the incident, appending would duplicate
them and break `identifier` uniqueness.

## 4. Prove the restore before unpausing

Run the map, then `dim_user`, then compare against a known-good reference.

```sql
-- Must be 0. A duplicate identifier fans out the dim_user join.
select count(*) from (
  select identifier from ol_warehouse_production_intermediate.int__combined__user_key_map
  group by identifier having count(*) > 1
);

-- Must be 0.
select count(*) from ol_warehouse_production_dimensional.dim_user where user_pk is null;
```

Then the real check — how many people the restore re-keyed, against whatever copy of
`dim_user` predates the incident (an Iceberg snapshot from before it is ideal):

```sql
select count(*) as people_rekeyed
from <pre_incident_dim_user> o
join ol_warehouse_production_dimensional.dim_user n on o.email = n.email
where o.user_pk is distinct from n.user_pk;
```

Expect **0**. A non-zero count is the number of people whose downstream `user_fk` values
just became wrong, and it needs a decision — not an unpause.

## 5. Gap between the backup and the incident

Accounts first seen after the backup was taken are absent from the restored map. That is
fine and self-healing: the next incremental run treats them as new, and they either adopt
their group's incumbent key (which the backup restored) or mint a fresh one. They were
never in the map, so nothing downstream can be holding their key.

## If there is no usable backup

In order of preference:

1. **Iceberg time travel**, if the incident is inside the model's 30-day snapshot
   retention. Cheapest and exact.
2. **Reconstruct from `dim_user` itself**, if a pre-incident copy exists: it carries
   `user_pk` and enough identity columns to rebuild `(identifier, user_pk)` pairs. You
   lose true `assigned_at` values — synthesize a single early timestamp for all of them so
   the relative order of everything assigned *after* the restore is still correct.
3. **Re-mint** (`dbt run` with an empty map) — accept that this re-keys everyone whose
   group winner has changed since first assignment, and treat it as a coordinated re-key:
   notify `reverse_etl` consumers, and expect the `relationships` tests on `user_fk` to
   fail until every fact table is rebuilt.

Option 3 is the outcome the backup exists to avoid. Do not reach for it because it is the
one that runs without thinking.
