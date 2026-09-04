# ADR: Durable surrogate key for `dim_user`

Status: **accepted, implemented in the same PR** ·
Project: `wp-dbt-warehouse-audit-semantic-accuracy-consolidat-b4244e` ·
Witan tasks: `tk-design-durable-user-surrogate-key-replace-email--04b0a2` (design),
`tk-implement-the-durable-user-pk-key-map-adr-adr-du-b2c576` (implementation) ·
Supersedes the "Plan/Design" section of
[mitodl/ol-data-platform#2383](https://github.com/mitodl/ol-data-platform/issues/2383) ·
Date: 2026-08-24

## Context

`dim_user.user_pk` is the join key for 27 downstream models (14 `tfact_*`/`bridge_*` in the
dimensional layer, 2 marts, 11 reporting), plus the four schema YAML files that test them. It
is not a key. It is an attribute recomputed from scratch on every build.

### What it is today

`dim_user.sql:690` computes:

```sql
{{ dbt_utils.generate_surrogate_key(['user_identity_source', 'user_identity_id']) }} as user_pk
```

Both inputs come from a window function at `dim_user.sql:666-686`:

```sql
first_value(...) over (
    partition by email
    order by has_no_source_id, id_source_rank, user_global_id desc,
             sort_id desc nulls last, id_source_user_id desc
)
```

That is "the winning account in this email group". The model is `materialized='table'`
(`dim_user.sql:1-3`), so nothing persists the decision. The key changes whenever the winner
changes.

PR #2497 (2026-08-04) removed the worst instability. The key used to be
`hash(lower(email))`, so any email edit re-keyed the person. It now hashes a source-system id
where one exists. That fixed the id *half*. It did not make the key durable, because the
*grouping* and the *choice of winner within the group* are still recomputed.

### Re-key triggers, all of them routine

1. **A higher-ranked account joins the email group.** `id_source_rank`
   (`dim_user.sql:641-654`) is a fixed platform preference: mitlearn 1, mitxonline 2,
   edxorg 3, micromasters 4, mitxonline_openedx 5, mitxpro 6, residential 7, bootcamps 8,
   emeritus 9, global_alumni 10. An xPro-only learner who later enrols on MITx Online
   re-keys from `mitxpro:<id>` to `mitxonline:<id>`.
2. **A `user_global_id` appears.** It short-circuits to rank 0. This is a continuous stream
   while Keycloak / MIT Learn identity linkage rolls out, and is the most likely source of
   the churn now being observed.
3. **An email edit.** The id half is durable, but `partition by email` still does the
   grouping. An edited email moves the account to a different partition; if it was the
   winner, the accounts left behind get a new winner and a new key. Accounts with no source
   id key on `coalesce(..., email)` directly, so they re-key outright.
4. **An activity-flag toggle.** The partition email is itself derived
   (`dim_user.sql:290-322`) from `user_is_active_on_mitxonline` plus a join-date comparison,
   so flipping an activity flag can move a person between partitions.

### Production evidence, 2026-08-18 14:52 UTC

`relationships_bridge_user_courserun_role_user_fk__user_pk__ref_dim_user_` failed with 189
orphans against the project-wide `+error_if: ">10"` (`dbt_project.yml:220`), failing a
1058-node Dagster dbt build.

**Why only that test failed is a severity artefact, not evidence that the facts were
clean.** Counted from the manifest: eleven models carry a relationships test to
`dim_user.user_pk`, and **eight of them are at `severity: warn`** (`tfact_certificate`,
`tfact_enrollment`, `tfact_feedback`, `tfact_grade`, `tfact_order`, `tfact_payment`,
`tfact_problem_events`, `tfact_studentmodule_problems`). Only the three bridges
(`bridge_user_courserun_role`, `bridge_user_organization`, and the new
`bridge_user_key_alias`) are at error.

So `bridge_user_courserun_role` is simply the model whose orphans were configured to fail
the build. Whether the fact tables also had orphans in that run is **not known** and cannot
be recovered from the run result, because a warn does not stop the build. Three of the eight
warns carry an explicit "warn until the post-cutover full refresh" comment
(`_fact_tables.yml:338`, `_dim__models.yml:610`, `_dim__models.yml:693`); the rest predate
it. Treat the eight warns as suppressed signal rather than as passing tests.

## Constraints

- **Trino on Starburst Galaxy over Iceberg.** dbt-core 1.12.2, dbt-trino 1.10.3. Valid
  incremental strategies are `append`, `merge`, `delete+insert`, `microbatch`
  (`dbt/adapters/trino/impl.py:124-125`). All 21 incremental models in the project use
  `delete+insert`.
- **No `snapshots/` exist.** `snapshot-paths` is declared (`dbt_project.yml:23`) and the
  directory is empty. SCD2 is hand-rolled with `incremental` + `delete+insert`
  (`dim_course_run.sql:1-6`, `dim_product.sql`).
- **StarRocks migration is the strategic direction.** Nothing on this path may depend on
  Galaxy-only functionality. The design below is plain ANSI SQL plus dbt incremental
  materialization, so it ports.
- **`user_pk` leaves the warehouse.** The `dimensional` schema grants `select` to
  `reverse_etl` (`dbt_project.yml:104-107`). Consumers outside this repo hold `user_pk`
  values. A re-key is not contained by a dbt build.

## Decision

Persist the account-to-person assignment in an **append-only key map**. Never reassign an
existing account. Derive person-level survivorship from assignment order, which is immutable,
instead of from the platform ranking, which is volatile.

Four files:

| file | role |
|---|---|
| `models/intermediate/combined/int__combined__user_accounts.sql` | the account list, extracted from `dim_user`'s former `combined_accounts` CTE |
| `models/intermediate/combined/int__combined__user_key_map.sql` | the append-only key map |
| `models/dimensional/bridge_user_key_alias.sql` | retired-to-surviving key map |
| `macros/user_identity.sql` | the identifier set and the ranking, shared so the three cannot drift |

The macros are not incidental. The identifier set is the join key between the map and its
consumers, and the ranking decides both which account a new group mints from and which
account's attributes `dim_user` reports. Three literal copies of either would drift, and the
drift would be silent.

### 1. `int__combined__user_key_map` (new, append-only state)

Grain: one row per **identifier**, not per account.

An account is a row of `int__combined__user_accounts`. The obvious move is to key it on its
own primary key, and it is wrong: a combined MITx/Learn row holds several platform ids at
once, so any single-key scheme has to pick one by priority, and whichever it picks moves the
moment a higher-priority id appears. Measured on the 7.68M-account production snapshot:

| keyed on | people re-keyed over the MIT Learn rollout |
|---|---|
| `(id_source, id_source_user_id)`, priority-picked | **795,312** |
| `coalesce(user_global_id, ...)` | **796,593** |
| the whole identifier set | **0** |

The Keycloak global id looks like the fix and is not: it and the mitlearn id arrive in the
*same* event (866,750 accounts carry both, 19 carry mitlearn alone), so keying on it
relocates the flip rather than removing it.

Keying on the whole set removes the choice. Each account contributes one row per id it
holds, all carrying the same `user_pk`:

```
global:<keycloak sub>   mitlearn:<id>   mitxonline:<id>   edxorg:<id>   …
email:<email>           -- only for accounts with no source id anywhere
```

A newly appearing id is an *additional* identifier, so the account's older identifiers still
resolve and the map adopts rather than mints. Verified before building on it: 9,718,753
identifiers across the snapshot with **zero collisions** between account rows.

`account_nk` survives only as a within-run row id. It is still priority-picked and still
unstable, and nothing durable keys on it — the macro says so.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    on_schema_change='append_new_columns'
) }}
```

| column | notes |
|---|---|
| `identifier` | unique; `<namespace>:<id>` |
| `user_pk` | the person key assigned to this identifier, forever |
| `assigned_at` | assignment timestamp; the survivorship ordering |
| `assigned_invocation_id` | dbt `invocation_id`, for forensics |

`full_refresh=false` is load-bearing, not decoration. `should_full_refresh()`
(dbt-core `macros/materializations/configs.sql:6-12`) reads the model config *before* the CLI
flag, so `dbt build --full-refresh` becomes a no-op for this model specifically. Without it,
one `--full-refresh` re-keys the warehouse.

The `unique` test on `identifier` overrides the project-wide `+error_if: ">10"` to `">0"`.
A duplicate identifier can assign conflicting person keys, and an append-only table cannot
repair that with a rebuild, so it must fail on the first one rather than the eleventh.

Assignment rule for identifiers not yet in the map:

1. If any identifier anywhere in the same person group already has a map row, adopt that
   group's incumbent `user_pk` (the one with the oldest `assigned_at`). Reaching through the
   whole set is what makes a newly appearing id adopt instead of mint.
2. Otherwise mint
   `generate_surrogate_key(['winner_identity_source', 'winner_identity_id'])`, taking both
   from the group winner under the existing `ranked_accounts` ordering, exactly as
   `account_identity` computes them today.

Rule 2 must be *today's expression verbatim*. Today's key is a two-argument hash whose first
argument is the literal `'global'` when the winner carries a `user_global_id`; minting from
anything else would re-key every person on the cutover run.

Identifiers are unique across account rows for every id-bearing namespace, but the email
fallback is not — two id-less Emeritus rows can share an address — so the map dedupes to one
row per identifier and the join in `dim_user` cannot fan out.

### 2. `dim_user` resolves through the map

Replace `combined_users` (`dim_user.sql:688-696`) with a join. For each person group, the
`user_pk` is the **survivor**: the mapped key with the oldest `assigned_at`, tie-broken on
the key value itself. Everything downstream of `combined_users` (`ranked_users`,
`base_info`, `agg_view`) is unchanged, because it only ever partitions and groups on
`user_pk`.

The ranking CTEs (`ranked_accounts`, `account_identity`) stay. They still decide which
account's attributes surface and which natural key a *new* group mints from. They no longer
decide the key of an existing person.

### 3. `bridge_user_key_alias` (new, derived every run)

Grain: one row per retired key. Recomputed as a plain table, not persisted state:

```sql
select map.user_pk as retired_user_pk
     , survivor.user_pk as surviving_user_pk
from <groups with count(distinct map.user_pk) > 1>
where map.user_pk != survivor.user_pk
```

Because it is derived from the current grouping every run, every non-survivor points
directly at the one survivor. Alias chains cannot form, so no recursive resolution is needed.

This is the piece the failure mode actually requires. When two previously-separate people
turn out to be one (a `user_global_id` appears and links a `mitxpro` account to a
`mitxonline` account), downstream FKs pointing at the retired key do not silently orphan.
They have a published remapping. Reverse-ETL consumers get an explicit merge feed instead of
discovering a vanished id.

### Why cutover is a no-op

On the first run the map is empty, so every existing group takes the rule-2 mint path, and
rule 2 is exactly today's computation. Every person's `user_pk` on cutover day equals the
value they already have. No downstream churn, no coordinated re-key, no reverse-ETL outage.
The change buys stability going forward without paying a migration cost.

This is the property that makes the change shippable, so it was verified at production
scale rather than asserted.

**Method.** Comparing the two `dim_user` models directly does not work: the old model
inlines the platform union and reads staging live, the new one reads the frozen
`int__combined__user_accounts` table, and production rebuilds staging every few minutes, so
the two see different data minutes apart. The drift-immune test recomputes the **pre-change
key expression** over the same frozen snapshot the new pipeline read.

```
=== cutover: key sets on identical input ===
 in_old_formula_not_new  in_new_not_old_formula
                      2                       0

=== per-email key equality ===
 emails_where_key_differs
                        1
```

**Not zero, and the two rows are the design working rather than failing.** The identifier
set links account rows that share a real platform id, and the snapshot contains 5 such
identifiers spanning two email addresses each (3 `mitxpro_openedx`, 2 `global_alumni`) —
one open edX account behind two application accounts with different emails. Those people
were two identities under the old expression and are one now, which is what identity
resolution is for. Both retired keys are published:

```
=== keys in the map but not in dim_user, vs the alias table ===
 keys_merged_away  alias_rows  published_as_retired
                2           2                    2
```

So no key vanishes silently; downstream FKs get a remapping. Every other one of the
7,574,333 keys is unchanged.

**The property the previous design failed**, tested directly against the map — every account
that would flip namespace when MIT Learn linkage lands:

```
 at_risk_people  keep_their_key_through_linkage  would_lose_their_key
        6493461                         6493461                     0
```

Supporting integrity checks on the same build: `dim_user` has 0 null and 0 duplicate
`user_pk`; the map holds 11,546,537 identifiers with `distinct identifier = row count`.

**The `full_refresh=false` guard was verified too, by accident.** A run with
`--full-refresh` left the map's earlier `assigned_invocation_id` values intact instead of
rebuilding it. Forcing a true first build required dropping the relation out of band.

## Residual instability, and the endgame

One trigger survives: an account that *moves between* person groups. If an email edit pulls
an already-mapped account into a different group, and that account's `assigned_at` is older
than the destination group's incumbent, the destination group re-keys to the incoming key.

Two responses:

- **Bound it.** Survivorship could be persisted (append-only merge decisions, never revisited)
  instead of recomputed. That closes the hole but reintroduces alias chains and needs
  transitive resolution. Not worth it for the volume this represents.
- **Remove the cause.** The real fix is to stop grouping on `email`. `user_global_id`
  (Keycloak `sub`) is durable by construction and is already the rank-0 signal. Once its
  coverage is complete for the platforms that will ever have it, the grouping key becomes
  `coalesce(user_global_id, email)` and the residual disappears for every account that
  carries one.

**The key map is a bridge, not the destination.** Its permanent job is the accounts that will
never get a global id: the edxorg archive, Emeritus, Global Alumni, Bootcamps. For everyone
else it holds the line until Keycloak coverage lands.

## Alternatives rejected

**dbt snapshots on `dim_user`.** A snapshot records history of a key; it does not make the
key durable. It would faithfully record the re-key, not prevent it. It also introduces
snapshot infrastructure the project has deliberately avoided.

**`delete+insert` key map keyed on `account_nk`.** Functionally equivalent to append for the
insert-only case, but it makes destructive writes possible against the most load-bearing
state in the warehouse. Append cannot lose a prior assignment even under a buggy model
revision.

**Persisting the alias chain instead of deriving it.** Requires recursive resolution and
grows monotonically. Deriving from current state gives depth-1 aliases for free.

**Waiting for full Keycloak coverage and skipping the map.** Leaves the archive platforms
permanently unstable, and leaves the three `severity: warn` overrides in place indefinitely.

## Operability

- **Recovery.** The map is state that cannot be rebuilt reproducibly from sources alone: the
  assignment *order* is not recoverable. Recovery paths, in order: Iceberg time travel
  (the model sets 30 days of its own, above the 14 the `dimensional` folder uses); a
  periodic export of the map to a seed or S3 prefix; and, as a partial last resort,
  re-minting, which is deterministic per group and so restores the key for every group whose
  winner has not changed. **Add the export before enabling the model in production.**
  Iceberg snapshot retention is not a backup for a table whose loss re-keys the warehouse.
- **Growth.** One row per account, insert-only, no updates. Bounded by total accounts across
  all platforms.
- **Maintenance.** Inherits the `intermediate` folder config. It will need its own
  `iceberg_maintenance` block (`optimize_after_every_n_runs: 1`) because append accumulates
  one small file per run.

## Follow-ons once the key is durable

1. Raise the three placeholder overrides from `warn` to `error`: `_fact_tables.yml:338`,
   `_dim__models.yml:610`, `_dim__models.yml:693`.
2. Publish `bridge_user_key_alias` to reverse-ETL consumers and agree a merge-handling
   contract before the first real merge lands.
3. Resolves the root cause behind
   `tk-relationships-tests-assert-fk-integrity-against--323ade` (relationships tests asserting
   FK integrity against a half-rebuilt star schema). That task's selection-scoping problem is
   still worth fixing on its own, but a durable key removes the failure class it protects
   against.
4. Reconsider `tk-rebase-marts-combined-users-on-dim-user-eliminat-e81ba3`.
   `marts__combined__users` implements a competing identity rule; a durable `dim_user` key is
   the precondition for retiring it.

## Not verified

- **`user_global_id` coverage.** The rate at which trigger 2 fires, and therefore the
  expected merge volume, was not measured. It needs a production query against
  `dim_user` grouped by `user_pk_source`. Sizing the alias-table contract should wait for it.
- **Which external systems hold `user_pk`.** `reverse_etl` is a Trino role
  (`dbt_project.yml:104-107`); the consumers behind it are configured outside this repo. No
  Hightouch configuration exists here. Enumerate them before item 2 above.
- **Historical churn rate.** No measurement of how often `user_pk` has actually changed
  per build. The 2026-08-18 failure is a single observed instance, not a trend.
- **The no-op is proven; a direct old-vs-new table diff is not.** The equivalence above is
  established on identical input, which is the claim that matters. A naive diff of the two
  materialised models is NOT a clean test, because the old model reads staging live while
  the new one reads a frozen snapshot, and production rebuilds staging every few minutes.
  Do not treat a small delta from that comparison as a defect without first checking it is
  not drift.
- **No unit test guards the mint expression.** dbt unit tests do not currently run in this
  project: `macros/override_ref.sql` returns a bare string on its Glue-fallback path, which
  the unit-test machinery cannot consume, and on the credential-free `dev` target the
  upstream relations do not exist for column resolution. Until that is fixed, a change to
  the mint expression will not fail CI, and it would silently re-key the warehouse. This is
  the single most valuable follow-up in this list.
