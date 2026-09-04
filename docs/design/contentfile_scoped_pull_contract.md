# Cohort 4 — ContentFile Scoped-Pull Trigger Contract

Status: **spec** · Project: `wp-mit-learn-etl-migration-to-data-platform-bad80f`
Date: 2026-08-19 · Resolves `tk-define-contentfile-dagster-mit-learn-trigger-mec-8641eb`
Companion to [`learn_marts_contract.md`](../learn_marts_contract.md).

Spans two repos: the sender is `ol-data-platform`, the receiver is
[`mitodl/mit-learn`](https://github.com/mitodl/mit-learn).

---

## 0. What this replaces, and why a new endpoint

Today three Dagster locations already POST to `/api/v1/webhooks/content_files/`:

| Sender | Partitioned by |
|---|---|
| `dg_projects/openedx/openedx/assets/openedx.py:731` | `partitions_def` (per deployment, per course) |
| `dg_projects/edxorg/edxorg/assets/openedx_course_archives.py:332` | `course_and_source_partitions` |
| `dg_projects/canvas/canvas/assets/canvas.py:294` | `canvas_course_ids` (`DynamicPartitionsDefinition`) |

All three send a **pointer**: `{course_id, course_readable_id, content_path, source}`. On receipt
MIT Learn fires `ingest_edx_run_archive` (or `ingest_canvas_course` for Canvas) — i.e. the message
means *"a new archive is at this path, go extract it yourself."* **MIT Learn does the extraction.**

Cohort 4 inverts that: the platform runs Tika extraction and publishes
`integrations__learn__ocw_content_files` / `integrations__learn__content_files`, and MIT Learn
*consumes* extracted text. The message has to change meaning from "go extract" to
"extraction is done, come get it."

**This is a new endpoint, not a reinterpretation of the existing one.** All three senders share
one path, so changing what `process_create_content_file_request` does would flip openedx, edxorg
and Canvas simultaneously — a big-bang cutover with no per-source rollback. Keeping them
independent under the old path means branching the handler per `ETLSource`, which is the same
dual-meaning problem in a different place. A new path lets sources migrate one at a time, with
the old one still serving whatever has not moved, and makes the coexistence explicit in routing.

Canvas migrates too, so the old path eventually retires rather than living on (§7).

---

## 1. Endpoint

```
POST /api/v1/webhooks/extracted_content_files/
```

Named for what it carries. `content_files/` stays live and unchanged until §7 retires it.

Auth and transport are unchanged from the existing webhooks: subclass `BaseWebhookView`, so
`require_POST` + `require_signature` (HMAC-SHA256 over the exact request body, header
`X-MITLearn-Signature`, shared Vault secret `secret-global/shared_hmac` → `learn` → `token`) +
`non_atomic_requests` + `csrf_exempt` all apply. Sender side is
`ol_orchestrate.resources.learn_api.MITLearnApiClient._post_signed_webhook`, which already signs
exactly the bytes it posts.

## 2. Payload

```jsonc
{
  "courses": [
    {
      "source": "mit_edx",                                  // required, ETLSource member
      "readable_id": "course-v1:MITx+6.00.1x+2T2024",        // required
      "content_file_count": 412,                             // required, see §4
      "extracted_at": "2026-08-19T04:31:07+00:00"            // optional, provenance only
    }
  ]
}
```

A **list**, always — even for one course. This is what lets §5's per-asset cadence choice be made
on the platform side without a second payload shape: a per-partition condition sends a
one-element list, a batched/scheduled run sends N.

**`content_path` is deliberately absent.** It points at a raw archive MIT Learn no longer reads.
Carrying it would invite exactly the confusion this endpoint exists to remove.

`readable_id` is the course/run identifier used as the pull's scope key, and must match
`ContentFile.run.run_id` on the MIT Learn side — **not** `LearningResource.readable_id`, which is
keyed at the course level and would unpublish every other run's files. This is what the existing
webhook already sends as `course_readable_id` (`process_create_content_file_request` passes it
straight through as `run_id=`), so the new payload keeps the same semantics under a new field name.
Getting this wrong is the failure mode from the podcast migration (`integrations__learn__podcasts`),
where a dlt-derived id differed from MIT Learn's own derivation by a trailing slash and would have
created duplicate resources instead of updating them. **Verify the derivation on both sides per
source before enabling.**

## 3. Receiver behaviour

For each entry, enqueue a **scoped** pull rather than doing work in the request:

```python
SyncOpenEdXContentFilesTask.apply_async(
    kwargs={"source": source, "readable_id": readable_id, "expected_count": content_file_count},
)
```

with `SyncOCWContentFilesTask` for `ocw`. Respond `200` once enqueued — the webhook acknowledges
receipt, not completion, matching the existing handlers.

An entry naming an unknown/unsupported `source` is logged and skipped, not 500'd, so one bad
source cannot reject an otherwise-valid batch. (Same resilience rule the `learning_resources`
handler uses.)

These tasks subclass `BaseWarehouseETLTask` from
[mit-learn#3807](https://github.com/mitodl/mit-learn/pull/3807)
(`learning_resources/lib/warehouse.py`), which already provides the StarRocks connection,
`iter_rows`, and the watermark bookkeeping (mit-learn#3566, the earlier Trino-backed version, was
closed in favor of #3807's StarRocks rewrite).

**Open for the implementer:** `iter_rows` currently only filters on `last_modified` (the
incremental path) and `BaseWarehouseETLTask.run()` discards `**kwargs`, so neither supports a scope
predicate yet. Landing the scoped pull needs new code on the mit-learn side: a scope predicate
parameter on `iter_rows`, `run()` forwarding `source`/`readable_id` through to it, and confirming a
scoped run does not advance the incremental watermark. Not designed here since it's mit-learn-side
work.

## 4. The scoping problem — the part that needs new code

`BaseWarehouseETLTask` has exactly two modes, and **neither is what this needs**:

| mode | `since` | prunes? |
|---|---|---|
| `full_refresh=True` | `None` | yes — "the only mode that ever sees deletes/unpublishes upstream" |
| `full_refresh=False` | last watermark | **no** — "a partial pull must never be treated as the complete state of the source" |

A per-course trigger needs a **third mode: scoped full refresh.** Within one course it *is* the
complete state — a file removed from that course must be unpublished — but globally it is not, and
pruning globally from a single-course pull would unpublish the entire corpus.

So the prune predicate must be scoped to the same key as the fetch:

> prune content files whose `run.run_id == readable_id` **and**
> `run.learning_resource.etl_source == source` **and** which are absent from this result set —
> never anything outside that course.

Both fields are needed: `run_id` alone is not unique across sources (Open edX run keys and Canvas
course/run pairs can collide), so the warehouse view must expose `etl_source` alongside the scope
key for the receiver to filter on (see §9).

This is the podcast full-sync hazard (`MIN_PODCASTS` / `MIN_EPISODES` in
`assets/podcasts.py`) at a different granularity, and it deserves the same guard.
`content_file_count` in the payload is that guard: the platform states how many rows it published,
and the task **refuses to prune** if the warehouse returns materially fewer. A course legitimately
going to zero files is rare enough to be worth an explicit override rather than a silent mass
unpublish.

**Open for the implementer:** whether `content_file_count` mismatch should abort the whole pull or
merely skip pruning while still upserting. Skipping the prune is the safer default — it degrades
to "stale extra files" rather than "missing files."

## 5. Cadence — per asset, not one global answer

Cadence is chosen per sending asset via Dagster automation conditions / freshness policies, not a
hardcoded cron. The three existing senders (openedx, edxorg, canvas) are already partitioned per
course, so per-partition triggering needs no restructuring there; OCW's new sending asset (§8) will
need this designed from scratch.

**⚠ Constraint learned the hard way.** `dg_projects/openedx/openedx/assets/openedx.py:194`
documents an outage caused by exactly this:

> Deliberately carries no `automation_condition`. An AutomationCondition is evaluated per
> *partition*, so an hourly cron on a 3,500-partition asset asks for 3,500 observation runs an
> hour — and because the observe function sweeps the whole deployment regardless of which
> partition its run was requested for, each of those runs re-fetched every course. That is O(N²)
> against the LMS and it saturated the run queue to the point where no export ran at all.

The mitigation was `courseware_observation_sensor`: one sweep per tick, reporting versions
directly, with no run in between.

**Rule:** a per-partition `AutomationCondition` is safe only where *evaluating* it is genuinely
per-partition and cheap. Where evaluation sweeps globally, use a single sensor that batches into
one N-element POST instead. Prove the evaluation cost before attaching a condition to a
high-cardinality partitioned asset — this asset graph has already been taken down by getting it
wrong.

Existing pattern to build on: `_DBT_AUTOMATION_CONDITION = upstream_or_code_changes()` in
`dg_projects/lakehouse/lakehouse/assets/lakehouse/dbt.py:39`.

## 6. Deletes

`/api/v1/webhooks/content_files/delete/` (`ContentFileDeleteWebhookView` →
`process_delete_content_file_request`) deletes a whole `LearningResource` by `course_id` prefix.
That is course-level and orthogonal to extracted-file sync, so **it is out of scope here and stays
where it is.** Per-file removal within a still-live course is handled by §4's scoped prune, not by
a delete webhook.

## 7. Coexistence and retirement

Both paths run simultaneously; each sender moves independently.

1. Land the endpoint + the two pull tasks. No sender changes. Nothing happens yet.
2. Move senders one at a time — swap `notify_course_export` for the new client method on that
   asset. Rollback is reverting one asset.
3. Once all four (openedx, edxorg, canvas, **and** OCW — see §8) are on the new path, delete
   `ContentFileWebhookView`, `process_create_content_file_request`, the `ETLSource.canvas` branch,
   and `notify_course_export`.
4. `content_files/delete/` survives that retirement (§6).

Because Canvas migrates, the `ETLSource.canvas` branch in the current handler is transitional, not
permanent.

## 8. OCW has no sender at all

The three senders are openedx, edxorg and canvas. **There is no OCW content-file sender anywhere
in `dg_projects`, and no OCW dg project** — OCW's only Learn-facing model is the course-level
`integrations__learn__ocw_courses` (Cohort 1). So OCW needs a *new* sending asset built regardless
of anything above; it cannot ride the existing mechanism.
Tracked by `tk-ocw-text-extraction-assets-integrations-learn-oc-8ff9b4`.

## 9. Warehouse views the tasks read

| Task | View |
|---|---|
| `SyncOCWContentFilesTask` | `integrations.integrations__learn__ocw_content_files` |
| `SyncOpenEdXContentFilesTask` | `integrations.integrations__learn__content_files` |

Neither model exists yet. Both must expose at minimum the scope key (`readable_id` of the owning
course/run), `etl_source` (needed by the §4 prune predicate to disambiguate scope keys across
sources), plus the `ContentFile` fields MIT Learn persists — `key`, `title`, `description`,
`url`, `file_type`, `content`, `content_title`, `content_author`, `content_language`,
`content_type`, `image_src`, `uid` — and a `last_modified` for the incremental (`since`) path.
Column names must match `learn_marts_contract.md` conventions.

## 10. Checklist before enabling any source

- [ ] `readable_id` derivation verified identical on both sides for that source (§2)
- [ ] Scoped prune proven to touch only the named course (§4)
- [ ] `content_file_count` guard exercised against a deliberately short read (§4)
- [ ] Evaluation cost of the chosen automation condition measured, not assumed (§5)
- [ ] Parallel-validation diff between the Celery-extracted and platform-extracted text reviewed —
      Tika output will not be byte-identical to whatever the legacy path produced, so agree what
      counts as an acceptable delta *before* cutover rather than during it

---

## Open questions

1. **Abort vs skip-prune on count mismatch** (§4). Recommend skip-prune.
2. **Batch size ceiling** for the N-element form. Unbounded lists make one failed POST lose a whole
   sweep; a cap turns it into partial progress.
3. **Does `expected_count` belong in the task signature or re-read from the view?** In the payload
   it is a cross-check between two systems; re-read from the view it only catches read failures,
   not publish failures.
