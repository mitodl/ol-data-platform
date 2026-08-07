# Feedback Aggregation — Common Event Contract & Business-Key Strategy

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 · Resolves `tk-...-common-feedback-event-contract-bus-245a8e`
Consolidates design §5–6; companion to [`feedback_dimensional_model.md`](./feedback_dimensional_model.md).

This is the **highest-leverage migration de-risker** (do at spec time regardless of interim
path). If the interim learn-ai landing AND the eventual analytics-api/StarRocks data-bus
both emit this shape, the migration collapses to *source-swap + backfill + parity*; if they
diverge, it's a rebuild.

---

## 1. The contract (source-agnostic feedback event)

Every producer — the interim learn-ai feedback table, the edX feedback plugin, and each dbt
staging model that adapts an existing source (Zendesk, forum, tutor, ORA) — MUST present
this shape at the `stg__…__feedback` boundary:

| Field | Type | Meaning | Required |
|---|---|---|---|
| `source_slug` | string | maps to `dim_feedback_source.source_slug` | yes |
| `occurred_at` | ISO8601 timestamp | source event time | yes |
| `source_record_ref` | string | **stable source-native id of the TURN** — idempotency key + business key (§2) | yes |
| `text` | string | raw free text (redaction happens in-warehouse, design §7) | yes |
| `channel_slug` | string | maps to `dim_feedback_channel.channel_slug` — how the feedback arrived | yes |
| `title` | string | subject/heading; nullable | no |
| `conversation_ref` | string | thread/ticket id — roll turns → conversation | no |
| `turn_index` | integer | ordinal of this turn within the conversation | no |
| `subject_user_ref` | string | **global/openedx user id** (NOT a source-local PK); email only as last resort | no |
| `courserun_readable_id` | string | course scope; null for non-course sources | no |
| `platform` | string | platform readable id; nullable | no |
| `subject_type` | string | **what the feedback is about**: `courseware_block` \| `course_run` \| `course` \| `program` \| `page_url` \| `resource` \| `unspecified` | no |
| `subject_ref` | string | source-native id of that thing (edX usage key, courserun readable id, decoded URL) | no |
| `subject_url` | string | canonical/decoded deep link to the subject | no |
| `explicit_rating` | string | conformed explicit signal — Zendesk CSAT, tutor rating, ORA score | no |
| `created_at` | ISO8601 timestamp | source lifecycle timestamp | no |
| `updated_at` | ISO8601 timestamp | source lifecycle timestamp | no |
| `source_metadata` | JSON | **everything not yet conformed** — status, priority, brand, group, due date, custom fields | no |

**The conformance rule that decides what is a field vs. what is `source_metadata`:**

> An attribute earns a contract field only if **two or more sources can populate it**. Everything else rides
> in `source_metadata`, and is promoted to a field if and when a second source starts supplying it — in the
> same change that onboards that source.

This is why `channel_slug`, `explicit_rating`, `created_at`/`updated_at` and `turn_index` are fields (every
or most sources have them) while status, priority, brand, group and due date are not (Zendesk-only). Adopted
2026-08-07 after RFC review; the full per-attribute audit is in [`feedback_erd.md`](./feedback_erd.md) §1.

**Design rules:**
- `subject_user_ref` is a **global identity ref, never a source row PK** — this is what lets
  `user_fk` resolve against `dim_user` consistently across sources and survive the migration.
  (Zendesk, lacking an openedx id, uses requester email as the documented last-resort ref —
  which shares the `dim_user` NULL-email identity-collapse failure class; see design §3.)
- `text`/`title` carry **raw** text across the contract; redaction is a warehouse step
  (design §7), not a producer responsibility — so producers never need Presidio.
- `source_metadata` is the extension point, and it **survives into the fact as a variant column** rather
  than being flattened into facet columns (design §2c). Iceberg v2 has no native JSON type, so it persists
  as a varchar holding a JSON string, read back with the cross-db `json_query_string` /
  `json_extract_value` macros. This is the mechanism that makes a new source additive: a producer arriving
  with attributes nobody has seen before needs no schema change.
- **`source_record_ref` identifies a turn, not a conversation.** For Zendesk that is `comment_id`, with
  `ticket_id` in `conversation_ref` (design §1). Adapters that emit one row per conversation are not
  conformant — the grain is one atomic utterance, and every source's own model already works that way.
- **`subject_*` answers "what is this feedback about?"** — the axis you aggregate on, and the one
  the contract originally lacked (raised on [#2422](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-3157271372)).
  It is a *polymorphic degenerate triple*: always carryable, whatever the subject is. The fact resolves
  it to a conformed FK where one exists — `courserun_fk` for course runs, the new
  `content_block_fk → dim_course_content` for edX blocks (design §2a). Producers emit the raw ref;
  **normalising it is the adapter's job**, e.g. decoding the encoded Appzi URL in
  `int__feedback__zendesk`.

---

## 2. Business-key strategy (migration-proof)

```
feedback_pk = generate_surrogate_key([source_slug, source_record_ref])
```

`source_record_ref` is a **stable business identifier from the source system**, never a
warehouse/Airbyte/Postgres row PK:

| Source | `source_record_ref` (the **turn**) | `conversation_ref` |
|---|---|---|
| Zendesk | `comment_id` | `ticket_id` |
| edX forum | `post_id` | thread id |
| Learn AI tutor | `checkpoint_id` | `chatsession_thread_id` |
| ORA | `submission_uuid` | n/a |
| edX feedback plugin | plugin-native event/record id | n/a |

> **Changed 2026-08-07 (rev. 2).** Zendesk's `source_record_ref` moved from `ticket_id` to `comment_id` when
> the grain moved from ticket to turn (design §1). The formula is unchanged; the *value* is not — which is
> exactly why the grain decision had to land before the fact ships. Regenerating this key after the fact has
> consumers is the rebuild this strategy exists to avoid.

Because `feedback_pk` regenerates **identically** regardless of which pipe delivered the
event, the interim→data-bus swap re-lands the same rows with the same keys — enabling
backfill + parity validation instead of a rebuild. This is the linchpin of the whole
contract-first strategy (RFC Option 3).

---

## 3. Alignment to the general data-bus ingestion contract (dependency, not a decision here)

Per the 2026-07-06 correction: this contract should align to the **general StarRocks
data-bus ingestion contract** (shared write/dedup/flush semantics), *not* a feedback-specific
shape — so feedback and the future openedx tracking-logs workload share ingress semantics.

**But** the data-bus write path does not exist yet, and its sink topology (**one generic
sink vs. sink-per-topic**) is an unsettled platform-wide question that outranks feedback
(`pf-starrocks-data-bus-write-path-not-built-yet-gene-bf78da`; RFC Open Questions). So:

- This spec defines the **feedback event contract now** (the durable artifact) and keys the
  fact off stable business ids (§2) — both fully within feedback's control and sufficient to
  de-risk the migration.
- It does **not** unilaterally define the general bus contract. When the platform settles
  generic-vs-per-topic and builds the write path, this contract's field set is the proposed
  feedback *topic* payload; reconcile field names/envelope with the bus's shared envelope at
  that time. The business-key rule (§2) is invariant under that reconciliation.
- **Landing namespace** on eventual flush: a dedicated `feedback`/ingested schema (or a
  per-topic schema, depending on the sink decision), **not** `raw`. Decided before migration
  step 9, not now.

**Net:** feedback ships on the contract + business keys defined here (non-blocking for the
MVP); the general-bus alignment is a documented, bounded reconciliation gated on platform
decisions that feedback must conform to rather than set.

---

## 4. Conformance requirements (what each producer/adapter must do)

- **Interim learn-ai feedback table:** shape its columns to §1 so `stg__learn_ai__…__feedback`
  is a near pass-through (RFC Implementation step 2).
- **Zendesk adapter (MVP):** `int__feedback__zendesk` maps ticket columns → §1
  (`feedback_zendesk_mvp_spec.md` §2). `source_record_ref = ticket_id`.
- **Future producers (edX plugin, etc.):** emit §1 directly or via a thin staging adapter.
- **The fact never reads a source's raw shape** — only `int__feedback__unioned` in the §1
  contract shape. This is the isolation that makes new sources additive.
