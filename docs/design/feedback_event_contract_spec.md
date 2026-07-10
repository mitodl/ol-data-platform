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
| `source_record_ref` | string | **stable source-native id** — idempotency key + business key (§2) | yes |
| `text` | string | raw free text (redaction happens in-warehouse, design §7) | yes |
| `title` | string | subject/heading; nullable | no |
| `conversation_ref` | string | thread/ticket id — roll utterances → conversation | no |
| `subject_user_ref` | string | **global/openedx user id** (NOT a source-local PK); email only as last resort | no |
| `courserun_readable_id` | string | course scope; null for non-course sources | no |
| `platform` | string | platform readable id; nullable | no |
| `source_metadata` | JSON | tags, status, priority, channel, csat, etc. | no |

**Design rules:**
- `subject_user_ref` is a **global identity ref, never a source row PK** — this is what lets
  `user_fk` resolve against `dim_user` consistently across sources and survive the migration.
  (Zendesk, lacking an openedx id, uses requester email as the documented last-resort ref —
  which shares the `dim_user` NULL-email identity-collapse failure class; see design §3.)
- `text`/`title` carry **raw** text across the contract; redaction is a warehouse step
  (design §7), not a producer responsibility — so producers never need Presidio.
- `source_metadata` is the extension point: anything source-specific rides in the JSON and is
  projected into the fact's non-conformed facet columns (`source_status`, `source_priority`,
  `source_tags`, `source_channel`, `csat_score`) without changing the contract.

---

## 2. Business-key strategy (migration-proof)

```
feedback_pk = generate_surrogate_key([source_slug, source_record_ref])
```

`source_record_ref` is a **stable business identifier from the source system**, never a
warehouse/Airbyte/Postgres row PK:

| Source | `source_record_ref` |
|---|---|
| Zendesk | `ticket_id` |
| edX forum | `post_id` |
| Learn AI tutor | `thread_id` + `checkpoint_pk` (or message index) |
| ORA | `submission_uuid` |
| edX feedback plugin | plugin-native event/record id |

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
