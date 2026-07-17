# Feedback Aggregation — Dimensional Model Design

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-06 (schema) · See [`README_feedback_aggregation.md`](./README_feedback_aggregation.md) for the full spec set.

Conforms to the existing Kimball layer in `src/ol_dbt/models/dimensional` (`tfact_*` transactional facts,
`afact_*` aggregate facts, `dim_*` conformed dimensions, surrogate `*_pk` via
`dbt_utils.generate_surrogate_key`, `*_fk` in facts, `time_fk`/`date_fk`). Precedents: `tfact_discussion_events`,
`tfact_chatbot_events`.

---

## 1. Grain

**`tfact_feedback` — one row per atomic free-text feedback utterance.**

"Utterance" = the smallest independently-clusterable unit of a source:

| Source | One row = | Text column source |
|--------|-----------|--------------------|
| Zendesk | one ticket (first comment) | `ticket_description` (+ `ticket_subject`) |
| edX forum | one `*.created` post/response/comment | `post_content` (+ `post_title`) |
| Learn AI tutor | one human turn | `human_message` |
| ORA peer feedback | one feedback submission | `feedback_text` |
| edX feedback plugin (new) | one feedback event | `feedback_text` (contract §5) |

Rationale: the ML tasks (embed → cluster → label) operate per-utterance. Ticket *threads* and full tutor
*conversations* are deliberately **not** the grain — a `conversation_fk` / `thread_id` degenerate key preserves
the ability to roll up. Agent/bot messages are excluded (not feedback); forum view/vote/follow events are
excluded (filter to `*.created`).

---

## 2. The fact: `tfact_feedback`

```
-- surrogate + conformed FKs
feedback_pk           -- generate_surrogate_key([source_slug, source_record_ref])  (see §6)
feedback_source_fk    -> dim_feedback_source.feedback_source_pk
user_fk               -> dim_user.user_pk            (nullable; anonymous/aggregate sources)
platform_fk           -> dim_platform.platform_pk    (nullable; non-course sources e.g. Zendesk)
courserun_fk          -> dim_course_run.courserun_pk  (nullable; only course-scoped sources)
organization_fk       -> dim_organization.organization_pk (nullable; Zendesk group/org)
category_fk           -> dim_feedback_category.feedback_category_pk (late-arriving, §4a)
sentiment_fk          -> dim_sentiment.sentiment_pk           (late-arriving, §4b)
time_fk               -> dim_time
date_fk               -> dim_date

-- degenerate / conversational keys
conversation_id       -- thread/ticket id, source-native (roll up utterances → conversation)
source_record_id      -- source PK (zendesk ticket_id, forum post_id, checkpoint id, ora id)
source_url            -- deep link back to origin (ticket api url, forum page_url)

-- text (REDACTED — see §7; raw text never lands in the fact)
feedback_title        -- subject / post_title (redacted)
feedback_text         -- description / post_content / human_message / feedback_text (redacted)
feedback_text_chars   -- length metric (pre-redaction), for sizing/analytics
embedding_id          -- FK/handle into the embedding + cluster store (§4c); nullable until scored

-- source-native descriptive attributes (kept for facet filters; NOT conformed)
source_status         -- zendesk ticket_status / null
source_priority       -- zendesk ticket_priority / null
source_tags           -- array<varchar>: zendesk ticket_tags, forum roles, etc. (category SEEDS, §4a)
source_channel        -- zendesk source_channel / tutor agent / forum component
csat_score            -- zendesk satisfaction_rating_score / tutor rating / null (explicit sentiment)

-- audit
feedback_occurred_at  -- source event time (created_at / event_timestamp / checkpoint_created_on)
feedback_ingested_at
```

Materialized `table`. Nullable FKs are expected and correct: Zendesk has no courserun; forum/tutor have no
org; anonymous feedback has no user. The fact tolerates sparse conformance — that is what makes it
*source-flexible*.

---

## 3. Conformed dimensions — REUSED (no new work)

| Dim | Key | Populated for |
|-----|-----|---------------|
| `dim_user` | `user_pk` | forum, tutor, ORA, Zendesk (where requester resolves) |
| `dim_platform` | `platform_pk` | course-scoped sources |
| `dim_course_run` | `courserun_pk` | forum, tutor, ORA (via `courserun_readable_id`) |
| `dim_organization` | `organization_pk` | Zendesk (`group_name`/`organization_name`) |
| `dim_date` / `dim_time` | `date_fk`/`time_fk` | all |

**Identity is the highest-risk join** (cf. open p0 bug `tk-fix-dim-user-null-email-identity-collapse`).
Resolve `user_fk` via the same paths the existing facts use — `openedx_user_id` → `dim_user.mitlearn_openedx_user_id`
(forum/tutor), `user_global_id` (tutor `int__learn_ai__chatbot`), email (Zendesk requester, last resort).
Never key the fact off a source's local PK (§6).

---

## 4. New dimensions

### 4a. `dim_feedback_category` (LLM/embedding-discovered, curated)
```
feedback_category_pk   -- generate_surrogate_key([category_slug])
category_slug          -- stable machine key (survives relabeling)
category_label         -- human label (LLM-proposed, human-approved)
category_parent_slug   -- optional hierarchy (cluster → category → theme)
category_status        -- proposed | approved | merged | deprecated
category_source        -- 'llm_discovered' | 'seed' | 'manual'
first_seen_at / updated_at
```
Bootstrapped, **not cold-start**: seed from Zendesk `ticket_tags` (2,354 distinct) + `group_name`, then LLM-label
embedding clusters (task `tk-define-llm-driven-category-discovery`). SCD-lite: relabeling changes `category_label`,
never `category_slug`. Assignment (`category_fk` on the fact) is late-arriving — a ticket can be categorized after
insert.

### 4b. `dim_sentiment`
```
sentiment_pk           -- generate_surrogate_key([sentiment_slug])
sentiment_slug         -- 'positive' | 'neutral' | 'negative' | (aspect-based later)
polarity_score_bucket  -- optional coarse bucket for trend rollups
```
Small conformed dim. `sentiment_fk` derived by the sentiment task (`tk-define-sentiment-mapping`) —
semantic/embedding or LLM. Explicit signals (`csat_score`, tutor `rating`) seed/validate it.

### 4c. `dim_feedback_source`
```
feedback_source_pk     -- generate_surrogate_key([source_slug])
source_slug            -- 'zendesk' | 'edx_forum' | 'learn_ai_tutor' | 'ora' | 'edx_feedback_plugin'
source_name            -- display
source_medium          -- 'support_ticket' | 'forum_post' | 'chat' | 'assessment' | 'in_product'
source_audience_scope  -- 'operational' | 'course' | 'strategic' (see audience memory)
is_course_scoped       -- bool (whether courserun_fk applies)
```

### 4d. Embedding + cluster store (NOT in the dimensional schema)
Vectors and cluster assignments live in a **sidecar** table (`feedback_embeddings`) keyed by `embedding_id`,
not in `tfact_feedback` (facts stay narrow, embeddings churn on re-clustering). It holds: `embedding_id`,
`feedback_pk`, `vector`, `cluster_id`, `cluster_run_id`, `model_version`. `dim_feedback_category` is the
*curated* projection of stable clusters; the sidecar is the *mutable* ML working set. This keeps re-clustering
from rewriting the fact.

---

## 5. Common feedback event contract (interim↔target bridge)

Both the interim learn-ai landing (`raw__learn_ai__…__feedback`) and the eventual analytics-api/StarRocks
data-bus MUST emit this shape so `stg__…__feedback` → `tfact_feedback` is source-path-agnostic
(task `tk-define-stable-common-feedback-event-contract-bus-245a8e`):

```
source_slug            -- maps to dim_feedback_source
occurred_at            -- ISO8601
subject_user_ref       -- global/openedx user id (NOT a source-local PK)
courserun_readable_id  -- nullable
platform               -- nullable
conversation_ref       -- thread/ticket id
source_record_ref      -- source-native id (idempotency key)
title / text           -- raw free text (redaction happens in-warehouse, §7)
source_metadata        -- json: tags, status, priority, channel, csat, etc.
```
Align this to the **general data-bus ingestion contract**, not a feedback-specific one, so future openedx
tracking logs share ingress semantics.

---

## 6. Business-key strategy (migration-proof)

`feedback_pk = generate_surrogate_key([source_slug, source_record_ref])` where `source_record_ref` is a
**stable business identifier from the source system**, never a warehouse/Airbyte/Postgres row PK
(terminology matches the event contract + Zendesk MVP spec):

| Source | source_record_ref |
|--------|-------------------|
| Zendesk | `ticket_id` |
| edX forum | `post_id` |
| Learn AI tutor | `thread_id` + `checkpoint_pk` (or message index) |
| ORA | `submission_uuid` |
| edX plugin | contract `source_record_ref` |

This is what lets the interim→analytics-api migration be "swap source + backfill + parity" rather than a
rebuild: the same `feedback_pk` regenerates identically regardless of pipe.

---

## 7. PII handling (mandatory pre-embed)

`ticket_description` and `int__learn_ai__chatbot.human_message` carry emails/phones/names (profiler-flagged
`PII.Sensitive`). A **redaction step in the `int__` layer** (Presidio — precedent: the OM profiler already runs
Presidio recognizers) masks entities before text lands in `tfact_feedback` or the embedding store. Raw text
stays in `raw`/`stg` under existing PII classification + Lakekeeper/Cedar authz; the fact carries redacted text
only. Access to the fact is still governed per §audience (course instructors see their courserun; support/eng
see tickets; leadership sees aggregates).

---

## 8. Layering & build path

```
raw__<source>__…                         (existing per source; new: raw__learn_ai__…__feedback)
  → stg__<source>__…__feedback           (conform to §5 contract, light typing)
    → int__feedback__unioned             (UNION all sources into the common shape + redact PII §7)
      → tfact_feedback                    (resolve conformed FKs, generate feedback_pk §6)
        → afact_feedback_cluster_daily    (aggregate fact: cluster × category × sentiment × date × source — strategic rollup)
```
Clustering/labeling jobs (Dagster asset) read `int__feedback__unioned` (redacted) and write
`feedback_embeddings`, `dim_feedback_category`, and `category_fk`/`sentiment_fk` back onto the fact
(late-arriving update).

---

## 9. MVP vs. full

- **MVP (support/eng):** `dim_feedback_source` + `tfact_feedback` restricted to `source_slug='zendesk'`, category
  seeded from `ticket_tags`, sentiment from `csat_score` + model. ~198K rows, batch. No new dims beyond the three.
- **Phase 2:** add forum/tutor/ORA sources (fact already tolerates them), enable cross-source
  `afact_feedback_cluster_daily` for leadership, course-scoped views for instructors.
- **Phase 3:** analytics-api/data-bus ingress (contract §5 already lets the fact ignore the switch).

---

## 10. Open items handed to downstream tasks

- Category discovery & seeding → `tk-define-llm-driven-category-discovery-to-seed-pop-550aba`
- Sentiment method & `dim_sentiment` grain → `tk-define-sentiment-mapping-via-semantic-embedding--92988e`
- Clustering method + `feedback_embeddings` store + roll-up hierarchy → `tk-define-clustering-approach-for-systemic-issue-de-a1d7d6`
- Per-persona actions, surface, row-level access → `tk-define-ui-ux-audience-actions-and-where-the-expe-476d23`
- Contract finalization + business keys → `tk-define-stable-common-feedback-event-contract-bus-245a8e`
```
