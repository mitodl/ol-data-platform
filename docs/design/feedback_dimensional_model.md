# Feedback Aggregation — Dimensional Model Design

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-08-10 (rev. 3 — conversation-grain analysis fact; rev. 2 turn grain + conformance rule;
original schema 2026-07-06) ·
See [`README_feedback_aggregation.md`](./README_feedback_aggregation.md) for the full spec set.

Conforms to the existing Kimball layer in `src/ol_dbt/models/dimensional` (`tfact_*` transactional facts,
`afact_*` aggregate facts, `dim_*` conformed dimensions, surrogate `*_pk` via
`dbt_utils.generate_surrogate_key`, `*_fk` in facts, `time_fk`/`date_fk`). Precedents: `tfact_discussion_events`,
`tfact_chatbot_events`.

> **Diagrams:** [`feedback_erd.md`](./feedback_erd.md) renders this model (plus the contract, the conversation
> fact and the Phase-1 subset) as Mermaid ERDs, and carries the change log.

> **Two facts, one pair.** `tfact_feedback` (§2) records what was said, one row per turn, and is never
> rewritten. `afact_feedback_conversation` (§5a) is the **analysis unit** — one row per conversation, carrying
> everything a model generated: the summary, the embedding, the sentiment, the category and the cluster. The
> line between them is *ingested* vs. *inferred*, and it is what lets the whole inferred layer be rebuilt
> without touching the record of what people actually wrote.

---

## 0. The conformance rule

> **An attribute earns a column on the fact, or a conformed dimension, only if two or more sources can
> populate it.** Everything else lives in the `source_metadata` variant column (§2c), and is promoted to a
> real column if and when a second source starts supplying it.

Adopted 2026-08-07 after RFC review. `tfact_feedback` exists to aggregate Zendesk *plus* edX forum, Learn AI
tutor, ORA and the edX feedback plugin; shaping the core tables around what Zendesk happens to expose makes
every later source pay for it. The full per-attribute audit against all five sources is in
[`feedback_erd.md`](./feedback_erd.md) §1.

**Promotion path** (so the variant is a staging area, not a dumping ground): when a second source begins
populating a key inside `source_metadata`, promote it to a real column or a conformed dimension **in the same
change that onboards that source** — not later, and not speculatively in advance.

---

## 1. Grain

**`tfact_feedback` — one row per atomic free-text feedback utterance, where an utterance is one *turn* of a
conversation.**

| Source | One row = | Text column source | `source_record_ref` | `conversation_ref` |
|---|---|---|---|---|
| Zendesk | one **public, requester-authored comment** | `comment_plain_body` (+ `ticket_subject`) | `comment_id` | `ticket_id` |
| edX forum | one `*.created` post/response/comment | `post_content` (+ `post_title`) | `post_id` | thread id |
| Learn AI tutor | one human turn | `human_message` | `checkpoint_id` | `chatsession_thread_id` |
| ORA peer feedback | one feedback submission | `feedback_text` | `submission_uuid` | n/a |
| edX feedback plugin (new) | one feedback event | `feedback_text` | plugin event id | n/a |

Rationale: turn grain is what the other sources already do — `int__learn_ai__chatbot` is one row per
`djangocheckpoint` (ordered by `checkpoint_step`), and the forum sources are one row per tracking-log event —
and it is the only grain from which a *complete* conversation can be assembled. Recording the turns is what
makes the conversation-grain analysis in §5a possible; the ML tasks themselves operate on the assembled
conversation, not on isolated turns (**changed in rev. 3**).

**Changed in rev. 2:** Zendesk was previously modelled at *ticket* grain (text = `ticket_description`, the
first comment only), which made it the only source where "utterance" silently meant "whole conversation" —
a Zendesk-shaped exception of exactly the kind §0 exists to prevent. `int__zendesk__ticket_comment` already
exists at comment grain. First-comment-only is preserved as a **filter, not a grain**, via
`is_conversation_opening`.

**Exclusions, stated once as a single rule across sources — the author must be the person giving feedback,
not the platform answering:**

| Source | Keep | Drop |
|---|---|---|
| Zendesk | `comment_is_public = true` **and** author = ticket requester | internal notes, agent replies |
| Learn AI tutor | `human_message` | `agent_message` (bot turns) |
| edX forum | `*.created` events | view / vote / follow events |

Conversation-level attributes (status, priority, due date, resolution time) are **not** denormalized onto
this fact — at turn grain they would repeat across every row of a conversation. They live in
`afact_feedback_conversation` (§5a), which is also where every model-derived attribute lives (rev. 3).

**Sources with no thread** — ORA submissions and edX-plugin events — have `conversation_ref =
source_record_ref`, so they degenerate to a one-turn conversation and still get exactly one
`afact_feedback_conversation` row. `dim_feedback_source.is_conversational` marks the distinction; nothing
downstream has to special-case them.

---

## 2. The fact: `tfact_feedback`

```
-- surrogate + conformed FKs
feedback_pk           -- generate_surrogate_key([source_slug, source_record_ref])  (see §6)
feedback_source_fk    -> dim_feedback_source.feedback_source_pk
feedback_channel_fk   -> dim_feedback_channel.feedback_channel_pk  (conformed; every source has one)
user_fk               -> dim_user.user_pk            (nullable; anonymous/aggregate sources)
platform_fk           -> dim_platform.platform_pk    (nullable; non-course sources e.g. Zendesk)
courserun_fk          -> dim_course_run.courserun_pk  (nullable; only course-scoped sources)
content_block_fk      -> dim_course_content.content_block_pk (nullable; resolves subject_ref, §2a)
organization_fk       -> dim_organization.organization_pk (nullable; Zendesk group/org — see §2b)

-- NOTE (rev. 3): category_fk / sentiment_fk are NOT on this fact. They are model-derived, they are
--   assigned at conversation grain, and they used to be the only reason anything ever wrote to
--   tfact_feedback after insert. They live on afact_feedback_conversation (§5a).

-- role-playing date/time FKs (§2d)
occurred_date_fk      -> dim_date    -- the utterance itself; the grain's timestamp
occurred_time_fk      -> dim_time
created_date_fk       -> dim_date    -- source lifecycle
created_time_fk       -> dim_time
updated_date_fk       -> dim_date
updated_time_fk       -> dim_time

-- degenerate / conversational keys
conversation_id       -- thread/ticket id, source-native (roll up turns → conversation)
turn_index            -- ordinal of this turn within the conversation
is_conversation_opening -- boolean; recovers the first-turn-only view as a filter
source_record_id      -- the TURN business key (zendesk comment_id, forum post_id, tutor checkpoint_id)
source_url            -- deep link back to origin (ticket api url, forum page_url)

-- subject: WHAT the feedback is about (§2a) — polymorphic, degenerate
subject_type          -- courseware_block | course_run | course | program | page_url | resource | unspecified
subject_ref           -- source-native id of that thing (edX usage key, courserun readable id, decoded URL)
subject_url           -- canonical/decoded deep link to the subject (nullable)

-- text (REDACTED — see §7; raw text never lands in the fact)
feedback_title        -- subject / post_title (redacted)
feedback_text         -- comment body / post_content / human_message / feedback_text (redacted)
feedback_text_chars   -- length metric (pre-redaction), for sizing/analytics

-- conformed measure
explicit_rating       -- zendesk satisfaction_rating_score / tutor rating / ORA score (§4b seed)

-- everything not yet conformed (§2c)
source_metadata       -- VARIANT (varchar holding JSON): status, priority, brand, group, due,
                      --   custom fields, and whatever the next source brings

-- audit
feedback_occurred_at  -- source event time (comment_created_at / event_timestamp / checkpoint_created_on)
feedback_created_at   -- source lifecycle timestamp
feedback_updated_at   -- source lifecycle timestamp
feedback_ingested_at
```

Materialized `table`. Nullable FKs are expected and correct: Zendesk has no courserun; forum/tutor have no
org; anonymous feedback has no user. The fact tolerates sparse conformance — that is what makes it
*source-flexible*.

**Insert-only as of rev. 3.** With `category_fk`/`sentiment_fk` gone, no process updates a row of this fact
after it lands. That is a stronger property than "the sidecar keeps re-clustering off the fact": there is now
no late-arriving write path to `tfact_feedback` at all, so re-summarizing, re-embedding, re-scoring and
re-clustering are all confined to a table that can be dropped and rebuilt.

Multi-valued `source_tags` is **not** a column on the fact — it is `bridge_feedback_tag` + `dim_feedback_tag`
(§4e). An `array<varchar>` on a fact is a modelling smell, and the tag set is also the category seed, so it
earns a real dimension.

### 2a. Subject — what the feedback is *about*

Added after review feedback on [#2422](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-3157271372):
the model captured *who* said it, *when*, and *in what conversation*, but had no unambiguous slot for what it
concerns — which is exactly the axis you aggregate on. Known cases: the edX feedback plugin captures the
courseware block id; some Zendesk tickets carry course metadata; Appzi-originated tickets capture the
(encoded) URL the user was viewing.

Modelled as a **polymorphic degenerate triple plus one conformed FK**, deliberately *not* a new
`dim_feedback_subject` — the subject is polymorphic across dimensions that already exist (a block is
`dim_course_content`, a run is `dim_course_run`, a program is `dim_program`), and a new dim would shadow them.

- `subject_type` / `subject_ref` / `subject_url` — the universal fallback, always populated where known,
  including for URLs and references that resolve to nothing in the warehouse.
- Resolve to a conformed FK where one exists. `courserun_fk` already covers the course-run case; the only new
  conformed join is **`content_block_fk → dim_course_content.content_block_pk`** — the existing SCD for Open
  edX blocks (courses, chapters, subsections, problems, videos) used by `tfact_problem_events` and
  `tfact_course_navigation_events`. edX-plugin and ORA feedback therefore joins the courseware star with no
  new dimensional work.
- **Appzi URL decoding is the source adapter's job** (`int__feedback__zendesk`), populating
  `subject_url`/`subject_ref` — not every consumer's query.

### 2b. `organization_fk` is unresolvable for Zendesk under the current key

`dim_organization.organization_pk = generate_surrogate_key(['platform', 'source_id'])`
(`src/ol_dbt/models/dimensional/dim_organization.sql:38`). Zendesk supplies neither, so `organization_fk`
stays **null** for Zendesk and the Zendesk organisation/group rides in `source_metadata` instead.
Either that is acceptable, or `dim_organization` needs a Zendesk-aware alternate key — recorded here so it is
an explicit decision rather than a surprise at build time.

### 2c. `source_metadata` — the variant column

Everything that fails the §0 rule lives here rather than becoming a Zendesk-shaped facet column on the fact.
At MVP that is `ticket_status`, `ticket_priority`, `brand_name`, `group_name`, `ticket_due_at`,
`organization_name` and Zendesk custom fields.

**Storage.** Iceberg v2 has **no native JSON type**, so the repo's established pattern is a **varchar holding
a JSON string** — written with `json_format(...)`, read with the cross-db `json_query_string` /
`json_extract_value` macros in `src/ol_dbt/macros/cross_db_functions.sql`, which already dispatch across
Trino / DuckDB / StarRocks. In-repo precedent: `dim_course_content.block_metadata` ("a JSON string
representing the metadata field… different block types may have different member fields"). StarRocks has a
native JSON type and the read macros already target it, so the eventual migration is a column-type change,
not a redesign.

**This is the mechanism that makes a new source additive.** A source arriving with attributes nobody has
seen before does not require a schema change — its extras land in the variant, and only graduate to a column
when a second source needs them (§0 promotion path).

### 2d. Role-playing date/time FKs

Every existing `tfact_*` in the repo uses bare `date_fk`/`time_fk`, with no qualified variant anywhere. That
convention holds only because every existing fact is single-dated. This fact carries three timestamps, so
the bare name would be genuinely ambiguous: `date_fk`/`time_fk` are renamed to
`occurred_date_fk`/`occurred_time_fk`, and `created_*`/`updated_*` join the same `dim_date`/`dim_time` in
role-playing positions.

`occurred_at` and `created_at` are the same value for a Zendesk comment today, and are kept separate
deliberately: `occurred_at` is the **contract** field every producer must supply, `created_at` is the
source's own lifecycle timestamp. They diverge wherever the utterance and its container are not created
together — which at turn grain is the common case, not the exception.

`due_at` is **not** a role-playing FK: it is Zendesk-only, so per §0 it lives in `source_metadata`.

---

## 3. Conformed dimensions — REUSED (no new work)

| Dim | Key | Populated for |
|-----|-----|---------------|
| `dim_user` | `user_pk` | forum, tutor, ORA, Zendesk (where requester resolves) |
| `dim_platform` | `platform_pk` | course-scoped sources |
| `dim_course_run` | `courserun_pk` | forum, tutor, ORA (via `courserun_readable_id`) |
| `dim_organization` | `organization_pk` | none at MVP — see §2b |
| `dim_course_content` | `content_block_pk` | edX plugin, ORA, forum (via `subject_ref`, §2a) |
| `dim_date` / `dim_time` | role-playing, §2d | all |

**Identity is the highest-risk join** (cf. open p0 bug `tk-fix-dim-user-null-email-identity-collapse`).
Resolve `user_fk` via the same paths the existing facts use — `openedx_user_id` → `dim_user.mitlearn_openedx_user_id`
(forum/tutor), `user_global_id` (tutor `int__learn_ai__chatbot`), email (Zendesk comment author, last resort).
Never key the fact off a source's local PK (§6).

At turn grain the Zendesk identity path resolves against the **comment author**, not the ticket requester —
which needs `comment_author_user_id` carried through `int__zendesk__ticket_comment` (it exists in
`stg__zendesk__ticket_comment` but is not in the int model, which exposes only `comment_author` as a *name*).
That same column is what distinguishes requester turns from agent turns, so it is a prerequisite for the
grain change, not an optimisation.

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
cluster_run_id         -- provenance: which cluster run proposed this category (§4d)
first_seen_at / updated_at
```
Bootstrapped, **not cold-start**: seed from Zendesk `ticket_tags` (2,354 distinct) + `group_name`, then LLM-label
embedding clusters (task `tk-define-llm-driven-category-discovery`). SCD-lite: relabeling changes `category_label`,
never `category_slug`. **Assignment (`category_fk`) is made on `afact_feedback_conversation`, not on the fact**
(rev. 3) — a conversation is categorized whenever the ML batch next runs, and the dimension itself is unaffected
by where the FK sits.

### 4b. `dim_sentiment`
```
sentiment_pk           -- generate_surrogate_key([sentiment_slug])
sentiment_slug         -- 'positive' | 'neutral' | 'negative' | (aspect-based later)
polarity_score_bucket  -- optional coarse bucket for trend rollups
```
Small conformed dim. `sentiment_fk` derived by the sentiment task (`tk-define-sentiment-mapping`) —
semantic/embedding or LLM — and assigned on `afact_feedback_conversation` (rev. 3).

**Rev. 3 fixes a grain mismatch here.** Zendesk's `satisfaction_rating_score` is a *ticket*-level signal. At
turn grain it had to be propagated to every turn of a rated ticket, which rev. 2 had to flag as a weak,
potentially misleading label. At conversation grain the explicit rating and the derived sentiment are the same
grain, so `explicit_rating` seeds and validates `sentiment_fk` exactly rather than approximately.

### 4c. `dim_feedback_source`
```
feedback_source_pk     -- generate_surrogate_key([source_slug])
source_slug            -- 'zendesk' | 'edx_forum' | 'learn_ai_tutor' | 'ora' | 'edx_feedback_plugin'
source_name            -- display
source_medium          -- 'support_ticket' | 'forum_post' | 'chat' | 'assessment' | 'in_product'
source_audience_scope  -- 'operational' | 'course' | 'strategic' (see audience memory)
is_course_scoped       -- bool (whether courserun_fk applies)
is_conversational      -- bool (whether conversation_id/turn_index are meaningful, §1)
```

### 4d. `dim_feedback_channel` (conformed)
```
feedback_channel_pk    -- generate_surrogate_key([channel_slug])
channel_slug           -- 'email' | 'web_form' | 'in_product_widget' | 'chat' | 'forum_post'
                       --   | 'assessment' | 'api'
channel_name           -- display
is_solicited           -- bool: did we ask for this feedback, or did the user volunteer it
```

*How* the feedback arrived, normalized across sources — Zendesk `ticket_source_channel` /
`comment_source_channel`, the tutor's agent, the forum component, the plugin's widget all map into one value
set. This is the **only** attribute from the withdrawn `dim_feedback_context` junk dimension that survives
the §0 rule; status/priority/brand/group are Zendesk-only and live in `source_metadata`.

`is_solicited` is the analytically interesting split the conformed view unlocks: volunteered feedback (a
support ticket, a forum complaint) and solicited feedback (a survey widget, a CSAT prompt) have different
selection biases and should rarely be pooled without saying so.

Open item: confirm the value set against each source's actual channel values before seeding.

### 4e. `dim_feedback_tag` + `bridge_feedback_tag`
```
dim_feedback_tag
    feedback_tag_pk    -- generate_surrogate_key([source_slug, tag_slug])
    tag_slug           -- slugified source tag
    tag_label          -- as it appears in the source
    source_slug        -- tags are SOURCE-SCOPED, not global

bridge_feedback_tag    -- grain (feedback_pk, feedback_tag_pk)
```

Replaces the multi-valued `source_tags` array column. Repo precedent: `bridge_course_topic`,
`bridge_user_organization`. Tags are keyed with `source_slug` because a Zendesk tag and a forum role that
happen to share a string are not the same thing — deliberately *not* conformed, which is the honest modelling
of a source-scoped vocabulary.

This also makes the ~2,354-tag category seed (§4a) a real dimension to select from rather than an array to
unnest.

### 4f. Where ML output lives (rev. 3: on the aggregate fact, not a per-turn sidecar)

**Summaries, vectors, sentiment, categories and cluster membership all live on
`afact_feedback_conversation` (§5a)** — one row per conversation, one generation at a time. The per-turn
`feedback_embeddings` / `feedback_cluster_assignment` sidecar tables from rev. 2 are **withdrawn**; the
aggregate fact *is* the sidecar, promoted to a first-class dimensional table under the existing `afact_`
convention (precedent: `afact_discussion_engagement`).

Two tables survive alongside it, because they are genuinely different grains:

```
feedback_cluster_run       -- grain (cluster_run_id) — one row per clustering run
    embedding_model_version, algorithm, run_params, cluster_count, noise_count, silhouette,
    run_status ('candidate'|'approved'), run_at

feedback_cluster_candidate -- grain (feedback_conversation_pk, cluster_run_id)
    cluster_id, cluster_probability
    -- ONLY for runs still in 'candidate' status. Promoting a run copies its assignment onto
    -- afact_feedback_conversation and the candidate rows are dropped.
```

`feedback_cluster_candidate` exists for one reason: comparing a proposed run against the live one, which is
what the embedding-model bake-off (`feedback_ml_approach.md` §B.1) and every subsequent re-tune need. It is
not a consumption table and nothing outside the ML pipeline should read it.

**What this trades away, stated plainly.** Rev. 2 split the sidecar so that re-clustering could never rewrite
vector rows. Collapsing onto one row per conversation gives that up: a promoted run rewrites the afact row,
vector column included. The cost is real but small — the *vector value* is carried forward, not recomputed, so
nothing is re-embedded; what is lost is the ability to hold several model generations live in production at
once, which is a bake-off need, and the candidate table covers it off the critical path. What is bought is
that a consumer reads one table instead of joining four, and that `tfact_feedback` becomes insert-only (§2).

`dim_feedback_category` remains the *curated, stable* projection of clusters that a human approved; the
aggregate fact is the *churny* generated layer. That decoupling is unchanged and is what still lets clustering
re-run freely.

---

## 5. Common feedback event contract (interim↔target bridge)

Both the interim learn-ai landing (`raw__learn_ai__…__feedback`) and the eventual analytics-api/StarRocks
data-bus MUST emit this shape so `stg__…__feedback` → `tfact_feedback` is source-path-agnostic
(task `tk-define-stable-common-feedback-event-contract-bus-245a8e`):

```
source_slug            -- maps to dim_feedback_source
occurred_at            -- ISO8601
source_record_ref      -- source-native TURN id (idempotency + business key)
text                   -- raw free text (redaction happens in-warehouse, §7)
channel_slug           -- maps to dim_feedback_channel
title                  -- nullable
conversation_ref       -- thread/ticket id
turn_index             -- ordinal within the conversation
subject_user_ref       -- global/openedx user id (NOT a source-local PK)
courserun_readable_id  -- nullable
platform               -- nullable
subject_type / subject_ref / subject_url  -- what the feedback is about (§2a)
explicit_rating        -- CSAT / tutor rating / ORA score (conformed)
created_at / updated_at
source_metadata        -- json: everything NOT conformed — status, priority, brand, group, due,
                       --   custom fields. Survives into the fact as a variant (§2c).
```
Align this to the **general data-bus ingestion contract**, not a feedback-specific one, so future openedx
tracking logs share ingress semantics.

Full field table, types and required-ness: [`feedback_event_contract_spec.md`](./feedback_event_contract_spec.md) §1.

---

## 5a. `afact_feedback_conversation` — the analysis fact (conversation grain)

**One row per conversation**, keyed on the source's own thread identifier. Two things live here, and rev. 3
is the decision to put them in the same table:

1. **Conversation lifecycle** — attributes that would repeat across every turn at §1's grain (status,
   rating, counts, durations).
2. **Everything a model generated** — the summary, the embedding, the sentiment, the category and the
   cluster.

```
-- identity
feedback_conversation_pk    -- generate_surrogate_key([source_slug, conversation_ref])
conversation_id             -- degenerate; the source-native thread/ticket id (joins tfact_feedback)
feedback_source_fk          -> dim_feedback_source
opened_by_user_fk           -> dim_user (nullable)

-- lifecycle
opened_date_fk              -> dim_date          [available today]
last_turn_date_fk           -> dim_date          [available today]
first_response_date_fk      -> dim_date          [BLOCKED — needs ticket_metrics]
resolved_date_fk            -> dim_date          [BLOCKED]
closed_date_fk              -> dim_date          [BLOCKED]
turn_count                  -- integer           [available today]
participant_count           -- integer           [available today]
conversation_text_chars     -- summed pre-redaction length of the kept turns  [available today]
resolution_duration_seconds -- the "how long was it open" measure  [BLOCKED]
final_status                -- current-state snapshot              [available today]
explicit_rating             -- conversation-level rating           [available today]

-- generated: summary (§5b)
conversation_summary        -- LLM-generated abstract of the whole exchange (REDACTED-derived)
summary_model_version       -- which model/prompt produced it; null = not summarized
summarized_at

-- generated: embedding
embedding_vector            -- Iceberg ARRAY<float>
embedding_dim               -- Matryoshka sweep: 256 | 512 | 1024
embedding_model_version     -- makes the model choice reversible without touching tfact_feedback
embedding_input             -- 'summary' | 'concatenated_turns' — what was actually embedded
embedded_at

-- generated: assignments
category_fk                 -> dim_feedback_category (nullable; §4a)
sentiment_fk                -> dim_sentiment         (nullable; §4b)
sentiment_source            -- 'explicit_rating' | 'model' — which tier produced it (§4b)
cluster_run_id              -> feedback_cluster_run  (which run produced the assignment below)
cluster_id                  -- -1 = noise = one-off, not systemic
cluster_probability         -- cohesion signal for ranking systemic issues

-- audit
conversation_ingested_at
```

**Why the conversation is the analysis unit.** A support ticket's actual complaint frequently emerges over
several exchanges. Embedding turns independently splits one issue into several weak cluster members, scores
sentiment off a fragment ("still not working"), and makes cluster size a function of how talkative the
reporter was. At conversation grain a cluster's size *is* its distinct-conversation count, which is the
number the systemic-vs-one-off judgement actually wants.

**This is not a return to rev. 1's ticket grain.** Rev. 2's argument was that a problem first articulated in
turn 4 must reach the warehouse at all — it fixed *what gets recorded*. Rev. 3 fixes *what gets analyzed*:
turn 4's text is now part of the same analysis unit as turn 1 rather than a separate one. Both depend on the
turn grain in §1; the fact still records every turn, and the assembled conversation is built from all of
them, not from `is_conversation_opening`.

### 5b. The summary, and what it costs

`conversation_summary` is an LLM abstract of the assembled (redacted) turns. It is the thing a human reads in
a cluster listing instead of scrolling a thread, and — per `feedback_ml_approach.md` §B.1 — it is also a
candidate for *what gets embedded*, which the 2026 support-ticket-clustering evidence identifies as the
largest single lever on cluster quality for short, noisy text.

It is also the one genuinely per-record LLM cost in this design, so it is stated rather than buried:

- **Summarize only where it can add something.** Skip single-turn conversations and conversations under a
  length threshold — the raw text already *is* the summary. ORA (~140 chars) and the plugin are single-turn
  by construction and are never summarized; short one-and-done Zendesk tickets are skipped by the same rule.
  `summary_model_version` is null for those rows and `embedding_input` is `concatenated_turns`.
- **Order of magnitude, to be validated on a sample before the backfill:** the multi-turn share of ~198K
  Zendesk tickets at roughly 1–2K input tokens and ~100 output tokens each puts the one-time backfill in the
  low hundreds of dollars at Haiku-class pricing, with steady state (~24K tickets/yr, a fraction multi-turn)
  in the tens of dollars a year. That is one to two orders of magnitude above the $2–16 embedding backfill
  and still small in absolute terms — but it is a real change to the "trivial batch cost" posture the
  project has carried since discovery, and it should be confirmed with a measured sample rather than assumed.
- **PII:** the summary is generated *from redacted text only* and inherits that classification. It is never
  generated from `stg`/`raw`.

**Partially blocked on ingestion.** The Zendesk streams landed in the lake
(`src/ol_dbt/models/staging/zendesk/_zendesk__sources.yml`) are exactly seven — `tickets`,
`ticket_comments`, `ticket_fields`, `brands`, `groups`, `organizations`, `users`. `solved_at`, `closed_at`,
`initially_assigned_at`, `first_resolution_time` and `full_resolution_time` live in Zendesk's
**`ticket_metrics`** endpoint; full state history lives in **`ticket_audits`**. Neither is synced. In the
`tickets` stream, "solved"/"closed" appear only as *values of `ticket_status`* — a snapshot with no history.

So this table ships with the available-today columns and the duration measures land once `ticket_metrics` is
added to the connector (a separate ingestion ticket). The grain does not move when they arrive.

It conforms beyond Zendesk: a tutor thread and a forum thread also open, run, and go quiet — and a
single-utterance ORA submission is a conversation of length one (§1).

---

## 6. Business-key strategy (migration-proof)

`feedback_pk = generate_surrogate_key([source_slug, source_record_ref])` where `source_record_ref` is a
**stable business identifier from the source system**, never a warehouse/Airbyte/Postgres row PK
(terminology matches the event contract + Zendesk MVP spec):

| Source | `source_record_ref` (the **turn** id) | `conversation_ref` |
|--------|-------------------|---|
| Zendesk | `comment_id` | `ticket_id` |
| edX forum | `post_id` | thread id |
| Learn AI tutor | `checkpoint_id` | `chatsession_thread_id` |
| ORA | `submission_uuid` | n/a |
| edX plugin | plugin-native event id | n/a |

This is what lets the interim→analytics-api migration be "swap source + backfill + parity" rather than a
rebuild: the same `feedback_pk` regenerates identically regardless of pipe.

> **Changed in rev. 2, and this is the reason the grain decision (§1) had to land before the fact ships.**
> Zendesk's `source_record_ref` moved from `ticket_id` to `comment_id`. The *formula* is unchanged, but the
> *value* is not — so the key would have had to be regenerated after the fact had consumers, which is exactly
> the rebuild this strategy exists to avoid.

---

## 7. PII handling (mandatory pre-embed)

Zendesk ticket text and `int__learn_ai__chatbot.human_message` carry emails/phones/names —
`ticket_description` is profiler-flagged `PII.Sensitive`. A **redaction step in the `int__` layer**
(Presidio — precedent: the OM profiler already runs Presidio recognizers) masks entities before text lands
in `tfact_feedback` or the embedding store. Raw text stays in `raw`/`stg` under existing PII classification
+ Lakekeeper/Cedar authz; the fact carries redacted text only. Access to the fact is still governed per
§audience (course instructors see their courserun; support/eng see tickets; leadership sees aggregates).

**Turn grain widens the exposure surface, so this gets *more* important, not less.** The redaction target is
now `comment_plain_body` across every kept turn, not just `ticket_description`. Follow-up turns are where
users paste order numbers, screenshots-as-text, alternate email addresses and phone numbers in response to
an agent asking for them — plausibly a *higher* PII density than the opening comment. The profiler has only
classified `ticket_description`; **run it over `comment_plain_body` before the first embed run** rather than
assuming the classification carries over.

---

## 8. Layering & build path

```
raw__<source>__…                         (existing per source; new: raw__learn_ai__…__feedback)
  → stg__<source>__…__feedback           (conform to §5 contract, light typing)
    → int__feedback__<source>            (per-source adapter; TURN grain + source filters §1)
      → int__feedback__unioned           (UNION all sources into the common shape + redact PII §7)
        → tfact_feedback                  (resolve conformed FKs, generate feedback_pk §6 — INSERT-ONLY)
          → bridge_feedback_tag           (explode source tags → dim_feedback_tag §4e)
          → int__feedback__conversation   (assemble kept turns per conversation_id, ordered by turn_index)
            → afact_feedback_conversation (§5a — lifecycle + summary + vector + sentiment/category/cluster)
              → afact_feedback_cluster_daily  (cluster × category × sentiment × date × source rollup)
```
The ML batch (Dagster asset) reads `int__feedback__conversation` (redacted, assembled) and writes
`afact_feedback_conversation` plus proposed rows on `dim_feedback_category`. **It never writes to
`tfact_feedback`** — the late-arriving update path that existed through rev. 2 is gone (§2).

`afact_feedback_cluster_daily` now aggregates the conversation fact rather than the turn fact, which
collapses rev. 2's awkward `feedback_count` vs. `distinct_conversation_count` distinction: at this grain
they are the same number.

---

## 9. MVP vs. full

- **MVP (support/eng):** `tfact_feedback` restricted to `source_slug='zendesk'` at **turn grain**, plus
  `dim_feedback_source`, `dim_feedback_channel`, `dim_feedback_category` (seeded from `ticket_tags`),
  `dim_sentiment`, `dim_feedback_tag` + `bridge_feedback_tag`, and `afact_feedback_conversation` — its
  lifecycle columns first (one row per ticket, ~198K, a number that *is* known), then the generated columns
  as the ML asset lands. Batch. The turn-fact row count is public-requester **comments**, not tickets —
  **to be measured before committing** (§10).
- **Phase 2:** add forum/tutor/ORA sources (fact already tolerates them), enable cross-source
  `afact_feedback_cluster_daily` for leadership, course-scoped views for instructors, and the
  `ticket_metrics`-gated duration measures on `afact_feedback_conversation`.
- **Phase 3:** analytics-api/data-bus ingress (contract §5 already lets the fact ignore the switch).

**Cost note.** Rev. 2 moved MVP from 3 new dimensions to 4 dimensions + 1 bridge + 1 conversation fact — all
`select distinct` cheap or a straight aggregation, so the dbt build cost is small. Rev. 3 adds **no** new
tables: it withdraws two (`feedback_embeddings`, `feedback_cluster_assignment`) and widens
`afact_feedback_conversation`. The cost it *does* add is the per-conversation LLM summary (§5b), which is a
step up from the embedding-only budget and is the number to confirm on a sample before the backfill.

The argument for paying the rev. 2/3 reshaping now rather than later is unchanged: reshaping a fact after it
has consumers is the expensive version, and the §1 grain change has to land pre-ship regardless because it
moves the business key (§6).

---

## 10. Open items handed to downstream tasks

**Prerequisites for the rev. 2 grain change:**

| Item | Kind | Blocks |
|---|---|---|
| Carry `comment_author_user_id` through `int__zendesk__ticket_comment` (present in `stg__zendesk__ticket_comment`; the int model exposes only `comment_author` as a name) | small dbt change | requester-vs-agent turn classification; `user_fk` resolution |
| Measure public, requester-authored comments per ticket | measurement | sizing the fact and the embedding budget (§9) |
| Add the `ticket_metrics` stream to the Zendesk Airbyte connector | ingestion, separate ticket | duration measures on `afact_feedback_conversation` (§5a) |
| Confirm the conformed `channel_slug` value set against each source's actual channel values | modeling | `dim_feedback_channel` seed (§4d) |
| Run the PII profiler over `comment_plain_body` (only `ticket_description` is classified today) | classification | the first embed run at turn grain (§7) |
| Measure the multi-turn share of Zendesk tickets and sample-price the summary step | measurement | the §5b summarization budget, and the skip-threshold that keeps single-turn conversations free |
| Decide `embedding_input` — summary, concatenated turns, or both as an eval arm | modeling | the bake-off in `feedback_ml_approach.md` §B.1 |

**Handed to existing tasks:**

- Category discovery & seeding → `tk-define-llm-driven-category-discovery-to-seed-pop-550aba`
- Sentiment method & `dim_sentiment` grain → `tk-define-sentiment-mapping-via-semantic-embedding--92988e`
- Clustering method + embedding storage on the conversation fact + roll-up hierarchy → `tk-define-clustering-approach-for-systemic-issue-de-a1d7d6`
- Per-persona actions, surface, row-level access → `tk-define-ui-ux-audience-actions-and-where-the-expe-476d23`
- Contract finalization + business keys → `tk-define-stable-common-feedback-event-contract-bus-245a8e`

---

## 11. Change log

**rev. 3 (2026-08-10)** — the analysis unit moves from the turn to the conversation
([RFC #12210](https://github.com/mitodl/hq/discussions/12210)):

- **`afact_feedback_conversation` becomes the analysis fact** (§5a) — it now carries the LLM summary, the
  embedding vector, `category_fk`, `sentiment_fk` and cluster membership alongside the lifecycle columns it
  had in rev. 2, with `summary_model_version` / `embedding_model_version` / `cluster_run_id` recording which
  generation produced each.
- **`category_fk` and `sentiment_fk` are removed from `tfact_feedback`** (§2). With them gone there is no
  late-arriving write path to the fact at all: **`tfact_feedback` is insert-only**.
- **The per-turn ML sidecar is withdrawn** (§4f) — `feedback_embeddings` and `feedback_cluster_assignment`
  are gone. `feedback_cluster_run` survives (a genuinely different grain) and a small
  `feedback_cluster_candidate` covers run-vs-run comparison for the model bake-off.
- **Added `conversation_summary`** (§5b), with an explicit skip rule for single-turn/short conversations and
  a stated cost, because it is the one per-record LLM call in the design.
- **Sentiment's grain mismatch is fixed** (§4b) — Zendesk's ticket-level CSAT is now the same grain as the
  sentiment it seeds, instead of a label propagated across turns.
- `afact_feedback_cluster_daily` aggregates the conversation fact, so `feedback_count` and
  `distinct_conversation_count` collapse into one measure (§8).
- **Unchanged:** the turn grain of `tfact_feedback` (§1), the Zendesk comment sourcing, the business-key
  strategy (§6), the conformance rule (§0) and the event contract (§5).

**rev. 2 (2026-08-07)** — from [@KatelynGit's RFC
review](https://github.com/mitodl/hq/discussions/12210#discussioncomment-17937328):

- **Grain moved from ticket to turn** (§1) — Zendesk `source_record_ref` is now `comment_id`. Added
  `turn_index`, `is_conversation_opening`.
- **Adopted the ≥2-sources conformance rule** (§0) and re-audited every attribute against it.
- **Added** `dim_feedback_channel` (§4d), `dim_feedback_tag` + `bridge_feedback_tag` (§4e),
  `afact_feedback_conversation` (§5a), `is_conversational` on `dim_feedback_source`.
- **Renamed** `date_fk`/`time_fk` → `occurred_date_fk`/`occurred_time_fk`; **added** created/updated
  role-playing FKs and timestamps (§2d).
- **`source_metadata` is persisted as a variant on the fact** (§2c) instead of being flattened into
  Zendesk-shaped facet columns. **Removed** `source_status`, `source_priority`, `source_channel`,
  `source_brand`, `source_group`, `source_tags` from the fact; renamed `csat_score` → `explicit_rating`.
- **Withdrawn** from the rev. 1 ERD addendum: the `dim_feedback_context` junk dimension (Zendesk-shaped,
  fails §0), the `source_brand`/`source_group` facet columns added for
  [hq#12607](https://github.com/mitodl/hq/issues/12607) (still filterable from the variant), and
  `due_date_fk` (Zendesk-only).

**rev. 1 (2026-08-07)** — subject reference `subject_type`/`subject_ref`/`subject_url` + `content_block_fk`
(§2a) from [@pdpinch's review](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-5169778966);
ML sidecar grain split (§4f), dropping `embedding_id` from the fact.
```
