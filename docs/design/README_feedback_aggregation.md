# Feedback Aggregation & Clustering System — Spec Index

Project: `wp-feedback-aggregation-clustering-system-2e9750` · Phase: **spec** (2026-07-10)
RFC (team review): [mitodl/hq#12210](https://github.com/mitodl/hq/discussions/12210)
(supersedes [#10793](https://github.com/mitodl/hq/discussions/10793)).

A feedback aggregation system that ingests free-text feedback from multiple sources
(Zendesk, edX forum, Learn AI tutor, ORA, a forthcoming edX feedback plugin) into one
source-agnostic dimensional fact and clusters it to surface systemic issues and positive
signals for four audiences (support, engineering, instructors, leadership).

## The spec set

| Doc | Scope | Resolves task |
|---|---|---|
| [`feedback_erd.md`](./feedback_erd.md) | **Start here for the shape:** the conformance rule, Mermaid ERDs for the star schema, the event contract, the conversation analysis fact, run provenance and the Phase-1 subset; carries the change log | (visual index) |
| [`feedback_dimensional_model.md`](./feedback_dimensional_model.md) | Dimensional model: the fact pair — `tfact_feedback` at turn grain and `afact_feedback_conversation` at conversation grain — conformed dims, sparse FKs, subject reference, the `source_metadata` variant, the summary, PII redaction, phasing | schema design (discovery) |
| [`feedback_event_contract_spec.md`](./feedback_event_contract_spec.md) | Common feedback event contract + migration-proof business keys; data-bus alignment as a bounded dependency | `...contract-bus-245a8e` |
| [`feedback_zendesk_mvp_spec.md`](./feedback_zendesk_mvp_spec.md) | Build-ready Zendesk MVP: exact dbt models/columns `int__feedback__zendesk → __unioned → feedback_redacted → tfact_feedback` + 3 dims + tests | (MVP implementation) |
| [`feedback_ml_approach.md`](./feedback_ml_approach.md) | Embedding (local, PII-safe), clustering (UMAP+HDBSCAN), category discovery (seed + LLM-label), sentiment (explicit + kNN/classifier) | `...clustering-...a1d7d6`, `...category-...550aba`, `...sentiment-...92988e` |
| [`feedback_dagster_asset_spec.md`](./feedback_dagster_asset_spec.md) | Batch ML Dagster asset cloned from `student_risk_probability`; Vault-backed LLM resource; net-new deps; scheduling | (ML pipeline orchestration) |
| [`feedback_consumption_ux_spec.md`](./feedback_consumption_ux_spec.md) | Audiences × altitude, surface options (Superset / Marimo notebook-as-webapp / net-new app), per-persona actions, access control | `...ui-ux-...476d23` |
| [`adr_embedding_compute_strategy.md`](./adr_embedding_compute_strategy.md) | ADR: where embedding/AI inference runs — Starburst Galaxy AI functions (Bedrock, in-SQL) vs. Fenic vs. local vs. StarRocks | (revises embedding default) |

## Key decisions (spec phase)

1. **Contract-first, interim landing** (RFC Option 3): ship on the learn-ai landing now,
   migrate to the analytics-api/StarRocks data bus later — durable artifact is the **event
   contract + business keys**, so migration = source-swap + backfill + parity.
2. **One common `tfact_feedback`** conforming to the Kimball layer (mirrors
   `tfact_discussion_events`), with **sparse nullable conformed FKs** as the source-flexibility
   mechanism. Explicit **stable `feedback_pk`** (diverges from precedent) so the interim→data-bus source
   swap regenerates the same key. (Rev. 2's second reason — anchoring late-arriving category/sentiment
   updates — lapsed with 2d; the migration reason still holds on its own.)
2b. **Grain is one row per conversation *turn*** (revised 2026-08-07): Zendesk is modelled at
   comment grain like every other source, not ticket grain, sourced from `int__zendesk__ticket_comment`.
   First-comment-only survives as the `is_conversation_opening` filter. Zendesk's `source_record_ref` is
   therefore `comment_id`, which is why this had to land before the fact ships.

2d. **The analysis unit is the conversation, and `afact_feedback_conversation` holds everything generated**
   (revised 2026-08-10): the aggregate fact carries the LLM summary, the embedding vector, `category_fk`,
   `sentiment_fk` and cluster membership alongside the conversation lifecycle columns. `category_fk` and
   `sentiment_fk` are **removed from `tfact_feedback`**, which makes that fact **insert-only** — there is no
   post-insert write path to it at all. The per-turn ML sidecar (`feedback_embeddings`,
   `feedback_cluster_assignment`) is withdrawn; `feedback_cluster_run` survives and a small
   `feedback_cluster_candidate` covers run-vs-run comparison.
   Rationale: a complaint usually emerges across several turns, so embedding turns independently splits one
   issue into several weak cluster members and scores sentiment off a fragment. This is *not* a return to
   ticket grain — the fact still records every turn, and the assembled conversation is built from all of
   them. Cost consequence: the summary is the one per-record LLM call in the design (low hundreds of dollars
   one-time for Zendesk, skipped entirely for single-turn conversations) — measure it on a sample before the
   backfill.
2c. **Conformance rule — ≥2 sources or it's a variant** (revised 2026-08-07): an attribute earns a column
   on the fact or a conformed dimension only if two or more sources can populate it; everything else lives
   in the `source_metadata` variant (a varchar-held JSON string, since Iceberg v2 has no native JSON type)
   and is promoted when a second source needs it. This is what stops Zendesk's shape from becoming the
   model's shape. It added `dim_feedback_channel` (the one genuinely conformed facet) and
   `dim_feedback_tag`/`bridge_feedback_tag`, and it withdrew the `dim_feedback_context` junk dimension and
   the `source_brand`/`source_group` facet columns proposed a week earlier.
3. **ML is an additive consumer**, not a prerequisite: the warehouse layer ships useful with
   tag-seeded categories + CSAT-derived sentiment; summaries, embeddings and clustering fill nullable
   columns on `afact_feedback_conversation` later and touch nothing else (**superseded in part by 2d**:
   the three-table per-turn sidecar is withdrawn; only `feedback_cluster_run` and a candidate table remain
   alongside the aggregate fact). Embeddings persisted **once** (the one adopted lesson from prototype
   #10793).
4. **Engine-portable AI compute via Fenic; embedding model chosen by effectiveness** (revised
   2026-07-10 rev. 4, ADR): because the strategic direction is to **retire Trino for StarRocks**,
   no Galaxy-only functionality sits on the critical path (Starburst `generate_embedding`
   **rejected**). AI compute stays engine-external using **Fenic (Apache-2.0)** in a Dagster asset
   — batching/caching/lineage for free, portable across Trino→StarRocks. **Bedrock is not
   required; the embedding model is selected by task effectiveness** — MTEB to narrow, then
   benchmark the shortlist (Fenic-native `gemini-embedding-001`/Cohere `embed-v4`/OpenAI
   `text-embedding-3-large`; or self-hosted Qwen3/BGE-M3) on a labeled Zendesk sample by
   clustering agreement/coherence + cost, sweeping Matryoshka dims (`feedback_ml_approach.md`
   §B.1). Choice is reversible via `embedding_model_version`. Vectors → an open Iceberg `ARRAY<float>` column
   on `afact_feedback_conversation` (per 2d, no longer a separate sidecar table);
   **StarRocks HNSW is the intended vector-serving tier**. Clustering stays our own HDBSCAN (Fenic
   is K-means only); LLM semantic-normalization-before-embedding is a first-class eval arm (2026
   evidence: biggest lever for short-ticket cluster quality). Presidio redaction mandatory
   pre-embed (data-minimization, independent of egress).
5. **The fact records what feedback is *about*, not just who/when** (added 2026-08-07 from
   [#2422 review feedback](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-3157271372)):
   a polymorphic `subject_type`/`subject_ref`/`subject_url` triple on the contract and the fact, resolved to
   a conformed FK where one exists — `courserun_fk` for runs, the new **`content_block_fk →
   dim_course_content`** for edX courseware blocks. No new subject dimension: the subject is polymorphic
   across dims that already exist. Zendesk `brand_name`/`group_name`
   ([hq#12607](https://github.com/mitodl/hq/issues/12607)) are carried and filterable —
   **superseded by 2c**: they ride in `source_metadata` rather than in dedicated facet columns.
6. **Consumption surface is an open, per-audience choice** — Superset dashboard, deployed
   **Marimo notebook-as-webapp**, or a net-new app; all read the same modeled tables, so the
   choice is reversible. Recommended phasing: Superset for MVP trend/cluster dashboards (RLS
   already solved in `src/ol_superset/`), a Marimo notebook-as-webapp (existing image +
   Keycloak/Trino/IRSA) for interactive exploration + the category-curation loop and to
   prototype the UX, and a net-new app only when write-back/product-embedding justifies it.
   Support + engineering are the MVP-served audiences (Zendesk is not course-scoped, so
   instructors wait for Phase 2 sources).

## Phasing

- **MVP (Phase 1):** Zendesk-only fact at turn grain + 4 dims + `bridge_feedback_tag` +
  `int__feedback__conversation` + `afact_feedback_conversation` (lifecycle columns first, generated columns
  as the ML asset lands) + support/eng cluster dashboards. Batch. The conversation fact is ~198K rows (one
  per ticket, a known figure); the **turn fact's row count is unmeasured** — it is public requester comments.
- **Phase 2:** add forum/tutor/ORA (additive CTEs); `afact_feedback_cluster_daily`; instructor course views;
  conversation duration measures once `ticket_metrics` is synced.
- **Phase 3:** migrate ingress to the data bus (gated on the write path existing + sink-topology decision).

## Prerequisites (from the 2026-08-07 revision)

- Carry `comment_author_user_id` through `int__zendesk__ticket_comment` — **blocking** for the turn grain
  (classifies requester-vs-agent turns and resolves `user_fk`). Verified against the repo: the column exists
  in `stg__zendesk__ticket_comment` and the int model joins it away, exposing only `comment_author` as a name.
- Carry `ticket_requester_user_id` through `int__zendesk__ticket` (added 2026-08-13) — **blocking**, same
  reason: the requester-vs-agent filter compares ids, and the int model exposes only `ticket_requester`
  (a name), even though the join it needs already exists in the model.
- Measure public, requester-authored comments per ticket **and the multi-turn share** — sizes the turn fact,
  and the multi-turn share drives the summarization budget (2d).
- Add the `ticket_metrics` Airbyte stream — non-blocking; unblocks conversation duration measures.
- Confirm the conformed `channel_slug` value set against each source's actual channel values.
- Decide `embedding_input` (summary vs. concatenated turns) via the bake-off — non-blocking for the schema.

## Open questions carried to team / platform (non-blocking for MVP)

- Data-bus write path (net-new platform work) + generic-sink-vs-per-topic (platform-wide, outranks feedback).
- Final embedding model + vector store at the full ~600K-conversation scale (MVP proves it at ~198K).
- What gets embedded — the summary or the concatenated turns — and whether the summary's per-conversation
  LLM cost is justified by measured cluster-quality lift. (This subsumes prototype #10793's
  semantic-summary-before-embedding question, now a scored eval arm rather than a rejected default.
  Hierarchical truncation remains a hypothesis to test, not inherit.)
- Qualtrics/course-survey onboarding; HubSpot NPS tickets need Iceberg modeling first.

## Status / next

RFC #12210 is posted for team review (Draft). It was rewritten on 2026-08-10 to carry the conversation-grain
analysis fact (2d) and cut to a ~1,500-word read. Three rounds of feedback have been folded in:

- **@pdpinch** ([#2422](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-5169778966)) — the
  subject reference (`feedback_dimensional_model.md` §2a) and Zendesk brand/group
  ([hq#12607](https://github.com/mitodl/hq/issues/12607)); ERDs added in
  [`feedback_erd.md`](./feedback_erd.md), also
  [posted as an RFC addendum](https://github.com/mitodl/hq/discussions/12210#discussioncomment-17935060).
- **@KatelynGit** ([RFC #12210](https://github.com/mitodl/hq/discussions/12210#discussioncomment-17937328)) —
  date/time FK naming, lifecycle timestamps, and moving descriptive fields off the fact. This round produced
  the turn-grain change and the conformance rule (key decisions 2b/2c) and superseded parts of the first
  round.
- **Owner direction (2026-08-10)** — cluster the turn records into a coherent conversation and make that
  aggregate fact the home for the generated summary, embeddings and sentiment. This produced key decision
  2d and rev. 3 across the spec set.
- **@copilot + @rachellougee** ([#2422](https://github.com/mitodl/ol-data-platform/pull/2422), 2026-08-13) —
  a round of implementation-blocking fixes with no grain/key/shape changes: a second missing prerequisite
  column (`ticket_requester_user_id`, same class as `comment_author_user_id`); redaction split into its own
  Phase-1 asset (`feedback_redacted`) to remove a dependency cycle in rev. 3's placement; `conversation_ref`
  and `source_url` added/required on the event contract; `(feedback_source_fk, conversation_id)` replacing
  bare `conversation_id` everywhere the turn fact joins the conversation fact; a stale `category_slug`
  hash-of-hash, a non-existent `unknown` sentiment member, a `participant_count` that was always 1, and an
  `avg_explicit_rating` on a non-numeric column, all corrected; `feedback_clusters` split into a
  `@multi_asset`; `dim_user.user_pk`'s formula corrected to match current `main`; and `subject_user_ref`
  retained on `tfact_feedback` so identity can be re-matched later without a rebuild. Detail and rationale
  are in each doc's own rev. 4/5 changelog entry.

Before flipping RFC → Accepted, the prerequisites above should be closed — particularly the volume
measurement and the `comment_author_user_id`/`ticket_requester_user_id` changes, both of which gate the turn
grain. Then begin implementation per the build order in `feedback_dagster_asset_spec.md` §7 (dbt facts
first, ML asset additive).
