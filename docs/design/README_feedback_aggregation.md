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
| [`feedback_erd.md`](./feedback_erd.md) | **Start here for the shape:** Mermaid ERDs for the star schema, the event contract, the ML sidecar and the Phase-1 subset; records the schema deltas from #2422 review feedback | (visual index) |
| [`feedback_dimensional_model.md`](./feedback_dimensional_model.md) | Dimensional model: `tfact_feedback` grain, conformed dims, sparse FKs, subject reference, PII redaction, phasing | schema design (discovery) |
| [`feedback_event_contract_spec.md`](./feedback_event_contract_spec.md) | Common feedback event contract + migration-proof business keys; data-bus alignment as a bounded dependency | `...contract-bus-245a8e` |
| [`feedback_zendesk_mvp_spec.md`](./feedback_zendesk_mvp_spec.md) | Build-ready Zendesk MVP: exact dbt models/columns `int__feedback__zendesk → __unioned → tfact_feedback` + 3 dims + tests | (MVP implementation) |
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
   mechanism. Explicit **stable `feedback_pk`** (diverges from precedent) for migration + late-arriving
   category/sentiment updates.
3. **ML is an additive consumer**, not a prerequisite: the fact ships useful with
   tag-seeded categories + CSAT-derived sentiment; embeddings/clustering fill `category_fk`/
   `sentiment_fk` later and otherwise live entirely in the sidecar (three tables — vectors,
   cluster runs, cluster assignments — split by grain so re-clustering never rewrites vectors).
   Embeddings persisted **once** (the one adopted lesson from prototype #10793).
4. **Engine-portable AI compute via Fenic; embedding model chosen by effectiveness** (revised
   2026-07-10 rev. 4, ADR): because the strategic direction is to **retire Trino for StarRocks**,
   no Galaxy-only functionality sits on the critical path (Starburst `generate_embedding`
   **rejected**). AI compute stays engine-external using **Fenic (Apache-2.0)** in a Dagster asset
   — batching/caching/lineage for free, portable across Trino→StarRocks. **Bedrock is not
   required; the embedding model is selected by task effectiveness** — MTEB to narrow, then
   benchmark the shortlist (Fenic-native `gemini-embedding-001`/Cohere `embed-v4`/OpenAI
   `text-embedding-3-large`; or self-hosted Qwen3/BGE-M3) on a labeled Zendesk sample by
   clustering agreement/coherence + cost, sweeping Matryoshka dims (`feedback_ml_approach.md`
   §B.1). Choice is reversible via `model_version`. Vectors → open Iceberg `ARRAY<float>` sidecar;
   **StarRocks HNSW is the intended vector-serving tier**. Clustering stays our own HDBSCAN (Fenic
   is K-means only); LLM semantic-normalization-before-embedding is a first-class eval arm (2026
   evidence: biggest lever for short-ticket cluster quality). Presidio redaction mandatory
   pre-embed (data-minimization, independent of egress).
5. **The fact records what feedback is *about*, not just who/when** (added 2026-08-07 from
   [#2422 review feedback](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-3157271372)):
   a polymorphic `subject_type`/`subject_ref`/`subject_url` triple on the contract and the fact, resolved to
   a conformed FK where one exists — `courserun_fk` for runs, the new **`content_block_fk →
   dim_course_content`** for edX courseware blocks. No new subject dimension: the subject is polymorphic
   across dims that already exist. Zendesk `brand_name`/`group_name` land as `source_brand`/`source_group`
   facets ([hq#12607](https://github.com/mitodl/hq/issues/12607)).
6. **Consumption surface is an open, per-audience choice** — Superset dashboard, deployed
   **Marimo notebook-as-webapp**, or a net-new app; all read the same modeled tables, so the
   choice is reversible. Recommended phasing: Superset for MVP trend/cluster dashboards (RLS
   already solved in `src/ol_superset/`), a Marimo notebook-as-webapp (existing image +
   Keycloak/Trino/IRSA) for interactive exploration + the category-curation loop and to
   prototype the UX, and a net-new app only when write-back/product-embedding justifies it.
   Support + engineering are the MVP-served audiences (Zendesk is not course-scoped, so
   instructors wait for Phase 2 sources).

## Phasing

- **MVP (Phase 1):** Zendesk-only fact + 3 dims + support/eng cluster dashboards. ~198K rows, batch.
- **Phase 2:** add forum/tutor/ORA (additive CTEs); `afact_feedback_cluster_daily`; instructor course views.
- **Phase 3:** migrate ingress to the data bus (gated on the write path existing + sink-topology decision).

## Open questions carried to team / platform (non-blocking for MVP)

- Data-bus write path (net-new platform work) + generic-sink-vs-per-topic (platform-wide, outranks feedback).
- Final embedding model + vector store at full 1.18M scale (MVP proves it at 198K).
- Which prototype #10793 mechanics earn their place (semantic-summary-before-embedding,
  hierarchical truncation) — test as hypotheses, don't inherit.
- Qualtrics/course-survey onboarding; HubSpot NPS tickets need Iceberg modeling first.

## Status / next

RFC #12210 is posted for team review (Draft). Review feedback on this PR from @pdpinch (2026-08-03) has
been folded in as of 2026-08-07: the subject reference (`feedback_dimensional_model.md` §2a), Zendesk
brand/group facets ([hq#12607](https://github.com/mitodl/hq/issues/12607)), and the ERDs in
[`feedback_erd.md`](./feedback_erd.md) — also
[posted as an RFC addendum](https://github.com/mitodl/hq/discussions/12210#discussioncomment-17935060).

On team consensus, flip RFC → Accepted and begin implementation per the build order in
`feedback_dagster_asset_spec.md` §7 (dbt fact first, ML asset additive).
