# Feedback Aggregation — ML/LLM Approach Spec

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 · Companion to [`feedback_dimensional_model.md`](./feedback_dimensional_model.md)

Resolves the open items the dimensional-model design handed to downstream tasks:
clustering (`tk-...-clustering-approach-...a1d7d6`), category discovery
(`tk-...-llm-driven-category-discovery-...550aba`), and sentiment
(`tk-...-sentiment-mapping-...92988e`). Grounded in the discovery cost model
(~1.18M text records; one-time embed ≈ $2–16; MVP = ~198K Zendesk tickets, batch).

The guiding principle from the RFC: **the durable artifact is the feedback fact +
persisted embeddings; clustering/category/sentiment are re-runnable consumers of it.**
Nothing here rewrites `tfact_feedback`; all ML output lands in the `feedback_embeddings`
sidecar or the late-arriving `category_fk`/`sentiment_fk` update.

---

## A. Pipeline shape (single batch Dagster asset graph, MVP)

```
int__feedback__unioned (redacted text, feedback_pk)          [dbt]
  → embed        : text → vector, persisted once             [py asset: feedback_embeddings]
  → cluster      : vectors → cluster_id per cluster_run       [py asset: feedback_embeddings.cluster_id]
  → label        : cluster centroid/samples → category label  [py asset: dim_feedback_category (proposed)]
  → sentiment    : text/vector → sentiment_slug               [py asset: sentiment_fk]
  → assign       : write category_fk/sentiment_fk back        [dbt incremental or py upsert]
```

Each stage is idempotent and keyed by `feedback_pk` + `model_version`/`cluster_run_id`,
so a re-run never duplicates and never mutates the fact grain. Embedding is computed
**once per (feedback_pk, model_version)**; only cluster/label/sentiment re-run cheaply.

---

## B. Embedding (foundation for clustering AND sentiment)

**Decision: one shared embedding, computed once, stored in the `feedback_embeddings`
sidecar.** Both clustering and (semantic) sentiment consume it — do not embed twice.

> **REVISED 2026-07-10 — see [`adr_embedding_compute_strategy.md`](./adr_embedding_compute_strategy.md).**
> Production runs Trino on **Starburst Galaxy**, whose in-SQL AI functions
> (`starburst.ai.generate_embedding` → `ARRAY(DOUBLE)`, public preview) can generate
> embeddings **inside the engine we already run**, on **AWS Bedrock in our own AWS account**,
> writing vectors straight into the Iceberg sidecar. That both eliminates the net-new
> torch/sentence-transformers service *and* removes the third-party-egress objection below
> (Bedrock-in-account over Presidio-redacted text). **New default: Starburst AI
> `generate_embedding` as a `trino_only` dbt model.** The local option below drops to a
> last-resort fallback. StarRocks vector functions are irrelevant for now (not deployed) — see
> the ADR for the Phase-3 note. The rest of §B (persist-once, `model_version`, Iceberg ARRAY
> storage, redaction-upstream) is unchanged and applies regardless of who computes the vector.

- **Model choice (fallback path, if the Starburst AI preview is unavailable/undesired) —
  an open/self-hostable model** (e.g. `BAAI/bge-small-en-v1.5` or
  `sentence-transformers/all-MiniLM-L6-v2`, 384-dim): even post-redaction, sending ~1.18M
  feedback utterances to a *third-party* embedding API is avoidable egress — but note this
  concern does **not** apply to Bedrock-in-account via Starburst AI (the new default). A local
  sentence-transformers model runs the full corpus in a single batch job on CPU/GPU for
  effectively $0 marginal provider cost, at the price of the heaviest image/infra.
  - Open question deferred to implementation: GPU vs CPU throughput at 1.18M rows. At
    MVP scale (198K) CPU is fine (single-digit minutes to low hours).
- **Redaction is upstream and mandatory** (design §7): embeddings are computed on the
  Presidio-redacted text only. Raw text never reaches the embedding step.
- **`model_version` is a first-class column** on `feedback_embeddings`. Changing the model
  = new `model_version` rows, old ones retained until re-cluster completes (no in-place
  overwrite → no torn state).
- **Vector storage:** at ~1.18M × 384 float32 ≈ 1.8 GB. Store as an Iceberg `ARRAY<float>`
  column in `feedback_embeddings` for the MVP (no new operational service; aligns with the
  lake). Revisit a dedicated vector index (StarRocks vector column, S3 Vectors, Qdrant)
  only when an online nearest-neighbour/serving need appears — the batch clustering job
  reads the whole set into memory, which is fine at this scale. This defers the RFC's
  "embedding model & vector store" open question to a reversible, low-cost default.

**Rejected:** re-embedding on every run (the prototype #10793 flaw the RFC explicitly
fixes); semantic-summary-before-embedding as a default (see §C, treated as a hypothesis
to test, not a baseline — an LLM call per record turns a ~$2–16 job into a per-record LLM
cost across 1M+ records and can distort short-text sources).

---

## C. Clustering (systemic-issue detection) — `tk-...-a1d7d6`

**Goal:** turn per-utterance embeddings into clusters that distinguish a *systemic issue*
(many tickets, one root theme) from a *one-off*. Output = `cluster_id` + a cluster-size /
cohesion signal that lets a human say "this is recurring."

- **Algorithm: HDBSCAN** (density-based) as the default, over UMAP-reduced embeddings.
  Rationale vs. k-means:
  - No pre-set `k` — the number of systemic themes is unknown and grows; k-means forces a
    guess and splits/merges arbitrarily.
  - **Native noise class** — HDBSCAN labels sparse points as noise (`cluster_id = -1`),
    which *is* the "one-off complaint" bucket we explicitly want to separate from systemic
    signal. This maps directly to the motivation ("not just one-off complaints").
  - Produces a `probability`/`persistence` per point → a cohesion signal for ranking
    clusters by how tight/recurring they are.
  - Precedent alignment: the #10793 prototype's hierarchical intent is served by
    HDBSCAN's condensed tree without baking in its manual truncation.
- **Dimensionality reduction: UMAP** to ~5–15 dims before HDBSCAN (HDBSCAN degrades in
  raw high-dim space). `n_neighbors`/`min_cluster_size` are the two knobs to tune on a
  Zendesk sample; `min_cluster_size` is effectively the "how many tickets before we call
  it systemic" threshold and should be a config, not hardcoded.
- **Re-clustering is cheap and expected:** each run writes a new `cluster_run_id`; the
  sidecar keeps prior runs. `dim_feedback_category` (curated) only advances when a human
  approves labels from a run (design §4a), decoupling churny clustering from the stable
  category dimension.
- **Cross-source clustering (Phase 2):** because all sources share one embedding space in
  `int__feedback__unioned`, a cluster can span Zendesk + forum + tutor — this is the
  mechanism behind `afact_feedback_cluster_daily` (cluster × category × sentiment × date ×
  source). No algorithm change needed; just don't filter `source_slug` at cluster time.
- **Deps:** `umap-learn`, `hdbscan` (or `scikit-learn`'s `HDBSCAN` ≥1.3 to avoid the
  separate compiled dep — decide at implementation based on the Dagster image's build
  constraints). All CPU, no service.

**Cluster-quality columns** on `feedback_embeddings` are added *only if the chosen
algorithm produces them* (RFC step 6): `cluster_id`, `cluster_probability`,
`cluster_run_id`, `model_version` — silhouette/persistence optional.

---

## D. Category discovery & seeding — `tk-...-550aba`

**Bootstrapped, not cold-start** (design §4a). Two inputs seed `dim_feedback_category`:

1. **Seed from existing structure (no LLM):** Zendesk `ticket_tags` (~2,354 distinct) +
   support `group_name` give an immediate, human-meaningful starter taxonomy. These become
   `category_source='seed'` rows with `category_status='proposed'`. This alone makes the
   MVP useful before any clustering runs.
2. **LLM-label the clusters (§C output):** for each HDBSCAN cluster, sample N
   representative (redacted) utterances near the centroid + the cluster's dominant seed
   tags, and prompt an LLM to propose a short `category_label` + a stable `category_slug`
   + a one-line description. `category_source='llm_discovered'`, `category_status='proposed'`
   until a human approves (`approved`), merges (`merged`), or deprecates.

**Key design invariants (from §4a):**
- **SCD-lite on `category_slug`:** relabeling changes `category_label`, never the slug —
  so `category_fk` on the fact is stable across renames.
- **Assignment is late-arriving:** a ticket gets `category_fk` after insert, by mapping its
  `cluster_id` → the approved category for that cluster. Uncategorized = `category_fk` null
  (a valid, queryable state).
- **LLM cost is bounded:** one LLM call *per cluster*, not per record (there are hundreds
  of clusters, not millions of tickets). This is the critical cost distinction from the
  rejected per-record semantic-summary approach.
- **Model:** any capable instruction model (Claude Haiku/Sonnet class) — labeling a few
  hundred clusters is a trivial, cheap batch. Keep the labeler behind a resource interface.

**Human-in-the-loop is required, not optional:** LLM proposes, a human curates the
category dimension. The dimension is a *curated projection* of clusters (design §4d), which
is why it is a dbt/warehouse dimension and not raw model output.

---

## E. Sentiment mapping — `tk-...-92988e`

**Grain:** `sentiment_fk` on `tfact_feedback`, one sentiment per utterance.
`dim_sentiment` starts coarse: `positive | neutral | negative` (design §4b), with a
`polarity_score_bucket` for trend rollups. Aspect-based sentiment is a later refinement,
not MVP.

**Two-tier derivation, cheapest-signal-first:**
1. **Explicit signals seed & validate (free, high-precision):** where the source carries
   an explicit rating — Zendesk `satisfaction_rating_score`, tutor `rating`, ORA scores —
   map it directly to a sentiment bucket. This gives a labeled validation set for free and
   covers a real fraction of rows at zero model cost.
2. **Model the rest:** for utterances with no explicit signal, derive sentiment from the
   text. Two options, in preference order:
   - **(recommend MVP) A lightweight local classifier / lexicon** over the redacted text —
     e.g. a fine-tuned or off-the-shelf sentiment model (`distilbert-sst2`-class) run in the
     same batch job as embedding. CPU-cheap, no per-record API cost, keeps PII local.
     Validate its output against the tier-1 explicit signals to pick a threshold.
   - **(fallback) LLM classification** only if the local classifier's accuracy against the
     explicit-signal validation set is inadequate. Even then, batch it and cap it — do not
     make it a per-record online cost.
   - **Semantic/embedding mapping** (the task title's phrasing): the embeddings already
     computed (§B) can be nearest-neighbour-mapped to the explicit-signal-labeled examples
     as a zero-extra-model-cost sentiment estimate. This is the most economical option and
     reuses the one embedding — evaluate it against the local-classifier option on a sample.

**Decision handed to implementation with a concrete evaluation:** run all three (explicit
+ embedding-kNN, explicit + local classifier, explicit + LLM) on a labeled Zendesk sample,
pick by accuracy-vs-cost. Default assumption: **explicit signals + embedding-kNN** wins on
cost and is "good enough" for trend-level sentiment; upgrade only if the accuracy gap is
material. `dim_sentiment` and the fact column are unaffected by which wins.

---

## F. What is explicitly deferred (non-blocking for spec/MVP)

- Embedding model final pick + GPU/CPU throughput at full 1.18M scale (MVP proves it at 198K).
- Dedicated vector store / online serving (Iceberg ARRAY suffices for batch).
- Aspect-based sentiment; multi-lingual handling.
- Semantic-summary-before-embedding and hierarchical truncation from prototype #10793 —
  test as hypotheses on a sample, do not inherit (RFC Open Questions).
- Cross-source `afact_feedback_cluster_daily` tuning (Phase 2).
