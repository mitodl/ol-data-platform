# ADR: Where feedback embedding / AI inference runs

Status: **proposed** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 · Amends [`feedback_ml_approach.md`](./feedback_ml_approach.md) §B/§E and
[`feedback_dagster_asset_spec.md`](./feedback_dagster_asset_spec.md).

## Context

The original ML spec (`feedback_ml_approach.md` §B) proposed a **net-new Dagster code
location** running `sentence-transformers`/`torch` to embed feedback locally, chosen for the
learner-PII posture (avoid shipping ~1.18M utterances to a third-party API). The Dagster
explore flagged this as the heaviest part of the whole system: a large image (torch + spaCy),
a first-of-its-kind ML service in the repo, and all-net-new deps.

A prompt to evaluate **in-engine AI functions** (Trino/Starburst, StarRocks) and a
**DataFrame-based AI framework** (Fenic) against that approach surfaced a decisive fact from
the repo:

- **Production runs Trino on Starburst Galaxy** (`*.trino.galaxy.starburst.io`;
  `src/ol_dbt/profiles.yml` production/qa targets `type: trino`). Local dev is DuckDB.
- **StarRocks is NOT deployed** — it exists only as `starrocks__` dispatch branches in
  `macros/cross_db_functions.sql` and "future" language in `DBT_DIALECT_COMPATIBILITY.md`.
- There is **zero existing use** of engine-native AI/embedding/vector SQL functions.

This means the engine we already run has AI functions available, and the vector-DB engine we
don't run is the only one the "StarRocks vector index" idea depended on.

## The three candidates, disambiguated

They are **not** the same kind of thing — the original prompt ("use a dataframe for the AI
functions in Trino/Starburst or StarRocks") blends them; they separate cleanly:

| Candidate | What it is | Runs where | Generates embeddings? | Vector search? |
|---|---|---|---|---|
| **Starburst Galaxy AI functions** | In-SQL `starburst.ai.generate_embedding(text, model)` → `ARRAY(DOUBLE)`, plus `analyze_sentiment`, `classify`, `mask`, `prompt` | **inside our production Trino engine** | **yes** (Bedrock/OpenAI) | via generated vectors + similarity |
| **Fenic** (typedef.ai) | PySpark-style **client-side** DataFrame lib with semantic operators (`embed`, `with_cluster_labels` K-means, `analyze_sentiment`, `classify`, `semantic.join`) | a Python process (a Dagster asset) | **yes** (OpenAI/Google/Cohere APIs — cloud only) | K-means clustering built in |
| **StarRocks vector index** | HNSW/IVFPQ index + `approx_cosine_similarity` | inside StarRocks — **not deployed** | **no** (store/search only) | yes (ANN) |

Key correction to the original framing: **Fenic does not "use" Trino/StarRocks AI functions.**
It has its *own* embedding/semantic operators and runs client-side; it is an alternative to a
hand-rolled Python pipeline, not a way to invoke engine AI functions. StarRocks *stores &
searches* vectors but does **not** generate them. Only Starburst pushes embedding generation
into the engine we already run.

## Decision

**Primary: push embedding + model-based sentiment into SQL/dbt via Starburst Galaxy AI
functions, on AWS Bedrock models, over Presidio-redacted text. Keep only clustering +
LLM cluster-labeling as a (now lightweight) Python Dagster asset.**

Revised pipeline (replaces `feedback_ml_approach.md` §A for the embed/sentiment stages):

```
int__feedback__unioned (redacted text, feedback_pk)                       [dbt/SQL]
  → feedback_embeddings   : generate_embedding() → ARRAY(DOUBLE) in Iceberg [dbt, trino_only]
  → sentiment_fk          : analyze_sentiment() (+ explicit CSAT seed §E)   [dbt, trino_only]
  → feedback_clusters     : read vectors from Iceberg → UMAP+HDBSCAN         [Dagster py asset]
  → dim_feedback_category : LLM-label clusters (Bedrock via prompt())        [Dagster py asset]
```

Rationale:
1. **Eliminates the heaviest infra.** No torch/sentence-transformers, no embedding model in a
   Docker image. Embedding + sentiment become `trino_only`-tagged dbt models. The Python
   surface shrinks to *just* clustering + labeling — which read pre-computed vectors, so the
   asset stays tiny (sklearn/UMAP, CPU) and still clones `student_risk_probability`.
2. **Vectors land where dbt already lives.** `generate_embeddings` procedure (or
   `generate_embedding()` in an INSERT) writes straight into the `feedback_embeddings` Iceberg
   `ARRAY(DOUBLE)` column — no new store, no data movement.
3. **PII posture improves, doesn't regress.** Starburst AI runs against **AWS Bedrock**, which
   can be in our own AWS account/region (we already run on AWS: Glue/S3/EKS). Bedrock-in-account
   over Presidio-redacted text removes the "third-party egress" objection that drove the
   original local-only recommendation. (Presidio stays the deterministic redaction guarantee;
   `ai.mask` is an LLM-based masker to *evaluate as an add-on*, not to trust for compliance.)
4. **Already on the engine.** No new service to run or operate — it's a feature of the Trino
   cluster we already pay for.

**Clustering stays Python** because neither Trino nor DuckDB does density clustering, and we
specifically want **HDBSCAN's noise class** to separate one-off complaints from systemic
signal (`feedback_ml_approach.md` §C) — Fenic's `with_cluster_labels` is **K-means only** (no
noise class), so it does not satisfy that requirement.

## Consequences / caveats (must be resolved before committing)

1. **Gating question — is the AI-workflows feature enabled on our Galaxy plan?**
   `generate_embedding` is **public preview** in Starburst Galaxy. Confirm it's available on
   our subscription and that we accept depending on a preview feature for a production pipeline
   (API-stability risk). This is the single decision that makes-or-breaks the primary option.
2. **Bedrock model access + config.** Requires configuring an embedding model (e.g.
   `bedrock_titan`) and a language model in Galaxy AI Model Access Management, and enabling
   Bedrock in our AWS account. New platform setup, but far lighter than a torch service.
3. **DuckDB local-dev / CI incompatibility.** AI functions are Trino-only; the embedding and
   `analyze_sentiment` models must carry `tags: ['trino_only']` (existing repo pattern) and
   will **not** run under the DuckDB `dev_local` target or the DuckDB-based docs CI. Embedding
   is not something we'd want running on every local build anyway (cost), so gating it is
   acceptable — but you lose full local reproducibility of those two models.
4. **Cost shifts from one-time-local to per-invocation-provider.** Original: ~$2–16 one-time
   local compute. Now: Bedrock embedding cost per record (Titan text embeddings are cheap —
   order low-tens-of-dollars for 1.18M short redacted utterances — but it's a provider line
   item, and re-embedding must still be avoided via persist-once + `model_version`). Do a
   quick Bedrock-pricing sanity check at spec-approval time.
5. **`analyze_sentiment`/`classify` are LLM calls per row.** Fine at MVP volume (198K, batch),
   but confirm the batch cost and that per-row LLM sentiment beats the cheaper
   explicit-CSAT-seed + embedding-kNN option (`feedback_ml_approach.md` §E) — it may not, in
   which case keep sentiment on the kNN path and use Starburst AI only for embedding.

## Alternatives (kept, in preference order)

- **Fenic (fallback if the Galaxy AI preview is unavailable/undesired).** Gives
  batching/caching/cost-accounting/row-level lineage for the Python path and does embed +
  sentiment + K-means in one DataFrame pipeline reading Parquet/S3. Downsides vs. the primary:
  still client-side (no engine push-down), **cloud-API egress only** (no self-hosted / no
  in-account Bedrock embedding path documented), K-means clustering (no HDBSCAN noise class),
  and a new framework dependency. Reasonable middle ground; less aligned than SQL push-down
  given we're already on Galaxy. (License: OSS public repo — confirm Apache-2.0 before adopting.)
- **Local `sentence-transformers` in Dagster (original §B).** Now the *last* resort: only if
  both the Starburst AI feature and cloud/Bedrock egress are ruled out. Zero per-record
  provider cost and no egress, but the heaviest image/infra and the most net-new code.
- **StarRocks vector index — deferred to Phase 3.** Not applicable now (not deployed). When/if
  the data bus lands on StarRocks, migrate the `feedback_embeddings` sidecar from Iceberg
  `ARRAY(DOUBLE)` brute-force to a StarRocks HNSW index for ANN serving (similar-feedback / RAG
  at query time). StarRocks still won't *generate* embeddings — those keep coming from Starburst
  AI (or a UDF). Fold into the `afact`/serving work, not the MVP.

## Net change to the spec

- `feedback_ml_approach.md` §B default flips: **Starburst Galaxy AI `generate_embedding`
  (Bedrock, in-account, on redacted text)** becomes the recommended embedding path;
  local self-hosted drops to fallback.
- `feedback_dagster_asset_spec.md`: the `feedback_embeddings` and `feedback_sentiment` stages
  move from Python assets to `trino_only` dbt models; the Dagster asset shrinks to
  `feedback_clusters` + `feedback_category_proposals` (read vectors, UMAP+HDBSCAN, LLM-label).
- No change to the fact, the dims, the contract, or the business keys — this ADR only changes
  *where the vector/sentiment compute runs*, which the schema was deliberately built to be
  agnostic about (embeddings live in a sidecar; category/sentiment are late-arriving FKs).
