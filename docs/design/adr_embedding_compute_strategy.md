# ADR: Where feedback embedding / AI inference runs

Status: **proposed** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 (rev. 2 — engine-portability constraint) · Amends
[`feedback_ml_approach.md`](./feedback_ml_approach.md) §B/§E and
[`feedback_dagster_asset_spec.md`](./feedback_dagster_asset_spec.md).

## Context

The original ML spec (`feedback_ml_approach.md` §B) proposed a net-new Dagster code location
running `sentence-transformers`/`torch` to embed feedback locally, for the learner-PII posture.
The Dagster explore flagged this as the heaviest part of the system (large image, first ML
service, all-new deps).

Two facts then reshaped the decision:

1. **Production runs Trino on Starburst Galaxy** (`src/ol_dbt/profiles.yml`); local dev is
   DuckDB; **StarRocks is not deployed** (only `starrocks__` dispatch branches in
   `macros/cross_db_functions.sql`).
2. **Strategic direction (owner, 2026-07-10): retire Trino in favour of StarRocks** as the
   single engine for warehouse/Iceberg transformations, the "data bus", and OLAP serving.
   **Therefore: no Galaxy-only (Starburst-proprietary) functionality may sit on the critical
   path** — anything we build on the SQL engine must survive the Trino→StarRocks migration.

Rev. 1 of this ADR recommended pushing embedding into SQL via Starburst Galaxy's
`generate_embedding`. **Fact 2 vetoes that**: it is a Galaxy-proprietary SQL function living in
exactly the layer being migrated to StarRocks, and StarRocks has **no** embedding-generation
function to migrate it to (StarRocks vector support is *store + ANN search* only —
`cosine_similarity`, HNSW/IVFPQ indexes — not generation). Adopting it would guarantee a
rewrite. This revision removes it from the critical path.

## The three candidates, disambiguated

| Candidate | What it is | Runs where | Generates embeddings? | Engine-portable (Trino→StarRocks)? |
|---|---|---|---|---|
| **Starburst Galaxy AI functions** | in-SQL `starburst.ai.generate_embedding` etc. (Galaxy preview) | inside the SQL engine | yes (Bedrock/OpenAI **wrapper**) | **no — Galaxy-only; StarRocks has no equivalent** |
| **Direct AWS Bedrock client** (boto3) | `bedrock-runtime.invoke_model('amazon.titan-embed-text-v2:0', …)` | an **engine-external** Dagster asset | yes (Bedrock, **in our AWS account**) | **yes — indifferent to the SQL engine** |
| **Fenic** (typedef.ai) | client-side DataFrame lib w/ semantic ops + K-means | an engine-external Dagster asset | yes (OpenAI/Google/Cohere APIs — cloud only) | yes (engine-external) but **cloud-egress**, K-means only |
| **local `sentence-transformers`** | self-hosted model in a Dagster asset | engine-external | yes (local, $0 egress) | yes, but heaviest image |
| **StarRocks vector index** | HNSW/IVFPQ + `approx_cosine_similarity` | inside StarRocks (future) | **no** (store/search only) | it *is* the target — for serving, not generation |

Two corrections to the earlier framing that matter here:
- **Starburst's `generate_embedding` is only a SQL wrapper over Bedrock models.** We can call the
  *same* Bedrock model directly with boto3 from the Dagster layer — keeping the in-account /
  PII-safe benefit while dropping the Galaxy dependency. The wrapper buys nothing we can't get
  portably.
- **StarRocks does not generate embeddings.** So "do it in the future engine's SQL" is not an
  option either. Embedding generation is inherently engine-external; the only question is which
  external tool.

## Decision

**Keep all AI/embedding compute in the engine-external orchestration (Dagster/Python) layer,
reading and writing open Iceberg tables — so it is indifferent to whether the SQL engine is
Trino today or StarRocks tomorrow. Generate embeddings by calling AWS Bedrock
(`amazon.titan-embed-text-v2:0`) directly via boto3, not through any engine's SQL AI function.**

Revised pipeline:

```
int__feedback__unioned (redacted text, feedback_pk)                         [dbt/SQL — portable, no AI funcs]
  → feedback_embeddings  : Dagster asset, boto3 Bedrock invoke_model         [engine-external]
                           → vectors into Iceberg ARRAY<float> sidecar
  → feedback_clusters    : Dagster asset, read vectors → UMAP+HDBSCAN         [engine-external]
  → dim_feedback_category: Dagster asset, LLM-label clusters (Bedrock)        [engine-external]
  → sentiment_fk         : explicit CSAT seed + embedding-kNN (reuse vectors) [portable dbt/py, NO analyze_sentiment]
```

Rationale:
1. **Zero engine lock-in.** Nothing AI-specific touches Trino or StarRocks SQL. The dbt models
   stay pure, portable, and continue to compile through the existing Trino/DuckDB/`starrocks__`
   dispatch. When transforms move to StarRocks, the embedding path doesn't change at all.
2. **Keeps the PII win without the wrapper.** Bedrock Titan runs in **our own AWS account**
   (we're already on AWS: Glue/S3/EKS) over Presidio-redacted text — same posture Rev. 1 gained
   from Starburst AI, obtained instead by a direct boto3 call. Auth via the Dagster pod's IAM
   role (IRSA) with `bedrock:InvokeModel` — likely **no Vault secret** needed (unlike a
   third-party API key).
3. **Light, not heavy.** A boto3 Bedrock client is a thin dependency — **no torch, no
   sentence-transformers, no model in the image**. It removes the original spec's heaviest
   infra *and* avoids the Galaxy dependency. Batching/retries are a modest amount of code around
   `invoke_model` (or use Bedrock batch inference for the 1.18M backfill).
4. **Open landing = free StarRocks path.** Vectors persist in an Iceberg `ARRAY<float>` sidecar
   (open format). When StarRocks becomes the serving/OLAP + data-bus engine, it **reads those
   Iceberg vectors and builds an HNSW index over them** — a load, not a re-embed. So the
   target-state vector-serving tier (StarRocks ANN) is reached without rework, and it is now
   **on the intended path**, not a Phase-3 afterthought.
5. **Sentiment stays engine-agnostic.** Use the cheaper explicit-CSAT-seed + embedding-kNN path
   (`feedback_ml_approach.md` §E) — which reuses the vectors and needs no per-row LLM call and
   no engine AI function. Explicitly **do not** adopt Starburst `analyze_sentiment` (Galaxy-only).
6. **Clustering stays Python** (sklearn/UMAP/**HDBSCAN** for the noise class — one-off vs.
   systemic; Fenic's K-means can't do this) — already engine-external and portable.

## Consequences / caveats

1. **Slightly more code than a SQL one-liner.** We write a small Bedrock-client Dagster asset
   instead of a `generate_embedding` dbt model. That is the price of portability, and it is
   small (thin boto3 wrapper cloned from `student_risk_probability`'s asset shape).
2. **Bedrock model access + IAM.** Enable `amazon.titan-embed-text-v2:0` (and a text model for
   cluster labels) in our AWS account/region and grant the Dagster role `bedrock:InvokeModel`.
   No Galaxy AI feature, no preview dependency.
3. **Cost is per-invocation Bedrock**, but cheap: Titan V2 text embeddings at ~$0.02 / 1M tokens
   → low tens of dollars for ~1.18M short redacted utterances, one-time (persist-once +
   `model_version`; never re-embed). Use Bedrock **batch inference** for the backfill to cut
   cost/throughput pressure. Sanity-check at approval time.
4. **Dimensions choice:** Titan V2 supports 256/512/1024 — pick 256 or 512 for a smaller sidecar
   and faster HDBSCAN/HNSW unless retrieval quality needs 1024. Tune on the Zendesk sample.
5. **No StarRocks work now.** StarRocks HNSW is the documented *target* serving tier but is not
   built until StarRocks is deployed; MVP uses Iceberg-ARRAY brute-force (fine at ≤1.18M for
   batch clustering; the "similar feedback"/RAG serving need is what later justifies the HNSW
   index).

## Alternatives (kept, in preference order)

- **Fenic** — optional ergonomics wrapper for the Python embedding/label path (batching, caching,
  cost-accounting, row-level lineage, K-means + `analyze_sentiment` operators). Still
  engine-external and portable, **but** its embedding providers are cloud APIs
  (OpenAI/Google/Cohere) with **no in-account Bedrock path documented**, so it reintroduces
  third-party egress and is less aligned than a direct Bedrock client on the PII axis; its
  clustering is K-means (no noise class). Adopt only if the ergonomics outweigh the egress and
  we keep clustering on our own HDBSCAN. (Confirm OSS license before use.)
- **Local `sentence-transformers`** — the zero-egress fallback if in-account Bedrock is somehow
  ruled out. Heaviest image/infra; no provider cost. Still engine-external/portable.
- **Starburst Galaxy `generate_embedding` — REJECTED for the critical path.** Galaxy-proprietary,
  sits in the SQL layer being migrated off, no StarRocks equivalent. Only reconsider as a throwaway
  spike if we needed embeddings before any Dagster/Bedrock wiring existed — not worth the eventual
  rewrite.

## Net change to the spec

- `feedback_ml_approach.md` §B default: **engine-external Dagster asset calling Bedrock
  `amazon.titan-embed-text-v2:0` via boto3**, vectors in an Iceberg `ARRAY<float>` sidecar. (Rev. 1's
  "Starburst AI `trino_only` dbt model" is withdrawn for engine lock-in.)
- `feedback_dagster_asset_spec.md`: the embedding stage stays a Dagster asset but is a thin
  Bedrock-client call (no torch); sentiment stays the CSAT-seed + kNN path; clustering + labeling
  unchanged. No `trino_only` AI dbt models.
- StarRocks HNSW vector index is elevated from a Phase-3 afterthought to the **intended
  target vector-serving tier**, reached by loading the open Iceberg vectors (no re-embed).
- Schema, contract, and business keys unchanged — compute location was always meant to be
  swappable (vectors in a sidecar; category/sentiment late-arriving FKs), which is exactly what
  makes this a compute-only, engine-portable revision.
