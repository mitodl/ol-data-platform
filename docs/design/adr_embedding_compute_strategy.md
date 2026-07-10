# ADR: Where feedback embedding / AI inference runs

Status: **proposed** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 (rev. 3 — Fenic as the engine-external framework) · Amends
[`feedback_ml_approach.md`](./feedback_ml_approach.md) §B/§E and
[`feedback_dagster_asset_spec.md`](./feedback_dagster_asset_spec.md).

## Context

Original spec (`feedback_ml_approach.md` §B) proposed a net-new Dagster location running
`sentence-transformers`/`torch` to embed locally — the heaviest infra in the system. Two
constraints then reshaped the decision:

1. **Production runs Trino on Starburst Galaxy**; local dev is DuckDB; **StarRocks is not yet
   deployed**. And the **strategic direction is to retire Trino for StarRocks** (warehouse
   transforms + data bus + OLAP serving). ⇒ **No Galaxy-only functionality on the critical
   path.** This vetoes pushing embedding into SQL via Starburst's `generate_embedding`
   (Galaxy-proprietary; StarRocks has no embedding-generation equivalent — its vector support
   is *store + ANN search* only). Embedding generation is inherently **engine-external**.
2. **Fenic** ([typedef-ai/fenic](https://github.com/typedef-ai/fenic)) is a viable
   engine-external framework for this layer — this revision evaluates it as a first-class option
   rather than a footnote.

### What Fenic actually is (verified against the source, 2026-07-10)

- **License: Apache-2.0** (`LICENSE` on `main`). Genuinely open source — no Galaxy-style
  lock-in.
- **Engine-external**, PySpark-style DataFrame framework with a query engine built for LLM
  inference: semantic operators (`embed`, `semantic.classify`, `semantic.analyze_sentiment`,
  `semantic.extract`, `semantic.join`, `semantic.reduce`), a native `EmbeddingType`, plus
  `with_cluster_labels(by, num_clusters)` (**K-means**), and built-in batching, rate-limiting,
  retries, token/cost accounting, and row-level lineage. Reads CSV/Parquet/S3/HF datasets.
- **Embedding providers in the shipped `SessionConfig`:** `OpenAIEmbeddingModel`,
  `CohereEmbeddingModel`, `GoogleDeveloperEmbeddingModel`, `GoogleVertexEmbeddingModel`
  (language models: OpenAI, Anthropic, Google Developer/Vertex, OpenRouter). So it is
  **embedding-provider-agnostic** across those vendors.
- **AWS Bedrock status — important nuance:** there is **no `BedrockEmbeddingModel` config class
  today.** In `config.py`, "Bedrock" appears only as an *OpenRouter routing target for language
  models* (a third-party hop through openrouter.ai, and for LLMs, not embeddings), and
  `ROADMAP.md` lists Bedrock as planned. So **native, in-account Bedrock *embeddings* through
  Fenic are not yet available** — but because Fenic is Apache-2.0, a Bedrock embedding provider
  can be contributed/added if we require it. (If a Fenic+Bedrock embedding path has landed in a
  newer release than what `main` showed here, confirm the exact `SessionConfig` class before
  relying on it.)

## Decision

**Keep all AI/embedding compute engine-external so it is indifferent to Trino-today vs.
StarRocks-tomorrow, and use Fenic (Apache-2.0) as that engine-external framework** for the
embed → (classify/sentiment) → label pipeline, running in a Dagster asset that reads the dbt
outputs and writes vectors/labels back to open Iceberg. **Clustering stays our own
sklearn/UMAP + HDBSCAN** (Fenic offers only K-means; we need HDBSCAN's noise class to separate
one-off complaints from systemic signal — `feedback_ml_approach.md` §C).

```
int__feedback__unioned (redacted text, feedback_pk)                    [dbt/SQL — portable, no AI funcs]
  → Fenic pipeline in a Dagster asset:                                  [engine-external, Apache-2.0]
       .semantic.embed        → EmbeddingType column → Iceberg ARRAY<float> sidecar
       .semantic.analyze_sentiment / .classify (optional; see §E note)
  → feedback_clusters : our sklearn UMAP+HDBSCAN over the vectors       [engine-external]
  → dim_feedback_category : LLM-label clusters (Fenic semantic op or a client call)
  → sentiment_fk : explicit-CSAT seed + embedding-kNN (default) or Fenic analyze_sentiment
```

**Embedding model is chosen by TASK EFFECTIVENESS, not by provider/PII** (owner direction,
2026-07-10: Bedrock/in-account is **not** a requirement; select the model on how well it clusters
and retrieves *our* feedback). Egress of Presidio-redacted text to a managed provider is
acceptable. See `feedback_ml_approach.md` §B.1 for the selection harness (MTEB to narrow →
benchmark the shortlist on a labeled Zendesk sample → pick by clustering agreement/coherence and
cost; sweep Matryoshka dims). Candidate providers: Fenic-native managed
(`gemini-embedding-001` / Cohere `embed-v4` / OpenAI `text-embedding-3-large`) and self-hosted
open (Qwen3-Embedding / BGE-M3 via `sentence-transformers`) if we host for effectiveness. Because
vectors persist with `model_version`, the choice is reversible (re-embed later without touching
the fact). Bedrock (`amazon.titan-embed-text-v2:0` via boto3) remains *a* candidate — no longer
the default, just one option in the bake-off.

Either way the **vectors land in an open Iceberg `ARRAY<float>` sidecar**, so when StarRocks
becomes the serving/OLAP + data-bus engine it **reads those vectors and builds an HNSW index**
over them (a load, not a re-embed) — **StarRocks ANN is the intended vector-serving tier**,
reached with no rework. dbt models stay pure and portable (existing
`default__`/`duckdb__`/`starrocks__` dispatch); **no engine-native AI SQL functions, no
`trino_only` AI models.**

## Why Fenic over a hand-rolled boto3 pipeline

Both are engine-external and portable; Fenic adds, for free, what we'd otherwise hand-build:
automatic batching, rate-limiting, retries, token/cost accounting, response caching, row-level
lineage, and typed semantic operators (`classify`/`analyze_sentiment`/`extract`) we can reuse
for category assignment and sentiment. Apache-2.0 means no lock-in and the option to extend it
(e.g. the Bedrock embedding provider). The one thing it does **not** replace is HDBSCAN — keep
that ours.

## Consequences / caveats

1. **New dependency (Fenic) + provider setup.** Add `fenic` to the new Dagster project's
   `pyproject.toml`. Configure the chosen embedding provider (Bedrock-via-boto3/contrib, or a
   Fenic-native provider) and credentials (Bedrock → Dagster pod IAM role `bedrock:InvokeModel`,
   likely no Vault secret; a managed provider → a Vault-stored API key via the existing
   `ConfigurableResource` pattern).
2. **Bedrock-embedding gap (see Context).** If in-account Bedrock embeddings are required *now*
   and not yet native to Fenic, use the boto3-embed + Fenic-for-the-rest split, or add the
   provider. Don't assume a `BedrockEmbeddingModel` exists without checking the installed version.
3. **Iceberg I/O is not native to Fenic.** Fenic reads CSV/Parquet/S3/HF, not Iceberg directly.
   Integration: load the dbt table to a DataFrame (existing `get_dbt_model_as_dataframe` →
   Polars) and stage to Parquet/S3 for `session.read.parquet(...)`, or hand Fenic the frame if
   in-memory ingestion is supported in the installed version; write vectors out as Parquet and
   load to the Iceberg sidecar. Confirm the exact read/write surface at implementation.
4. **Clustering is not Fenic's.** UMAP+HDBSCAN stays a small sklearn step reading the vectors.
5. **Cost/PII depend on the provider, not Fenic.** Titan V2 embeddings ~$0.02/1M tokens → low
   tens of $ one-time for ~1.18M redacted utterances; persist-once + `model_version`, never
   re-embed; use batch inference for the backfill. A managed provider has similar cost but
   egresses redacted text — a policy call.

## Alternatives (kept)

- **Direct boto3 Bedrock client, no Fenic** — the minimal engine-external path; use if we don't
  want the Fenic dependency. Loses the batching/caching/lineage ergonomics (hand-build them).
- **Local `sentence-transformers`** — zero-egress, no provider cost, but the heaviest image;
  fallback only if all managed/Bedrock options are ruled out. Still engine-external/portable;
  could even be wired behind Fenic if/when it supports local models.
- **Starburst Galaxy `generate_embedding` — REJECTED for the critical path** (Galaxy-proprietary,
  in the layer being migrated off, no StarRocks equivalent).
- **StarRocks vector index** — the *target serving tier*, not a generation option; built once
  StarRocks is deployed, by loading the open Iceberg vectors.

## Net change to the spec

- `feedback_ml_approach.md` §B default: **engine-external Fenic pipeline (Apache-2.0), embedding
  provider chosen by PII policy** (in-account Bedrock via boto3/contrib preferred; managed
  provider on redacted text acceptable if policy allows). Vectors → Iceberg `ARRAY<float>` sidecar.
- `feedback_dagster_asset_spec.md`: embedding/sentiment run via Fenic in the Dagster asset (not
  torch, not a Starburst SQL function); clustering stays sklearn HDBSCAN; no `trino_only` AI models.
- StarRocks HNSW is the **intended** vector-serving tier, reached by loading Iceberg vectors.
- Schema, contract, and business keys unchanged — compute location was always meant to be
  swappable (vectors in a sidecar; category/sentiment late-arriving FKs).
