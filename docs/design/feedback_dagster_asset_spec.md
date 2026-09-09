# Feedback Aggregation — Dagster ML Asset Spec (MVP)

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-08-13 (rev. 5 — `feedback_clusters` is a `@multi_asset`; see
[#2422 review](https://github.com/mitodl/ol-data-platform/pull/2422)) · rev. 4 (2026-08-10, conversation
grain) · Companion to [`feedback_zendesk_mvp_spec.md`](./feedback_zendesk_mvp_spec.md)
and [`feedback_ml_approach.md`](./feedback_ml_approach.md)

The scheduled batch job that turns assembled, redacted conversations into summaries, embeddings, clusters,
LLM-proposed categories, and sentiment. Grounded in the existing repo orchestration patterns. **The fact
ships without this asset**; this is purely additive — it fills the generated columns on
`afact_feedback_conversation` and writes `feedback_cluster_run` (see
[`feedback_erd.md`](./feedback_erd.md) §4/§5).

> **REVISED 2026-08-13 (rev. 5) — `feedback_clusters` is a `@multi_asset`.** §2 specified one stage
> producing two target tables (`feedback_cluster_run`, `feedback_cluster_candidate`) through a single
> `@asset`; a plain `@asset` has one materialized output, so that could not actually run as written. Split
> into two named `AssetOut`s on one `@multi_asset` — same stage, same UMAP+HDBSCAN call, two writes.

> **REVISED 2026-08-10 (rev. 4) — the analysis unit is the conversation** (design §5a). The asset reads
> `int__feedback__conversation` (one row per conversation, turns assembled and ordered) instead of
> `int__feedback__unioned`, gains a **summarization stage** (§2), and writes **one target table** instead of
> a three-table per-turn sidecar. **It never writes to `tfact_feedback`** — that fact is now insert-only, so
> the rev. 3 "late-arriving `category_fk`/`sentiment_fk` upsert onto the fact" is gone entirely.

> **REVISED 2026-07-10 (rev. 3) — see [`adr_embedding_compute_strategy.md`](./adr_embedding_compute_strategy.md).**
> Because the strategic direction is to **retire Trino for StarRocks**, all AI compute stays
> **engine-external and portable** — no engine-native AI SQL functions (Starburst's are
> Galaxy-only; StarRocks has none). The embedding/sentiment/labeling stages run via **Fenic
> (Apache-2.0)** inside a Dagster asset — not torch, not a Starburst `trino_only` dbt model.
> **Embedding model is chosen by task effectiveness** (Bedrock not required) — MTEB-narrowed
> shortlist benchmarked on a labeled Zendesk sample (`feedback_ml_approach.md` §B.1): Fenic-native
> managed (`gemini-embedding-001`/Cohere `embed-v4`/OpenAI `text-embedding-3-large`) or
> self-hosted open (Qwen3/BGE-M3). Egress of Presidio-redacted text is acceptable. A managed
> provider uses a Vault-stored key via the existing `ConfigurableResource` pattern; a self-hosted
> model needs no secret but a heavier image. Choice is reversible via `model_version`. Vectors land in an open
> Iceberg `ARRAY<float>` sidecar (StarRocks reads it later to build an HNSW index — a load, not a
> re-embed). **Clustering stays our own sklearn UMAP+HDBSCAN** (Fenic has K-means only, no noise
> class). Sentiment defaults to the explicit-CSAT + embedding-kNN path. Fenic does not read
> Iceberg natively — stage via Parquet/S3 or hand it the in-memory frame (confirm at
> implementation). §2–§6 below describe the asset shape; substitute Fenic (or a boto3 Bedrock
> client) for the local `sentence-transformers` model where §4 lists it (now a fallback).

---

## 1. Template: clone `student_risk_probability`

There is a near-exact precedent code location: `dg_projects/student_risk_probability/` — a
standalone `dg`-managed Dagster project whose single Python `@asset` reads a dbt-modeled
Iceberg table into a Polars frame, runs scikit-learn ML, and writes results back via the
Iceberg IO manager. **Clone its structure** for a new
`dg_projects/feedback_clustering/` code location:

```
dg_projects/feedback_clustering/
  pyproject.toml            # + NEW deps: embeddings + clustering + LLM + presidio (§4)
  uv.lock
  Dockerfile
  build.yaml
  feedback_clustering/
    definitions.py          # Definitions(assets, resources={io_manager, vault, llm}, jobs, schedules)
    assets/feedback_clustering.py   # thin @asset(s)
    resources/llm.py        # NEW Vault-backed LLM/embeddings client factory
    lib/summarize.py        # conversation summarization + the skip rule (ml §A.1)
    lib/embed.py            # embedding + redaction helpers
    lib/cluster.py          # UMAP+HDBSCAN helpers
    lib/label.py            # LLM cluster-labeling + sentiment helpers
    schedules/              # cron ScheduleDefinition (optional; declarative default)
```

Scaffold via the `dagster-code-location-structure` skill (`dg`), copying
`student_risk_probability`'s `definitions.py` (Iceberg `PolarsIcebergIOManager`, Vault
auth, `define_asset_job`) verbatim as the skeleton. Replicate its Python pin
(`>=3.14,<3.15`) and its `pyiceberg`/`grpcio` overrides.

---

## 2. Asset graph (MVP)

Model each stage as its own `@asset` so re-runs are granular and cached independently. All
read/write Iceberg via `get_dbt_model_as_dataframe(...)`
(`ol_orchestrate.lib.glue_helper`) and the `io_manager` (`PolarsIcebergIOManager`).

```
[dbt] int__feedback__conversation  (redacted turns assembled per conversation)  ← upstream AssetKey dep
   │
   ▼
feedback_summaries             @asset  → conversation_summary + summary_model_version
   │                                     SKIPS turn_count = 1 and short conversations (ml §A.1)
   ▼
feedback_embeddings            @asset  → embedding_vector, embedding_dim, embedding_input,
   │                                     embedding_model_version   [computed ONCE per version]
   ▼
feedback_clusters              @multi_asset (two AssetOuts — fixed rev. 4, @copilot: a plain @asset has one
   │                                     materialized output and cannot populate two target tables from one
   │                                     DataFrame through PolarsIcebergIOManager)
   │           ├─ out: feedback_cluster_run       → one row: algorithm, params, cluster_count, noise_count,
   │           │                                     silhouette   (UMAP→HDBSCAN, run-level)
   │           └─ out: feedback_cluster_candidate → cluster_id / cluster_probability per conversation
   │                                                 (per-conversation, this run only — §4f/design §4f)
   ├─────────────► feedback_category_proposals  @asset → LLM-labels clusters → dim_feedback_category
   │                                                     (category_source='llm_discovered', status='proposed',
   │                                                      cluster_run_id = provenance)
   └─────────────► feedback_sentiment           @asset → sentiment per conversation
                                                          (explicit rating + kNN/classifier)
   ▼
afact_feedback_conversation  ← the generated columns, one row per conversation
```

Asset names are retained from rev. 3 where the stage is the same; what changed is the grain (conversation,
not turn) and the target (one fact table, not three sidecars). Each stage stamps its own version column, so
re-running one stage does not invalidate the others: re-clustering reuses the stored vectors, and re-embedding
reuses the stored summaries.

**Unpromoted runs** go to `feedback_cluster_candidate`, not straight onto the fact — that is what lets a
proposed run be compared against the live one during the embedding bake-off (`feedback_ml_approach.md` §B.1).
Promotion copies the assignment onto `afact_feedback_conversation` and drops the candidate rows.

- **Redaction placement (design §7 / MVP spec §3):** unchanged in substance — Presidio is Python, so
  redaction happens in a Python asset upstream of both the fact and this pipeline. **Decision for
  implementation:** either (a) an asset writes `text_redacted` back to a table the fact reads, or (b)
  `int__feedback__unioned` is itself a Python-materialized step. Pick (a) to keep dbt pure-SQL; documented as
  the one interleave point. Note the assembly step (`int__feedback__conversation`) is plain SQL over
  already-redacted turns, so it adds no new interleave.
- **`code_version`** on each asset (as `student_risk_probability` does) so a helper/model
  change re-triggers via declarative automation.
- **`pool=`** set per asset (concurrency governed by the production instance pool config —
  slot limits live in the Dagster UI, not repo config).

---

## 3. Reading & writing data (exact repo helpers)

- **Read** the assembled conversations:
  `get_dbt_model_as_dataframe(database_name="ol_warehouse_production_<schema>",
  table_name="int__feedback__conversation")` → Polars. (Same call `student_risk_probability`
  uses against `reporting.cheating_detection_report`.)
- **Write** results: return a `pl.DataFrame` from the asset; the `io_manager` key
  (`PolarsIcebergIOManager`, configured in `definitions.py`) persists it to the target
  Iceberg table. `feedback_clusters` (§2) returns **two** DataFrames, one per `AssetOut`, since it is a
  `@multi_asset`. `feedback_cluster_run` and `feedback_cluster_candidate` are new Iceberg tables with the
  schemas in `feedback_ml_approach.md` §A (ERD: [`feedback_erd.md`](./feedback_erd.md) §5).
- **Getting the generated columns onto `afact_feedback_conversation`:** dbt owns that table, so the assets
  write per-stage Iceberg output tables (`feedback_summaries`, `feedback_embeddings`,
  `feedback_cluster_assignments`, `feedback_sentiment_assignments`) keyed by `feedback_conversation_pk`, and
  the dbt model left-joins them onto the conversation aggregate. Same pattern as rev. 3, one grain up — and
  because the target is a derived aggregate rather than a transactional fact, this join is a plain rebuild
  rather than an incremental `merge` into a fact with consumers.
- **Nothing writes to `tfact_feedback`.** There is no ML-owned column on it any more.

---

## 4. New dependencies (net-new to the repo)

Confirmed absent repo-wide today (no openai/anthropic/sentence-transformers/transformers/
torch/hdbscan/umap/faiss/qdrant/pgvector anywhere). Add to the **new project's**
`pyproject.toml` only (keep them out of the shared lib and other locations):

| Purpose | Dep | Notes |
|---|---|---|
| Embeddings (local, PII-safe) | `sentence-transformers` (pulls `torch`) | default; CPU ok at MVP. Heavy image — consider a CPU-only torch wheel. |
| Dim-reduction + clustering | `umap-learn`, `hdbscan` | or use in-stack `scikit-learn` `HDBSCAN`≥1.3 to avoid `hdbscan` compiled dep — decide on image build constraints. `scikit-learn` already proven in-stack. |
| Conversation summarization + LLM cluster labeling + (fallback) sentiment | Anthropic client (Claude Haiku/Sonnet class) | **Two different cost profiles:** labeling is one call per *cluster* (hundreds — trivial); summarization is one call per *multi-turn conversation* (`feedback_ml_approach.md` §A.1 — the one per-record LLM cost, low hundreds of dollars one-time for Zendesk, to be sample-measured). Batch both; cap and checkpoint the summarizer so a failed run does not re-pay for work already done. |
| PII redaction | `presidio-analyzer`, `presidio-anonymizer` (+ spaCy model) | precedent: OM profiler already runs Presidio recognizers. |

**Image-size caveat:** `torch` + spaCy make a large image. If that is a problem, the
fallback is a hosted embedding API behind the same resource interface (accepting the
PII-egress tradeoff called out in `feedback_ml_approach.md` §B) — but default to local.

---

## 5. Secrets — Vault `ConfigurableResource` (repo pattern)

API keys come from **HashiCorp Vault via a `ConfigurableResource`**, not `EnvVar`
(repo convention; `EnvVar` is not used for secrets here). Build an
`LLMClientFactory(ConfigurableResource)` holding `vault: Vault`, modeled exactly on
`packages/ol-orchestrate-lib/src/ol_orchestrate/resources/github.py`:

```python
class LLMClientFactory(ConfigurableResource):
    vault: Vault = Field(...)
    vault_mount_point: str = Field(default="secret-data")
    vault_secret_path: str = Field(
        default="pipelines/feedback-llm"
    )  # NEW Vault path to provision
    vault_secret_key: str = Field(default="api_key")
    _client: Anthropic | None = PrivateAttr(default=None)

    def get_client(self):
        if self._client is None:
            data = self.vault.client.secrets.kv.v1.read_secret(
                mount_point=self.vault_mount_point, path=self.vault_secret_path
            )
            self._client = Anthropic(api_key=data["data"][self.vault_secret_key])
        return self._client
```

Register in `Definitions.resources` alongside the `vault` resource
(`authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)` with the resilient-load fallback, as in
`student_risk_probability/definitions.py`). **Action item:** provision a Vault secret path
(e.g. `pipelines/feedback-llm`) for the LLM key. A local embedding model needs no secret.

---

## 6. Scheduling

Two options, both in use in the repo:
- **(recommend) Declarative automation:** put `automation_condition=upstream_or_code_changes()`
  (`ol_orchestrate.lib.automation_policies`) on the `feedback_summaries` asset so it re-runs
  when `int__feedback__conversation` refreshes or the code version changes. Downstream embed/cluster/
  label/sentiment assets chain off it. This is what `student_risk_probability` and the dbt
  assets use — no cron to maintain. **Caveat now that summarization costs money per conversation:** make the
  summarizer incremental on `feedback_conversation_pk` (only unsummarized or changed conversations), or an
  upstream refresh re-pays for the whole corpus.
- **Cron alternative:** wrap the assets in `define_asset_job(...)` + a
  `dg.ScheduleDefinition(cron_schedule="0 4 * * *", execution_timezone="Etc/UTC")` (pattern:
  `dg_projects/data_loading/.../schedules.py`) if a fixed cadence is preferred over
  data-driven triggering. MVP volume is ~198K Zendesk **conversations** (`feedback_ml_approach.md` §B.2),
  which runs comfortably in one nightly batch; only the first backfill is large, and it is bounded by the
  summarizer's throughput rather than the embedder's.

---

## 7. Build order (so the fact ships first)

1. dbt models (`feedback_zendesk_mvp_spec.md`) — `tfact_feedback` + dims +
   `int__feedback__conversation` + `afact_feedback_conversation` with its **lifecycle columns only**.
   **Ships and is useful with tag-seeded categories + CSAT-derived sentiment, no ML.**
2. Scaffold `dg_projects/feedback_clustering/`; add deps; provision Vault path.
3. `feedback_summaries` asset (multi-turn conversations only) — **sample-measure the cost first** (§4).
4. `feedback_embeddings` asset → vectors keyed by `feedback_conversation_pk`.
5. `feedback_clusters` asset (UMAP+HDBSCAN) → `feedback_cluster_run` + assignments.
6. `feedback_category_proposals` + `feedback_sentiment` assets → assignment tables.
7. dbt join of the stage outputs onto `afact_feedback_conversation`'s generated columns.
8. Human curation loop on `dim_feedback_category` (approve/merge proposed labels), and run promotion
   (candidate → live cluster assignment).

---

## 8. Explicit non-goals (MVP)

- No online/real-time embedding or serving (batch only).
- No dedicated vector DB (Iceberg `ARRAY<float>` column on the conversation fact; revisit at Phase 2/serving
  need).
- No GPU requirement (CPU batch at MVP scale; revisit at the full ~600K-conversation scale).
- No cross-source clustering until forum/tutor/ORA sources land (Phase 2).
- **No turn-level embeddings.** Conversations only; the turn grain in `tfact_feedback` keeps the option open
  if a retrieval use case ever needs it.
