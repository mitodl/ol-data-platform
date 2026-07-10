# Feedback Aggregation — Dagster ML Asset Spec (MVP)

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 · Companion to [`feedback_zendesk_mvp_spec.md`](./feedback_zendesk_mvp_spec.md)
and [`feedback_ml_approach.md`](./feedback_ml_approach.md)

The scheduled batch job that turns the redacted feedback fact into embeddings, clusters,
LLM-proposed categories, and sentiment. Grounded in the existing repo orchestration
patterns. **The fact ships without this asset**; this is purely additive (fills
`embedding_id`, `category_fk`, `sentiment_fk`, and the `feedback_embeddings` sidecar).

> **REVISED 2026-07-10 — see [`adr_embedding_compute_strategy.md`](./adr_embedding_compute_strategy.md).**
> Because production runs **Starburst Galaxy** (Trino), the **embedding** and **model-based
> sentiment** stages move OUT of this Python asset and INTO `trino_only` dbt models using
> Starburst AI functions (`generate_embedding` → Iceberg `ARRAY(DOUBLE)`; `analyze_sentiment`).
> This asset then shrinks to **only what SQL cannot do: clustering + LLM cluster-labeling** —
> read pre-computed vectors from the Iceberg sidecar → UMAP+HDBSCAN → LLM-label clusters. No
> embedding model in the image (no torch/sentence-transformers), so it stays a small
> sklearn/UMAP CPU asset still cloned from `student_risk_probability`. §2–§6 below describe the
> full-fat Python version; treat the embed/sentiment assets there as the fallback used only if
> the Starburst AI preview is not adopted (ADR "Alternatives").

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
[dbt] int__feedback__unioned   (redacted text + feedback_pk)      ← upstream AssetKey dep
   │
   ▼
feedback_embeddings            @asset  → writes feedback_embeddings (feedback_pk, vector,
   │                                     model_version)  [embedding computed ONCE per model_version]
   ▼
feedback_clusters              @asset  → adds cluster_id, cluster_probability, cluster_run_id
   │                                     to feedback_embeddings (UMAP→HDBSCAN)
   ├─────────────► feedback_category_proposals  @asset → LLM-labels clusters → dim_feedback_category
   │                                                     (category_source='llm_discovered', status='proposed')
   └─────────────► feedback_sentiment           @asset → sentiment per feedback_pk (explicit + kNN/classifier)
                                                          → writes sentiment_fk assignments
   ▼
tfact_feedback late-arriving update  ← category_fk / sentiment_fk / embedding_id upsert by feedback_pk
```

- **Redaction placement (design §7 / MVP spec §3):** the `feedback_embeddings` asset does
  the Presidio redaction on its input (or reads an already-redacted `int__feedback__unioned`
  column). Recommend redaction lives here in Python since Presidio is Python — the fact then
  reads redacted text produced by this asset. **Decision for implementation:** either (a)
  the asset writes `text_redacted` back to a table the fact reads, or (b) `int__feedback__unioned`
  is itself a Python-materialized step. Pick (a) to keep dbt pure-SQL; documented as the one
  interleave point.
- **`code_version`** on each asset (as `student_risk_probability` does) so a helper/model
  change re-triggers via declarative automation.
- **`pool=`** set per asset (concurrency governed by the production instance pool config —
  slot limits live in the Dagster UI, not repo config).

---

## 3. Reading & writing data (exact repo helpers)

- **Read** the dbt fact/union: `get_dbt_model_as_dataframe(database_name="ol_warehouse_production_<schema>",
  table_name="int__feedback__unioned")` → Polars. (Same call `student_risk_probability`
  uses against `reporting.cheating_detection_report`.)
- **Write** results: return a `pl.DataFrame` from the asset; the `io_manager` key
  (`PolarsIcebergIOManager`, configured in `definitions.py`) persists it to the target
  Iceberg table. `feedback_embeddings` is a new Iceberg table with the schema in
  `feedback_ml_approach.md` §B/§C.
- **Late-arriving `category_fk`/`sentiment_fk` back onto `tfact_feedback`:** because dbt owns
  `tfact_feedback`, the cleanest MVP path is an incremental dbt model / `merge` keyed by
  `feedback_pk` that reads the assignment table this asset writes — not a direct Python
  mutation of the fact. So the asset writes `feedback_category_assignments` /
  `feedback_sentiment_assignments` Iceberg tables, and a dbt step joins them onto the fact.
  This keeps the fact dbt-owned and the ML output append-only.

---

## 4. New dependencies (net-new to the repo)

Confirmed absent repo-wide today (no openai/anthropic/sentence-transformers/transformers/
torch/hdbscan/umap/faiss/qdrant/pgvector anywhere). Add to the **new project's**
`pyproject.toml` only (keep them out of the shared lib and other locations):

| Purpose | Dep | Notes |
|---|---|---|
| Embeddings (local, PII-safe) | `sentence-transformers` (pulls `torch`) | default; CPU ok at MVP. Heavy image — consider a CPU-only torch wheel. |
| Dim-reduction + clustering | `umap-learn`, `hdbscan` | or use in-stack `scikit-learn` `HDBSCAN`≥1.3 to avoid `hdbscan` compiled dep — decide on image build constraints. `scikit-learn` already proven in-stack. |
| LLM cluster labeling + (fallback) sentiment | Anthropic client (Claude Haiku/Sonnet class) | one call per *cluster*, not per record — cheap batch. |
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
    vault_secret_path: str = Field(default="pipelines/feedback-llm")  # NEW Vault path to provision
    vault_secret_key: str = Field(default="api_key")
    _client = PrivateAttr(default=None)

    def get_client(self):
        if self._client is None:
            data = self.vault.client.secrets.kv.v1.read_secret(
                mount_point=self.vault_mount_point, path=self.vault_secret_path)
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
  (`ol_orchestrate.lib.automation_policies`) on the `feedback_embeddings` asset so it re-runs
  when `int__feedback__unioned` refreshes or the code version changes. Downstream cluster/
  label/sentiment assets chain off it. This is what `student_risk_probability` and the dbt
  assets use — no cron to maintain.
- **Cron alternative:** wrap the assets in `define_asset_job(...)` + a
  `dg.ScheduleDefinition(cron_schedule="0 4 * * *", execution_timezone="Etc/UTC")` (pattern:
  `dg_projects/data_loading/.../schedules.py`) if a fixed cadence is preferred over
  data-driven triggering. MVP volume (~198K) runs comfortably in one nightly batch.

---

## 7. Build order (so the fact ships first)

1. dbt models (`feedback_zendesk_mvp_spec.md`) — `tfact_feedback` + dims. **Ships and is
   useful with tag-seeded categories + CSAT-derived sentiment, no ML.**
2. Scaffold `dg_projects/feedback_clustering/`; add deps; provision Vault path.
3. `feedback_embeddings` asset (embed + redact) → sidecar table.
4. `feedback_clusters` asset (UMAP+HDBSCAN).
5. `feedback_category_proposals` + `feedback_sentiment` assets → assignment tables.
6. dbt late-arriving join of assignment tables onto `tfact_feedback` (`category_fk`/`sentiment_fk`).
7. Human curation loop on `dim_feedback_category` (approve/merge proposed labels).

---

## 8. Explicit non-goals (MVP)

- No online/real-time embedding or serving (batch only).
- No dedicated vector DB (Iceberg `ARRAY<float>` sidecar; revisit at Phase 2/serving need).
- No GPU requirement (CPU batch at MVP scale; revisit at full 1.18M-row scale).
- No cross-source clustering until forum/tutor/ORA sources land (Phase 2).
