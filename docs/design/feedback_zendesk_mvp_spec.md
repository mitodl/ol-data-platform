# Feedback Aggregation — Zendesk MVP Implementation Spec

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 · Companion to [`feedback_dimensional_model.md`](./feedback_dimensional_model.md)
and [`feedback_ml_approach.md`](./feedback_ml_approach.md)

Concrete, build-ready spec for the first slice: **Zendesk tickets → `tfact_feedback`**,
serving support + engineering. Grounded in the actual repo models/columns. Everything here
is Phase-1 MVP scope (design §9); forum/tutor/ORA and the data-bus migration are later.

Convention baseline: mirrors `tfact_discussion_events` (the existing multi-source fact) —
same timestamp macros, same FK-by-lookup-join pattern, same `_dim__models.yml` contract
style. Divergence from precedent is called out explicitly where intentional.

---

## 1. dbt model DAG (MVP)

```
raw__thirdparty__zendesk_support__tickets        (existing Airbyte raw, in _zendesk__sources.yml)
  → stg__zendesk__ticket        (existing)
  → int__zendesk__ticket        (existing, one row per ticket — grain source)
      │
      ├─ stg__zendesk__user     (existing — for requester email → dim_user)
      │
      ▼
  int__feedback__zendesk        (NEW — conform ticket to the common event contract §5 of design doc)
      ▼
  int__feedback__unioned        (NEW — UNION of all sources; MVP = zendesk only; + PII redaction §7)
      ▼
  tfact_feedback                (NEW — resolve conformed FKs, generate feedback_pk)

  dim_feedback_source           (NEW — seed rows, static/near-static)
  dim_feedback_category         (NEW — seeded from ticket_tags + group_name; LLM-labeled later)
  dim_sentiment                 (NEW — seeded static buckets)
```

The two-hop `int__feedback__zendesk → int__feedback__unioned` looks redundant at MVP (one
source) but is deliberate: `__unioned` is where new sources plug in without touching the
fact, and where redaction happens once for all sources. Keeping it from day one makes
Phase 2 a pure additive change.

---

## 2. `int__feedback__zendesk` (NEW) — conform Zendesk to the common contract

Reads `int__zendesk__ticket` (grain: one row per ticket, `ticket_id` unique+not_null),
left-joined to `stg__zendesk__ticket` (for `ticket_requester_user_id`) → `stg__zendesk__user`
(for `user_email`, needed because `int__zendesk__ticket` only carries `ticket_requester`
as a *name*, not an id/email — confirmed in source inventory).

Output columns = the common feedback event contract (design §5), source-typed:

| Contract field | Zendesk expression |
|---|---|
| `source_slug` | literal `'zendesk'` |
| `occurred_at` | `ticket_created_at` (ISO8601) |
| `subject_user_ref` | `user_email` from `stg__zendesk__user` via `ticket_requester_user_id` (last-resort identity path, see §4) |
| `courserun_readable_id` | `null` (Zendesk is not course-scoped) |
| `platform` | `null` |
| `conversation_ref` | `ticket_id` (thread/ticket id) |
| `source_record_ref` | `ticket_id` (**idempotency + business key**, §6 of design) |
| `title` | `ticket_subject` |
| `text` | `ticket_description` (= first comment body) |
| `source_metadata` | struct/JSON: `ticket_tags`, `ticket_status`, `ticket_priority`, `ticket_source_channel`, `ticket_satisfaction_rating_score`, `ticket_satisfaction_rating_comment`, `group_name`, `organization_name`, `ticket_api_url` |

Notes:
- **`ticket_description` = first comment only** (confirmed). That is the correct MVP grain
  per design §1 (Zendesk row = first comment). Full-thread text via
  `int__zendesk__ticket_comment` is a Phase-2 option, not MVP.
- Carry `ticket_api_url` through as the `source_url` deep-link.
- Do **not** redact here — redaction is centralized in `int__feedback__unioned` (§3).

---

## 3. `int__feedback__unioned` (NEW) — union + redact

- **Union** all per-source `int__feedback__<source>` models into the single common shape.
  MVP: just `int__feedback__zendesk`. The model is written as an explicit `union all` of
  CTEs (mirrors `tfact_discussion_events`' per-source CTE style) so adding forum/tutor is a
  new CTE + one `union all` line.
- **PII redaction (design §7, mandatory pre-embed):** apply the Presidio-based masking to
  `title` and `text` here, producing `title_redacted` / `text_redacted`. Raw `title`/`text`
  do **not** propagate past this model — only redacted text flows to `tfact_feedback` and
  the embedding store. Raw text remains available upstream in `stg`/`int__zendesk` under
  existing PII classification + Lakekeeper/Cedar authz.
  - Implementation note: Presidio is Python, not SQL. Two viable placements — (a) a Python
    Dagster asset that materializes the redacted column between `int__feedback__unioned`
    and the fact, or (b) a dbt Python model if the warehouse adapter supports it. **Recommend
    (a)** for MVP: the same batch asset that embeds also redacts, single Python surface,
    and it keeps the dbt layer pure-SQL (repo convention). Revisit if a SQL-native masking
    macro is preferred. This is the one place the spec's dbt DAG and the Dagster asset graph
    interleave — see §7.
- Carry `feedback_text_chars = length(text)` (pre-redaction length metric, design §2) for
  sizing/analytics; computing length before masking is fine (no PII in an integer).

---

## 4. `tfact_feedback` (NEW) — the fact

Mirrors `tfact_discussion_events` conventions. Reads `int__feedback__unioned`.

**Grain:** one row per atomic feedback utterance. `feedback_pk` unique.

**Surrogate + FK resolution:**
```sql
-- surrogate business key (DIVERGES from precedent — see note)
{{ dbt_utils.generate_surrogate_key(['source_slug', 'source_record_ref']) }} as feedback_pk

-- conformed FK: source
{{ dbt_utils.generate_surrogate_key(['source_slug']) }} as feedback_source_fk

-- conformed FK: user (nullable). Zendesk = last-resort email path.
users.user_pk as user_fk
...
left join dim_user as users
    on lower(unioned.subject_user_ref) = users.email   -- dim_user.user_pk = surrogate_key(lower(email))

-- conformed FKs not populated for Zendesk (nullable, correct):
-- courserun_fk, platform_fk  -> null at MVP
-- organization_fk -> resolve from source_metadata.organization_name if/when a
--    dim_organization join key exists; null-tolerant otherwise

-- late-arriving, null at insert, updated by ML asset (design §4a/§4b):
cast(null as varchar) as category_fk
cast(null as varchar) as sentiment_fk

-- time
{{ iso8601_to_time_key('occurred_at') }} as time_fk
{{ iso8601_to_date_key('occurred_at') }} as date_fk
```

**Identity resolution — the highest-risk join (design §3, RFC Consequences).** Zendesk has
no openedx user id, so `user_fk` resolves via **email → `dim_user.email`** (which is how
`dim_user.user_pk` itself is generated: `generate_surrogate_key(['lower(email)'])`). This is
the last-resort path and shares the failure class of the open p0 `dim_user` NULL-email
identity-collapse bug (`tk-re-derive-identity-conformed-dimension-joins-pos-b7ca16`). Guard:
- Never key `feedback_pk` off the resolved `user_fk` (it keys off `source_record_ref`), so a
  bad identity join can never collapse or duplicate the fact grain.
- `user_fk` stays **nullable**; an unresolved requester email = null `user_fk`, not a wrong
  join. Do not coalesce to a sentinel.
- Re-run `tk-...-b7ca16` before enabling any cross-source identity rollups on this fact.

**Divergence from precedent (intentional):** `tfact_chatbot_events`/`tfact_discussion_events`
mint no `*_pk` and rely on a model-level `expect_compound_columns_to_be_unique` test. This
fact mints an explicit `feedback_pk` from the stable source business key because (1) the
migration strategy (design §6) requires the same PK to regenerate identically across the
interim→data-bus source swap, and (2) `category_fk`/`sentiment_fk` are late-arriving updates
that need a stable row key to target. We keep the compound-uniqueness test *as well* (§8).

**Output columns** (fact): `feedback_pk`, `feedback_source_fk`, `user_fk`, `courserun_fk`,
`platform_fk`, `organization_fk`, `category_fk`, `sentiment_fk`, `time_fk`, `date_fk`,
`conversation_id` (=`conversation_ref`), `source_record_id` (=`source_record_ref`),
`source_url`, `feedback_title` (redacted), `feedback_text` (redacted), `feedback_text_chars`,
`embedding_id` (nullable), `source_status`, `source_priority`, `source_tags`,
`source_channel`, `csat_score`, `feedback_occurred_at`, `feedback_ingested_at`.

---

## 5. New dimensions (MVP)

### `dim_feedback_source` — static seed
Rows for MVP: one, `zendesk`. Columns per design §4c
(`feedback_source_pk = generate_surrogate_key(['source_slug'])`, `source_slug`, `source_name`,
`source_medium='support_ticket'`, `source_audience_scope='operational'`, `is_course_scoped=false`).
Implement as a dbt seed (`seeds/`) or a small `select ... union all` model — recommend a
seed CSV since the set is tiny and hand-curated.

### `dim_sentiment` — static seed
Rows: `positive`, `neutral`, `negative` (design §4b), `sentiment_pk = generate_surrogate_key(['sentiment_slug'])`,
`polarity_score_bucket`. dbt seed CSV.

### `dim_feedback_category` — seeded, then ML-curated
- **MVP seed (no ML):** distinct `ticket_tags` (~2,354) + `group_name` from
  `int__zendesk__ticket`, materialized as `category_source='seed'`,
  `category_status='proposed'`, `category_slug = generate_surrogate_key([slugified tag])`.
  A dbt model that unnests `ticket_tags` and selects distinct, plus group names.
- **ML curation (later, per `feedback_ml_approach.md` §D):** LLM-labeled clusters upsert
  `category_source='llm_discovered'` rows; humans flip `category_status` to `approved`.
- SCD-lite: relabel changes `category_label`, never `category_slug`.

---

## 6. Sentiment & category assignment at MVP

- **Sentiment (`sentiment_fk`):** MVP can populate a *coarse* sentiment immediately from the
  explicit signal with **no model**: map `ticket_satisfaction_rating_score`
  (`'good'`→positive, `'bad'`→negative, `'offered'`/null→neutral/unknown) → `dim_sentiment`.
  The model-based sentiment (`feedback_ml_approach.md` §E) upgrades the null/`offered` rows
  later. This gives a working sentiment facet on day one for the rated subset.
- **Category (`category_fk`):** MVP can assign the tag-seed category by mapping a ticket's
  dominant `ticket_tag` → its seed `category_slug`. Cluster-based reassignment comes with the
  ML asset. Unassigned = null (queryable).

Both are late-arriving updates, so the fact builds and is useful before the ML asset exists.

---

## 7. Dagster asset (MVP) — SEE `feedback_dagster_asset_spec.md`

The scheduled batch asset (pull → redact → embed → cluster → LLM-label → write
category/sentiment back) is specified separately once the orchestration layout is confirmed.
The dbt models above are independently buildable and testable *without* the ML asset — the
ML asset only fills `embedding_id`, `category_fk`, `sentiment_fk`, and the `feedback_embeddings`
sidecar. This ordering lets the fact ship first.

---

## 8. Tests / contract (`_dim__models.yml` entries)

Mirror the `tfact_discussion_events` yml style:
- Per-column `not_null` on: `feedback_pk`, `feedback_source_fk`, `source_record_id`,
  `feedback_occurred_at`, `time_fk`, `date_fk`.
- `unique` on `feedback_pk`.
- Nullable (description-only, no not_null): `user_fk`, `courserun_fk`, `platform_fk`,
  `organization_fk`, `category_fk`, `sentiment_fk`, `embedding_id`.
- Model-level `dbt_expectations.expect_compound_columns_to_be_unique` on
  `['feedback_source_fk', 'source_record_id']` (belt-and-suspenders alongside the `feedback_pk`
  unique test — matches the precedent's compound-uniqueness convention; both columns exist on the
  fact, and `feedback_source_fk = generate_surrogate_key([source_slug])` so this is the same
  business grain as `[source_slug, source_record_ref]`).
- `relationships` tests from each `*_fk` to its dim PK (richer-contract style, as
  `dim_course_run` does).
- New dims get their own entries; `dim_feedback_category` gets a `unique` on `category_slug`.

---

## 9. Build & verify path (local)

Per repo convention (`ol-dbt` CLI, DuckDB-over-Iceberg local): after writing models, run
`local register` + a targeted `dbt build --select +tfact_feedback` to validate the fact and
its upstreams compile and pass tests against live Iceberg data. Expected MVP volume ~198K
rows, single batch (design §9). Validate: `feedback_pk` uniqueness, null-`user_fk` rate
(sanity-check identity resolution isn't silently collapsing), and row count vs.
`int__zendesk__ticket`.

---

## 10. Scope boundary (what this MVP does NOT do)

- No forum/tutor/ORA sources (Phase 2 — additive CTEs in `int__feedback__unioned`).
- No `afact_feedback_cluster_daily` aggregate (Phase 2).
- No data-bus/analytics-api ingress (Phase 3, gated on the write path — RFC Open Questions).
- No embedding/clustering *required* for the fact to be useful (ML asset is additive).
- No cross-source identity rollups until `tk-...-b7ca16` is re-derived.
