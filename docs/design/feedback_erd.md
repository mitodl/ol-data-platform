# Feedback Aggregation — Entity-Relationship Diagrams

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-08-07 · Companion to [`feedback_dimensional_model.md`](./feedback_dimensional_model.md),
[`feedback_event_contract_spec.md`](./feedback_event_contract_spec.md) and
[`feedback_ml_approach.md`](./feedback_ml_approach.md).

Visual counterpart to the column lists in the specs above, so the schema is reviewable as a shape rather
than as prose. Also posted as an addendum on
[RFC mitodl/hq#12210](https://github.com/mitodl/hq/discussions/12210#discussioncomment-17935060).

Drawing the model surfaced three schema changes — two from review feedback on
[#2422](https://github.com/mitodl/ol-data-platform/pull/2422), one from the diagram itself. They are applied
in the diagrams below and in the companion specs; §5 records what changed and why.

**Reading the diagrams:** `||--o{` = required FK (never null). `|o--o{` = **nullable** FK — sparse
conformance is the flexibility mechanism (`feedback_dimensional_model.md` §2), so most of the star is
deliberately optional.

---

## 1. Target star schema — `tfact_feedback` and its dimensions

```mermaid
erDiagram
    dim_feedback_source   ||--o{ tfact_feedback : "feedback_source_fk"
    dim_date              ||--o{ tfact_feedback : "date_fk"
    dim_time              ||--o{ tfact_feedback : "time_fk"
    dim_feedback_category |o--o{ tfact_feedback : "category_fk (late-arriving)"
    dim_sentiment         |o--o{ tfact_feedback : "sentiment_fk (late-arriving)"
    dim_user              |o--o{ tfact_feedback : "user_fk (identity: highest-risk join)"
    dim_platform          |o--o{ tfact_feedback : "platform_fk"
    dim_course_run        |o--o{ tfact_feedback : "courserun_fk (course-scoped sources)"
    dim_course_content    |o--o{ tfact_feedback : "content_block_fk (NEW - what it is about)"
    dim_organization      |o--o{ tfact_feedback : "organization_fk"
    tfact_feedback        ||--o{ feedback_embeddings : "feedback_pk"

    tfact_feedback {
        varchar feedback_pk PK "surrogate_key(source_slug, source_record_ref) - stable across the data-bus migration"
        varchar feedback_source_fk FK "required"
        varchar user_fk FK "nullable - anonymous or unresolved requester"
        varchar platform_fk FK "nullable"
        varchar courserun_fk FK "nullable - Zendesk is not course-scoped"
        varchar content_block_fk FK "nullable - NEW, resolves subject_ref for edX blocks"
        varchar organization_fk FK "nullable"
        varchar category_fk FK "nullable at insert - ML or seed fills later"
        varchar sentiment_fk FK "nullable at insert - CSAT or model fills later"
        varchar date_fk FK "required"
        varchar time_fk FK "required"
        varchar conversation_id "degenerate - roll utterances up to a ticket or thread"
        varchar source_record_id "degenerate - source-native business key, idempotency key"
        varchar source_url "deep link back to origin"
        varchar subject_type "NEW - courseware_block, course_run, course, program, page_url, resource, unspecified"
        varchar subject_ref "NEW - source-native id of the thing the feedback is about"
        varchar subject_url "NEW - canonical or decoded deep link to that thing"
        varchar feedback_title "REDACTED"
        varchar feedback_text "REDACTED - raw text never lands in the fact"
        integer feedback_text_chars "pre-redaction length metric"
        varchar source_status "facet - Zendesk ticket_status"
        varchar source_priority "facet - Zendesk ticket_priority"
        varchar source_channel "facet - Zendesk source_channel, tutor agent, forum component"
        varchar source_brand "NEW facet - Zendesk brand_name (hq#12607)"
        varchar source_group "NEW facet - Zendesk group_name (hq#12607)"
        array source_tags "array of varchar - category seeds"
        varchar csat_score "explicit sentiment signal"
        timestamp feedback_occurred_at "source event time"
        timestamp feedback_ingested_at
    }

    dim_feedback_source {
        varchar feedback_source_pk PK "surrogate_key(source_slug)"
        varchar source_slug UK "zendesk, edx_forum, learn_ai_tutor, ora, edx_feedback_plugin"
        varchar source_name
        varchar source_medium "support_ticket, forum_post, chat, assessment, in_product"
        varchar source_audience_scope "operational, course, strategic"
        boolean is_course_scoped
    }

    dim_feedback_category {
        varchar feedback_category_pk PK "surrogate_key(category_slug)"
        varchar category_slug UK "stable machine key - survives relabelling"
        varchar category_label "LLM-proposed, human-approved"
        varchar category_parent_slug "optional hierarchy: cluster to category to theme"
        varchar category_status "proposed, approved, merged, deprecated"
        varchar category_source "seed, llm_discovered, manual"
        varchar cluster_run_id "NEW - provenance: which run proposed this category"
        timestamp first_seen_at
        timestamp updated_at
    }

    dim_sentiment {
        varchar sentiment_pk PK "surrogate_key(sentiment_slug)"
        varchar sentiment_slug UK "positive, neutral, negative"
        varchar polarity_score_bucket "coarse bucket for trend rollups"
    }
```

The three new dimensions are the only new dimensional work. `dim_user`, `dim_platform`, `dim_course_run`,
`dim_organization`, `dim_course_content` and `dim_date`/`dim_time` are reused as-is.

---

## 2. Common event contract → the fact

The contract is the durable artifact (RFC Option 3): every producer presents this shape at the
`stg__…__feedback` / `int__feedback__<source>` boundary, so the fact is indifferent to which pipe delivered
the row.

```mermaid
erDiagram
    feedback_event_contract ||--|| int__feedback__unioned : "union all + Presidio redaction"
    int__feedback__unioned  ||--|| tfact_feedback : "resolve conformed FKs, mint feedback_pk"

    feedback_event_contract {
        varchar source_slug "REQUIRED - maps to dim_feedback_source"
        timestamp occurred_at "REQUIRED - ISO8601 source event time"
        varchar source_record_ref "REQUIRED - stable source-native id, idempotency and business key"
        varchar text "REQUIRED - raw free text, redacted in-warehouse"
        varchar title "subject or heading"
        varchar conversation_ref "thread or ticket id"
        varchar subject_user_ref "global or openedx user id - NEVER a source row PK"
        varchar courserun_readable_id "course scope"
        varchar platform "platform readable id"
        varchar subject_type "NEW - what the feedback is ABOUT"
        varchar subject_ref "NEW - source-native id of that thing"
        varchar subject_url "NEW - canonical or decoded deep link"
        json source_metadata "extension point - tags, status, priority, channel, csat, brand, group"
    }
```

Per source:

| Source | `source_record_ref` | `subject_type` / `subject_ref` |
|---|---|---|
| Zendesk (agent/web) | `ticket_id` | `course_run` / course readable id from ticket metadata, where present |
| Zendesk (Appzi) | `ticket_id` | `page_url` / **decoded** Appzi URL (decoding is the adapter's job, §5a) |
| edX forum | `post_id` | `courseware_block` / usage key of the discussion block |
| Learn AI tutor | `thread_id` + `checkpoint_pk` | `course_run` / `courserun_readable_id` |
| ORA | `submission_uuid` | `courseware_block` / ORA block usage key |
| edX feedback plugin | plugin-native event id | `courseware_block` / block usage key |

---

## 3. ML sidecar — embeddings, cluster runs, categories

```mermaid
erDiagram
    tfact_feedback              ||--o{ feedback_embeddings : "feedback_pk"
    tfact_feedback              ||--o{ feedback_cluster_assignment : "feedback_pk"
    feedback_cluster_run        ||--o{ feedback_cluster_assignment : "cluster_run_id"
    feedback_cluster_run        |o--o{ dim_feedback_category : "cluster_run_id (provenance)"
    dim_feedback_category       |o--o{ tfact_feedback : "category_fk (late-arriving)"
    tfact_feedback              ||--o{ afact_feedback_cluster_daily : "aggregated"

    feedback_embeddings {
        varchar feedback_pk PK "part of compound key"
        varchar model_version PK "part of compound key - makes the model choice reversible"
        array vector "Iceberg ARRAY of float - StarRocks HNSW indexes this later, a load not a re-embed"
        integer vector_dim "Matryoshka sweep: 256, 512, 1024"
        varchar text_variant "raw or llm_normalized - the semantic-normalisation eval arm"
        timestamp embedded_at
    }

    feedback_cluster_run {
        varchar cluster_run_id PK
        varchar model_version FK "which embeddings this run consumed"
        varchar algorithm "umap+hdbscan"
        json run_params "min_cluster_size, n_neighbors - config not hardcoded"
        integer cluster_count
        integer noise_count "the one-off bucket"
        float silhouette "only if the algorithm produces it"
        varchar run_status "candidate, approved"
        timestamp run_at
    }

    feedback_cluster_assignment {
        varchar feedback_pk PK "part of compound key"
        varchar cluster_run_id PK "part of compound key"
        integer cluster_id "-1 = noise = one-off complaint"
        float cluster_probability "cohesion signal for ranking systemic issues"
    }

    afact_feedback_cluster_daily {
        varchar date_fk FK
        varchar feedback_source_fk FK
        varchar category_fk FK
        varchar sentiment_fk FK
        integer cluster_id
        integer feedback_count
        integer distinct_user_count
        float avg_csat_score
    }
```

`dim_feedback_category` is the *curated, stable* projection; `feedback_cluster_*` is the *churny* ML working
set. Only an approved run advances the dimension — that decoupling is what lets clustering re-run freely.

---

## 4. What Phase 1 (Zendesk MVP) actually builds

```mermaid
erDiagram
    dim_feedback_source   ||--o{ tfact_feedback : "feedback_source_fk = zendesk"
    dim_date              ||--o{ tfact_feedback : "date_fk"
    dim_time              ||--o{ tfact_feedback : "time_fk"
    dim_feedback_category |o--o{ tfact_feedback : "category_fk - seeded from ticket_tags"
    dim_sentiment         |o--o{ tfact_feedback : "sentiment_fk - from satisfaction_rating_score"
    dim_user              |o--o{ tfact_feedback : "user_fk - requester email, last-resort path"

    tfact_feedback {
        varchar feedback_pk PK "surrogate_key(zendesk, ticket_id)"
        varchar courserun_fk "NULL at MVP - Zendesk is not course-scoped"
        varchar platform_fk "NULL at MVP - but see the brand note in section 5b"
        varchar organization_fk "NULL at MVP - see the dim_organization key note in section 5d"
        varchar content_block_fk "NULL at MVP - arrives with the edX plugin source"
    }
```

~198K rows, one batch. No ML required for the fact to be useful.

Build path:

```mermaid
flowchart TD
    A["raw__thirdparty__zendesk_support__tickets<br/>(existing Airbyte)"] --> B["stg__zendesk__ticket<br/>(existing)"]
    B --> C["int__zendesk__ticket<br/>(existing - grain source, already carries brand_name + group_name)"]
    C --> D["int__feedback__zendesk<br/>(NEW - conform to the event contract)"]
    D --> E["int__feedback__unioned<br/>(NEW - union all sources + Presidio redaction)"]
    E --> F["tfact_feedback<br/>(NEW - resolve FKs, mint feedback_pk)"]
    F --> G["afact_feedback_cluster_daily<br/>(Phase 2)"]
    E -.->|Fenic, engine-external| H["feedback_embeddings<br/>feedback_cluster_run<br/>feedback_cluster_assignment"]
    H -.->|LLM label, human approve| I["dim_feedback_category"]
    I -.->|late-arriving update| F
    F2["forum / tutor / ORA / edX plugin<br/>(Phase 2 - additive CTEs)"] --> E
```

---

## 5. Schema deltas introduced with these diagrams

### 5a. "What is the feedback *about*?" — `subject_type` / `subject_ref` / `subject_url` + `content_block_fk`

Raised by @pdpinch on [#2422](https://github.com/mitodl/ol-data-platform/pull/2422#issuecomment-3157271372).
The contract had *who* said it, *when*, and *in what conversation*, but no unambiguous slot for what it
concerns — which is precisely the axis you aggregate on. Named cases: the edX feedback plugin captures the
courseware block id; some Zendesk tickets carry course metadata; Appzi-originated tickets capture the URL the
user was viewing (encoded).

Deliberately **not** a new `dim_feedback_subject`: the subject is polymorphic across dimensions that already
exist — a block is `dim_course_content`, a run is `dim_course_run`, a program is `dim_program`. A new dim
would shadow them. Instead:

- `subject_type` / `subject_ref` / `subject_url` are a degenerate triple on the contract and the fact — the
  universal fallback that always works, including for URLs and unresolvable references.
- Resolve to a conformed FK where one exists. `courserun_fk` already covers the course-run case; the only
  new conformed join needed is **`content_block_fk → dim_course_content.content_block_pk`**, the existing
  SCD for Open edX blocks (courses, chapters, subsections, problems, videos) that `tfact_problem_events` and
  `tfact_course_navigation_events` already use. edX-plugin and ORA feedback therefore lands in the courseware
  star with zero new dimensional work.
- **Appzi URL decoding belongs in `int__feedback__zendesk`**, populating `subject_url`/`subject_ref` — not in
  every consumer's query.

### 5b. Zendesk brand and group as first-class facets

Raised by @pdpinch citing [hq#12607](https://github.com/mitodl/hq/issues/12607). Cheap:
`int__zendesk__ticket` already carries `brand_name` and `group_name` (joined from `stg__zendesk__brand` /
`stg__zendesk__group`). Carry both in `source_metadata` at the contract boundary and project them to
`source_brand` / `source_group` facet columns on the fact, so Superset filters on a plain varchar rather than
a JSON extract.

Worth evaluating during implementation: Zendesk **brand** is effectively "which help centre / product the
ticket came in through", the closest thing Zendesk has to a platform. If brands map cleanly onto
`dim_platform`, `platform_fk` need not be null for Zendesk after all.

### 5c. Sidecar grain split

`feedback_ml_approach.md` §A put vectors and cluster assignments in one `feedback_embeddings` table, but they
have **different grains**: a vector is one per `(feedback_pk, model_version)`; a cluster assignment is one per
`(feedback_pk, cluster_run_id)`. Re-clustering against an unchanged model would rewrite or duplicate vector
rows — losing the "re-clustering never touches the durable artifact" property the design is buying. Split
into `feedback_embeddings` / `feedback_cluster_run` / `feedback_cluster_assignment` as drawn in §3.

Two consequences:

- `dim_feedback_category` gains a `cluster_run_id` provenance column — trace a category back to the run that
  proposed it.
- The fact's `embedding_id` column is **dropped**. The sidecar is keyed by `feedback_pk`, so `embedding_id`
  adds no reachability and would mean writing to the fact when embeddings land.

### 5d. Known-null FK, flagged rather than discovered later

`dim_organization.organization_pk = generate_surrogate_key(['platform', 'source_id'])`
(`src/ol_dbt/models/dimensional/dim_organization.sql:38`). Zendesk has neither, so `organization_fk` cannot
resolve for Zendesk tickets under the current key. It stays null at MVP and the Zendesk organisation/group is
carried as a facet instead. Either that is acceptable, or `dim_organization` needs a Zendesk-aware alternate
key — a decision worth making explicitly rather than by omission.

---

Unchanged by all of the above: the business-key strategy
(`feedback_pk = generate_surrogate_key([source_slug, source_record_ref])`), so the interim → data-bus
migration is still "swap source + backfill + parity".
