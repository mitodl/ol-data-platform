# Feedback Aggregation — Zendesk MVP Implementation Spec

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-08-10 (rev. 3 — conversation-grain analysis fact) · Companion to
[`feedback_dimensional_model.md`](./feedback_dimensional_model.md),
[`feedback_erd.md`](./feedback_erd.md) and [`feedback_ml_approach.md`](./feedback_ml_approach.md)

Concrete, build-ready spec for the first slice: **Zendesk → `tfact_feedback`**,
serving support + engineering. Grounded in the actual repo models/columns. Everything here
is Phase-1 MVP scope (design §9); forum/tutor/ORA and the data-bus migration are later.

Convention baseline: mirrors `tfact_discussion_events` (the existing multi-source fact) —
same timestamp macros, same FK-by-lookup-join pattern, same `_dim__models.yml` contract
style. Divergence from precedent is called out explicitly where intentional.

> **rev. 2 changes the grain source, and it stands.** Zendesk is modelled at **comment (turn) grain**, not
> ticket grain (design §1). The adapter reads `int__zendesk__ticket_comment` — verified to exist at one row
> per comment, carrying `ticket_id` and `comment_plain_body` — filters to public requester-authored comments,
> and joins `int__zendesk__ticket` for conversation-level attributes. `source_record_ref` is `comment_id`;
> `conversation_ref` is `ticket_id`.

> **rev. 3 changes where the models write, not where the turns come from.** `category_fk`/`sentiment_fk`
> leave `tfact_feedback` (§4) and join the summary, embedding and cluster columns on
> `afact_feedback_conversation` (§5), which becomes the analysis fact. A new
> `int__feedback__conversation` assembles each ticket's kept turns for the ML batch. The Zendesk sourcing,
> the filter, the business key and the turn grain are all unchanged.

---

## 0. Prerequisites (must land before or with the adapter)

| Item | What | Why |
|---|---|---|
| `comment_author_user_id` on `int__zendesk__ticket_comment` | it exists in `stg__zendesk__ticket_comment` but the int model exposes only `comment_author` (a *name*) | needed to classify requester-vs-agent turns **and** to resolve `user_fk`. Blocking. |
| Volume measurement | count public, requester-authored comments per ticket, and the **multi-turn share** | sizes `tfact_feedback`, and the multi-turn share drives the summarization budget (`feedback_ml_approach.md` §A.1). The embedding budget is back to the ~198K *conversation* count under rev. 3 |
| `ticket_metrics` Airbyte stream | not currently synced (see §5a of the design doc) | duration measures on `afact_feedback_conversation`. **Non-blocking** — that table ships without them |

---

## 1. dbt model DAG (MVP)

```
raw__thirdparty__zendesk_support__tickets          (existing Airbyte raw, in _zendesk__sources.yml)
raw__thirdparty__zendesk_support__ticket_comments  (existing Airbyte raw)
  → stg__zendesk__ticket          (existing)
  → stg__zendesk__ticket_comment  (existing — carries comment_author_user_id)
      │
      ├─ int__zendesk__ticket_comment  (existing, one row per comment — GRAIN SOURCE; needs §0 change)
      ├─ int__zendesk__ticket          (existing, one row per ticket — conversation attributes)
      ├─ stg__zendesk__user            (existing — for author email → dim_user)
      │
      ▼
  int__feedback__zendesk        (NEW — conform comments to the common event contract; filter §2)
      ▼
  int__feedback__unioned        (NEW — UNION of all sources; MVP = zendesk only; + PII redaction §7)
      ▼
  tfact_feedback                (NEW — resolve conformed FKs, generate feedback_pk; INSERT-ONLY)
      ▼
  bridge_feedback_tag           (NEW — explode ticket tags → dim_feedback_tag)

  int__feedback__conversation   (NEW — assemble a ticket's kept turns, ordered by turn_index; ML input)
      ▼
  afact_feedback_conversation   (NEW — the analysis fact: lifecycle from int__zendesk__ticket + turn
                                 aggregates, plus the generated summary/embedding/sentiment/category/cluster)

  dim_feedback_source           (NEW — seed rows, static/near-static)
  dim_feedback_channel          (NEW — seed rows, conformed channel value set)
  dim_feedback_category         (NEW — seeded from ticket_tags + group_name; LLM-labeled later)
  dim_sentiment                 (NEW — seeded static buckets)
  dim_feedback_tag              (NEW — distinct source-scoped tags)
```

The two-hop `int__feedback__zendesk → int__feedback__unioned` looks redundant at MVP (one
source) but is deliberate: `__unioned` is where new sources plug in without touching the
fact, and where redaction happens once for all sources. Keeping it from day one makes
Phase 2 a pure additive change.

---

## 2. `int__feedback__zendesk` (NEW) — conform Zendesk comments to the common contract

Reads `int__zendesk__ticket_comment` (grain: one row per comment), joined to
`int__zendesk__ticket` (for ticket-level attributes and the requester id) and
`stg__zendesk__user` (for the author's `user_email`).

**Filter — keep only turns where the author is the person giving feedback:**

```sql
where comment.comment_is_public = true          -- drop internal agent notes
  and comment.comment_author_user_id = ticket.ticket_requester_user_id   -- drop agent replies
```

This is the same rule the other sources follow (tutor keeps `human_message` and drops `agent_message`;
forum keeps `*.created`), stated per source rather than as a Zendesk quirk — design §1.

`turn_index` = `row_number() over (partition by ticket_id order by comment_created_at)` over the *kept*
comments; `is_conversation_opening` = `turn_index = 1`.

Output columns = the common feedback event contract (design §5), source-typed:

| Contract field | Zendesk expression | From |
|---|---|---|
| `source_slug` | literal `'zendesk'` | — |
| `occurred_at` | `comment_created_at` (ISO8601) | comment |
| `source_record_ref` | `comment_id` (**idempotency + business key**, design §6) | comment |
| `text` | `comment_plain_body` | comment |
| `channel_slug` | `comment_source_channel` mapped to the conformed value set (design §4d) | comment |
| `title` | `ticket_subject` (constant across a ticket's turns) | ticket |
| `conversation_ref` | `ticket_id` | comment |
| `turn_index` | `row_number()` over kept comments, partitioned by `ticket_id`, ordered by `comment_created_at` | derived |
| `subject_user_ref` | `user_email` from `stg__zendesk__user` via `comment_author_user_id` (last-resort identity path, §4) | comment |
| `courserun_readable_id` | `null` (Zendesk is not course-scoped) | — |
| `platform` | `null` (but see the brand note below) | — |
| `subject_type` | `'page_url'` for Appzi-originated tickets; `'course_run'` where ticket metadata names a course; else `'unspecified'` | ticket |
| `subject_ref` | decoded Appzi URL / course readable id / null | ticket |
| `subject_url` | decoded Appzi URL, else null | ticket |
| `explicit_rating` | `ticket_satisfaction_rating_score` | ticket |
| `created_at` | `comment_created_at` | comment |
| `updated_at` | `ticket_updated_at` | ticket |
| `source_metadata` | JSON string via `json_format(...)`: `ticket_status`, `ticket_priority`, `ticket_due_at`, `brand_name`, `group_name`, `organization_name`, `ticket_satisfaction_rating_comment`, `custom_fields` | ticket |

Tags are **not** in `source_metadata` — `ticket_tags` feeds `bridge_feedback_tag` / `dim_feedback_tag`
(design §4e), so they are carried alongside for the bridge model to explode.

Notes:
- **Grain (rev. 2):** one row per **public, requester-authored comment**, not per ticket. The previous
  `ticket_description` (first comment only) view is recoverable as `where is_conversation_opening`.
- Carry `ticket_api_url` through as the `source_url` deep-link. (Per-comment deep links are not exposed by
  the connector; the ticket URL is the best available and is stable per conversation.)
- Do **not** redact here — redaction is centralized in `int__feedback__unioned` (§3).
- **Ticket-level fields repeat across a ticket's turns** (`ticket_subject`, status, priority, rating). That is
  expected at turn grain and is why the conversation-level view lives in `afact_feedback_conversation`
  (design §5a) rather than being the only place these are queryable.
- **Brand & group** (@pdpinch, [hq#12607](https://github.com/mitodl/hq/issues/12607)): `int__zendesk__ticket`
  already carries `brand_name` and `group_name` (joined from `stg__zendesk__brand` / `stg__zendesk__group`),
  so this is a pass-through. **rev. 2:** they ride in `source_metadata` and are read back with
  `json_query_string` — the dedicated `source_brand`/`source_group` facet columns proposed in rev. 1 are
  withdrawn, because they are Zendesk-only and fail the design §0 conformance rule. Both fields remain
  present and filterable. **Evaluate during implementation:** Zendesk *brand* is effectively "which help
  centre/product the ticket came in through" — the closest thing Zendesk has to a platform. If brands map
  cleanly onto `dim_platform`, `platform_fk` need not be null for Zendesk after all.
- **Channel** is the one Zendesk facet that *is* conformed (design §4d): `ticket_source_channel` /
  `comment_source_channel` map onto the shared `channel_slug` value set. Confirm the actual Zendesk channel
  values against that set when seeding.
- **Subject** (design §2a): the Appzi URL is **encoded in the source** — decode it *here*, in the adapter, not
  in consumers. Where a ticket's metadata names a course, emit `subject_type='course_run'` and let the fact
  resolve `courserun_fk`. `content_block_fk` is not populated by Zendesk; it arrives with the edX plugin
  source in Phase 2.

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

**Grain:** one row per atomic feedback utterance = one **turn**. `feedback_pk` unique.

**Surrogate + FK resolution:**
```sql
-- surrogate business key (DIVERGES from precedent — see note)
-- rev. 2: source_record_ref is the TURN id (comment_id), not ticket_id
{{ dbt_utils.generate_surrogate_key(['source_slug', 'source_record_ref']) }} as feedback_pk

-- conformed FK: source
{{ dbt_utils.generate_surrogate_key(['source_slug']) }} as feedback_source_fk

-- conformed FK: channel (design §4d) — required, every source has one
{{ dbt_utils.generate_surrogate_key(['channel_slug']) }} as feedback_channel_fk

-- conformed FK: user (nullable). Zendesk = last-resort email path, resolved from the
-- COMMENT author (not the ticket requester) now that the grain is per turn.
users.user_pk as user_fk
...
left join dim_user as users
    on lower(unioned.subject_user_ref) = users.email   -- dim_user.user_pk = surrogate_key(lower(email))

-- conformed FKs not populated for Zendesk (nullable, correct):
-- courserun_fk    -> null at MVP unless subject_type='course_run' resolves a readable id
-- content_block_fk -> null at MVP (arrives with the edX plugin source, design §2a)
-- platform_fk     -> null at MVP; re-evaluate once brand_name is landed (see §2 notes)
-- organization_fk -> NULL at MVP, and structurally so: dim_organization.organization_pk =
--    generate_surrogate_key(['platform','source_id']) and Zendesk supplies neither
--    (dim_organization.sql:38). The Zendesk org/group rides in source_metadata instead.
--    Design §2b records this as an explicit decision, not an oversight.

-- rev. 3: NO category_fk / sentiment_fk here. They are model-derived and live on
-- afact_feedback_conversation (§5). With them gone this fact has no post-insert write path.

-- role-playing date/time (design §2d) — bare date_fk/time_fk are renamed because the
-- fact now carries three timestamps and the unqualified name would be ambiguous
{{ iso8601_to_time_key('occurred_at') }} as occurred_time_fk
{{ iso8601_to_date_key('occurred_at') }} as occurred_date_fk
{{ iso8601_to_time_key('created_at') }}  as created_time_fk
{{ iso8601_to_date_key('created_at') }}  as created_date_fk
{{ iso8601_to_time_key('updated_at') }}  as updated_time_fk
{{ iso8601_to_date_key('updated_at') }}  as updated_date_fk
```

**Identity resolution — the highest-risk join (design §3, RFC Consequences).** Zendesk has
no openedx user id, so `user_fk` resolves via **email → `dim_user.email`** (which is how
`dim_user.user_pk` itself is generated: `generate_surrogate_key(['lower(email)'])`). This is
the last-resort path and shares the failure class of the open p0 `dim_user` NULL-email
identity-collapse bug (`tk-re-derive-identity-conformed-dimension-joins-pos-b7ca16`). Guard:
- Never key `feedback_pk` off the resolved `user_fk` (it keys off `source_record_ref`), so a
  bad identity join can never collapse or duplicate the fact grain.
- `user_fk` stays **nullable**; an unresolved author email = null `user_fk`, not a wrong
  join. Do not coalesce to a sentinel.
- Re-run `tk-...-b7ca16` before enabling any cross-source identity rollups on this fact.
- **rev. 2:** the email resolves from `comment_author_user_id`, not `ticket_requester_user_id`. At turn grain
  the author of a given turn is the correct identity, and the §2 filter already restricts to requester turns
  — so the two agree today, but keying off the comment author is the honest expression and stays correct if
  the filter is ever widened (e.g. to include CC'd participants).

**Divergence from precedent (intentional):** `tfact_chatbot_events`/`tfact_discussion_events`
mint no `*_pk` and rely on a model-level `expect_compound_columns_to_be_unique` test. This
fact mints an explicit `feedback_pk` from the stable source business key because the
migration strategy (design §6) requires the same PK to regenerate identically across the
interim→data-bus source swap. (Rev. 2's second reason — giving late-arriving
`category_fk`/`sentiment_fk` a stable row to target — no longer applies, since rev. 3 moved both off this
fact. The migration reason alone still justifies the divergence.) We keep the compound-uniqueness test
*as well* (§8).

**Output columns** (fact): `feedback_pk`, `feedback_source_fk`, `feedback_channel_fk`, `user_fk`,
`courserun_fk`, `content_block_fk`, `platform_fk`, `organization_fk`,
`occurred_date_fk`, `occurred_time_fk`, `created_date_fk`, `created_time_fk`, `updated_date_fk`,
`updated_time_fk`, `conversation_id` (=`conversation_ref`), `turn_index`, `is_conversation_opening`,
`source_record_id` (=`source_record_ref`), `source_url`, `subject_type`, `subject_ref`, `subject_url`,
`feedback_title` (redacted), `feedback_text` (redacted), `feedback_text_chars`, `explicit_rating`,
`source_metadata`, `feedback_occurred_at`, `feedback_created_at`, `feedback_updated_at`,
`feedback_ingested_at`.

**Removed in rev. 2** (design §0 conformance rule): `source_status`, `source_priority`, `source_channel`,
`source_brand`, `source_group` → `source_metadata`; `source_tags` → `bridge_feedback_tag`; `csat_score`
renamed `explicit_rating`.
**Removed in rev. 3:** `category_fk`, `sentiment_fk` → `afact_feedback_conversation` (design §5a).

No embedding column and no `embedding_id`: vectors live on the conversation fact (design §4f), which joins to
this one on `conversation_id`.

---

## 5. New dimensions (MVP)

### `dim_feedback_source` — static seed
Rows for MVP: one, `zendesk`. Columns per design §4c
(`feedback_source_pk = generate_surrogate_key(['source_slug'])`, `source_slug`, `source_name`,
`source_medium='support_ticket'`, `source_audience_scope='operational'`, `is_course_scoped=false`,
`is_conversational=true`).
Implement as a dbt seed (`seeds/`) or a small `select ... union all` model — recommend a
seed CSV since the set is tiny and hand-curated.

### `dim_feedback_channel` — static seed (NEW in rev. 2)
The conformed channel value set (design §4d): `email`, `web_form`, `in_product_widget`, `chat`,
`forum_post`, `assessment`, `api`. `feedback_channel_pk = generate_surrogate_key(['channel_slug'])`,
plus `channel_name` and `is_solicited`. dbt seed CSV.

**Open item before seeding:** confirm the value set against Zendesk's actual `ticket_source_channel` /
`comment_source_channel` values, and map them onto it in `int__feedback__zendesk`. An unmapped source value
must fail loudly rather than silently bucketing to a catch-all — an unnoticed mapping gap makes the one
genuinely conformed facet useless for cross-source comparison.

### `dim_sentiment` — static seed
Rows: `positive`, `neutral`, `negative` (design §4b), `sentiment_pk = generate_surrogate_key(['sentiment_slug'])`,
`polarity_score_bucket`. dbt seed CSV.

### `dim_feedback_tag` + `bridge_feedback_tag` — derived (NEW in rev. 2)
- `dim_feedback_tag`: `select distinct` over `ticket_tags` from `int__zendesk__ticket`, slugified.
  `feedback_tag_pk = generate_surrogate_key(['source_slug', 'tag_slug'])` — tags are **source-scoped**
  (design §4e), so a Zendesk tag and a forum role that share a string stay distinct.
- `bridge_feedback_tag`: unnest `ticket_tags` per turn and join to `dim_feedback_tag`. Grain
  `(feedback_pk, feedback_tag_pk)`, both `not_null`, compound-unique.
- Note the tags are a *ticket* attribute applied to every turn of that ticket. That is correct — a tag
  describes the conversation, and a per-turn bridge lets you filter turns by their conversation's tags
  without a join back to the conversation fact.

### `dim_feedback_category` — seeded, then ML-curated
- **MVP seed (no ML):** distinct tags from `dim_feedback_tag` + `group_name` from
  `int__zendesk__ticket`, materialized as `category_source='seed'`,
  `category_status='proposed'`, `category_slug = generate_surrogate_key([slugified tag])`.
  With `dim_feedback_tag` in place this is a `select` from a dimension rather than an array unnest.
- **ML curation (later, per `feedback_ml_approach.md` §D):** LLM-labeled clusters upsert
  `category_source='llm_discovered'` rows; humans flip `category_status` to `approved`.
- SCD-lite: relabel changes `category_label`, never `category_slug`.

### `int__feedback__conversation` — the ML input (NEW in rev. 3)
One row per `(source_slug, conversation_ref)`, assembling that conversation's kept turns from
`int__feedback__unioned` in `turn_index` order — the redacted text concatenated with a turn delimiter, plus
`turn_count`, `participant_count` and `conversation_text_chars`. This is what the summarizer and embedder
read; it exists so the ML batch never has to re-derive conversation assembly in Python, and so the assembly
logic is testable in dbt.

For non-conversational sources (`is_conversational = false`) this is a pass-through of a single turn —
`conversation_ref = source_record_ref` — so ORA and the edX plugin need no special case.

### `afact_feedback_conversation` — the analysis fact (NEW in rev. 2, widened in rev. 3)
Per design §5a. `feedback_conversation_pk = generate_surrogate_key(['source_slug', 'conversation_ref'])`.
Two column groups:

- **Lifecycle** — reads `int__zendesk__ticket` for conversation attributes and aggregates `tfact_feedback`
  for `turn_count` / `participant_count` / `last_turn_date_fk` / `conversation_text_chars`.
- **Generated (rev. 3)** — `conversation_summary`, `embedding_vector`, `category_fk`, `sentiment_fk`,
  `cluster_id` and their version stamps, left-joined from the ML asset's per-stage output tables
  (`feedback_dagster_asset_spec.md` §3). All nullable; the table is queryable and useful before any of them
  are populated.

MVP ships the available-today lifecycle columns (`opened_date_fk`, `last_turn_date_fk`, `turn_count`,
`participant_count`, `final_status`, `explicit_rating`) first. The duration measures
(`first_response_date_fk`, `resolved_date_fk`, `closed_date_fk`, `resolution_duration_seconds`) are
**blocked on the `ticket_metrics` Airbyte stream** (§0); the generated columns arrive with the ML asset.
Both land additively — the grain does not move when they arrive.

**Row count is known:** one row per Zendesk ticket, ~198K. Unlike the turn fact, this table needs no volume
measurement before it can be sized.

---

## 6. Sentiment & category assignment at MVP

Both now land on `afact_feedback_conversation`, not on `tfact_feedback` (rev. 3).

- **Sentiment (`sentiment_fk`):** MVP can populate a *coarse* sentiment immediately from the
  explicit signal with **no model**: map `explicit_rating`
  (`'good'`→positive, `'bad'`→negative, `'offered'`/null→neutral/unknown) → `dim_sentiment`, with
  `sentiment_source = 'explicit_rating'`. The model-based sentiment (`feedback_ml_approach.md` §E) upgrades
  the null/`offered` rows later with `sentiment_source = 'model'`.
  **Rev. 3 removes rev. 2's caveat here:** the Zendesk rating is a ticket-level signal and the target row is
  now a ticket, so this is a grain-matched label rather than a value propagated across turns. It is a real
  label for the rated ~6% of tickets, which is exactly what the model tier needs for validation.
- **Category (`category_fk`):** MVP can assign the tag-seed category by mapping a ticket's
  dominant tag (via `bridge_feedback_tag`, aggregated to the ticket) → its seed `category_slug`.
  Cluster-based reassignment comes with the ML asset. Unassigned = null (queryable).

Both are nullable columns on a derived aggregate, so the whole warehouse layer builds and is useful before
the ML asset exists.

---

## 7. Dagster asset (MVP) — SEE `feedback_dagster_asset_spec.md`

The scheduled batch asset (assemble → summarize → embed → cluster → LLM-label → sentiment) is specified
separately. The dbt models above are independently buildable and testable *without* the ML asset — it only
fills the generated columns on `afact_feedback_conversation` and writes `feedback_cluster_run`. This ordering
lets both facts ship first, and rev. 3 strengthens it: the ML asset no longer touches `tfact_feedback` at
all, so there is no dbt↔Dagster write ordering to coordinate on the transactional fact.

---

## 8. Tests / contract (`_dim__models.yml` entries)

Mirror the `tfact_discussion_events` yml style:
- Per-column `not_null` on: `feedback_pk`, `feedback_source_fk`, `feedback_channel_fk`, `source_record_id`,
  `feedback_occurred_at`, `occurred_date_fk`, `occurred_time_fk`.
- `unique` on `feedback_pk`.
- Nullable (description-only, no not_null): `user_fk`, `courserun_fk`, `content_block_fk`,
  `platform_fk`, `organization_fk`, `subject_ref`, `subject_url`,
  `explicit_rating`, `source_metadata`.
- `accepted_values` on `subject_type` (`courseware_block`, `course_run`, `course`, `program`,
  `page_url`, `resource`, `unspecified`) — it is the discriminator for the polymorphic subject ref,
  so an unconstrained value silently breaks every consumer that switches on it.
- Model-level `dbt_expectations.expect_compound_columns_to_be_unique` on
  `['feedback_source_fk', 'source_record_id']` (belt-and-suspenders alongside the `feedback_pk`
  unique test — matches the precedent's compound-uniqueness convention; both columns exist on the
  fact, and `feedback_source_fk = generate_surrogate_key([source_slug])` so this is the same
  business grain as `[source_slug, source_record_ref]`).
- **Turn-grain tests (new in rev. 2):**
  - `expect_compound_columns_to_be_unique` on `['feedback_source_fk', 'conversation_id', 'turn_index']` —
    catches a broken window function or a duplicated comment, which the `feedback_pk` test alone would not.
  - `not_null` on `turn_index` and `is_conversation_opening` for conversational sources
    (`dim_feedback_source.is_conversational = true`); both are legitimately null otherwise.
  - Exactly one `is_conversation_opening = true` per `(feedback_source_fk, conversation_id)` — a singular
    test. This is the guard that the previous first-comment-only view is faithfully recoverable.
- `relationships` tests from each `*_fk` to its dim PK (richer-contract style, as
  `dim_course_run` does).
- New dims get their own entries; `dim_feedback_category` gets a `unique` on `category_slug`,
  `dim_feedback_channel` a `unique` on `channel_slug` plus `accepted_values` matching the conformed set, and
  `dim_feedback_tag` a compound-unique on `['source_slug', 'tag_slug']`.
- `bridge_feedback_tag`: `not_null` on both columns, compound-unique on the pair, `relationships` to
  `tfact_feedback.feedback_pk` and `dim_feedback_tag.feedback_tag_pk`.
- `afact_feedback_conversation`: `unique` + `not_null` on `feedback_conversation_pk`; `not_null` on
  `conversation_id` and `feedback_source_fk`; `turn_count >= 1`; `relationships` from `conversation_id` to
  `tfact_feedback.conversation_id` **and the reverse** — every turn's `conversation_id` must resolve here, so
  a conversation cannot go missing from the analysis fact and quietly drop its turns out of every cluster.
  Generated columns are all nullable (description-only): `conversation_summary`, `embedding_vector`,
  `category_fk`, `sentiment_fk`, `cluster_id`. Two consistency tests worth having once the ML asset lands:
  `embedding_model_version` is not null wherever `embedding_vector` is, and `summary_model_version` is null
  exactly where the §A.1 skip rule applies (`turn_count = 1` or under the length threshold) — that second one
  is the guard that a silent summarizer failure doesn't read as "short conversation".
- `int__feedback__conversation`: compound-unique on `['source_slug', 'conversation_ref']`; `turn_count`
  matches the turn count in `tfact_feedback` for the same conversation.

---

## 9. Build & verify path (local)

Per repo convention (`ol-dbt` CLI, DuckDB-over-Iceberg local): after writing models, run
`local register` + a targeted `dbt build --select +tfact_feedback` to validate the fact and
its upstreams compile and pass tests against live Iceberg data.

**Two different volumes now.** `afact_feedback_conversation` is ~198K rows (one per ticket — a known figure).
`tfact_feedback` is public, requester-authored *comments*, whose multiplier over tickets is still unmeasured;
that measurement is a §0 prerequisite. Rev. 3 lowers its stakes — the embedding budget is now driven by the
conversation count, not the turn count — but it still sizes the turn fact and, via the multi-turn share, the
summarization budget.

Validate: `feedback_pk` uniqueness; the three turn-grain tests (§8); null-`user_fk` rate (sanity-check
identity resolution isn't silently collapsing); distinct `conversation_id` count vs. `int__zendesk__ticket`
row count (these *should* match — a mismatch means the filter dropped whole tickets, e.g. tickets whose only
public comment is from an agent, which is worth knowing rather than discovering later); the
`is_conversation_opening` count vs. the same; and that `afact_feedback_conversation` has exactly one row per
distinct `conversation_id` in the turn fact.

---

## 10. Scope boundary (what this MVP does NOT do)

- No forum/tutor/ORA sources (Phase 2 — additive CTEs in `int__feedback__unioned`).
- No `afact_feedback_cluster_daily` aggregate (Phase 2).
- No conversation **duration** measures — blocked on the `ticket_metrics` stream (§0); the rest of
  `afact_feedback_conversation` ships.
- No full-thread agent replies — the §2 filter keeps requester turns only. Agent responses are a
  support-quality dataset, not feedback, and would need their own justification to include.
- No data-bus/analytics-api ingress (Phase 3, gated on the write path — RFC Open Questions).
- No embedding/clustering *required* for either fact to be useful (ML asset is additive).
- No turn-level embeddings or summaries — the analysis unit is the conversation (design §5a).
- No cross-source identity rollups until `tk-...-b7ca16` is re-derived.
