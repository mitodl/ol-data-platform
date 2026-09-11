# Feedback Aggregation — ML/LLM Approach Spec

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-09-11 (rev. 4, continuous cluster assignment; rev. 3, 2026-08-10, conversation-grain analysis) ·
Companion to
[`feedback_dimensional_model.md`](./feedback_dimensional_model.md)

Resolves the open items the dimensional-model design handed to downstream tasks:
clustering (`tk-...-clustering-approach-...a1d7d6`), category discovery
(`tk-...-llm-driven-category-discovery-...550aba`), and sentiment
(`tk-...-sentiment-mapping-...92988e`). Grounded in the discovery cost model (~1.18M text records; one-time
embed ≈ $2–16). At rev. 3's conversation grain the embedding input is **~198K Zendesk conversations**
(~600K across all sources) rather than the turn corpus — see §B.2.

The guiding principle from the RFC: **the durable artifact is the feedback fact; everything a model produces
is a re-runnable derived layer.** Nothing here writes to `tfact_feedback` — as of rev. 3 that fact is
insert-only and has no late-arriving update path at all. All ML output lands on
`afact_feedback_conversation`.

> **REVISED 2026-09-11 (rev. 4): cluster assignment is continuous, and no human promotes a clustering run**
> (§C.1). A conversation is placed into the live clusters as soon as it is embedded. Re-clustering runs
> automatically and carries cluster identity forward by membership overlap, so a cluster keeps a stable
> `cluster_key` across runs and across embedding-model changes. The rev. 3 promotion step (a human approves
> a run, which then copies its assignments onto the fact) is withdrawn: it put a person between every new
> conversation and its cluster, which guarantees a backlog and stale assignments. People still choose the
> clustering configuration, through code review, and curate category labels (§D). Labels now attach to
> `cluster_key` instead of to one run's integer ids. Raised in review of
> [#2662](https://github.com/mitodl/ol-data-platform/pull/2662).

> **REVISED 2026-08-10 (rev. 3) — the analysis unit is the conversation, not the turn** (design §5a).
> `tfact_feedback` keeps its turn grain, but summarization, embedding, sentiment and clustering all operate
> on the **assembled conversation** and write one row per conversation. Consequences worked through below:
> the record count is conversations (~198K for Zendesk, a *known* number) rather than an unmeasured comment
> multiplier (§B.2); a new **summarization stage** is the one per-record LLM cost (§A.1); `min_cluster_size`
> once again reads as "how many conversations before we call it systemic" (§C); and the Zendesk CSAT signal
> becomes grain-matched to the sentiment it seeds (§E).

> **REVISED 2026-08-07 (rev. 2) — `tfact_feedback` grain moved from ticket to turn** (design §1). Still in
> force: the fact records every requester turn, sourced from `int__zendesk__ticket_comment`. Rev. 3 changes
> what the *models* consume, not what the fact records — and the turn grain is precisely what makes a
> complete conversation assemblable.

---

## A. Pipeline shape (single batch Dagster asset graph, MVP)

```
int__feedback__conversation (redacted turns assembled per conversation) [dbt]
  → summarize    : turns → conversation_summary               [py; SKIPPED for single-turn/short — §A.1]
  → embed        : summary or turns → vector                  [py]
  → assign       : new vector → nearest live cluster_key       [py; every embedding refresh, §C.1]
  → recluster    : all vectors → new partition, matched to     [py; scheduled/triggered, + feedback_cluster_run]
                   existing cluster_keys by membership overlap
  → label        : new/split/merged cluster_key → label        [py: dim_feedback_category (proposed)]
  → sentiment    : rating or text/vector → sentiment_slug     [py]
  → write        : one row per conversation                   [afact_feedback_conversation]
```

Every stage is keyed by `feedback_conversation_pk` and stamps the version that produced it
(`summary_model_version`, `embedding_model_version`, `cluster_run_id`), so a re-run is an idempotent
overwrite of a derived row. **No stage writes to `tfact_feedback`.**

**One target table, not a sidecar** (design §4f/§5a; ERD in [`feedback_erd.md`](./feedback_erd.md) §4). The
rev. 2 sidecar split — `feedback_embeddings` at `(feedback_pk, model_version)` and
`feedback_cluster_assignment` at `(feedback_pk, cluster_run_id)` — is withdrawn. All of it collapses onto one
row per conversation, and the clustering tables sit alongside it at genuinely different grains (rev. 4, §C.1):

| Table | Grain | Holds |
|---|---|---|
| `afact_feedback_conversation` | `(feedback_conversation_pk)` | summary, `embedding_vector`, `embedding_dim`, `embedding_input`, `category_fk`, `sentiment_fk`, `cluster_key`, `cluster_similarity`, `cluster_assignment_method` + version stamps |
| `feedback_cluster_run` | `(cluster_run_id)` | `embedding_model_version`, `algorithm`, `run_params`, `cluster_count`, `noise_count`, `silhouette`, `run_status` (`completed` \| `failed`, an audit value), `run_at` |
| `feedback_cluster_candidate` | `(feedback_conversation_pk, cluster_run_id)` | run-local `cluster_id`, `cluster_probability`: one run's raw HDBSCAN output, the input to identity matching. Kept for the last N runs for run-vs-run comparison |
| `feedback_cluster` | `(cluster_key)` | stable identity: `centroid`, `radius`, `embedding_model_version`, `member_count`, `cluster_status` (`active` \| `retired`), `first_seen_run_id`, `last_seen_run_id` |
| `feedback_cluster_lineage` | `(cluster_run_id, cluster_key, prior_cluster_key)` | `relation` (`continued` \| `split` \| `merged` \| `new` \| `retired`), `jaccard` |
| `feedback_cluster_membership` | `(feedback_conversation_pk)` | the live assignment: `cluster_key` (null = noise or not yet near a cluster), `cluster_similarity`, `cluster_assignment_method` (`recluster` \| `incremental`), `cluster_run_id`, `assigned_at` |

**What the collapse costs, honestly.** Re-clustering now rewrites the afact row, vector column included — the
property rev. 2's split was bought to preserve. The vector *value* is carried forward rather than recomputed,
so nothing is re-embedded; what is lost is holding several generations live in production simultaneously.
That is a bake-off need, and `feedback_cluster_candidate` (plus scratch tables during the §B.1 evaluation)
covers it off the critical path. What is bought is a single table for consumers and an insert-only fact.

`embedding_input` (`summary` \| `concatenated_turns`) is what makes the summarize-before-embed question
(§B.1) a *column* on the eval rather than a separate pipeline. `dim_feedback_category.cluster_run_id` still
records which run proposed a category.

### A.1 Summarization — the one per-record LLM call

New in rev. 3. `conversation_summary` is an LLM abstract of the assembled redacted turns. It serves two
purposes: it is what a human reads in a cluster listing instead of scrolling a thread, and it is a candidate
for `embedding_input` — which is the same lever §B.1 already identified as the largest single contributor to
cluster quality on short, noisy ticket text. Rev. 3 promotes it from an eval arm to a first-class artifact,
so its cost has to be stated rather than assumed away:

- **Skip rule:** conversations with `turn_count = 1` or under **500 characters** of assembled turns are
  **not** summarized — the raw text already is the summary. `summary_model_version` stays null and
  `embedding_input` is `concatenated_turns`. ORA and the edX plugin are single-turn by construction and are
  free. The 500-character cutoff comes from the measured distribution (§B.2): it sits below the 601 p25, so it
  skips 9,680 of the 52,218 multi-turn Zendesk conversations (18.5%) — the ones that amount to two short
  replies. A 1,000-character cutoff would sit at the 1,040 median and skip 48.5%.
- **Cost** (measured 2026-08-14, #2536): 52,218 multi-turn Zendesk conversations at a mean of 1,847
  characters puts the one-time backfill at **~$37 on Haiku 4.5** via the Batch API (50% off, and a backfill is
  exactly its shape), or ~$74 on Sonnet 5. Steady state (~24K conversations/yr, a fraction multi-turn) is
  single-digit dollars a year. Applying the skip threshold above brings the backfill to ~$32, so the threshold
  is a summary-quality decision rather than a cost lever.
- **PII:** summaries are generated from Presidio-redacted text only and inherit that classification.

---

## B. Embedding (foundation for clustering AND sentiment)

**Decision: one shared embedding per conversation, computed once, stored on
`afact_feedback_conversation`.** Both clustering and (semantic) sentiment consume it — do not embed twice.

> **REVISED 2026-07-10 (rev. 4) — see [`adr_embedding_compute_strategy.md`](./adr_embedding_compute_strategy.md).**
> Compute stays engine-external via **Fenic (Apache-2.0)** in a Dagster asset, writing vectors to
> an open Iceberg `ARRAY<float>` column (rev. 3: on `afact_feedback_conversation`; portable across
> Trino→StarRocks, and StarRocks later indexes those vectors with HNSW — a load, not a re-embed).
> **Bedrock/in-account is NOT a requirement** — the **embedding model is
> chosen by task effectiveness** (clustering + retrieval on OUR feedback corpus), with egress of
> Presidio-redacted text to a managed provider acceptable. Model selection is specified as an
> evaluation below, not a fixed pick. Persist-once, `model_version`, Iceberg storage, and
> mandatory upstream redaction are unchanged.

### B.1 Embedding-model selection — by effectiveness, on our own labeled corpus

Selection principle (industry consensus): **use the MTEB leaderboard to *narrow*, then benchmark
the shortlist on our own labeled Zendesk sample — the decisive metric is performance on our
corpus, not the public average.** Because embeddings are persisted with `model_version` (below),
the choice is **reversible**: re-embed with a better model later without touching the fact.

**Candidate shortlist (narrow with current MTEB *clustering + retrieval* standings at eval time;
these move monthly).** Two tiers:

| Tier | Candidates (2026) | Via | Notes |
|---|---|---|---|
| Managed (Fenic-native) | Google `gemini-embedding-001`; Cohere `embed-v4`; OpenAI `text-embedding-3-large` | Fenic `GoogleVertex/GoogleDeveloper/Cohere/OpenAI EmbeddingModel` | gemini + Cohere v4 currently edge OpenAI on MTEB; all support **Matryoshka dim truncation** (test 256/512/1024 — smaller = faster HDBSCAN/HNSW + smaller sidecar, usually minimal quality loss). Egress of redacted text. |
| Self-hosted open (top of MTEB) | Qwen3-Embedding; BGE-M3; NV-Embed-v2 | `sentence-transformers` (outside Fenic's provider set) | Currently top the MTEB average / best open quality-cost (BGE-M3); $0 marginal cost, no egress, full control — at the price of hosting a model (heavier Dagster image / a GPU batch). Include if we're willing to self-host for effectiveness. |

**Evaluation harness (run once on a labeled sample, ~2–5k tickets):**
1. **Label set:** reuse the existing structure as ground truth — Zendesk `ticket_tags` /
   `group_name` give a free (noisy) cluster/label reference; optionally hand-label a few hundred
   for a cleaner set.
2. **Task-aligned metrics, not MTEB average:**
   - *Clustering* (our primary task): run the §C pipeline (UMAP+HDBSCAN) on each candidate's
     vectors; score **silhouette** + **agreement with the tag reference** (adjusted Rand /
     normalized mutual information) + a small **human-coherence** rating of the top clusters.
   - *Retrieval* (future "similar feedback"/RAG serving): nearest-neighbour precision@k on
     tag-matched pairs.
3. **Also sweep dimension** (Matryoshka 256/512/1024) and **cost/latency per 1M** as tie-breakers.
4. **Pick** the model×dim that maximizes clustering agreement/coherence at acceptable cost.

**Starting default for the eval** (so implementation isn't blocked on the bake-off): a strong
current all-rounder available via Fenic — `gemini-embedding-001` or Cohere `embed-v4` at 512-dim
— with `text-embedding-3-large` as the "safe baseline" comparison and BGE-M3 as the self-hosted
comparison. Let the harness decide; do not hardcode a winner in the spec.

- **HDBSCAN clustering stays our own sklearn step** — Fenic offers only K-means
  (`with_cluster_labels`), which lacks the noise class we need (§C). Fenic covers embed +
  classify/sentiment + labeling; not clustering.
- **Redaction is upstream and mandatory** (design §7): embeddings are computed on the
  Presidio-redacted text only. Raw text never reaches the embedding step. (Redaction remains
  required even though provider egress is now acceptable — it is a data-minimization guarantee,
  not just an egress control.)
- **`embedding_model_version` is a first-class column** on `afact_feedback_conversation`. Changing the model
  (or the dimension) = a rebuild of the derived table under a new version stamp, with the bake-off run
  against scratch/candidate tables so production is never in a torn state. This is what makes the model
  choice reversible and the eval low-risk — and it costs nothing on `tfact_feedback`, which is untouched.
- **Vector storage:** at conversation grain the vector count is *conversations*, not turns — ~198K for the
  Zendesk MVP and roughly 600K across all sources (Zendesk tickets + forum threads + tutor threads + ORA
  submissions), against the ~1.18M *turn* figure the earlier estimate used. At (256–1024) float32 that is
  ~0.2–2.5 GB. Store as an Iceberg `ARRAY<float>` column for the MVP (no new service; the batch clustering
  job reads the set into memory — comfortable at this scale). StarRocks HNSW becomes the serving-tier index
  once deployed (ADR).

### B.2 Volume and text profile at conversation grain (rev. 3)

**The embedding input count is now a known number, not an unmeasured multiplier.** Rev. 2 put the ML input at
public, requester-authored *comments*, whose ratio to tickets nobody had measured. Rev. 3 embeds
conversations, so the MVP input is **~198K Zendesk tickets** — the figure discovery actually measured — and
roughly ~600K across all sources once forum threads, tutor threads and ORA submissions are onboarded, versus
the ~1.18M turn-level corpus. Embedding cost returns to the original $2–16 order of magnitude.

**The turn-count measurement is done** (2026-08-14, #2536), against production
`stg__zendesk__ticket_comment` filtered to `comment_is_public` and
`comment_author_user_id = ticket_requester_user_id`:

| | Measured |
|---|---|
| Zendesk tickets | 200,485 |
| No public requester comment (never enter the fact) | 9,659 (4.8%) |
| Conversations — the embedding input | **190,826** |
| Turn-grain rows in `tfact_feedback` | **282,470** (1.5 turns/conversation) |
| Multi-turn (≥2 turns) — the summarizer input | **52,218 (27.4%)** |
| Assembled characters, multi-turn conversations | p25 601 · p50 1,040 · p90 4,016 · p99 11,691 · max 553,230 · mean 1,847 |
| Multi-turn conversations under 500 characters (skipped by §A.1) | 9,680 (18.5%) |
| Multi-turn conversations under 1,000 characters (skipped at the rejected cutoff) | 25,326 (48.5%) |

`tfact_feedback` carries 282,470 rows at 1.5 turns per conversation — immaterial for storage or batch
runtime. `distinct conversation_id` is **190,826**: agent-only tickets carry no requester text and never enter
the fact, so it runs 4.8% below the ticket count by construction. The summarizer runs on the 52,218 multi-turn
conversations, which is what puts its cost at ~$37 (§A.1).

The character distribution sets §A.1's 500-character skip threshold. It also argues for capping summarizer
input: the p99 is 11,691 characters against a 553,230 maximum (~138K tokens), a tail thin enough that
truncating the longest conversations costs nothing in coverage while bounding the cost of outliers.

Three substantive effects, not just a different number:

- **Conversation grain fixes the recall gap without reintroducing the old one.** Rev. 1's ticket grain
  embedded only the opening comment, so a problem first articulated in turn 4 ("actually the real issue is
  the certificate never generated") was invisible to the systemic-issue detector. Embedding the *assembled*
  conversation includes turn 4 — while also keeping it attached to the turn-1 context that makes it
  interpretable, which per-turn embedding threw away.
- **The text profile improves in the direction §B.1 cares about.** Isolated follow-up turns skew short and
  referential ("still not working", "same as before") — the worst case for an embedding model. An assembled
  conversation, and more so its summary, is self-contained. This does not retire the summarize-before-embed
  eval arm; it makes it a comparison between two coherent inputs (`embedding_input = summary` vs.
  `concatenated_turns`) rather than a rescue operation on fragments.
- **Cluster size means what it should again.** A cluster's member count *is* its distinct-conversation count,
  so `min_cluster_size` reads directly as "how many conversations before we call this systemic" and one
  talkative reporter can no longer manufacture a systemic issue. Rev. 2's rank-by-`distinct conversation_id`
  workaround and `afact_feedback_cluster_daily`'s twin count columns both go away.

**Rejected:** re-embedding on every run (the prototype #10793 flaw the RFC fixes).
**Upgraded from "rejected" to "evaluate seriously" (new evidence, 2026-07):** LLM
**semantic-normalization before embedding**. Recent support-ticket-clustering literature reports it
is *the single largest contributor to cluster quality* on short/noisy ticket text (improved
silhouette + human-rated coherence over baselines) — stronger than the RFC's earlier skeptical
stance. Treat it as a **first-class arm of the §C evaluation**, not a default: it adds an LLM call
per record (cost), so gate by measured lift vs. cost, and expect it to help most on the long noisy
Zendesk descriptions (avg ~1,000 chars) and least on already-short sources (tutor ~58, ORA ~140).

---

## C. Clustering (systemic-issue detection) — `tk-...-a1d7d6`

**Goal:** turn per-conversation embeddings into clusters that distinguish a *systemic issue*
(many conversations, one root theme) from a *one-off*. Output = `cluster_id` + a cluster-size /
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
  Zendesk sample and should be config, not hardcoded.
  **rev. 3:** at conversation grain `min_cluster_size` counts conversations, so it reads directly as "how
  many conversations before we call it systemic" and needs no correction. (Rev. 2's rank-by-distinct-
  `conversation_id` workaround, required when clustering turns, is retired.)
- **Summary-vs-raw is the eval arm** (see §B.1/§A.1): recent support-ticket-clustering evidence (2026)
  reports LLM normalization before embedding is the single largest lever on cluster quality for short/noisy
  ticket text. Rev. 3 makes the summary a first-class artifact, so this becomes a comparison between
  `embedding_input = 'summary'` and `embedding_input = 'concatenated_turns'` on the labeled sample
  (silhouette + tag-agreement + human coherence). Adopt the summary as the embedding input only where the
  measured lift justifies its per-conversation LLM cost — expected to help most on long multi-turn Zendesk
  tickets and not at all on single-turn sources, which the §A.1 skip rule never summarizes anyway.
- **Re-clustering is cheap, expected, and automatic** (rev. 4, §C.1): each run writes a new `cluster_run_id`
  to `feedback_cluster_run` and its raw assignments to `feedback_cluster_candidate`. An identity-matching step
  then maps the run's clusters onto the existing `cluster_key`s and rewrites `feedback_cluster_membership`.
  No human approves a run. Stable keys, not a promotion gate, are what decouple churny clustering from the
  curated category dimension: a cluster that continues across runs keeps its key, and therefore its category.
- **Cross-source clustering (Phase 2):** because all sources share one embedding space in
  `int__feedback__conversation`, a cluster can span Zendesk + forum + tutor — this is the
  mechanism behind `afact_feedback_cluster_daily` (cluster × category × sentiment × date ×
  source). No algorithm change needed; just don't filter `source_slug` at cluster time.
- **Deps:** `umap-learn`, `hdbscan` (or `scikit-learn`'s `HDBSCAN` ≥1.3 to avoid the
  separate compiled dep — decide at implementation based on the Dagster image's build
  constraints). All CPU, no service. (rev. 4: §C.1's incremental placement is nearest-centroid and needs
  neither. If it is ever replaced by HDBSCAN's own out-of-sample prediction, that is `approximate_predict`
  in the standalone `hdbscan` package; scikit-learn's `HDBSCAN` exposes only `fit_predict` as of 1.9.)

**Cluster-quality columns** are added *only if the chosen algorithm produces them*:
`cluster_key` + `cluster_similarity` on `afact_feedback_conversation` (rev. 4; the run-local `cluster_id` and
`cluster_probability` stay on `feedback_cluster_candidate`); run-level `silhouette`, `cluster_count`,
`noise_count` on `feedback_cluster_run` (§A) — persistence optional.

### C.1 Continuous assignment and stable cluster identity (rev. 4)

**No human sits between a conversation and its cluster.** Assignment is a derived value, maintained as
conversations are embedded and as the corpus grows. A human gate on assignment produces a backlog by
construction, and every conversation embedded while the queue waits has no cluster. The decisions that need
people are made elsewhere: the clustering configuration is chosen by the §B.1 bake-off and shipped as config
through code review, and category labels are curated in §D.

Three mechanisms keep assignment current:

1. **Incremental placement.** A `feedback_cluster_assignment` asset runs on every `feedback_embeddings`
   refresh. For each conversation whose embedding is newer than its membership row (new, re-embedded, or
   never placed), it computes cosine similarity to every `active` centroid built from the same
   `embedding_model_version` / `embedding_dim` / `embedding_input`, and assigns the nearest cluster if the
   similarity clears that cluster's radius. Otherwise `cluster_key` stays null with
   `cluster_assignment_method = 'incremental'`: the conversation is not near any live cluster yet. Placement
   never moves an already-placed conversation, and needs no model state beyond `feedback_cluster`.
   - *Radius* is per cluster: the similarity at a fixed low percentile of its members' similarities (p5 as a
     starting point, so 95% of members are at least this close) as of the last re-cluster. A per-cluster
     radius respects HDBSCAN's variable density, which one global threshold would not.
   - *Why nearest centroid rather than out-of-sample HDBSCAN:* it is deterministic, explainable, and needs no
     persisted UMAP/HDBSCAN models. A persisted UMAP `transform` plus `hdbscan.approximate_predict` is the
     upgrade path if placement quality measurably needs it.
2. **Automatic re-clustering.** `feedback_clusters` runs over the full embedded corpus on a schedule, and
   early when either trigger fires: the share of conversations left unplaced since the last run crosses a
   threshold, or the corpus has grown past a threshold since the last run. Unplaced conversations are where
   new themes accumulate, and a re-cluster is what turns them into clusters. Runs use the configuration in
   code; changing parameters or the embedding model is a PR, and the next run picks it up.
3. **Identity matching.** After each completed run, the new partition is matched to the current
   `cluster_key`s by membership, not by vectors:
   - Build the contingency matrix of new clusters × current keys over conversations present in both, scoring
     each pair by Jaccard overlap.
   - A one-to-one best match above the match threshold (0.5 as a starting point) **continues** the old key.
   - A new cluster drawn mostly from one old key whose best match continued elsewhere gets a new key, with
     lineage `split` from that key.
   - A new cluster holding the majority of two or more old keys gets a new key, with lineage `merged` from
     each of them.
   - Anything else gets a new key with lineage `new`. An old key with no successor is `retired`.
   - `feedback_cluster_membership` is then rewritten for every conversation in the run
     (`cluster_assignment_method = 'recluster'`; HDBSCAN noise gets a null `cluster_key`), and centroids and
     radii are recomputed.

   Matching compares members rather than vectors, so it works across an embedding-model change: re-embed,
   re-cluster in the new space, and keys continue wherever the grouping does. Centroids are stored per
   `embedding_model_version`, and placement only uses the current model's.

**What a run is now.** A `feedback_cluster_run` row is an audit record: which configuration ran, over how many
conversations, with what outcome (`completed` | `failed`). A failed run changes nothing downstream. There is
no `candidate` / `approved` status to set.

**Automated checks instead of a human gate.** Placement and re-clustering both emit asset metadata: the
unplaced share, per-run continuity (the share of conversations that keep their key), and counts of
continued / split / merged / new / retired keys. A blocking asset check holds the membership rewrite when
continuity falls below a floor, so a bad configuration cannot silently reshuffle every cluster. The remedy is
a configuration change, not a runtime approval.

**A null `cluster_key` now means two things.** HDBSCAN noise from a re-cluster (the "one-off complaint"
bucket) and a conversation that incremental placement could not put near any cluster both leave the key null.
`cluster_assignment_method` tells them apart. Both are "not part of a systemic theme yet".

**Open parameters**, calibrated on the labeled sample (§B.1): the radius percentile, the Jaccard match
threshold, the re-cluster triggers and schedule, the continuity floor, and how many runs of
`feedback_cluster_candidate` to retain.

---

## D. Category discovery & seeding — `tk-...-550aba`

**Bootstrapped, not cold-start** (design §4a). Two inputs seed `dim_feedback_category`:

1. **Seed from existing structure (no LLM):** Zendesk `ticket_tags` (~2,354 distinct) +
   support `group_name` give an immediate, human-meaningful starter taxonomy. These become
   `category_source='seed'` rows with `category_status='proposed'`. This alone makes the
   MVP useful before any clustering runs.
2. **LLM-label the clusters (§C output):** for each new, split or merged `cluster_key` (§C.1; a continued
   key keeps its label and is not re-proposed), sample N
   representative (redacted) utterances near the centroid + the cluster's dominant seed
   tags, and prompt an LLM to propose a short `category_label` + a stable `category_slug`
   + a one-line description. `category_source='llm_discovered'`, `category_status='proposed'`
   until a human approves (`approved`), merges (`merged`), or deprecates.

**Key design invariants (from §4a):**
- **SCD-lite on `category_slug`:** relabeling changes `category_label`, never the slug —
  so `category_fk` is stable across renames.
- **Assignment lands on `afact_feedback_conversation`** (rev. 3), by mapping a conversation's `cluster_key` →
  the category for that key (rev. 4: the stable key replaces the run-local `cluster_id`, so an approval
  survives every re-cluster in which its cluster continues; a split or merged key gets a fresh proposal, with
  its predecessors' categories offered to the LLM as context). Uncategorized = `category_fk` null (a valid,
  queryable state).
- **Category approval never gates cluster assignment** (rev. 4). Open: whether `category_fk` is filled from a
  `proposed` category or only from an `approved` one. The §C.1 backlog argument applies here too, and
  `category_status` on the dimension already lets consumers filter to approved categories.
  This is no longer a "late-arriving update to the fact" — the fact has no update path; it is a rebuild of a
  derived column.
- **LLM cost is bounded:** one LLM call *per cluster*, not per record (there are hundreds
  of clusters, not millions of tickets). This is the critical cost distinction from the
  rejected per-record semantic-summary approach.
- **Model:** any capable instruction model (Claude Haiku/Sonnet class) — labeling a few
  hundred clusters is a trivial, cheap batch. Keep the labeler behind a resource interface.

**Human-in-the-loop is required, not optional:** LLM proposes, a human curates the
category dimension. The dimension is a *curated projection* of clusters (design §4d), which
is why it is a dbt/warehouse dimension and not raw model output. The loop curates *labels*; it never decides
which cluster a conversation belongs to (§C.1).

---

## E. Sentiment mapping — `tk-...-92988e`

**Grain:** `sentiment_fk` on `afact_feedback_conversation`, one sentiment per **conversation** (rev. 3).
`dim_sentiment` starts coarse: `positive | neutral | negative` (design §4b), with a
`polarity_score_bucket` for trend rollups. Aspect-based sentiment is a later refinement,
not MVP.

> **Rev. 3 fixes a grain mismatch.** Zendesk's `satisfaction_rating_score` is a *ticket*-level signal. At
> turn grain it had to be propagated to every turn of a rated ticket, which rev. 2 had to flag as a weak and
> potentially misleading label. It is now the same grain as the sentiment it seeds, so tier 1 below is an
> exact label rather than an approximation — which also makes the tier-2 validation set trustworthy.
> `sentiment_source` (`explicit_rating` | `model`) records which tier produced each row.

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
material. `dim_sentiment` and the conversation-fact column are unaffected by which wins.

---

## F. What is explicitly deferred (non-blocking for spec/MVP)

- Embedding model final pick + GPU/CPU throughput at full scale (MVP proves it on ~198K Zendesk
  conversations).
- Dedicated vector store / online serving (Iceberg ARRAY suffices for batch).
- Aspect-based sentiment; multi-lingual handling.
- **Turn-level embeddings.** Rev. 3 embeds conversations only. If a later retrieval use case needs
  "find me the exact turn where this was said", turn-level vectors can be added as a genuine sidecar then —
  the turn grain in `tfact_feedback` preserves the option. Nothing in the MVP needs it.
- Hierarchical truncation from prototype #10793 — test as a hypothesis on a sample, do not inherit
  (RFC Open Questions). Semantic-summary-before-embedding is no longer deferred: rev. 3 makes the summary a
  first-class artifact and its use as `embedding_input` a scored eval arm (§A.1, §C).
- Cross-source `afact_feedback_cluster_daily` tuning (Phase 2).
