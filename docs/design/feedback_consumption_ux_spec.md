# Feedback Aggregation — Consumption / UX Spec

Status: **spec** · Project: `wp-feedback-aggregation-clustering-system-2e9750`
Date: 2026-07-10 · Resolves `tk-...-ui-ux-audience-actions-and-where-the-expe-476d23`
Companion to [`feedback_dimensional_model.md`](./feedback_dimensional_model.md).

Defines **who consumes the feedback fact, at what altitude, what actions they take, and
where the experience lives.** The fact + dims are the substrate; this spec is the demand
side that validates the model actually serves its audiences.

---

## 1. Audiences × altitude (from the RFC)

Four audiences at three altitudes. Each maps to a specific slice of `tfact_feedback` +
`dim_feedback_category`/`dim_sentiment` and a specific row-level access rule.

| Audience | Altitude | Core question | Grain they work at | Row-level access |
|---|---|---|---|---|
| **Customer support** | operational | "What recurring issues are coming in this week?" | cluster → individual ticket drill-down | all feedback |
| **Engineering** | operational | "Which systemic platform defects does feedback point to?" | cluster (systemic-only, filter noise) → ticket | all feedback |
| **Instructors / instructional designers** | course-scoped | "What are my learners saying about *my* course?" | feedback filtered to their `courserun_fk` | only their courserun(s) |
| **Department leadership** | strategic | "How are category/sentiment trends moving; where are new-offering opportunities?" | `afact_feedback_cluster_daily` rollups | aggregates only (no raw text) |

Three altitudes = the same fact read three ways: **operational drill-down** (row-level,
support/eng), **course-scoped** (filtered fact, instructors), **strategic rollup**
(aggregate fact, leadership). This is why `dim_feedback_source.source_audience_scope`
(`operational | course | strategic`) exists on the source dim.

---

## 2. Surface — where the experience lives

**Recommendation: start in Superset (BI), not a custom app.** Rationale:
- The warehouse already exposes facts to Superset; zero new app infrastructure (matches the
  RFC's "effectively zero new infrastructure" MVP posture).
- Support/eng/leadership are already BI consumers; instructors are the only audience that
  might warrant an embedded surface later.
- Row-level security in Superset covers the instructor courserun-scoping and the
  leadership aggregates-only rule.

**MVP surfaces (Superset dashboards):**
1. **Support triage** — cluster list ranked by size/recency, filterable by
   `source_status`/`source_priority`/`source_channel`/`source_tags`, each cluster
   drill-through to constituent (redacted) tickets with `source_url` deep-links back to
   Zendesk. Sentiment facet from `dim_sentiment`.
2. **Engineering systemic view** — same cluster list but filtered to systemic clusters
   (HDBSCAN non-noise, above a `min_cluster_size` threshold; noise/one-offs hidden), sorted
   by cluster cohesion/size, with category labels from `dim_feedback_category`.
3. **Leadership trends** — `afact_feedback_cluster_daily` time-series of category × sentiment
   volume; no raw text. (Phase 2, needs the aggregate fact + cross-source data.)
4. **Instructor course view** — feedback filtered to a courserun; **Phase 2** (needs
   course-scoped sources: forum/tutor/ORA — Zendesk MVP is not course-scoped, so instructors
   get nothing from the MVP slice. Documented gap.)

**When to graduate to an embedded/custom app:** only if write-back actions (§3) outgrow
Superset — e.g. support wants in-context triage/routing, or eng wants one-click issue
filing. Treat that as a deliberate later decision, not MVP.

---

## 3. Actions per persona (write-back)

MVP is **read-only insight**; write-back is explicitly deferred but the design should not
preclude it. Documented target actions (open question, RFC):

| Persona | Read (MVP) | Write-back (later) |
|---|---|---|
| Support | view clusters, drill to tickets, filter | mark cluster as triaged / route to team |
| Engineering | view systemic clusters, category labels | file a linked eng issue from a cluster |
| Instructor | (Phase 2) view own-course feedback | acknowledge / respond |
| Leadership | view trends | none (consumption only) |
| Curator (any) | — | approve/merge/deprecate `dim_feedback_category` labels (the curation loop) |

The **category curation loop** (approve LLM-proposed labels) is the one write-back that IS
needed early — it can live as a lightweight admin surface or even a manual dbt-seed edit at
MVP, upgrading to a UI when volume warrants (`feedback_ml_approach.md` §D).

---

## 4. Access control (ties to design §7 + Lakekeeper/Cedar)

- **Redacted text only** is exposed in any consumption surface (raw text stays in
  `raw`/`stg` under existing PII classification + Lakekeeper/Cedar authz — design §7).
- **Instructor** dashboards are row-filtered to their `courserun_fk` (Superset RLS or a
  Cedar policy on the fact).
- **Leadership** sees the aggregate fact only (no row-level text) — enforced by pointing
  their dashboards at `afact_feedback_cluster_daily`, not `tfact_feedback`.
- Re-applying PII classification + Lakekeeper/Cedar authz at the new fact/dims is tracked by
  `tk-re-apply-pii-classification-lakekeeper-cedar-aut-1b3516` (p3).

---

## 5. What the MVP consumption slice actually delivers

Given the MVP is Zendesk-only and not course-scoped: **support + engineering get working
cluster-triage dashboards; leadership gets Zendesk-only category/sentiment trends;
instructors get nothing until Phase 2 course-scoped sources land.** That is the honest
scope — the model supports all four, but only two-and-a-half audiences are served by the
first data slice. This is acceptable because support/eng recurring-issue triage is the
primary stated motivation.
