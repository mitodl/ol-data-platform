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

**The surface is an open choice, and it need not be one surface for all audiences.** Because the
fact + dims are the substrate and every surface just *reads the modeled tables*, the surface is a
swappable, reversible consumer — pick per audience by interactivity and write-back need, and you
can run more than one against the same tables. Three viable options, all grounded in tooling the
platform **already operates**:

| Option | What it is | Already in-repo? | Best for | Auth / row-level access | Interactivity & write-back | Effort |
|---|---|---|---|---|---|---|
| **Superset dashboard** | Managed BI; dashboards/charts/datasets as code | **Yes** — `src/ol_superset/` (`ol-superset` CLI export/validate/sync/promote) + `dg_projects/lakehouse/.../assets/superset.py`; **programmatic RLS** via `apply_rls.py` | Leadership trend dashboards; support/eng cluster tables with drill-through; anything chart-shaped and read-mostly | **RLS already solved** (per-role row filters) — covers instructor courserun-scoping + leadership aggregates-only | Rich filtering/drill; **little/no write-back**; bespoke interactions are hard | **Lowest** — a new dataset + dashboards on the fact |
| **Marimo notebook-as-webapp** | A reactive Python notebook served as an app (`marimo run`) | **Partly** — `images/marimo-jupyterlab/` image exists (marimo + Keycloak OIDC + per-user Trino token + IRSA to S3/Glue), currently in JupyterHub *notebook* mode; serving one as an app is a small new deploy reusing that image/auth | Analyst/eng **interactive cluster exploration**, semantic-search over clusters, the **category-curation loop**, and **prototyping the UX** before committing to an app | Runs queries **as the user** (Keycloak OIDC → Trino/StarRocks token) → can push **row-level authz to the engine** (Lakekeeper/Cedar) rather than reimplement it | High interactivity; **Python** so it can call the embedding/cluster/LLM code directly; light write-back via widgets (e.g. approve labels) | **Medium** — write a notebook; add an app-serve deploy behind APISIX |
| **Net-new application** | A purpose-built web app | No | **Instructor-embedded** views (in MIT Learn), **support triage/route** workflows at scale, product integration | Full control via the org's **APISIX/Keycloak**; bespoke per-persona policies | Full custom UX + rich **write-back** workflows | **Highest** — a service to build, deploy, own |

**Recommendation — phased and audience-matched, not one surface:**
1. **MVP (fastest value): Superset** for leadership trends and the support/eng cluster tables —
   it's already operated, RLS is already solved as code, and it matches the RFC's
   "near-zero new infrastructure" posture. Datasets point at `tfact_feedback` /
   `afact_feedback_cluster_daily` / the dims.
2. **Interactive + curation surface: a deployed Marimo notebook-as-webapp** — the right home for
   cluster exploration, semantic search, and the **category-curation loop** (approve/merge
   LLM-proposed labels), and the **cheapest way to prototype the real UX** before deciding whether
   a net-new app is warranted. It reuses the existing marimo image + Keycloak/Trino/IRSA plumbing,
   and being Python it can call the same embedding/clustering/label code the pipeline uses.
3. **Net-new app: only when write-back or product-embedding justifies it** — e.g. instructor-facing
   views inside MIT Learn, or support triage-and-routing at scale. Defer until the Marimo prototype
   has shown what the workflow actually needs; the app then formalizes a proven UX rather than
   guessing.

This ordering keeps commitment low (all three read the same tables) and treats Marimo as the
discovery vehicle between "a dashboard is enough" and "we need a real app."

**MVP surface content (Superset unless noted):**
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
5. **Cluster exploration + curation** (Marimo) — interactive nearest-neighbour/semantic browse of
   clusters and the label-approval loop; the one surface where MVP write-back genuinely lives.

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
needed early. Its natural MVP home is the **Marimo notebook-as-webapp** (§2) — Python widgets over
`dim_feedback_category`, reusing the same code path as the labeling pipeline — with a manual
dbt-seed edit as the zero-UI fallback. It upgrades into a net-new app only if/when the other
write-back workflows (§3) do.

---

## 4. Access control (ties to design §7 + Lakekeeper/Cedar)

Row-level access can be enforced at **two layers**, and the surface choice (§2) decides which:
- **Surface-layer (Superset):** per-role **RLS is already solved as code** (`src/ol_superset/.../apply_rls.py`) — instructor rows filtered to their `courserun_fk`, leadership pointed at the aggregate fact only.
- **Engine-layer (Marimo / net-new app):** these run queries **as the user** via the Keycloak OIDC → Trino/StarRocks token (the marimo image already does this), so authz can be **pushed to the engine (Lakekeeper/Cedar)** instead of reimplemented in the app — one policy set governs every surface. This aligns with the platform's authz direction and is the preferred model for non-Superset surfaces.

- **Redacted text only** is exposed in any consumption surface (raw text stays in
  `raw`/`stg` under existing PII classification + Lakekeeper/Cedar authz — design §7).
- **Leadership** sees the aggregate fact only (no row-level text) — enforced by pointing
  their dashboards at `afact_feedback_cluster_daily`, not `tfact_feedback`.
- Re-applying PII classification + Lakekeeper/Cedar authz at the new fact/dims is tracked by
  `tk-re-apply-pii-classification-lakekeeper-cedar-aut-1b3516` (p3).

---

## 5. What the MVP consumption slice actually delivers

Given the MVP is Zendesk-only and not course-scoped: **support + engineering get working
cluster-triage dashboards (Superset); a curator gets the label-approval loop (Marimo);
leadership gets Zendesk-only category/sentiment trends; instructors get nothing until Phase 2
course-scoped sources land.** That is the honest scope — the model supports all four audiences and
all three surfaces, but only two-and-a-half audiences are served by the first data slice. This is
acceptable because support/eng recurring-issue triage is the primary stated motivation. The
surface decision stays reversible: start with Superset + a Marimo curation/exploration notebook,
and only build a net-new app once the Marimo prototype proves the workflow warrants it.
