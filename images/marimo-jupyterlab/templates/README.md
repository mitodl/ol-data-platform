# Notebooks on the OL Data Platform

A quick reference you can read without leaving the notebook. The full guide —
including how the environment is deployed and operated — lives at
<https://engineering.ol.mit.edu/application_specific_guides/jupyterhub/data_platform_notebooks/>.

## The three files

| File | Purpose |
|---|---|
| `getting_started.py` | Minimal template. Signs in, registers the `warehouse` engine, loads the usual libraries. Copy it to start something new. |
| `demo.py` | Full tour — finding tables, both query styles, star-schema joins, reactive inputs, charts, and the traps. Read once, then raid for patterns. |
| `README.md` | This file. |

## Signing in

There are two sign-ins. The first gets you into this notebook environment. The
second gets you into the warehouse: Starburst Galaxy authenticates query clients
itself, federating the login to the same MIT OL SSO, so the first query of a
session prints a link for you to open.

The token is cached in memory and reused while Galaxy accepts it, so signing in
is not per-query — but it is not guaranteed to be once per session either. If
the token expires or is revoked, the next query 401s and a fresh login link
appears. Restarting the kernel also means signing in again.

Queries run as **you** — your access is exactly what your role grants, and your
queries are attributable.

Configured for you:

| Variable | Meaning |
|---|---|
| `TRINO_HOST` | Starburst Galaxy endpoint |
| `TRINO_PORT` | `443` |
| `TRINO_CATALOG` | Default warehouse catalog |

## Adding a package

Each notebook has its own isolated environment, built from the `/// script`
header at the top of the file. **`pip install` does not persist.** Add the
package to that list and restart the kernel:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "polars>=1.0",
#   "trino[sqlalchemy]>=0.330",
# ]
# ///
```

The templates ship with polars, pandas, numpy, pyarrow, and altair.

## Finding data

Open **Explore variables and data sources** in the sidebar and expand
`warehouse`. Schemas load when you expand them, so an unexpanded tree is not an
empty one.

The schema suffix names the dbt layer:

| Suffix | Contents | Query it? |
|---|---|---|
| `_raw` | Untransformed source loads, ~1,400 tables | No — no cleaning, no keys |
| `_staging` | One model per source table | Rarely — no conformed keys |
| `_intermediate` | Reusable joins | No — implementation detail |
| `_dimensional` | The star schema | **Start here** |
| `_mart` | Wide tables per business area | Yes, if one fits |
| `_reporting` | Shaped for dashboards | Only to reproduce a dashboard figure |

In `_dimensional`: `dim_*` groups or filters, `tfact_*` is one row per event,
`afact_*` is pre-summarised, `bridge_*` is a many-to-many link.

**Most of what you see in the panel is not on that list.** There are also dozens
of `ol_warehouse_production_<username>_*` schemas — developer sandboxes from
`ol-dbt local`, not authoritative and not maintained. If a schema name has a
person in it, it is not for you.


## The one thing that will silently give you wrong numbers

Several dimensions are slowly-changing (type 2): editing a course run writes a
new row and keeps the old one, marked by `is_current`. The surrogate key is
*stable across versions* — the same `courserun_pk` is on the current row and on
every historical row.

So a join on the key alone matches every version, and counts multiply by the
number of times that row has been edited. **Every join to an SCD2 dimension
needs a version filter.** For current-state questions that is
`AND <dim>.is_current`; for a point-in-time question, match the fact's date
against the row's `effective_date`/`end_date` interval instead, because
`is_current` would assign today's attributes to a past fact. `demo.py` §4
measures the difference.

## Two ways to query

**SQL cells** — `mo.sql("SELECT ...", engine=warehouse)`. SQL editing with
column completion, result is a dataframe, cell joins the reactive graph. One
statement, whole result in memory, no parameter binding.

**The cursor** — `cur.execute("SELECT ...")`. Ordinary Python: loops, dynamic
SQL, `fetchmany` streaming, and connection-scoped state like `SET SESSION`,
which a SQL cell cannot hold because it opens a fresh connection per statement.
`cur.stats` is how you find out why a query is slow. Returns tuples; marimo
cannot see the dependency.

Reach for a SQL cell — it runs `SHOW`, `EXPLAIN` and DDL too, not only
`SELECT`. Drop to the cursor for a loop, a stream, session state, or
`cur.stats`. `demo.py` §3 has the full trade-off.

## Personal data

`dim_user` holds names and email addresses, and neither template touches it.
Before you write a query that does: aggregate if you can (the facts carry
`user_fk`, so `count(DISTINCT user_fk)` answers most questions without
returning a person), select named columns rather than `*`, put a floor on group
sizes, and remember your home directory is a persistent volume — a CSV of
learner rows written there outlives the question you wrote it to answer.

## Limits

Standard is 2 CPU / 8 GB, Large is 4 CPU / 32 GB. Home is 5 GB and persists.
Kernels are culled after 4 hours idle; there is no absolute session limit.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `401 Authentication required` | Sign-in never completed, or the kernel restarted. Re-run the connect cell and open the link. |
| Login link never appears | It renders as a callout in the cell's own output while the cell blocks. If that is empty, check the cell's **console** pane — the `print()` there is a fallback. |
| The login link does not work | Each attempt mints a new link and retires the previous one. Re-run the connect cell to get a fresh one. |
| `Catalog must be specified` | Unqualified table name with no session catalog. Qualify it, or use the `warehouse` engine. |
| Counts look too high | A join to an SCD2 dimension missing `AND is_current`. |
| Panel looks empty | Expand `warehouse`; schemas load lazily. An entry reading *no databases available* is a raw DB-API connection and can never show a tree. |
| Query never returns | Missing `LIMIT` on a fact table. `cur.stats` shows what the cluster is processing. |
| `ModuleNotFoundError` after `pip install` | Sandbox mode. Add the package to the `/// script` header and restart the kernel. |

## Sharing

`marimo run --sandbox <file>.py` serves the notebook as an app with the code
hidden. Export to self-contained HTML, or to `.ipynb` for a Jupyter
collaborator. The notebook is a `.py` file, so it commits and code-reviews like
source.

`--sandbox` is what builds the environment from the `/// script` header. Without
it marimo runs in the base environment, which has none of the notebook
packages.
