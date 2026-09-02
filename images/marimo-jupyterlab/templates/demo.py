# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "altair>=5.4",
#   "numpy>=2.0",
#   "pandas>=2.0",
#   "polars>=1.0",
#   "pyarrow>=17.0",
#   "sqlalchemy>=2.0",
#   "trino[sqlalchemy]>=0.330",
# ]
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # A tour of the OL warehouse in marimo

    This notebook is meant to be read top to bottom once, then raided for
    patterns. It covers signing in, finding tables, the two ways to run a
    query, the star-schema joins the dimensional model is built for, reactive
    inputs, charts, and the traps specific to this warehouse.

    For a blank starting point, use `getting_started.py`.

    **No example here returns learner-level data.** Nothing selects individual
    learner rows and none of these queries touch `dim_user` — course and
    course-run rows appear, but people never do. See *Working with personal
    data* at the bottom before you write anything that does.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Connect

    Identical to `getting_started.py` — see the comments in the cell below for
    why each piece is shaped the way it is.
    """)
    return


@app.cell
def _(mo):
    import os

    import sqlalchemy
    import trino
    import trino.sqlalchemy  # registers the "trino://" SQLAlchemy dialect

    def _show_login_url(url: str) -> None:
        # mo.output.append() writes to the cell's OUTPUT area and pushes to the
        # frontend immediately, while this cell is still blocked inside
        # execute() waiting for the login. That matters: printing alone puts the
        # link in the cell's *console* pane, which is easy to miss, and a missed
        # link means a restart. Rendering it as a callout with a real hyperlink
        # is the difference between "it hung" and "click here".
        #
        # It no-ops outside a marimo runtime (ContextNotInitializedError), so
        # the print below is the fallback for `python <file>.py` and for the
        # console pane.
        mo.output.append(
            mo.callout(
                mo.md(
                    f"""
                    ### Sign in to Starburst Galaxy

                    **[Open the login page]({url})** — sign in with MIT OL SSO
                    and this cell resumes on its own.

                    The link is single-use and expires. If you miss it, re-run
                    this cell to get a new one.
                    """
                ),
                kind="warn",
            )
        )
        print(  # noqa: T201
            "\nStarburst Galaxy login required. Open this URL, sign in with "
            f"MIT OL SSO, then come back — the query resumes on its own:\n\n"
            f"    {url}\n",
            flush=True,
        )

    # One auth object shared by the engine and the cursor, so you sign in once.
    # Galaxy is itself the authorization server: it federates the login to
    # Keycloak SSO and issues its own token. It does not accept a
    # Keycloak-issued JWT, so JWTAuthentication cannot be used here.
    _auth = trino.auth.OAuth2Authentication(redirect_auth_url_handler=_show_login_url)

    _host = os.environ["TRINO_HOST"]
    _port = int(os.environ.get("TRINO_PORT", "443"))
    catalog = os.environ.get("TRINO_CATALOG", "ol_data_lake_production")

    # http_scheme must be explicit: the SQLAlchemy dialect's
    # create_connect_args() never sets it and would default to plain http.
    # request_timeout is the HTTP read timeout for every request, including the
    # poll that waits for you to finish the Galaxy login. The library default is
    # 30 seconds, which is not enough time to open a browser, get through SSO
    # and come back — and each retry mints a NEW login URL, so the one already
    # printed goes dead. 600 seconds gives a realistic window, and also lets a
    # slow query keep polling instead of erroring.
    _connect_args = {
        "auth": _auth,
        "http_scheme": "https",
        "request_timeout": 600,
    }

    warehouse = sqlalchemy.create_engine(
        trino.sqlalchemy.URL(host=_host, port=_port, catalog=catalog),
        connect_args=_connect_args,
    )

    # Connection stays cell-local so the data sources panel does not show a
    # second, permanently empty entry; the cursor keeps it alive.
    _conn = trino.dbapi.connect(
        host=_host, port=_port, catalog=catalog, **_connect_args
    )
    cur = _conn.cursor()
    return catalog, cur, warehouse


@app.cell
def _(cur, mo):
    cur.execute("SHOW CATALOGS")
    mo.md(
        "**Catalogs available to you:** "
        + ", ".join(f"`{row[0]}`" for row in cur.fetchall())
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Finding your way around

    Open **Explore variables and data sources** in the left sidebar and expand
    `warehouse`. Schemas load when you expand them rather than up front —
    marimo will not eagerly scan a remote warehouse, so an unexpanded tree is
    not an empty one.

    The warehouse is built by dbt in layers, and the schema name tells you
    which layer you are in. dbt appends the model's folder to the target
    schema, so `ol_warehouse_production` + `dimensional` becomes
    `ol_warehouse_production_dimensional`:

    | Schema | What lives there | Use it? |
    |---|---|---|
    | `_raw` | Untransformed source loads, ~1,400 tables | No — no cleaning, no keys |
    | `_staging` | One model per source table, lightly cleaned | Rarely — no conformed keys |
    | `_intermediate` | Reusable joins and reshapes | No — implementation detail |
    | `_dimensional` | The star schema: `dim_*`, `tfact_*`, `afact_*`, `bridge_*` | **Yes, start here** |
    | `_mart` | Purpose-built wide tables per business area | Yes, if one fits your question |
    | `_reporting` | Shaped for specific dashboards | Only to reproduce a dashboard number |
    | `_external` | Extracts shaped for outside consumers | Only if you are that consumer |
    | `_integrations` | Payloads for other MIT applications | No |

    **Most of what you see in the panel is not on that list.** Alongside these
    there are dozens of schemas named `ol_warehouse_production_<username>_*` —
    developer sandboxes from `ol-dbt local`, holding whatever someone was
    working on. They are not authoritative and not maintained; one is even
    called `ol_warehouse_production_<your name>_staging`. If a schema name has
    a person in it, it is not for you.

    Table name prefixes in the dimensional layer:

    - `dim_*` — a thing you group or filter by (course, platform, date)
    - `tfact_*` — a *transaction* fact, one row per event (an enrollment, a grade)
    - `afact_*` — an *aggregate* fact, pre-summarised
    - `bridge_*` — a many-to-many link (a course has many instructors)
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### What is actually in there

    The query below is the panel's information in table form, which is handy
    when you want to search or filter it. `information_schema` is a Trino
    built-in and exists in every catalog.
    """)
    return


@app.cell
def _(mo, warehouse):
    layers = mo.sql(
        """
        SELECT
            table_schema
            , count(*) AS tables
        FROM information_schema.tables
        -- Only the canonical layers. Dropping this filter returns ~80 schemas,
        -- most of them per-developer sandboxes, which buries the ones you want.
        WHERE table_schema IN (
            'ol_warehouse_production_raw'
            , 'ol_warehouse_production_staging'
            , 'ol_warehouse_production_intermediate'
            , 'ol_warehouse_production_dimensional'
            , 'ol_warehouse_production_mart'
            , 'ol_warehouse_production_reporting'
            , 'ol_warehouse_production_external'
            , 'ol_warehouse_production_integrations'
        )
        GROUP BY table_schema
        ORDER BY tables DESC
        """,
        engine=warehouse,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Two ways to run a query

    Both go to the same warehouse over the same authenticated session. They
    differ in what they hand back and how much of marimo you get.

    ### SQL cells — `mo.sql(..., engine=warehouse)`

    **Advantages**

    - The editor treats it as SQL: syntax highlighting, and column completion
      fed by the data sources panel.
    - The result is a dataframe (polars here, because polars is installed;
      pandas if it were not), so charting and `mo.ui.table` work directly on it.
    - The cell joins marimo's reactive graph. Name the result, use that name in
      another cell, and the second cell re-runs by itself when the query does.
    - An engine dropdown appears on the cell, so switching catalogs is a click
      once you have more than one engine.
    - It is the form a colleague can read without knowing Python.

    **Disadvantages**

    - One statement, one result. No `fetchmany` loop, no cursor state.
    - Building the query text conditionally means f-string interpolation, which
      gets awkward past a couple of variables and has no parameter binding — so
      never interpolate anything you did not construct yourself.
    - The whole result is materialised in memory. A missing `LIMIT` on a fact
      table will hurt.
    - Session-level statements (`SET SESSION`, `USE`) do not belong here; they
      apply to a connection, and the engine pools those.

    ### The DB-API cursor — `cur.execute(...)`

    **Advantages**

    - Ordinary Python. Loop it, build SQL from a list, wrap it in a function,
      call it from a helper module.
    - `fetchmany(n)` streams, so you can process more than fits in memory.
    - Carries connection-scoped state. `SET SESSION` and `USE` apply to a
      connection, and a SQL cell opens a fresh one per statement, so state set
      in one cell is gone by the next. On the cursor it persists.
    - `cur.stats` and `cur.query_id` expose Galaxy's own view of the running
      query, which is how you find out why something is slow.

    **Disadvantages**

    - You get tuples and a `description`; turning that into a dataframe is on
      you.
    - No SQL editing support — it is a Python string.
    - Invisible to the reactive graph. marimo cannot tell that a cell using
      `cur` depends on a query in another cell, so ordering is your problem.

    **Rule of thumb:** reach for a SQL cell. It is not limited to `SELECT` —
    `SHOW`, `EXPLAIN` and DDL all run there, and a statement that returns rows
    comes back as a dataframe. Drop to the cursor when you need a loop, a
    stream, session state, or `cur.stats`.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### The same question both ways

    Live courses per platform — first as a SQL cell.
    """)
    return


@app.cell
def _(mo, warehouse):
    courses_by_platform = mo.sql(
        """
        SELECT
            primary_platform
            , count(*) AS live_courses
        FROM ol_warehouse_production_dimensional.dim_course
        WHERE is_current AND course_is_live
        GROUP BY primary_platform
        ORDER BY live_courses DESC
        """,
        engine=warehouse,
    )
    return


@app.cell
def _(mo):
    mo.md("""
    Then through the cursor, ending up at the same dataframe.
    """)
    return


@app.cell
def _(cur, pl):
    cur.execute("""
        SELECT
            primary_platform
            , count(*) AS live_courses
        FROM ol_warehouse_production_dimensional.dim_course
        WHERE is_current AND course_is_live
        GROUP BY primary_platform
        ORDER BY live_courses DESC
    """)
    # description[0] is the column name; the rest of the tuple is type info.
    _columns = [d[0] for d in cur.description]
    courses_via_cursor = pl.DataFrame(cur.fetchall(), schema=_columns, orient="row")
    courses_via_cursor
    return (courses_via_cursor,)


@app.cell
def _(courses_via_cursor, cur, mo):
    # Galaxy's own accounting for the query that cursor just ran — the reason
    # to keep a cursor around even when SQL cells do the querying.
    #
    # courses_via_cursor is taken as an argument purely to order this cell
    # after the one that ran the query. marimo cannot see that `cur` carries
    # state, so without that edge it could run this cell first and report the
    # stats of SHOW CATALOGS instead. This is the DB-API cursor's "invisible to
    # the reactive graph" disadvantage, in the flesh.
    _ = courses_via_cursor
    mo.md(f"""
    Stats from the last cursor query (`{cur.query_id}`):

    - rows: `{cur.stats.get("processedRows")}`
    - bytes scanned: `{cur.stats.get("processedBytes")}`
    - wall time (ms): `{cur.stats.get("elapsedTimeMillis")}`
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. The star schema, and the trap in it

    A fact table holds keys and measures; the dimensions hold the labels you
    group by. Every fact here carries `courserun_fk` pointing at
    `dim_course_run.courserun_pk`, and `dim_course_run.course_fk` points on at
    `dim_course.course_pk` — so one join chain gets you from any of
    `tfact_certificate`, `tfact_enrollment` or `tfact_grade` to a course title.

    **The trap.** `dim_course_run` and `dim_course` are slowly-changing
    dimensions (type 2): when a course run's attributes change, a new row is
    written and the old one is kept, with `effective_date`, `end_date` and
    `is_current` marking the versions. `courserun_pk` is *deliberately stable
    across versions* — the same key appears on the current row and on every
    historical row.

    So a join on the key alone matches every version of the run, and your
    counts multiply by however many times that run has been edited. Every join
    to an SCD2 dimension needs `AND <dim>.is_current`, on both dimensions. The
    dbt schema for `courserun_pk` says so outright: *"Always filter with
    is_current=true when looking up by this key."*

    **Why this hides.** Only 144 of ~8,000 course runs have more than one
    version, so a query that forgets the filter looks right on almost
    everything. The damage is concentrated: two runs currently carry over a
    thousand versions each. Forget `is_current` and those two multiply by
    ~1,000 while the rest are untouched.

    **And the filter is not quite sufficient.** Those same two runs each have
    *twelve* rows flagged `is_current`, which breaks the one-current-row-per-key
    rule an SCD2 dimension is supposed to guarantee. Run the next cell to see
    it: `current_rows` should never exceed `distinct_course_runs`, and it does.

    Those two runs have issued no certificates yet, so nothing below is
    currently wrong — which is the least reassuring reason for a query to be
    right. That is why the aggregate counts `count(DISTINCT cert.user_fk)` and
    `count(DISTINCT cert.certificate_key)` rather than `count(*)`: distinct on
    the fact's own key cannot be multiplied by a duplicated dimension row, so
    the query stays correct on the day those runs start certifying people.
    """)
    return


@app.cell
def _(mo, warehouse):
    scd_fanout = mo.sql(
        """
        SELECT
            count(*) AS courserun_rows
            , count(DISTINCT courserun_pk) AS distinct_course_runs
            , count(*) FILTER (WHERE is_current) AS current_rows
        FROM ol_warehouse_production_dimensional.dim_course_run
        """,
        engine=warehouse,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Certificates per course

    The join the dimensional model exists to make easy: a fact rolled up
    through two dimensions to readable labels, `tfact_certificate` →
    `dim_course_run` → `dim_course`. Both dimensions are SCD2, so both need
    `is_current` — miss either one and the counts inflate.

    Aggregate-only by construction: `count(DISTINCT user_fk)` counts learners
    without returning any, and the `HAVING` floor keeps courses with a handful
    of identifiable people out of the result.

    `tfact_certificate` is deliberately the fact here. It holds ~870k rows
    against `tfact_enrollment`'s ~16M and `tfact_grade`'s ~43M, so this
    finishes quickly — worth knowing when you pick a fact to explore from.
    """)
    return


@app.cell
def _(mo, warehouse):
    course_certificates = mo.sql(
        """
        SELECT
            course.course_readable_id
            , course.course_title
            , run.platform
            , count(DISTINCT run.courserun_pk) AS runs
            , count(DISTINCT cert.user_fk) AS learners_certified
            -- DISTINCT on the fact's own key, not count(*): if a dimension row
            -- is duplicated (see the note above), count(*) multiplies with it
            , count(DISTINCT cert.certificate_key) AS certificates
        FROM ol_warehouse_production_dimensional.tfact_certificate AS cert
        -- is_current on BOTH dimensions, or the SCD2 versions fan the counts out
        INNER JOIN ol_warehouse_production_dimensional.dim_course_run AS run
            ON cert.courserun_fk = run.courserun_pk
            AND run.is_current
        INNER JOIN ol_warehouse_production_dimensional.dim_course AS course
            ON run.course_fk = course.course_pk
            AND course.is_current
        WHERE NOT cert.certificate_is_revoked
        GROUP BY
            course.course_readable_id
            , course.course_title
            , run.platform
        HAVING count(DISTINCT cert.user_fk) >= 100
        ORDER BY learners_certified DESC
        LIMIT 100
        """,
        engine=warehouse,
    )
    return (course_certificates,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Many-to-many, and a dimension that is not SCD2

    A course *can* belong to several departments, so the link lives in a
    `bridge_` table of nothing but two foreign keys. Joining through it fans
    each course out to one row per department — which is what you want when
    counting by department, and what you must undo before summing a per-course
    measure.

    Worth knowing how thin that ice is: of the 398 courses in this bridge,
    exactly **two** have more than one department. A fan-out bug here would
    double-count those two rows and look perfectly correct everywhere else,
    which is why `count(DISTINCT course_pk)` below is deliberate rather than
    stylistic.

    Note the asymmetry too: `dim_course` needs `AND is_current`;
    `dim_department` has no such column. Not every dimension is versioned, so
    check rather than copying the filter everywhere.

    Two things the real data will show you. `department_number` is absent from
    these rows, because this bridge covers only mitxonline courses while the
    numbers arrive with the OCW ones. And EECS appears twice, under two
    spellings the source systems disagree on. Warehouse data is like this.
    """)
    return


@app.cell
def _(mo, warehouse):
    courses_by_department = mo.sql(
        """
        SELECT
            dept.department_name
            , count(DISTINCT course.course_pk) AS courses
            , count(DISTINCT CASE
                WHEN course.course_is_live THEN course.course_pk
              END) AS live_courses
        FROM ol_warehouse_production_dimensional.bridge_course_department AS link
        INNER JOIN ol_warehouse_production_dimensional.dim_department AS dept
            ON link.department_fk = dept.department_pk
        INNER JOIN ol_warehouse_production_dimensional.dim_course AS course
            ON link.course_fk = course.course_pk
            AND course.is_current
        GROUP BY dept.department_name
        HAVING count(DISTINCT course.course_pk) > 1
        ORDER BY courses DESC
        LIMIT 25
        """,
        engine=warehouse,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Reactive inputs

    This is the part that has no Jupyter equivalent. A `mo.ui` element is a
    value other cells can read, and marimo re-runs exactly the cells that read
    it when it changes — no re-execute button, and no stale output, because
    there is no hidden execution order to get out of sync with.

    Pick a platform below and watch the query under it follow.

    One thing worth copying from the next cell: its options come from a query
    over the same tables the driven queries read, not from a convenient nearby
    column. `primary_platform` in `dim_course` would have been the obvious
    source and it is quietly wrong — the three tables disagree about which
    platforms exist. `ocw` has courses but no course runs and no certificates,
    `residential` has runs but no certificates, `micromasters` has certificates
    but no runs. Offer any of those and the cells below return nothing, which
    reads as a broken notebook rather than an honest empty set.
    """)
    return


@app.cell
def _(mo, warehouse):
    queryable_platforms = mo.sql(
        """
        SELECT DISTINCT run.platform
        FROM ol_warehouse_production_dimensional.dim_course_run AS run
        WHERE run.is_current
          AND run.courserun_start_on IS NOT NULL
          AND run.platform IN (
            SELECT DISTINCT cert.platform
            FROM ol_warehouse_production_dimensional.tfact_certificate AS cert
          )
        ORDER BY run.platform
        """,
        engine=warehouse,
    )
    return (queryable_platforms,)


@app.cell
def _(mo, queryable_platforms):
    _options = sorted(
        p for p in queryable_platforms["platform"].to_list() if p is not None
    )
    # A default value means the cells below are live on first load rather than
    # waiting on a click; the mo.stop guards further down still cover the
    # empty-options case.
    platform = mo.ui.dropdown(
        options=_options,
        value=_options[0] if _options else None,
        label="Platform",
        searchable=True,
    )
    platform
    return (platform,)


@app.cell
def _(mo, platform, warehouse):
    # Interpolating a value from a dropdown whose options came from the
    # warehouse. There is no parameter binding in a SQL cell, so only ever
    # interpolate values you produced yourself — never raw text a user typed.
    platform_runs = mo.sql(
        f"""
        SELECT
            run.courserun_readable_id
            , run.courserun_title
            , run.semester
            , run.courserun_start_on
        FROM ol_warehouse_production_dimensional.dim_course_run AS run
        WHERE run.is_current
          AND run.platform = '{platform.value}'
          AND run.courserun_start_on IS NOT NULL
        ORDER BY run.courserun_start_on DESC
        LIMIT 50
        """,  # noqa: S608 — interpolates a closed dropdown list, not typed text
        engine=warehouse,
    )
    return (platform_runs,)


@app.cell
def _(mo):
    mo.md(r"""
    `mo.stop` halts a cell early. Note where it can and cannot help: the SQL
    cell above is a single `mo.sql(...)` call, which is what makes marimo render
    it as SQL, so there is no room for a guard inside it. If the dropdown had no
    value it would interpolate `'None'` and quietly return zero rows — which is
    the real reason the dropdown above sets a default.

    So the guard goes downstream, on the cell that would otherwise format a
    `None` into text.
    """)
    return


@app.cell
def _(mo, platform, platform_runs):
    mo.stop(
        platform.value is None,
        mo.md("Pick a platform above to see its course runs."),
    )
    mo.md(f"**{len(platform_runs)}** current runs on `{platform.value}`.")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 6. Charts

    `dim_date` is a calendar table: one row per date, with the parts already
    split out (`year`, `quarter`, `month`, `academic_term`). Facts carry a
    `*_date_key` into it, which is how you get a monthly series without
    date-truncation gymnastics — here `certificate_issued_date_key`.
    """)
    return


@app.cell
def _(mo, warehouse):
    certificates_by_month = mo.sql(
        """
        SELECT
            calendar.year
            , calendar.month
            , cert.platform
            , count(*) AS certificates
        FROM ol_warehouse_production_dimensional.tfact_certificate AS cert
        INNER JOIN ol_warehouse_production_dimensional.dim_date AS calendar
            ON cert.certificate_issued_date_key = calendar.date_key
        WHERE calendar.year >= 2020
          AND NOT cert.certificate_is_revoked
        GROUP BY calendar.year, calendar.month, cert.platform
        ORDER BY calendar.year, calendar.month
        """,
        engine=warehouse,
    )
    return (certificates_by_month,)


@app.cell
def _(alt, certificates_by_month, mo, pl):
    _plot_data = certificates_by_month.with_columns(
        pl.date(pl.col("year"), pl.col("month"), 1).alias("month_start")
    )

    _chart = (
        alt.Chart(_plot_data)
        .mark_line(point=True)
        .encode(
            x=alt.X("month_start:T", title="Month"),
            y=alt.Y("certificates:Q", title="Certificates issued"),
            color=alt.Color("platform:N", title="Platform"),
            tooltip=["month_start:T", "platform:N", "certificates:Q"],
        )
        .properties(height=320)
    )

    # mo.ui.altair_chart makes the selection readable from Python: drag on the
    # chart and the next cell sees the selected rows.
    certificate_chart = mo.ui.altair_chart(_chart)
    certificate_chart
    return (certificate_chart,)


@app.cell
def _(certificate_chart, mo):
    mo.stop(
        certificate_chart.value is None or len(certificate_chart.value) == 0,
        mo.md("*Drag a selection on the chart to filter these rows.*"),
    )
    certificate_chart.value
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 7. polars, pandas, and which one you get

    SQL cells return **polars** here. marimo picks the dataframe library by
    what is installed, preferring polars, so removing polars from the
    `/// script` header would silently switch every result to pandas.

    polars is worth learning for warehouse work: expressions instead of
    indexing, and no index to reason about.
    """)
    return


@app.cell
def _(course_certificates, pl):
    # Everything below happens locally, on rows the warehouse already returned.
    top_by_platform = (
        course_certificates.filter(pl.col("learners_certified") > 0)
        .with_columns(
            # >1 means learners certified in more than one run of the course
            (pl.col("certificates") / pl.col("learners_certified")).alias(
                "certificates_per_learner"
            )
        )
        .sort("learners_certified", descending=True)
        .group_by("platform")
        .head(3)
        .select(
            "platform",
            "course_readable_id",
            "runs",
            "learners_certified",
            "certificates_per_learner",
        )
        .sort(["platform", "learners_certified"], descending=[False, True])
    )
    top_by_platform
    return


@app.cell
def _(course_certificates, mo):
    import textwrap

    # to_pandas() is the escape hatch for anything that wants a pandas object —
    # statsmodels, scikit-learn, an older internal helper.
    _as_pandas = course_certificates.to_pandas()

    # The indent matters. mo.md() runs inspect.cleandoc() on the string, which
    # strips the indent COMMON to every line — so interpolating unindented
    # multi-line text into an indented literal drops the common indent to zero,
    # nothing is stripped, and the whole cell renders as an indented code block
    # instead of prose. Match the literal's indent, then lstrip the first line
    # because the literal already supplies its leading spaces.
    _summary = textwrap.indent(
        _as_pandas["learners_certified"].describe().to_string(), "    "
    ).lstrip()

    mo.md(f"""
    Converted to pandas: `{type(_as_pandas).__name__}`,
    shape `{_as_pandas.shape}`.

    Describing `learners_certified` through pandas:

    ```
    {_summary}
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 8. Sortable, searchable tables

    `mo.ui.table` beats a bare dataframe for anything you want to hand to
    someone: it paginates, sorts, searches, and its selection is readable from
    Python.
    """)
    return


@app.cell
def _(course_certificates, mo):
    certificates_table = mo.ui.table(
        course_certificates,
        selection="multi",
        page_size=10,
        label="Courses by certificates — select rows to inspect",
    )
    certificates_table
    return (certificates_table,)


@app.cell
def _(certificates_table, mo):
    mo.stop(
        len(certificates_table.value) == 0,
        mo.md("*Select rows above to summarise them.*"),
    )
    mo.md(
        f"Selected **{len(certificates_table.value)}** courses, "
        f"**{certificates_table.value['learners_certified'].sum():,}** certified learners."
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 9. Caching expensive queries

    Re-running a cell re-runs its query. `mo.cache` memoises on the arguments,
    so a query you are iterating around gets paid for once per distinct
    argument set. The cache lives in the kernel, so a restart clears it.

    Use it on a *function*, not on a cell — the point is to key on inputs.
    """)
    return


@app.cell
def _(catalog, mo, pl, warehouse):
    @mo.cache
    def certificate_summary(platform_name: str):
        # No return annotation: `pl` is an untyped marimo cell parameter, so
        # `-> pl.DataFrame` is not a resolvable type to a static checker.
        """Certificates by year for one platform, as a polars DataFrame.

        Cached per platform, in the kernel, for the life of the session.
        """
        query = f"""
            SELECT
                calendar.year
                , count(*) AS certificates
                , count(DISTINCT cert.user_fk) AS learners
            FROM {catalog}.ol_warehouse_production_dimensional.tfact_certificate
                AS cert
            INNER JOIN {catalog}.ol_warehouse_production_dimensional.dim_date
                AS calendar
                ON cert.certificate_issued_date_key = calendar.date_key
            WHERE cert.platform = '{platform_name}'
              AND NOT cert.certificate_is_revoked
            GROUP BY calendar.year
            ORDER BY calendar.year DESC
        """  # noqa: S608 — platform_name comes from the same dropdown
        return pl.read_database(query, connection=warehouse)

    return (certificate_summary,)


@app.cell
def _(certificate_summary, mo, platform):
    mo.stop(platform.value is None, mo.md("Pick a platform above."))
    # Change the dropdown and come back: the second visit is served from cache.
    certificate_summary(platform.value)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 10. Working with personal data

    `dim_user` exists, and joining to it is sometimes the right thing. Nothing
    in this notebook does, on purpose — a getting-started file should not
    normalise `SELECT *` on a table full of names and email addresses.

    Before you write a query that touches it:

    - Aggregate if you can. `count(DISTINCT user_fk)` answers most questions
      about people without returning a person. The facts carry `user_fk`, so
      per-learner *counting* rarely needs the user dimension at all.
    - Select the columns you need, never `*`. Identifiers travel further than
      you expect once they are in a dataframe.
    - Set a floor on group sizes. A pass rate over four learners identifies
      them to anyone who knows the cohort.
    - Your home directory is a persistent volume. A CSV of learner rows written
      there is a copy that outlives the question you wrote it to answer.
    - Everything you run is attributed to you: Galaxy authorises the query
      against your own SSO identity, so your access is exactly your role's
      access, and it is logged.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 11. Sharing what you made

    - **`mo.ui` inputs plus `marimo run --sandbox`** turn this file into an app
      with the code hidden — the notebook *is* the app, no port or rewrite
      involved.
    - **Export** to HTML (static, self-contained), or to `.ipynb` if a
      collaborator needs Jupyter.
    - **The file is a Python script.** It diffs and reviews like code — notebook
      JSON is a merge conflict waiting to happen; this is not. To run it outside
      JupyterLab, use `marimo edit --sandbox demo.py`: sandbox mode builds the
      environment from the `/// script` header and adds marimo itself. Plain
      `python demo.py` ignores that header and fails on the connect cell's
      imports, because this image installs no notebook-level packages.
    - The kernel is culled after 4 hours idle. Your home directory persists;
      anything held only in memory does not.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 12. When something goes wrong

    | Symptom | Cause |
    |---|---|
    | `401 Authentication required` | Sign-in never completed, or the kernel restarted. Re-run the connect cell and open the link. |
    | Login link never appears | It renders as a callout in the cell's own output while the cell blocks. If that is empty, check the cell's *console* pane — the `print()` there is the fallback for when `mo.output.append()` cannot reach the frontend. |
    | The login link does not work | Each attempt mints a new link and retires the previous one. Re-run the connect cell to get a fresh one. |
    | `Catalog must be specified` | Unqualified table name with no session catalog. Qualify it, or use the `warehouse` engine. |
    | Counts look inflated | A join to an SCD2 dimension missing `AND is_current` — see section 4. |
    | Data sources panel is empty | Expand `warehouse`; schemas load lazily. A `conn`-style entry saying *no databases* is a raw DB-API connection and can never show a tree. |
    | Query never returns | No `LIMIT` on a fact table. `cur.stats` shows what Galaxy is chewing through. |
    | `ModuleNotFoundError` after `pip install` | Sandbox mode. Add the package to the `/// script` header and restart the kernel. |
    """)
    return


@app.cell
def _():
    import altair as alt
    import numpy as np
    import pandas as pd
    import polars as pl

    return alt, pl


if __name__ == "__main__":
    app.run()
