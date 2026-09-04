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
    # Warehouse notebook

    A minimal starting point: signs you in to the data warehouse and gives you
    a `warehouse` engine to query. Everything below the connection cell is
    yours to replace.

    See `demo.py` for a tour of what this environment can do, and `README.md`
    for the data model.

    ## Sandbox mode

    Each notebook gets its own isolated Python environment, built from the
    `/// script` header at the top of this file. To add a package, put it in
    that list and restart the kernel — do not `pip install`, it will not
    persist.

    The last cell imports polars as `pl`, pandas as `pd`, numpy as `np` and
    altair as `alt`, so they are ready to use in any cell you add. pyarrow is
    declared but not imported: polars needs it internally, and
    `.to_pandas()` raises `ModuleNotFoundError` without it. Leave it in the
    list.

    ## Credentials

    | Variable | Value |
    |---|---|
    | `TRINO_HOST` | Starburst Galaxy endpoint |
    | `TRINO_PORT` | 443 |
    | `TRINO_CATALOG` | Default warehouse catalog |

    There is no token to manage. Galaxy authenticates query clients itself and
    federates the login to MIT OL SSO, so the first query of a session prints a
    link for you to open.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Connect
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
    # The default redirect handler is replaced because it calls
    # webbrowser.open_new(), which is a no-op inside the notebook pod.
    _auth = trino.auth.OAuth2Authentication(redirect_auth_url_handler=_show_login_url)

    _host = os.environ["TRINO_HOST"]
    _port = int(os.environ.get("TRINO_PORT", "443"))
    _catalog = os.environ.get("TRINO_CATALOG", "ol_data_lake_production")

    # http_scheme must be explicit: the SQLAlchemy dialect's
    # create_connect_args() never sets it, so it would default to plain http
    # against Galaxy's 443.
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

    # The SQLAlchemy engine is what populates "Explore variables and data
    # sources" in the sidebar, because marimo only builds a catalog tree for
    # engines it recognises as catalogs. It reads exactly the catalog named in
    # the URL, so browsing a second catalog means a second engine — one
    # top-level variable each, or marimo will not find it.
    #
    # The auth object goes in connect_args rather than the dialect's
    # ?externalAuthentication=true URL flag: that flag makes the dialect build
    # a fresh OAuth2Authentication per physical connection, and so a fresh
    # login prompt each time.
    warehouse = sqlalchemy.create_engine(
        trino.sqlalchemy.URL(host=_host, port=_port, catalog=_catalog),
        connect_args=_connect_args,
    )

    # The cursor is for the cases SQL cells cannot cover (see demo.py). The
    # connection itself stays cell-local: marimo would otherwise list it in the
    # data sources panel as a second, permanently empty entry, since a raw
    # DB-API connection cannot carry a catalog tree. The cursor holds a
    # reference, so the connection stays alive.
    _conn = trino.dbapi.connect(
        host=_host, port=_port, catalog=_catalog, **_connect_args
    )
    cur = _conn.cursor()
    return cur, warehouse


@app.cell
def _(mo):
    mo.md(r"""
    ## Sign in

    Running the next cell triggers the login. It blocks until you have opened
    the printed link and signed in, then lists the catalogs you can reach.

    The token is cached in memory and reused for as long as Galaxy accepts it,
    so signing in is not per-query. It is not guaranteed to be once per
    session either: if the token expires or is revoked, Galaxy answers the next
    query with a 401 and the client starts the flow again, so a fresh login
    callout appears mid-session. Restarting the kernel, or re-running the
    connection cell above, also asks again.
    """)
    return


@app.cell
def _(cur, mo):
    cur.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cur.fetchall()]
    mo.md(f"**Catalogs available to you:** {', '.join(catalogs)}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Your work starts here

    The cell below is a SQL cell: it runs against the `warehouse` engine and
    returns a dataframe (polars, since polars is installed). Expand the
    `warehouse` entry in the data sources panel to find tables, or start from
    the layer guide in `README.md`.
    """)
    return


@app.cell
def _(mo, warehouse):
    courses = mo.sql(
        """
        SELECT
            course_readable_id
            , course_title
            , primary_platform
        FROM ol_warehouse_production_dimensional.dim_course
        WHERE is_current AND course_is_live
        ORDER BY course_readable_id
        LIMIT 20
        """,
        engine=warehouse,
    )
    return


@app.cell
def _():
    import altair as alt
    import numpy as np
    import pandas as pd
    import polars as pl

    return


if __name__ == "__main__":
    app.run()
