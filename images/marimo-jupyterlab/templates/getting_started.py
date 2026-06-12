# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas>=2.0",
#   "trino>=0.330",
# ]
# ///
# ruff: noqa: PLC0415, B018
# PLC0415/B018: marimo requires imports inside cell functions and bare
# expressions for cell output — these are intentional patterns, not errors.

import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # OL Data Platform

    This notebook runs in **sandbox mode**: each notebook gets its own isolated
    Python environment. Packages are declared in the `/// script` header at the
    top of the file. Add a package there and restart the kernel to use it.

    Connection credentials are injected as environment variables by JupyterHub:

    | Variable | Value |
    |---|---|
    | `TRINO_HOST` | Starburst Galaxy endpoint |
    | `TRINO_PORT` | 443 |
    | `TRINO_TOKEN` | Your Keycloak OIDC access token (rotated per session) |

    AWS credentials for S3 and Glue are provided automatically via IRSA
    (no `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` needed).
    """)


@app.cell
def _(mo):
    mo.md("## Connect to Starburst")


@app.cell
def _():
    import os

    import trino

    conn = trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ.get("TRINO_PORT", "443")),
        http_scheme="https",
        auth=trino.auth.JWTAuthentication(os.environ["TRINO_TOKEN"]),
    )
    cur = conn.cursor()
    return conn, cur, os, trino


@app.cell
def _(cur, mo):
    cur.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cur.fetchall()]
    mo.md(f"**Available catalogs:** {', '.join(catalogs)}")
    return (catalogs,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Query example

    Replace `<catalog>`, `<schema>`, and `<table>` with real names from the
    catalog list above, then run the cell.
    """)


@app.cell
def _(cur, pd):
    _query = """
    SELECT *
    FROM <catalog>.<schema>.<table>
    LIMIT 100
    """
    cur.execute(_query)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    df
    return cols, df, rows


@app.cell
def _():
    import pandas as pd

    return (pd,)


if __name__ == "__main__":
    app.run()
