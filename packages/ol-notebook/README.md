# ol-notebook

Helpers for marimo notebooks in the data platform JupyterHub
(images/marimo-jupyterlab) and for notebooks published with marimo-operator
(images/marimo-runtime). Both images carry this package's wheel and point
`UV_FIND_LINKS` at it, so a notebook gets it by naming it in its header:

```python
# /// script
# dependencies = ["marimo", "ol-notebook", "polars"]
# ///
```

## Querying StarRocks as the viewer

```python
@app.cell
def _():
    from ol_notebook import warehouse

    engine = warehouse.engine()
    return (engine,)


@app.cell
def _(engine, mo):
    mo.sql("SELECT current_user(), current_role()", engine=engine)
```

`engine()` logs in to StarRocks with the current user's Keycloak token on every
connection, so queries run under that person's StarRocks roles, and different
viewers of one published app can see different data. A top-level `engine` also
fills marimo's data sources panel with the warehouse catalogs.

Where the token comes from:

- Published app: the `X-Access-Token` header APISIX forwards after the viewer
  logs in, read from `mo.app_meta().request`. A cell that runs without a
  browser request (e.g. from an anywidget callback) raises
  `WarehouseAccessError`; interacting with the app or reloading fixes it.
- Anywhere else: `eval "$(starrocks-auth --env qa --port-forward --output env)"`
  sets `STARROCKS_USER`, `STARROCKS_HOST` and `STARROCKS_ID_TOKEN_FILE`.

Configuration, all from the environment:

| Variable | Meaning |
| --- | --- |
| `STARROCKS_HOST` | FE host. Required. TLS is checked against this name, so it must be one the FE certificate covers. |
| `STARROCKS_PORT` | FE MySQL port, default 9030. |
| `STARROCKS_TLS_VERIFY_IDENTITY` | `false` skips the host name check, for a port-forward to 127.0.0.1. The chain is still verified. |
| `STARROCKS_ID_TOKEN_FILE`, `STARROCKS_USER` | Log in with this token file instead of the request header. |

StarRocks checks the token only at login, and `engine()` opens a new connection
for each query, so keep queries inside the cell that runs them rather than
holding a connection open across cells.
