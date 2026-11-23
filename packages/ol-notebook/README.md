# ol-notebook

Helpers for marimo notebooks in the data platform JupyterHub
(images/marimo-jupyterlab) and for notebooks published with marimo-operator
(images/marimo-runtime). Both images carry this package's wheel and serve it
to uv as an index (`image-uv.toml`), so a notebook gets it by naming it in its
header:

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
  logs in, read from `mo.app_meta().request`. Code that marimo runs outside a
  page load or UI interaction (anywidget, `mo.ui.chat` and table callbacks)
  has no request, so it raises `WarehouseAccessError`; query from a cell
  instead.
- Anywhere else: `bin/starrocks-auth` sets `STARROCKS_USER`, `STARROCKS_HOST`
  and `STARROCKS_ID_TOKEN_FILE`. `--output exec` keeps the port-forward open
  and the token file fresh while the command runs, and the forward ends at
  127.0.0.1, which the certificate doesn't name. From the repository root:

  ```sh
  STARROCKS_TLS_VERIFY_IDENTITY=false \
      bin/starrocks-auth --env qa --port-forward --output exec -- \
      uv run --no-project --with ./packages/ol-notebook python my_script.py
  ```

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
