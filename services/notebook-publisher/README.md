# notebook-publisher

Publishes marimo notebooks from the data platform JupyterHub as apps at
`https://<apps host>/<name>/`, run by marimo-operator in the `marimo` namespace
and gated by Keycloak through APISIX. The design is in ol-infrastructure's
[marimo-first data notebooks spec](https://github.com/mitodl/ol-infrastructure/blob/main/docs/plans/marimo-first-data-notebooks-spec.md#part-b-publishing-marimo_data-marimo-operator-publisher-service).

## What a publish does

1. Checks the notebook: a single PEP 723 `# /// script` header, no syntax
   errors, `marimo check` passes, and under 900 KiB.
2. Creates a `MarimoNotebook` that runs the notebook with `marimo run --sandbox`
   in the runtime image, as the `marimo-published` ServiceAccount, under the
   restricted Pod Security Standard.
3. Creates two Secrets for this revision, both owned by the `MarimoNotebook`: the
   marimo token, and the `Authorization` header APISIX injects with it.
4. Waits for the operator's pod, checks it is what was rendered (args,
   volumes, ServiceAccount, security contexts), and waits until marimo answers
   its health check.
5. Only then creates or updates the `ApisixRoute`, which logs the viewer in
   through Keycloak and forwards their access token and nothing else of theirs.

A failed publish deletes what it created. A failed update puts the previous
`MarimoNotebook` back; the route still points at the previous token, which is
kept until an update succeeds. Every revision gets a new token, because marimo
prints the token to its log.

The `MarimoNotebook`, its Secrets and its route are the only state. Pulumi
doesn't manage them: they are user content, unlike the platform configuration
Pulumi owns. Unpublishing deletes the `MarimoNotebook`, and Kubernetes garbage
collection removes the rest.

## API

Served behind the hub at `/services/notebook-publisher/api/notebooks`. Every
call needs `Authorization: token <JupyterHub token>`, e.g. the
`JUPYTERHUB_API_TOKEN` of a single-user server.

| Method and path | Does |
|---|---|
| `POST /api/notebooks` | Publish `{"name", "content", "git_remote"?, "git_commit"?}` under a new name |
| `GET /api/notebooks` | List the caller's notebooks |
| `GET /api/notebooks/<name>` | Status, pod events and recent log (token redacted) |
| `PUT /api/notebooks/<name>` | Publish a new version: `{"content", "git_remote"?, "git_commit"?}` |
| `DELETE /api/notebooks/<name>` | Unpublish |

Any hub user may publish. Only the user who published a notebook can update,
inspect or unpublish it.

## Deployment

The `marimo_data` stack in ol-infrastructure deploys it. It needs:

- `NOTEBOOK_PUBLISHER_APPS_HOST`, `NOTEBOOK_PUBLISHER_RUNTIME_IMAGE`,
  `NOTEBOOK_PUBLISHER_SHARED_PLUGIN_CONFIG_NAME`, and
  `NOTEBOOK_PUBLISHER_OIDC_PLUGIN_FILE` (a JSON file holding
  `OLApisixOIDCResources.get_full_oidc_plugin_config(unauth_action="auth")`).
  Optional settings are in `src/notebook_publisher/settings.py`.
- `JUPYTERHUB_API_URL` and `JUPYTERHUB_SERVICE_NAME=notebook-publisher`. The
  hub registers the service and proxies `/services/notebook-publisher/` to it.
- Callers' tokens to carry `access:services!service=notebook-publisher`. The hub
  grants that through `load_roles`, on both the `user` role and the `server`
  role (the token in a single-user server).
- A Role in `marimo` with `get`, `list`, `create`, `update` and `delete` on
  `marimos` (the MarimoNotebook plural); `get`, `create` and `update` on
  `apisixroutes`; `create` and `delete` on `secrets` (never `get` or `list`);
  `get` on `pods` and `pods/log`; and `list` on `events`.
- A ValidatingAdmissionPolicy on `marimos` that rejects `sidecars`, `mounts`,
  extra volumes, any ServiceAccount but `marimo-published`, and any
  `auth.password` Secret not named `<name>-token-*`. Without the last rule a
  compromised publisher could point an app at another Secret in `marimo`, and
  marimo would print its value in the log the publisher can read.

Requests are synchronous: a publish or update returns once the app serves, up
to `NOTEBOOK_PUBLISHER_ROLLOUT_TIMEOUT_SECONDS` (300s) later, so the hub proxy
in front of it needs a longer timeout. Nothing reconciles a request the
publisher was restarted in the middle of. A publish cut off that way leaves an
app with no route, and an update leaves the route on the previous token. The
owner fixes either by publishing again (`PUT`) or unpublishing.

## Development

```bash
cd services/notebook-publisher
uv sync
uv run pytest notebook_publisher_tests
```
