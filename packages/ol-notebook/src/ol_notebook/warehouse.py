"""Query the StarRocks warehouse as the person running the notebook.

StarRocks' ``keycloak_jwt`` security integration logs a user in with their own
Keycloak token over ``authentication_openid_connect_client``, so queries run
under that person's roles. The token comes from one of two places:

- In JupyterHub or a terminal, ``bin/starrocks-auth --output env`` sets
  ``STARROCKS_USER`` and ``STARROCKS_ID_TOKEN_FILE``, and that file is used.
- In a published app, APISIX logs the viewer in and forwards their access token
  in ``X-Access-Token``. marimo hands notebook code the request behind each
  kernel command through ``mo.app_meta().request`` (the WebSocket upgrade at
  page load, then the POST behind each UI interaction), and APISIX refreshes
  the token before it forwards an expired one.

``engine()`` reads the token again for every connection and pools nothing, so
each query logs in with the token of the request that triggered it. Nothing is
cached at module level, because marimo runs every viewer's session as a thread
in one process and a module-level connection would be shared between them.

The client plugin refuses to send a token without TLS, and the certificate is
checked against ``STARROCKS_HOST``. In a published app that host is the FE
certificate's name, which the publisher resolves to the in-cluster FE Service.
Set ``STARROCKS_TLS_VERIFY_IDENTITY=false`` for a ``kubectl port-forward`` to
127.0.0.1, where the name can't match.
"""

import base64
import binascii
import json
import os
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import mysql.connector
import sqlalchemy
from mysql.connector.abstracts import MySQLConnectionAbstract
from sqlalchemy.pool import NullPool

ACCESS_TOKEN_HEADER = "x-access-token"  # noqa: S105  # a header name
# Set by the user's Keycloak client mapper from the saml_uid attribute. StarRocks'
# keycloak_jwt integration requires it to equal the login name.
PRINCIPAL_CLAIM = "starrocks_username"
DEFAULT_MYSQL_PORT = 9030


class WarehouseAccessError(RuntimeError):
    """The notebook has no usable warehouse identity for this request."""


def engine() -> sqlalchemy.Engine:
    """Return a SQLAlchemy engine that logs in as the current user on each query.

    Assign it to a top-level variable so marimo's data sources panel lists the
    warehouse catalogs. Each connection reads the token afresh, so the engine
    can live for the whole session.

    :returns: An engine on ``ol_notebook.dialect`` with no connection pool.
    :rtype: sqlalchemy.Engine
    """
    return sqlalchemy.create_engine(
        "starrocks+mysqlconnector://", creator=connect, poolclass=NullPool
    )


def connect() -> MySQLConnectionAbstract:
    """Open a DB-API connection to StarRocks as the current user.

    :returns: An open mysql-connector-python connection. The caller closes it.
    :rtype: MySQLConnectionAbstract
    :raises WarehouseAccessError: If there is no token to log in with.
    """
    token_file = os.environ.get("STARROCKS_ID_TOKEN_FILE")
    if token_file:
        return _open(os.environ["STARROCKS_USER"], Path(token_file))
    token = viewer_access_token()
    with _token_file(token) as path:
        return _open(login_name(token), path)


def viewer_access_token() -> str:
    """Return the access token APISIX forwarded with the current request.

    :returns: The viewer's Keycloak access token.
    :rtype: str
    :raises WarehouseAccessError: If the cell runs outside a request, or the
        request carries no token.
    """
    import marimo as mo  # noqa: PLC0415  # only present inside a marimo kernel

    request = mo.app_meta().request
    if request is None:
        msg = (
            "This cell ran without a browser request, so there is no viewer to"
            " query as. Interact with the app or reload the page."
        )
        raise WarehouseAccessError(msg)
    token = request.headers.get(ACCESS_TOKEN_HEADER)
    if not token:
        msg = (
            f"The request has no {ACCESS_TOKEN_HEADER} header. Published apps get"
            " it from the gateway's login; elsewhere, run"
            " `starrocks-auth --output env` and set STARROCKS_ID_TOKEN_FILE."
        )
        raise WarehouseAccessError(msg)
    return token


def login_name(token: str) -> str:
    """Return the StarRocks login name a Keycloak token was issued to.

    StarRocks verifies the token's signature at login, so the payload is only
    decoded here to name the user.

    :param token: A Keycloak JWT.
    :returns: The token's ``starrocks_username`` claim.
    :rtype: str
    :raises WarehouseAccessError: If the token isn't a JWT or lacks the claim.
    """
    try:
        payload = token.split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
    except (IndexError, ValueError, binascii.Error) as error:
        msg = "The access token is not a JWT."
        raise WarehouseAccessError(msg) from error
    name = claims.get(PRINCIPAL_CLAIM)
    if not name:
        msg = (
            f"The access token has no {PRINCIPAL_CLAIM} claim, so StarRocks can't"
            " log this user in."
        )
        raise WarehouseAccessError(msg)
    return name


@contextmanager
def _token_file(token: str) -> Generator[Path]:
    # mysql-connector-python only takes the token as a file path. It reads the
    # file during the handshake, so the file can go as soon as connect returns.
    descriptor, name = tempfile.mkstemp(prefix="ol-notebook-", suffix=".jwt")
    path = Path(name)
    try:
        with os.fdopen(descriptor, "w") as token_file:
            token_file.write(token)
        yield path
    finally:
        path.unlink(missing_ok=True)


def _open(user: str, token_file: Path) -> MySQLConnectionAbstract:
    verify_identity = os.environ.get("STARROCKS_TLS_VERIFY_IDENTITY", "true") != "false"
    return mysql.connector.connect(
        host=os.environ["STARROCKS_HOST"],
        port=int(os.environ.get("STARROCKS_PORT", DEFAULT_MYSQL_PORT)),
        user=user,
        auth_plugin="authentication_openid_connect_client",
        openid_token_file=str(token_file),
        # The FE certificate is from a public CA, so the system trust store,
        # which mysql-connector-python always loads, verifies it.
        ssl_verify_cert=True,
        ssl_verify_identity=verify_identity,
        # The C extension crashed under concurrent connections in the dbt trial
        # (tk-provide-a-working-oidc-client-path-for-starrocks), and every viewer
        # of a published app is a thread in one process.
        use_pure=True,
    )
