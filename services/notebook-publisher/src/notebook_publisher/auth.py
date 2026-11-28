"""Identify callers by their JupyterHub token.

The publisher is a hub service, so it trusts the hub's answer for a token and
never a gateway header such as X-Userinfo. Pass 1 lets any hub user publish:
the only check is that the token belongs to a user and carries this service's
access scope.
"""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from jupyterhub.services.auth import HubAuth
from tornado.web import HTTPError


def build_hub_auth() -> HubAuth:
    """Return a HubAuth for this service, failing if it would admit no one.

    HubAuth reads JUPYTERHUB_API_URL, and JUPYTERHUB_SERVICE_NAME for the
    ``access:services!service=<name>`` scope a caller's token must carry.
    """
    hub_auth = HubAuth()
    if not hub_auth.access_scopes:
        msg = "Set JUPYTERHUB_SERVICE_NAME so HubAuth knows which scope to require."
        raise RuntimeError(msg)
    return hub_auth


def _hub_auth(request: Request) -> HubAuth:
    hub_auth: HubAuth = request.app.state.hub_auth
    return hub_auth


async def current_user(
    hub_auth: Annotated[HubAuth, Depends(_hub_auth)],
    authorization: Annotated[str | None, Header()] = None,
) -> str:
    """Return the hub username behind the request's token.

    :raises HTTPException: 401 without a valid user token, 403 when the token
        lacks this service's access scope.
    """
    match = HubAuth.auth_header_pat.fullmatch(authorization or "")
    if match is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "Send a JupyterHub token: `Authorization: token <JUPYTERHUB_API_TOKEN>`.",
            headers={"WWW-Authenticate": "token"},
        )
    try:
        model = await hub_auth.user_for_token(match[1], sync=False)
    except HTTPError as error:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY, "Couldn't check the token with JupyterHub."
        ) from error
    # A service's token has no hub user to own a notebook.
    if model is None or model.get("kind") != "user":
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "The token doesn't belong to a JupyterHub user.",
            headers={"WWW-Authenticate": "token"},
        )
    if not hub_auth.check_scopes(hub_auth.access_scopes, model):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"The token lacks {sorted(hub_auth.access_scopes)}.",
        )
    return str(model["name"])
