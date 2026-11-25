"""HTTP API, served behind the hub at /services/<service name>/.

Run with ``uvicorn --factory notebook_publisher.main:create_app``.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Request, status
from fastapi.responses import JSONResponse
from jupyterhub.services.auth import HubAuth
from kubernetes import client, config

from notebook_publisher.auth import build_hub_auth, current_user
from notebook_publisher.cluster import Cluster
from notebook_publisher.service import (
    ConcurrentChangeError,
    NameTakenError,
    NotebookNotFoundError,
    NotebookStatus,
    NotebookSummary,
    NotOwnerError,
    PodMismatchError,
    Publisher,
    RolloutFailedError,
)
from notebook_publisher.settings import Settings
from notebook_publisher.validation import (
    NotebookName,
    NotebookPublish,
    NotebookRejectedError,
    NotebookUpdate,
)

User = Annotated[str, Depends(current_user)]


def _publisher(request: Request) -> Publisher:
    publisher: Publisher = request.app.state.publisher
    return publisher


PublisherDep = Annotated[Publisher, Depends(_publisher)]

router = APIRouter(prefix="/api/notebooks")


@router.post("", status_code=status.HTTP_201_CREATED)
def publish(
    body: NotebookPublish, user: User, publisher: PublisherDep
) -> NotebookStatus:
    """Publish a notebook under a new name; returns once it serves."""
    return publisher.publish(user, body)


@router.get("")
def list_notebooks(user: User, publisher: PublisherDep) -> list[NotebookSummary]:
    """List the caller's published notebooks."""
    return publisher.list_owned(user)


@router.get("/{name}")
def notebook_status(
    name: NotebookName, user: User, publisher: PublisherDep
) -> NotebookStatus:
    """Return one of the caller's notebooks with its pod state and log."""
    return publisher.status(user, name)


@router.put("/{name}")
def update(
    name: NotebookName, body: NotebookUpdate, user: User, publisher: PublisherDep
) -> NotebookStatus:
    """Publish a new version of one of the caller's notebooks."""
    return publisher.update(user, name, body)


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
def unpublish(name: NotebookName, user: User, publisher: PublisherDep) -> None:
    """Take one of the caller's notebooks down."""
    publisher.unpublish(user, name)


def _error(status_code: int, detail: str, **extra: str) -> JSONResponse:
    return JSONResponse({"detail": detail, **extra}, status_code=status_code)


def _add_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(NotebookRejectedError)
    def rejected(_: Request, error: NotebookRejectedError) -> JSONResponse:
        return _error(status.HTTP_422_UNPROCESSABLE_CONTENT, str(error))

    @app.exception_handler(NotebookNotFoundError)
    def not_found(_: Request, error: NotebookNotFoundError) -> JSONResponse:
        return _error(status.HTTP_404_NOT_FOUND, f"No notebook named {error}.")

    @app.exception_handler(NotOwnerError)
    def not_owner(_: Request, error: NotOwnerError) -> JSONResponse:
        return _error(
            status.HTTP_403_FORBIDDEN, f"{error} was published by someone else."
        )

    @app.exception_handler(NameTakenError)
    def name_taken(_: Request, error: NameTakenError) -> JSONResponse:
        return _error(
            status.HTTP_409_CONFLICT,
            f"{error} is taken; update it if it's yours, or pick another name.",
        )

    @app.exception_handler(ConcurrentChangeError)
    def concurrent(_: Request, error: ConcurrentChangeError) -> JSONResponse:
        return _error(
            status.HTTP_409_CONFLICT, f"{error} changed during the update; retry."
        )

    @app.exception_handler(RolloutFailedError)
    def rollout_failed(_: Request, error: RolloutFailedError) -> JSONResponse:
        return _error(
            status.HTTP_422_UNPROCESSABLE_CONTENT, str(error), logs=error.logs
        )

    @app.exception_handler(PodMismatchError)
    def pod_mismatch(_: Request, error: PodMismatchError) -> JSONResponse:
        return _error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"The notebook's pod isn't what the publisher rendered: {error}",
        )


def _kubernetes_client() -> client.ApiClient:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.ApiClient()


def create_app(
    settings: Settings | None = None,
    publisher: Publisher | None = None,
    hub_auth: HubAuth | None = None,
) -> FastAPI:
    """Build the app; arguments replace the deployed dependencies in tests.

    :returns: The ASGI app.
    :rtype: FastAPI
    """
    settings = settings or Settings()
    app = FastAPI(title="notebook-publisher")
    app.state.publisher = publisher or Publisher(
        Cluster(_kubernetes_client(), settings.namespace), settings
    )
    app.state.hub_auth = hub_auth or build_hub_auth()
    # Fail at startup, not on the first publish, if the OIDC config is missing.
    _ = settings.oidc_plugin

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(router, prefix=settings.service_prefix.rstrip("/"))
    _add_error_handlers(app)
    return app
