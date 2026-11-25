"""Scaffolding shared by the stand-ins for OpenMetadata in the `ol-dbt ... sync` command tests."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from ol_dbt_cli.lib.openmetadata import OpenMetadataClient, OpenMetadataError

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
    from types import ModuleType

    import pytest

    from ol_dbt_cli.lib.manifest import ManifestRegistry

TEAM_ID = "team-1"
# What ``route`` returns for a request it does not answer.
NOT_FOUND = object()


class RecordingOpenMetadata(OpenMetadataClient):
    """Replaces only ``request``, recording every call.

    Answers the data-platform team's lookup itself, and 404s whatever
    :meth:`route` does not answer.
    """

    def __init__(self) -> None:
        super().__init__("https://om.example/api", "token")
        self.teams = {"data-platform": TEAM_ID}
        self.calls: list[tuple[str, str, Any]] = []

    def writes(self) -> list[tuple[str, str, Any]]:
        return [c for c in self.calls if c[0] != "GET"]

    def route(
        self, method: str, parts: tuple[str, ...], body: Any, query: dict[str, str] | None, content_type: str
    ) -> Any:
        """Answer one request, given the path after ``/v1`` split on ``/``, or return :data:`NOT_FOUND`."""
        return NOT_FOUND

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> Any:
        self.calls.append((method, path, copy.deepcopy(body)))
        parts = tuple(path.strip("/").split("/")[1:])
        match (method, *parts):
            case ("GET", "teams", "name", team) if team in self.teams:
                return {"id": self.teams[team]}
        found = self.route(method, parts, body, query, content_type)
        if found is NOT_FOUND:
            raise OpenMetadataError(f"{method} {path} -> 404: not found", 404)
        return found


def install(
    monkeypatch: pytest.MonkeyPatch,
    command: ModuleType,
    fake: OpenMetadataClient,
    repo_root: Path,
    manifest: Callable[[], ManifestRegistry] | None = None,
) -> None:
    """Point *command* at *fake* and at *repo_root* as its checkout.

    :param manifest: builds the production manifest the command loads, when it loads one.
    """
    monkeypatch.setattr(OpenMetadataClient, "from_env", classmethod(lambda cls: fake))
    monkeypatch.setattr(command, "get_repo_root", lambda: repo_root)
    if manifest is not None:
        monkeypatch.setattr(command, "load_catalog_manifest", lambda dbt_dir, path: manifest())
