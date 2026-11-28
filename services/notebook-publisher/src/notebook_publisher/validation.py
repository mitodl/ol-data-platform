"""Request models and the checks a notebook passes before it is published."""

import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StringConstraints

from notebook_publisher.settings import Settings

# An RFC 1035 label: the operator names the app's Service after it, and Service
# names must start with a letter. Short enough that every derived object name
# (<name>-route-auth-<revision>, <name>-content) stays a valid label too.
NAME_PATTERN = r"^[a-z]([-a-z0-9]{0,38}[a-z0-9])?$"

# Paths on the apps host that an app named after them would capture (the shared
# openid-connect plugin config logs out at /logout/oidc), and the publisher's
# own Service in the namespace, which the operator would take over for an app
# with its name.
RESERVED_NAMES = frozenset(
    {"logout", "login", "auth", "api", "healthz", "notebook-publisher"}
)

# The reference regex from PEP 723.
PEP_723_BLOCK = re.compile(
    r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"
)


def _not_reserved(name: str) -> str:
    if name in RESERVED_NAMES:
        msg = f"{name!r} is reserved on the apps host"
        raise ValueError(msg)
    return name


def _no_url_credentials(remote: str) -> str:
    # Annotations are readable by anyone who can read the MarimoNotebook, so an
    # https remote carrying a token must not land there.
    parts = urlsplit(remote)
    if parts.scheme and (parts.username or parts.password):
        msg = "git_remote must not contain credentials"
        raise ValueError(msg)
    return remote


NotebookName = Annotated[
    str, StringConstraints(pattern=NAME_PATTERN), AfterValidator(_not_reserved)
]
GitRemote = Annotated[
    str, StringConstraints(max_length=512), AfterValidator(_no_url_credentials)
]
GitCommit = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{7,64}$")]


class NotebookUpdate(BaseModel):
    """A new version of a notebook the caller already published."""

    model_config = ConfigDict(extra="forbid")

    content: str
    # Pass 2 adds "public", which goes through review before it is served.
    access: Literal["keycloak"] = "keycloak"
    git_remote: GitRemote | None = None
    git_commit: GitCommit | None = None


class NotebookPublish(NotebookUpdate):
    """A notebook to publish under a name nobody holds yet."""

    name: NotebookName = Field(description="The app's path on the apps host.")


class NotebookRejectedError(Exception):
    """The notebook can't be published as it is; the message says why."""


def check_notebook(content: str, settings: Settings) -> None:
    """Reject content the operator or marimo would fail on.

    :param content: The notebook source.
    :param settings: Limits to apply.
    :raises NotebookRejectedError: With a message for the author.
    """
    size = len(content.encode())
    if size > settings.max_content_bytes:
        msg = f"Notebook is {size} bytes; the limit is {settings.max_content_bytes}."
        raise NotebookRejectedError(msg)

    scripts = [m for m in PEP_723_BLOCK.finditer(content) if m["type"] == "script"]
    if len(scripts) != 1:
        msg = (
            "Notebook needs exactly one `# /// script` dependency header (PEP 723); "
            "published apps build their environment from it."
        )
        raise NotebookRejectedError(msg)
    header = "".join(
        line[2:] if line.startswith("# ") else line[1:]
        for line in scripts[0]["content"].splitlines(keepends=True)
    )
    try:
        tomllib.loads(header)
    except tomllib.TOMLDecodeError as error:
        msg = f"The `# /// script` header is not valid TOML: {error}"
        raise NotebookRejectedError(msg) from error

    # marimo check passes a file it can't parse at all.
    try:
        compile(content, "notebook.py", "exec")
    except SyntaxError as error:
        msg = f"Notebook has a syntax error: {error}"
        raise NotebookRejectedError(msg) from error

    run_marimo_check(content, settings)


def run_marimo_check(content: str, settings: Settings) -> None:
    """Run `marimo check` on the notebook, as `marimo run` would read it.

    :raises NotebookRejectedError: With marimo's diagnostics.
    """
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "notebook.py"
        path.write_text(content)
        try:
            result = subprocess.run(  # noqa: S603
                [sys.executable, "-m", "marimo", "check", str(path)],
                capture_output=True,
                text=True,
                timeout=settings.marimo_check_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            msg = f"`marimo check` took over {settings.marimo_check_timeout_seconds}s."
            raise NotebookRejectedError(msg) from error
    if result.returncode != 0:
        output = (result.stdout + result.stderr).replace(str(path), "notebook.py")
        msg = f"`marimo check` failed:\n{output}"
        raise NotebookRejectedError(msg)
